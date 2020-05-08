/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.orc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcInputFormat;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.orc.OrcShimUtils.VectorizedRowBatchWrap;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;


/**
 * 为了支持@WholeStageCodeGen 的向量化,这个读取器会返回一个列批次@ColumnarBatch.创建之后,@initialize和@initBatch可以调用
 */
public class OrcColumnarBatchReader extends RecordReader<Void, ColumnarBatch> {

  // 向量化批次的容量
  private int capacity;

  // 向量化ORC行批次的保证
  private VectorizedRowBatchWrap wrap;

  /**
   * 请求数据列编号列表
   * 物理orc文件的列id列表,这个读取器需要使用.-1意味着需要的列是分区列,或在orc文件中不存在.
   * 理想的情况下,分区列不会出现在orc文件中,且只能出现在目录名称中.但是spark允许分区列在物理文件中,但是spark会抛弃这个文件的这个值.
   * 且使用从目录名称中获取的分区值.
   */
  @VisibleForTesting
  public int[] requestedDataColIds;

  // ORC 行批次的记录读取器
  private org.apache.orc.RecordReader recordReader;

  // 需要的属性列表
  private StructField[] requiredFields;

  // 通过@WholeStageCodeGen 向量化得到的列批次
  @VisibleForTesting
  public ColumnarBatch columnarBatch;

  // 包装的ORC列向量表
  private org.apache.spark.sql.vectorized.ColumnVector[] orcVectorWrappers;

  public OrcColumnarBatchReader(int capacity) {
    this.capacity = capacity;
  }


  @Override
  public Void getCurrentKey() {
    return null;
  }

  // 获取当前列批次值
  @Override
  public ColumnarBatch getCurrentValue() {
    return columnarBatch;
  }

  @Override
  public float getProgress() throws IOException {
    return recordReader.getProgress();
  }

  // 确定是否有下一个批次的数据
  @Override
  public boolean nextKeyValue() throws IOException {
    return nextBatch();
  }

  // 关闭批次读取器
  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
    }
  }

  /**
   * 初始化ORC文件读取器和批次记录读取器
   * 注意到@InitBatch需要在之后调用
   */
  @Override
  public void initialize(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    FileSplit fileSplit = (FileSplit)inputSplit;
    Configuration conf = taskAttemptContext.getConfiguration();
    Reader reader = OrcFile.createReader(
      fileSplit.getPath(),
      OrcFile.readerOptions(conf)
        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
        .filesystem(fileSplit.getPath().getFileSystem(conf)));
    Reader.Options options =
      OrcInputFormat.buildOptions(conf, reader, fileSplit.getStart(), fileSplit.getLength());
    recordReader = reader.rows(options);
  }

  /**
   * 通过设置指定的schema和分区信息对列批次进行初始化.使用这些信息,这些可以使用完整的schema创建
   * @param orcSchema ORC文件读取器的schema
   * @param requiredFields 需要返回的属性,包括分区属性
   * @param requestedDataColIds 区域的数据列信息,-1表示不存在
   * @param requestedPartitionColIds 需要的分区schema中的列id列表,-1表示不存在
   * @param partitionValues 分区列的值
   */
  public void initBatch(
      TypeDescription orcSchema,
      StructField[] requiredFields,
      int[] requestedDataColIds,
      int[] requestedPartitionColIds,
      InternalRow partitionValues) {
    wrap = new VectorizedRowBatchWrap(orcSchema.createRowBatch(capacity));
    assert(!wrap.batch().selectedInUse); // `selectedInUse` should be initialized with `false`.
    assert(requiredFields.length == requestedDataColIds.length);
    assert(requiredFields.length == requestedPartitionColIds.length);
    // If a required column is also partition column, use partition value and don't read from file.
    for (int i = 0; i < requiredFields.length; i++) {
      if (requestedPartitionColIds[i] != -1) {
        requestedDataColIds[i] = -1;
      }
    }
    this.requiredFields = requiredFields;
    this.requestedDataColIds = requestedDataColIds;

    StructType resultSchema = new StructType(requiredFields);

    // Just wrap the ORC column vector instead of copying it to Spark column vector.
    orcVectorWrappers = new org.apache.spark.sql.vectorized.ColumnVector[resultSchema.length()];

    for (int i = 0; i < requiredFields.length; i++) {
      DataType dt = requiredFields[i].dataType();
      if (requestedPartitionColIds[i] != -1) {
        OnHeapColumnVector partitionCol = new OnHeapColumnVector(capacity, dt);
        ColumnVectorUtils.populate(partitionCol, partitionValues, requestedPartitionColIds[i]);
        partitionCol.setIsConstant();
        orcVectorWrappers[i] = partitionCol;
      } else {
        int colId = requestedDataColIds[i];
        // Initialize the missing columns once.
        if (colId == -1) {
          OnHeapColumnVector missingCol = new OnHeapColumnVector(capacity, dt);
          missingCol.putNulls(0, capacity);
          missingCol.setIsConstant();
          orcVectorWrappers[i] = missingCol;
        } else {
          orcVectorWrappers[i] = new OrcColumnVector(dt, wrap.batch().cols[colId]);
        }
      }
    }

    columnarBatch = new ColumnarBatch(orcVectorWrappers);
  }

  /**
   * 如果在下一个批次中还有数据,则返回true.且如果存在,准备从ORC的向量化行批次@VectorizedRowBatch 中拷贝下一个批次的数据到
   * spark的列批次@ColumnarBatch中
   */
  private boolean nextBatch() throws IOException {
    recordReader.nextBatch(wrap.batch());
    int batchSize = wrap.batch().size;
    if (batchSize == 0) {
      return false;
    }
    columnarBatch.setNumRows(batchSize);

    for (int i = 0; i < requiredFields.length; i++) {
      if (requestedDataColIds[i] != -1) {
        ((OrcColumnVector) orcVectorWrappers[i]).setBatchSize(batchSize);
      }
    }
    return true;
  }
}
