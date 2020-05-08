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

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.Type;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

/**
 * 一种记录读取器,向量化Parquet记录读取器,读取内部的行式数据或者列批次,使用parquet 列API.从某种意义上是基于parquet-mr的列式读取器.
 * 这些可以使用代码生成器进行简单高效的处理
 * 这个类既可以返回行式数据或者列批次.使用全程代码生成器,可以返回列批次,这个可以提供重要的性能提升.
 */
public class VectorizedParquetRecordReader extends SpecificParquetRecordReaderBase<Object> {
  // 向量化批次的容量
  private int capacity;

  /**
   * 行的批次编号,每次当这个批次执行完成的时候(batchIdx == numBatched),就会排出这个批次,表示这个批次处理完成
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * 对于每个需要的列来说,读取器会读取这个列.如果列丢失就会返回null.
   */
  private VectorizedColumnReader[] columnReaders;

  /**
   * 返回行的数量
   */
  private long rowsReturned;

  /**
   * 已经读取的行的数量,包括当前的正在使用的行组的数量
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * 对于每个列来说,如果文件丢失就会返回true,否则返回null
   */
  private boolean[] missingColumns;

  /**
   * 时区信息,使用时间戳INT96值.如果没有转换就是null
   */
  private TimeZone convertTz = null;

  /**
   * columnBatch object that is used for batch decoding. This is created on first use and triggers
   * batched decoding. It is not valid to interleave calls to the batched interface with the row
   * by row RecordReader APIs.
   * This is only enabled with additional flags for development. This is still a work in progress
   * and currently unsupported cases will fail with potentially difficult to diagnose errors.
   * This should be only turned on for development to work on this feature.
   *
   * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
   * code between the path that uses the MR decoders and the vectorized ones.
   *
   * TODOs:
   *  - Implement v2 page formats (just make sure we create the correct decoders).
   */
  // 列批次
  private ColumnarBatch columnarBatch;

  // 列向量表
  private WritableColumnVector[] columnVectors;

  /**
   * 如果返回true,这个类会返回批次而不是行数据
   */
  private boolean returnColumnarBatch;

  /**
   * 列批次的内存模式
   */
  private final MemoryMode MEMORY_MODE;

  public VectorizedParquetRecordReader(TimeZone convertTz, boolean useOffHeap, int capacity) {
    this.convertTz = convertTz;
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.capacity = capacity;
  }

  /**
   * 记录读取器的初始化
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  /**
   * 读取路径中的所有数据,会使用这个类创建hadoop对象.列中包含列的列表
   */
  @Override
  public void initialize(String path, List<String> columns) throws IOException,
      UnsupportedOperationException {
    super.initialize(path, columns);
    initializeInternal();
  }

  // 关闭列批次
  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    super.close();
  }

  // 移动到下一个记录值,如果批次中没有数据则跳转到下一个批次,如果没有下一个批次,返回false
  @Override
  public boolean nextKeyValue() throws IOException {
    // 1. 获取当前列批次数据
    resultBatch();
    // 2. 如果需要在返回批次的情况下,返回下一个批次的情况
    if (returnColumnarBatch) return nextBatch();
    // 3. 超出批次,确认下一个批次的情况
    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    //4. 在没有超出批的情况下,自会读取行数据,并移动行指针,返回true
    ++batchIdx;
    return true;
  }

  // 获取当前value值,如果是批次读取则返回批次,否则返回行数据,注意行数据的指针指向
  @Override
  public Object getCurrentValue() {
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]

  /**
   初始化批次
   @memMode 内存模式
   @partitionColumns 分区列
   @partitionValues 行数据
    */
  private void initBatch(
      MemoryMode memMode,
      StructType partitionColumns,
      InternalRow partitionValues) {
    // 1. 获取批次数据的schema信息
    StructType batchSchema = new StructType();
    for (StructField f: sparkSchema.fields()) {
      batchSchema = batchSchema.add(f);
    }
    if (partitionColumns != null) {
      for (StructField f : partitionColumns.fields()) {
        batchSchema = batchSchema.add(f);
      }
    }

    // 2. 为列分配内存,注意内存模式分配不同,分配位置也不同
    if (memMode == MemoryMode.OFF_HEAP) {
      columnVectors = OffHeapColumnVector.allocateColumns(capacity, batchSchema);
    } else {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, batchSchema);
    }
    columnarBatch = new ColumnarBatch(columnVectors);
    if (partitionColumns != null) {
      int partitionIdx = sparkSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        ColumnVectorUtils.populate(columnVectors[i + partitionIdx], partitionValues, i);
        columnVectors[i + partitionIdx].setIsConstant();
      }
    }

    // 使用null初始化丢失的列
    for (int i = 0; i < missingColumns.length; i++) {
      if (missingColumns[i]) {
        columnVectors[i].putNulls(0, capacity);
        columnVectors[i].setIsConstant();
      }
    }
  }

  private void initBatch() {
    initBatch(MEMORY_MODE, null, null);
  }

  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    initBatch(MEMORY_MODE, partitionColumns, partitionValues);
  }

  /**
   * 返回列批次对象,这个对读取器返回的所有行可用.这个对象可以从业.调用可以开启向量化读取器.这个需要在调用nextKeyValue/nextBatch
   * 之前调用
   */
  public ColumnarBatch resultBatch() {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
  }

  /**
   * 在行数据返回前开启,用于开启返回列批次
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * 移动到下一个批次,如果没有下一个批次则返回false
   */
  public boolean nextBatch() throws IOException {
    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }
    columnarBatch.setNumRows(0);
    if (rowsReturned >= totalRowCount) return false;
    checkEndOfRowGroup();

    int num = (int) Math.min((long) capacity, totalCountLoadedSoFar - rowsReturned);
    for (int i = 0; i < columnReaders.length; ++i) {
      if (columnReaders[i] == null) continue;
      columnReaders[i].readBatch(num, columnVectors[i]);
    }
    rowsReturned += num;
    columnarBatch.setNumRows(num);
    numBatched = num;
    batchIdx = 0;
    return true;
  }

  // 初始化内部设置
  private void initializeInternal() throws IOException, UnsupportedOperationException {
    // 1. 检查是否缺少对于的列
    missingColumns = new boolean[requestedSchema.getFieldCount()];
    // 2. 获取列的描述和路径
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<String[]> paths = requestedSchema.getPaths();
    // 3. 处理schema信息中的每个列
    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
      Type t = requestedSchema.getFields().get(i);
      // 如果是复杂类型(属性嵌入类型),则不支持这种操作
      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new UnsupportedOperationException("Complex types not supported.");
      }
      // 确定类的缺失情况
      String[] colPath = paths.get(i);
      if (fileSchema.containsPath(colPath)) {
        ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
        if (!fd.equals(columns.get(i))) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
        missingColumns[i] = false;
      } else {
        if (columns.get(i).getMaxDefinitionLevel() == 0) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException("Required column is missing in data file. Col: " +
            Arrays.toString(colPath));
        }
        missingColumns[i] = true;
      }
    }
  }

  // 检查row 组的结束
  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
          + rowsReturned + " out of " + totalRowCount);
    }
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<Type> types = requestedSchema.asGroupType().getFields();
    columnReaders = new VectorizedColumnReader[columns.size()];
    for (int i = 0; i < columns.size(); ++i) {
      if (missingColumns[i]) continue;
      columnReaders[i] = new VectorizedColumnReader(columns.get(i), types.get(i).getOriginalType(),
        pages.getPageReader(columns.get(i)), convertTz);
    }
    totalCountLoadedSoFar += pages.getRowCount();
  }
}
