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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * 分区读取器工厂@PartitionReaderFactory 的变形，返回一个连续分区读取器@ContinuousPartitionReader,而不是
 * 普通的分区读取器@PartitionReader.用于连续的流式处理.
 */
@Evolving
public interface ContinuousPartitionReaderFactory extends PartitionReaderFactory {
  @Override
  ContinuousPartitionReader<InternalRow> createReader(InputPartition partition);

  @Override
  default ContinuousPartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    throw new UnsupportedOperationException("Cannot create columnar reader.");
  }
}
