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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;

/**
 * 用于创建分区读取器的工厂类
 *
 * 如果spark在这个接口的实现中执行某个方法,或者在返回分区读取器@PartitionReader过程中抛出了异常,相应的spark 任务会失败,在达到最大
 * 失败次数之前都会进行尝试
 */
@Evolving
public interface PartitionReaderFactory extends Serializable {

  /**
   * 返回基于行的分区读取器,用于指定的输入分区@InputPartition 中读取数据
   * 这个方法的实现需要将输入分区转换为连续的输入分区类
   */
  PartitionReader<InternalRow> createReader(InputPartition partition);

  /**
   * 返回列式分区读取器,用于从指定的输入分区中读取数据
   *
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   */
  default PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    throw new UnsupportedOperationException("Cannot create columnar reader.");
  }

  /**
   * 如果给定的输入分区按照列的形式被spark读取,实现就必须要实现@createColumnarReader 方法
   * 对于spark 2.4版本,spakr可以按照列的方式读取所有的输入分区.数据源不能混合使用行数分区和列式分区.这个在未来的版本可能做出改变.
   */
  default boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
