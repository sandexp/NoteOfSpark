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
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

/**
 * 数据源扫描的逻辑表示.这个接口用于提供逻辑信息,比如说读取什么格式的schema.
 * 这个逻辑表示在批次扫描,微批次扫描,和流式扫描之间进行共享,数据源必须实现相应的方法.用于匹配表支持的功能.
 * 例如,如果表创建的扫描器存在有@BATCH_READ的功能,则必须要提供批次处理@toBatch()
 */
@Evolving
public interface Scan {

  /**
   * 获取数据源扫描的实际schema，在底层存储的物理schema中是不同的。因为会发生列的剪枝和其他优化情况。
   */
  StructType readSchema();

  /**
   * 扫描器的描述，包括下述信息：
   * 1. 扫描器的过滤器
   * 2. 重要参数(例如,路径信息)
   * 描述中不需要包括@readSchema信息,因为spark知道这些信息
   * 默认情况下,会返回实现的类名称.对这个方法进行重新,提供有意义的描述
   */
  default String description() {
    return this.getClass().toString();
  }

  /**
   * 返回批次查询的物理扫描表述.默认情况下,这个方法会抛出异常,数据源必须要对这个方法进行重写,并提供实现.如果表创建了一个扫描器(批次读取的).
   * 则必须要实现这个方法.
   * @throws UnsupportedOperationException
   */
  default Batch toBatch() {
    throw new UnsupportedOperationException(description() + ": Batch scan are not supported");
  }

  /**
   * 返回微批次模式下的扫描器的物理表述.默认情况下,方法会抛出异常,数据源必须重写这个方法,用于提供实现.如果表提供了微批次读取的功能,
   * 则必须要实现这个方法.
   *
   * @param checkpointLocation  HDFS的检查点位置,用于失败恢复
   *
   * @throws UnsupportedOperationException
   */
  default MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    throw new UnsupportedOperationException(description() + ": Micro-batch scan are not supported");
  }

  /**
   * 流式模式的扫描器物理表述,默认情况下不支持,但是当表提供了流式读取的时候,必须对其进行实现
   * @param checkpointLocation 检查点位置,用于失败恢复
   *
   * @throws UnsupportedOperationException
   */
  default ContinuousStream toContinuousStream(String checkpointLocation) {
    throw new UnsupportedOperationException(description() + ": Continuous scan are not supported");
  }
}
