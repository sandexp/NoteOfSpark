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

/**
 * 在spark 流式查询中代表可读数据流的基本接口.需要在流式查询的过程中管理流式数据源的偏移量.
 * 数据源需要实现连续的流式接口,例如@MicroBatchStream 和@ContinuousStream
 */
@Evolving
public interface SparkDataStream {

  /**
   * 返回流式查询的初始偏移量,用于启动读取数据.注意到流式数据源不应当在初始偏移量出开始读取:
   * 原因是如果spark在存在的查询中进行了重启,就会从设置了检查点的偏移量处进行重启,就不是初始的那个偏移量了.
   */
  Offset initialOffset();

  /**
   * 将json字符串反序列化为偏移量,
   *
   * @throws IllegalArgumentException if the JSON does not encode a valid offset for this reader
   */
  Offset deserializeOffset(String json);

  /**
   * 提示数据源spark已经完成所有数据的处理,偏移量小于等于结束偏移量.且仅仅在偏移量大于end位置的时候才会发送请求.
   *
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  void commit(Offset end);

  /**
   * 停止数据源,且释放分配的资源
   */
  void stop();
}
