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
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * 微批次流
 * 用于进行微批次处理的spark数据流@SparkDataStream
 */
@Evolving
public interface MicroBatchStream extends SparkDataStream {

  /**
   * 返回最近可获取的偏移量
   */
  Offset latestOffset();

  /**
   * 返回给定起始/终止偏移量的输入分区列表,每个输入分区代表一个数据分片,这个数据分片可以由spark任务处理.输入分区的数量与扫描的
   * RDD分区数量一致.
   * 如果扫描@Scan 操作支持过滤,流处理可以配置一个过滤器.且需要对于这个分区器创建一个分片,这样就不是全扫描了.
   * 这个方法会被调用多次,主要是为了在每个微批次中运行spark job.
   */
  InputPartition[] planInputPartitions(Offset start, Offset end);

  /**
   * 返回一个工厂类,用于创建每个输入分区的分区读取器@PartitionReader
   */
  PartitionReaderFactory createReaderFactory();
}
