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

/**
 * spark数据流@SparkDataStream的一种,用于连续模式的流式查询
 */
@Evolving
public interface ContinuousStream extends SparkDataStream {

  /**
   * 返回给定偏移量的输入分区@InputPartition列表.每个输入分区代表一个数据分片,这个数据分片可以被一个spark任务处理.
   * 输入分区的数量与RDD分区的数量是一致的.
   * 如果扫描器@Scan 支持过滤操作,这个流会使用过滤器配置,且需要对过滤器创建分片,这个就不是全扫描了.
   * 这个方法会运行spark job,用于读取数据流.会被调用超过一次.如果@needsReconfiguration返回true,则spark需要运行一个新的job.
   */
  InputPartition[] planInputPartitions(Offset start);

  /**
   * 获取一个工厂,用于创建连续分区读取器@ContinuousPartitionReader,用于对每个输入分区进行读取
   */
  ContinuousPartitionReaderFactory createContinuousReaderFactory();

  /**
   * 合并由连续分区读取器@ContinuousPartitionReader来的分区偏移量.将分区偏移量合并到全局偏移量上.
   */
  Offset mergeOffsets(PartitionOffset[] offsets);

  /**
   * 执行引擎会调用这个方法去决定是否新的输入分区需要被产生.如果底层系统分区添加或者删除的时候需要进行这样的操作.
   * 如果返回true,spark job会扫描连续的数据流,这个操作会中断,且spakr会根据新的分区列表@InputPartition 重新运行
   */
  default boolean needsReconfiguration() {
    return false;
  }
}
