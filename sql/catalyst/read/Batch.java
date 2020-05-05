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

/**
 * 代表数据源扫描的物理实现,用于批次查询.这个接口用于提供物理信息,例如扫描的数据有多少分区,且怎样从分区中读取记录
 */
@Evolving
public interface Batch {

  /**
   * 返回输入分区@InputPartition列表,每个输入分区代表数据的分片,这个数据分片可以由spark任务进行处理.返回的输入分区与扫描到的RDD
   * 分区数量一致.
   * 如果扫描@Scan 支持过滤器操作,这个批次配置过滤器的情况下,就会对过滤器创建分片,这样就不会全扫描了.
   * 这个方法只有当数据源扫描的时候才会调用,用于运行spark job.
   */
  InputPartition[] planInputPartitions();

  /**
   * 返回一个工厂类,用于对每个输入分区@InputPartition 创建分区阅读器@PartitionReader
   */
  PartitionReaderFactory createReaderFactory();
}
