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

package org.apache.spark.sql.connector.read.partitioning;

import org.apache.spark.annotation.Evolving;

/**
 * 分区
 * 代表输出数据对于数据源的数据分区接口。由SupportsReportPartitioning中的@outputPartitioning()返回,注意到这个作为快照运行.一旦
 * 创建,就是确定的.且总是汇报相同的分区数量,且对于同一个分布情况,其执行具有可预见性.
 */
@Evolving
public interface Partitioning {

  /**
   * 获取输入分区@InputPartition 的分区数量
   */
  int numPartitions();

  /**
   * 如果分区满足给定的分布,则返回true.意味着spark在这种情况下,不需要对数据源输出数据进行shuffle操作.
   * 注意,spark可能在新版本中添加对@Distribution的实现.这个方法需要能够意识到这个变化,且需要不能识别的分布情况时.返回false.
   * 建议检查每个spark的版本是否支持新的分布情况.避免在spark侧的shuffle.
   */
  boolean satisfy(Distribution distribution);
}
