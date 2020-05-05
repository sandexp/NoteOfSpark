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
import org.apache.spark.sql.connector.read.partitioning.Partitioning;

/**
 * 与扫描器@Scan的混合,数据源可以实现这个接口,从而去汇报数据分区信息,并尝试避免spark的shuffle.
 *
 * 注意到,当扫描器@Scan 实现创建了一个输入分区@InputPartition 的时候,spark会避免添加shuffle
 */
@Evolving
public interface SupportsReportPartitioning extends Scan {

  /**
   * 返回输出数据的分区情况
   */
  Partitioning outputPartitioning();
}
