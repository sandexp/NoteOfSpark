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
 * 与扫描器接口@Scan的混合,数据源可以实现这个接口,可以汇报统计信息给spark
 *
 *  spark 2.4中,统计值会在数据送给数据源之前汇报给优化器.实现方法会返回更加精确的统计值.
 */
@Evolving
public interface SupportsReportStatistics extends Scan {

  /**
   * 返回数据源扫描的统计值
   */
  Statistics estimateStatistics();
}
