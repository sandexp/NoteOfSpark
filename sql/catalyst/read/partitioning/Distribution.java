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
 * 代表了数据分布的需求,指定了记录需要在数据分区之间如何分配
 * 注意到这个接口不处理一个分区内部的数据排序问题.这个接口的实例由spark创建和提高.由分区@Partitioning 的@satisfy(Distribution)
 * 方法进行调用消费。这就意味着数据源开发者不需要实现这个接口，但是需要获取更多的这个接口的实现。
 *
 * Concrete implementations until now:
 * <ul>
 *   <li>{@link ClusteredDistribution}</li>
 * </ul>
 */
@Evolving
public interface Distribution {}
