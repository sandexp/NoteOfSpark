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
import org.apache.spark.sql.sources.Filter;

/**
 * 这个是与@ScanBuilder 的混合接口,数据源可以继承这个接口,用于对数据源过滤器进行栈操作,且降低需要读取数据的大小.
 */
@Evolving
public interface SupportsPushDownFilters extends ScanBuilder {

  /**
   * 对过滤器进行栈操作(后进先出),返回在扫描之后需要进行估测的过滤器
   * 当且仅当所有过滤器匹配的时候,行会从数据源中返回.这些过滤器以逻辑AND的模式连接.
   */
  Filter[] pushFilters(Filter[] filters);

  /**
   * 通过方法@pushFilters,返回添加到数据源的过滤器
   * 有3类过滤器
   * 1. 可以置入的过滤器,在扫描之后不需要重新评估
   * 2. 可置入过滤器,扫描之后仍然需要进行评估的过滤器.例如,parquet行式组过滤器
   * 3. 非置入式过滤器
   * 条件1和条件2都是可置入式过滤器,需要被这个方法返回
   */
  Filter[] pushedFilters();
}
