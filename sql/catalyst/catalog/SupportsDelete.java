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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.sources.Filter;

/**
 * 删除操作,表@Table的混合式接口,数据源可以实现这个接口,用于提供从表中删除数据(匹配与过滤表达式的)的功能.
 */
@Experimental
public interface SupportsDelete {
  /**
   * 从数据源表中删除满足过滤表达式的数据
   * 如果满足所有的过滤表达式才会删除行.也就是说,表达式必须被翻译成过滤器集合,且使用and连接逻辑
   * 如果删除操作是1不可能达到的条件的会,实现中会拒绝删除.例如,分区数据源可以拒绝删除没有通过分区列过滤的数据.因为过滤器需要在不删除记录的
   * 情况下,重新写入文件.
   * 为了拒绝删除的实现,需要抛出异常@IllegalArgumentException,并使用合适的信息,表示表达式被拒绝
   *
   * @param filters filter expressions, used to select rows to delete when all expressions match
   * @throws IllegalArgumentException If the delete is rejected due to required effort
   */
  void deleteWhere(Filter[] filters);
}
