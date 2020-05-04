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

package org.apache.spark.sql.connector.expressions;

import org.apache.spark.annotation.Experimental;

/**
 * 代表公共逻辑表达式API中的转换函数
 * 例如,日期转换函数,用于从时间戳映射日期值.转换名称就叫做日期,且参数就是ts列的引用
 */
@Experimental
public interface Transform extends Expression {
  /**
   * 返回转换函数的名称
   */
  String name();

  /**
   * 获取转换参数表中所有属性的引用
   */
  NamedReference[] references();

  /**
   * 获取传递到转换函数中的参数
   */
  Expression[] arguments();
}
