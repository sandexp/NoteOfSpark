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
import org.apache.spark.sql.types.DataType;

/**
 * 代表了公用表达式API中的常量字面量
 *
 * 字面量是JVM的类型,必须被spark 内部行式API用于字面量的数据类型
 *
 * @param <T> the JVM type of a value held by the literal
 */
@Experimental
public interface Literal<T> extends Expression {
  /**
   * 获取字面量值
   */
  T value();

  /**
   * 获取字母量的sql数据类型
   */
  DataType dataType();
}
