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
import scala.collection.JavaConverters;

import java.util.Arrays;

/**
 * 辅助方法,用于创建spark的逻辑转换
 */
@Experimental
public class Expressions {
  private Expressions() {
  }

  /**
   * 创建逻辑转换,用于使用给定名称的转换
   * 这个转换可以代表任意的命名的转换
   *
   * @param name 转换名称
   * @param args 转换的表达式
   * @return 逻辑转换
   */
  public static Transform apply(String name, Expression... args) {
    return LogicalExpressions.apply(name,
        JavaConverters.asScalaBuffer(Arrays.asList(args)).toSeq());
  }

  /**
   * Create a named reference expression for a (nested) column.
   *
   * @param name The column name. It refers to nested column if name contains dot.
   * @return a named reference for the column
   */
  public static NamedReference column(String name) {
    return LogicalExpressions.parseReference(name);
  }

  /**
   * 由给定值创建一个字面量
   *  JVM类型的字面量必须被spark的内部行 API使用,作为字面量的数据类型
   *
   * @param value value 值
   * @param <T> JVM类型的value
   * @return 当前value的字面量表达式
   */
  public static <T> Literal<T> literal(T value) {
    return LogicalExpressions.literal(value);
  }

  /**
   * 创建列与列的桶转换
   * 这个转换代表value --> (0-numBuckets)逻辑映射关系,这个机遇的是value的hash值
   * 使用这个方法创建的转换称作桶.
   *
   * @param numBuckets 输出桶的数量
   * @param columns 桶转换的列名称列表
   * @return 逻辑转换(叫做桶)
   */
  public static Transform bucket(int numBuckets, String... columns) {
    NamedReference[] references = Arrays.stream(columns)
      .map(Expressions::column)
      .toArray(NamedReference[]::new);
    return LogicalExpressions.bucket(numBuckets, references);
  }

  /**
   * 创建指定类的身份转换
   * 这个转换代表了value与其自己的逻辑映射
   * 这个转换的名称又叫做身份
   * @param column 输入列
   * @return 名为身份的逻辑标识符
   */
  public static Transform identity(String column) {
    return LogicalExpressions.identity(Expressions.column(column));
  }

  /**
   * 日志列的年转换,用于时间戳
   * 这个代表了,时间戳与年的映射,例如2018
   */
  public static Transform years(String column) {
    return LogicalExpressions.years(Expressions.column(column));
  }

  /**
   * 日志列的月转换,用于时间戳
   * 这个代表了,时间戳与月的映射,例如2018-05
   */
  public static Transform months(String column) {
    return LogicalExpressions.months(Expressions.column(column));
  }

  /**
   * 日志列的日转换,用于时间戳
   * 这个代表了,时间戳与日的映射,例如2018-05-13
   */
  public static Transform days(String column) {
    return LogicalExpressions.days(Expressions.column(column));
  }

  /**
   * 日志列的小时转换,用于时间戳
   * 这个代表了,时间戳与时的映射,例如2018-05-13 19
   */
  public static Transform hours(String column) {
    return LogicalExpressions.hours(Expressions.column(column));
  }

}
