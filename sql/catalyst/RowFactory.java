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

package org.apache.spark.sql;

import org.apache.spark.annotation.Stable;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

/**
 * 工厂类,用于构建@Row对象
 */
@Stable
public class RowFactory {

  /**
   * 由给定的参数创建@Row,参数列表的第i个位置并从创建的@Row对象的第i个位置
   */
  public static Row create(Object ... values) {
    return new GenericRow(values);
  }
}
