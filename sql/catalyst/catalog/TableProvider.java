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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * v2版本数据源的基础接口,没有实际的数据目录.实例必须是共有的无参构造器
 * 注意,这个类仅仅可以对现存的表进行数据操作.例如,读取,添加,删除,重写等.不支持元数据改变的操作,例如创建/删除表
 * </p>
 */
@Evolving
public interface TableProvider {

  /**
   * 获取一个表实例,可以使用指定的参数@options 进行读写
   */
  Table getTable(CaseInsensitiveStringMap options);

  /**
   * 获取一个表实例,可以使用指定的配置@options和@schema进行读写操作
   * 默认情况下不支持这类操作,实现可以对其进行重写
   * @param options the user-specified options that can identify a table, e.g. file path, Kafka
   *                topic name, etc. It's an immutable case-insensitive string-to-string map.
   * @param schema the user-specified schema.
   * @throws UnsupportedOperationException
   */
  default Table getTable(CaseInsensitiveStringMap options, StructType schema) {
    throw new UnsupportedOperationException(
      this.getClass().getSimpleName() + " source does not support user-specified schema");
  }
}
