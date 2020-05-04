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
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * 表:
 * 这个接口代表了数据源的一种逻辑结构化数据.例如,这个实现可以是文件系统的目录,kafka的topic,或者树数据目录的一个表等等.
 * 这个接口可以是可读,或者是可写的.分别提供数据的读写功能.
 *
 * 其中@partitioning 默认实现返回一个空分区数组,且@properties 默认实现返回一个空的map.需要在自己的实现中定义分区和表参数属性.
 */
@Evolving
public interface Table {

  /**
   * 表标识符,实现需要提供有意义的名称,可以是数据库也可以是数据目录的表名,或者是表的文件位置名称
   */
  String name();

  /**
   * 返回这个表的schema.如果表不可读且没有schema,就返回空schema
   */
  StructType schema();

  /**
   * 返回表的物理分区
   */
  default Transform[] partitioning() {
    return new Transform[0];
  }

  /**
   * 返回表参数的信息
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   *返回表的功能集合
   */
  Set<TableCapability> capabilities();
}
