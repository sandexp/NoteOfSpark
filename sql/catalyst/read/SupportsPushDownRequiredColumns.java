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
import org.apache.spark.sql.types.StructType;

/**
 * 与@ScanBuilder 的混合式接口,数据源可以实现这个接口,用于对需要的列进行栈操作,将其送入到数据源中.且仅仅到扫描的时候才读取行内容,
 * 用于减少需要读取的数据量.
 */
@Evolving
public interface SupportsPushDownRequiredColumns extends ScanBuilder {

  /**
   * 对给定的schema进行列删减
   * 实现方法中需要尽可能的删减不必要的列或者是嵌入式的属性.
   * 注意,扫描器中的@readSchema 实现需要注意这里的列删减的问题
   */
  void pruneColumns(StructType requiredSchema);
}
