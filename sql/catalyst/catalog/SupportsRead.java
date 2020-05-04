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
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * 表@Table的混合式接口,表示表可读.这个添加了@newScanBuilder,可以对批处理,微批次,连续处理创建一个扫描器
 */
@Experimental
public interface SupportsRead extends Table {

  /**
   * 返回一个扫描构建器@ScanBuilder,这个可以构建一个扫描器@Scan.spark会调用这个方法配置每个数据源的扫描.
   * @param options The options for reading, which is an immutable case-insensitive
   *                string-to-string map.
   */
  ScanBuilder newScanBuilder(CaseInsensitiveStringMap options);
}
