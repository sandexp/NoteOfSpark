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
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * 表@Table 的混合式接口,表示表可写.其中@newWriteBuilder 用于创建一个批处理,流式处理的写出器.
 */
@Experimental
public interface SupportsWrite extends Table {

  /**
   * 返回一个写出构建器,可以创建一个批次写出器@BatchWrite.spark调用这个方法区配置数据源写出功能.
   */
  WriteBuilder newWriteBuilder(CaseInsensitiveStringMap options);
}
