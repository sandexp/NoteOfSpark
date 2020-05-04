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

/**
 * 这个API基础于spark的会话目录。实现可以获取构建的会话目录，通过方法@setDelegateCatalog。实现的目录韩式可使用自定义的逻辑，
 * 且在结束调用会话目录。例如，可以实现一个建表的语义@createTable
 */
@Experimental
public interface CatalogExtension extends TableCatalog, SupportsNamespaces {

  /**
   * 这个只会调用一次，用于传递spark的会话目录，在@initialize 之后进行
   */
  void setDelegateCatalog(CatalogPlugin delegate);
}
