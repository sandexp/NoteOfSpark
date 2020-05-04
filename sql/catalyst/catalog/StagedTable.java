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
 * 阶段表
 * 代表一个表,这个表被阶段性地提交到元数据存储中
 * 这个用于实现原子性的`CREATE TABLE AS SELECT`和`REPLACE TABLE AS SELECT`查询.这个机会可以通过StagingTableCatalog中的
 * 方法@stageCreate 或者@stageReplace 进行创建.
 * 这个表经常需要实现@SupportsWrite,一个新的写出器需要通过@newWriteBuilder 方法进行构建.且写出操作会被提供.
 * 任务会使用方法@commitStagedChanges推断是否需要提交表元数据到元数据存储中.
 */
@Experimental
public interface StagedTable extends Table {

  /**
   * 表创建或者替换的最终动作 --> 提交阶段性的表变化
   */
  void commitStagedChanges();

  /**
   * 放弃阶段提交的变化,包括元数据和临时输出
   * Abort the changes that were staged, both in metadata and from temporary outputs of this
   * table's writers.
   */
  void abortStagedChanges();
}
