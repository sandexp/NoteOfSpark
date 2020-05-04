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
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * 阶段式表数据目录,动态混合了表目录的实现.支持阶段性的表创建(在提交表元数据前,具体可以使用`CREATE TABLE AS SELECT`或者
 * `REPLACE TABLE AS SELECT`进行提交).
 * 强烈建议实现这个特征,这样`CREATE TABLE AS SELECT`和`REPLACE TABLE AS SELECT`操作就是原子性的了.如果数据目录没有实现这个
 * 特征.
 * 计划处理器会首先通过TableCatalog中的@dropTable 抛弃掉指定的表,然后通过@createTable 的方法创建表.之后写出器SupportsWrite通过
 * 方法@newWriteBuilder 创建构建器.但是如果写出操作失败,数据目录以及删除表之后,计划处理器不能够回滚到删除表之前的状态.
 * 如果数据目录实现了这个插件,数据目录可以实现这些方法,从而去阶段性的创建和代替表.在调用表的批量写出器@BatchWrite 的提交功能@WriterCommitMessage
 * 之后,阶段性表StagedTable 就会阶段性的提交表变化@commitStagedChanges.调用时间为阶段表可以原子性的进行数据写出和元数据交换操作的情况下.
 */
@Experimental
public interface StagingTableCatalog extends TableCatalog {

  /**
   * 阶段性的创建表,并准备提交到元数据存储中
   * 当表提交的时候,任何spark计划处理器的写出都会表元数据进行提交.如果当方法调用的时候表存在,这个方法就会抛出异常.
   * 如果其他进程在阶段性变化提交之前,并发的创建这个表,就会抛出异常
   *
   * @param ident 创建表的表标识符
   * @param schema 新表的schema,结构体类型
   * @param partitions 表分区数据使用的转换
   * @param properties 表参数配置
   * @return 新表的元数据
   * @throws TableAlreadyExistsException If a table or view already exists for the identifier
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  StagedTable stageCreate(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException;

  /**
   * 阶段性的表替换，当调用@commitStagedChanges 的时候，会将其提交到元数据存储中
   * 当表提交的时候，任何spark计划处理器的写出内容都会使用表元数据提交.如果表存在的话,元数据和表的内容就会替换表中的元数据和内容.
   * 如果在写出提交之前,多个并发进程改变了表的数据或者是元数据,数据目录可以选择是否继续替换或者是放弃提交操作.
   * 如果表不存在，提交阶段性变化会失败。与@stageCreateOrReplace 的语义不同，后者在不存在的时候会创建表，并提交
   * @param ident 需要替换表的标识符
   * @param schema 新表的schema,结构体类型
   * @param partitions 表分区数据的转换
   * @param properties 参数映射表
   * @return 新表的元数据
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   * @throws NoSuchTableException If the table does not exist
   */
  StagedTable stageReplace(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws NoSuchNamespaceException, NoSuchTableException;

  /**
   * 阶段式创建或者替换表,当调用@commitStagedChanges 的时候会提交到元数据存储中.
   * 当表提交的时候,spark计划处理器写出的内容会使用表元数据提交.元数据和表的内容会替换已存在表的内容.如果在提交之前,
   * 多个进程并发提交,数据目录可以选择是继续执行还是放弃.
   * 如果当提交变化表不存在,这个边就需要在后备的数据源中创建.与替换@stageReplace 的语义不同,后者在没有表的时候会抛出异常.
   *
   * @param ident 表标识符
   * @param schema 表的schema,结构体类型
   * @param partitions 表分区数据的转换
   * @param properties 表属性映射表
   * @return 新表的元数据
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  StagedTable stageCreateOrReplace(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws NoSuchNamespaceException;
}
