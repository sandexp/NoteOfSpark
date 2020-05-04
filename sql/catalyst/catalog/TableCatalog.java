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
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 表数据目录
 * 表数据目录的实现可以是大小写敏感的或者是不明感的.spark会传递一个标识符@identifiers.
 *
 */
@Experimental
public interface TableCatalog extends CatalogPlugin {

  /**
   * 指定表位置的属性,表文件需要位于这个位置下
   */
  String PROP_LOCATION = "location";

  /**
   * 指定表描述的属性
   */
  String PROP_COMMENT = "comment";

  /**
   * 指定表提供器的属性
   */
  String PROP_PROVIDER = "provider";

  /**
   * 保留的属性列表
   */
  List<String> RESERVED_PROPERTIES = Arrays.asList(PROP_COMMENT, PROP_LOCATION, PROP_PROVIDER);

  /**
   * 从数据目录中列举命名空间中的表
   * 如果数据目录支持视图,必须返回表示是表的信息,而不是视图的
   *
   * @param namespace a multi-part namespace
   * @return an array of Identifiers for tables
   * @throws NoSuchNamespaceException If the namespace does not exist (optional).
   */
  Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException;

  /**
   * 从数据目录中,通过标识符加载表
   * 如果数据目录支持视图,且包含当前标识符的视图,但是没有表,这里需要抛出异常
   *
   * @param ident a table identifier
   * @return the table's metadata
   * @throws NoSuchTableException If the table doesn't exist or is a view
   */
  Table loadTable(Identifier ident) throws NoSuchTableException;

  /**
   * 是指定的表@ident 无效化,如果表已经加载或者缓存了,则删除缓存的数据.如果表不存在或者没有缓存,什么都不做.
   */
  default void invalidateTable(Identifier ident) {
  }

  /**
   * 检测指定的表@ident 是否存在,存在则返回true.如果其对应的是视图,但是没有对应的表,则会抛出异常.
   * @param ident a table identifier
   * @return true if the table exists, false otherwise
   */
  default boolean tableExists(Identifier ident) {
    try {
      return loadTable(ident) != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * 在数据目录中创建一个表
   *
   * @param ident a table identifier
   * @param schema the schema of the new table, as a struct type
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of table properties
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a table or view already exists for the identifier
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException;

  /**
   * 对指定的表@ident 进行修改@changes
   * 实现中可能拒绝请求的修改.如果改动被拒绝,则表将不会发生变化.如果指定的表对应的是一个视图,且没有对应的表,则会抛出异常.
   * @param ident a table identifier
   * @param changes changes to apply to the table
   * @return updated metadata for the table
   * @throws NoSuchTableException If the table doesn't exist or is a view
   * @throws IllegalArgumentException If any change is rejected by the implementation.
   */
  Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException;

  /**
   * 删除指定的表@ident
   * @param ident a table identifier
   * @return true if a table was deleted, false if no table exists for the identifier
   */
  boolean dropTable(Identifier ident);

  /**
   * 对指定的表进行重命名
   * 如果数据目录不支持跨命名空间的重命名,则会抛出异常@UnsupportedOperationException
   * @param oldIdent the table identifier of the existing table to rename
   * @param newIdent the new table identifier of the table
   * @throws NoSuchTableException If the table to rename doesn't exist or is a view
   * @throws TableAlreadyExistsException If the new table name already exists or is a view
   * @throws UnsupportedOperationException If the namespaces of old and new identiers do not
   *                                       match (optional)
   */
  void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException;
}
