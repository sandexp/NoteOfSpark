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
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * 授权数据字典扩展
 *
 * 数据字典扩展的简单实现,这个实现了所有数据字典的函数,主要通过调用内部会话目录的方式.为了创建方便,以便于用户可以按照自定义的方式
 * 重写这些方法.例如,可以重写方法@createTable,在调用创建表之前进行一些操作.
 */
@Experimental
public abstract class DelegatingCatalogExtension implements CatalogExtension {

  private CatalogPlugin delegate;

  public final void setDelegateCatalog(CatalogPlugin delegate) {
    this.delegate = delegate;
  }

  // 获取授权名称
  @Override
  public String name() {
    return delegate.name();
  }

  // 初始化数据字典
  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {}

  // 获取指定命名空间的标识符列表
  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return asTableCatalog().listTables(namespace);
  }

  // 根据指定的标识符加载表
  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return asTableCatalog().loadTable(ident);
  }

  //使指定标识符对应的表无效化
  @Override
  public void invalidateTable(Identifier ident) {
    asTableCatalog().invalidateTable(ident);
  }

  // 确定指定标识符对应的表是否存在
  @Override
  public boolean tableExists(Identifier ident) {
    return asTableCatalog().tableExists(ident);
  }

  // 创建指定标识符@ident 对应的表,其中schema是结构体类型
  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return asTableCatalog().createTable(ident, schema, partitions, properties);
  }

  // 对指定标识符@ident 的表进行指定的表修改@changes
  @Override
  public Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException {
    return asTableCatalog().alterTable(ident, changes);
  }

  // 删除指定标识符对应的表,删除成功返回true
  @Override
  public boolean dropTable(Identifier ident) {
    return asTableCatalog().dropTable(ident);
  }

  // 对指定标识符@oldIdent 的表进行重命名
  @Override
  public void renameTable(
      Identifier oldIdent,
      Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {
    asTableCatalog().renameTable(oldIdent, newIdent);
  }

  // 获取默认的命名空间
  @Override
  public String[] defaultNamespace() {
    return asNamespaceCatalog().defaultNamespace();
  }

  // 显示命名空间
  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return asNamespaceCatalog().listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return asNamespaceCatalog().listNamespaces(namespace);
  }

  // 确定指定的命名空间是否存在
  @Override
  public boolean namespaceExists(String[] namespace) {
    return asNamespaceCatalog().namespaceExists(namespace);
  }

  // 加载指定命名空间的元数据
  @Override
  public Map<String, String> loadNamespaceMetadata(
      String[] namespace) throws NoSuchNamespaceException {
    return asNamespaceCatalog().loadNamespaceMetadata(namespace);
  }

  // 使用指定的元数据表,创建命名空间
  @Override
  public void createNamespace(
      String[] namespace,
      Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    asNamespaceCatalog().createNamespace(namespace, metadata);
  }

  // 使用指定的新值,修改命名空间
  @Override
  public void alterNamespace(
      String[] namespace,
      NamespaceChange... changes) throws NoSuchNamespaceException {
    asNamespaceCatalog().alterNamespace(namespace, changes);
  }

  // 删除指定命名空间
  @Override
  public boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException {
    return asNamespaceCatalog().dropNamespace(namespace);
  }

  // 将数据目录转换为表目录
  private TableCatalog asTableCatalog() {
    return (TableCatalog)delegate;
  }

  // 将数据目录转换为命名空间
  private SupportsNamespaces asNamespaceCatalog() {
    return (SupportsNamespaces)delegate;
  }
}
