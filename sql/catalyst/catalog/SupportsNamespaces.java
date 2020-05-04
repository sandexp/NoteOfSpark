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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**命名空间相关的数据目录方法
 *
 * 如果一个对象,例如一个表,视图或者是函数,其父命名空间必须要存在,且必须有发现类方法@listNamespaces 或者@listNamespaces返回
 *
 * 数据目录实现不需要委会命名空间的存在性.例如,函数数据目录会使用反射加载函数,且使用java包作为命名空间.这个函数不需要支持创建,修改,删除
 * 命名空间的功能.实现类中可以发现对象的存在性,如果没有命名空间则会抛出异常.
 */
@Experimental
public interface SupportsNamespaces extends CatalogPlugin {

  /**
   * 指定命名空间的属性,如果命名空间需要存储文件,则需要在这个位置下
   */
  String PROP_LOCATION = "location";

  /**
   * 指定命名空间描述的属性,可以通过`DESCRIBE NAMESPACE` 指令返回描述
   */
  String PROP_COMMENT = "comment";

  /**
   * 指定命名空间持有者的属性
   */
  String PROP_OWNER_NAME = "ownerName";

  /**
   * 指定命名空间持有者的类型属性
   */
  String PROP_OWNER_TYPE = "ownerType";

  /**
   * 保留的命名空间属性列表
   */
  List<String> RESERVED_PROPERTIES = Arrays.asList(PROP_COMMENT, PROP_LOCATION);

  /**
   * 获取数据目录默认的命名空间
   * 当数据目录为当前数据目录的时候，这个方法返回的命名空间就是当前命名空间的集合。
   * @return 返回一个多段命名空间
   */
  default String[] defaultNamespace() {
    return new String[0];
  }

  /**
   * 列举数据目录的顶层命名空间。
   * 如果一个对象，例如表,视图,函数存在的时候,父命名空间必须存在.且必须被发现方法返回.
   * 例如,表a,b,t存在,方法必须在结果列表中返回一个a
   * @return 多段命名空间的名称
   */
  String[][] listNamespaces() throws NoSuchNamespaceException;

  /**
   * 列举指定命名空间的命名空间
   * 如果一个对象，例如表,视图,函数存在的时候,父命名空间必须存在.且必须被发现方法返回.
   * 例如,表a,b,t存在，调用@listNamespaces(["a"]) 的时候，必须要返回["a", "b"]
   * @param namespace 多段命名空间
   * @return 多段命名空间名称
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   */
  String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException;

  /**
   * 测试是否指定的命名空间存在
   * 如果一个对象，例如表,视图,函数存在的时候,父命名空间必须存在
   * 例如,表a,b,t存在
   * 方法调用namespaceExists(["a"]) 或者namespaceExists(["a", "b"]) 返回true
   */
  default boolean namespaceExists(String[] namespace) {
    try {
      loadNamespaceMetadata(namespace);
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  /**
   * 加载指定命名空间的参数，返回对应的参数映射表
   */
  Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException;

  /**
   * 在数据目录中创建命名空间
   * @param namespace a multi-part namespace
   * @param metadata a string map of properties for the given namespace
   * @throws NamespaceAlreadyExistsException If the namespace already exists
   * @throws UnsupportedOperationException If create is not a supported operation
   */
  void createNamespace(
      String[] namespace,
      Map<String, String> metadata) throws NamespaceAlreadyExistsException;

  /**
   * 使用指定的元数据编号,对命名空间做出修改
   * @param namespace 多段命名空间
   * @param changes 使用在命名空间的变化
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If namespace properties are not supported
   */
  void alterNamespace(
      String[] namespace,
      NamespaceChange... changes) throws NoSuchNamespaceException;

  /**
   * 从数据目录中删除指定的命名空间,迭代删除命名空间中的所有对象
   * 如果数据目录的实现不支持这个操作,会抛出异常,删除成功则会返回true
   * @param namespace a multi-part namespace
   * @return true if the namespace was dropped
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If drop is not a supported operation
   */
  boolean dropNamespace(String[] namespace) throws NoSuchNamespaceException;
}
