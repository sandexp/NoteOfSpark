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
 * 命名空间变换的类
 * 代表向命名空间申请的变化,这个通过SupportsNamespaces 的@alterNamespace 传递
 * 示例程序:
 * import NamespaceChange._
 * val catalog = Catalogs.load(name)
 * catalog.alterNamespace(ident,
 *    setProperty("prop", "value"),
 *    removeProperty("other_prop")
 * )
 */
@Experimental
public interface NamespaceChange {
  /**
   * 设定命名空间属性,创建一个@NamespaceChange,如果属性值已经存在,就会使用新值替换
   * @param property 属性名称
   * @param value 新属性值
   * @return NamespaceChange
   */
  static NamespaceChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * 创建一个用于移除属性的命名空间变化@NamespaceChange,如果属性不存在,则表示变化成功
   */
  static NamespaceChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * 设置参数的命名空间变化，如果值已经存在,则会使用新值更新
   * 参数列表:
   * @property 属性名称
   * @value 属性值
   */
  final class SetProperty implements NamespaceChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    public String property() {
      return property;
    }

    public String value() {
      return value;
    }
  }

  /**
   * 用于移除属性的命名空间变化
   * @property 属性名称
   */
  final class RemoveProperty implements NamespaceChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    public String property() {
      return property;
    }
  }
}
