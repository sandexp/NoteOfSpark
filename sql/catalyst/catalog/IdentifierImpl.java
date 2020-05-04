/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import com.google.common.base.Preconditions;
import org.apache.spark.annotation.Experimental;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 标识符的实现
 * 参数列表:
 * @namespace 命名空间
 * @name 标识名称
 */
@Experimental
class IdentifierImpl implements Identifier {

  private String[] namespace;
  private String name;

  IdentifierImpl(String[] namespace, String name) {
    Preconditions.checkNotNull(namespace, "Identifier namespace cannot be null");
    Preconditions.checkNotNull(name, "Identifier name cannot be null");
    this.namespace = namespace;
    this.name = name;
  }

  // 获取命名空间
  @Override
  public String[] namespace() {
    return namespace;
  }

  // 获取命名标识符
  @Override
  public String name() {
    return name;
  }

  private String escapeQuote(String part) {
    if (part.contains("`")) {
      return part.replace("`", "``");
    } else {
      return part;
    }
  }

  // 信息显示(命名空间的信息)
  @Override
  public String toString() {
    return Stream.concat(Stream.of(namespace), Stream.of(name))
        .map(part -> '`' + escapeQuote(part) + '`')
        .collect(Collectors.joining("."));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IdentifierImpl that = (IdentifierImpl) o;
    return Arrays.equals(namespace, that.namespace) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(namespace), name);
  }
}
