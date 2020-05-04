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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * 标记接口，用于提供spark的目录实现
 * 可以通过实现额外的表接口,来提供这个目录函数
 * 目录实现必须要实现这个标记接口,这样可以被Catalog的@load 方法加载.加载器会实例化这个目录类.(使用公用的无参构造器).
 * 创建实例之后,可以调用@initialize(String, CaseInsensitiveStringMap) 进行配置
 * 目录实现通过添加配置参数来注册.例如@spark.sql.catalog.catalog-name=com.example.YourCatalogClass.所有的配置都共享一个
 * 目录名称前缀，可以通过@spark.sql.catalog.catalog-name.(key)=(value)进行参数配置。
 */
@Experimental
public interface CatalogPlugin {
  /**
   * 初始化配置，这个方法仅仅调用一次，在提供器初始化之后调用
   *
   * @param name 用于表示和加载catalog的名称
   * @param options  大小写不敏感的映射表
   */
  void initialize(String name, CaseInsensitiveStringMap options);

  /**
   * 获取目录名称
   * 这个方法在调用@initialize(String, CaseInsensitiveStringMap) 之后才可以使用，用于传输目录名称
   */
  String name();
}
