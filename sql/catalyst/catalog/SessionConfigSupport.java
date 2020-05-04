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

import org.apache.spark.annotation.Evolving;

/**
 * 表提供器@TableProvider 的混合接口.数据源可以实现这个接口,从而使用指定的key前缀传输会话配置到所有的数据源中
 */
@Evolving
public interface SessionConfigSupport extends TableProvider {

  /**
   * 会话的key前缀,用于传输,通常是数据源的名称.spark会提取所有会话的配置(以spark.datasource.keyPrefix)开头的,在会话中将其传输
   * 给这些数据源
   */
  String keyPrefix();
}
