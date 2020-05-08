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
package org.apache.spark.sql;

import org.apache.spark.annotation.Stable;

/**
 * 保存模式用于指定DF保存到数据源中的行为
 */
@Stable
public enum SaveMode {
  /**
   * 添加模式意味着当将DF保存到数据源的时候,如果数据/表已经存在,DF的内容需要添加到存在的数据后面
   * @since 1.3.0
   */
  Append,
  /**
   * 重写模式,意味着当将DF写入到数据源的时候.如果数据/表已经存在了,存在的数据会被DF的数据覆盖
   * @since 1.3.0
   */
  Overwrite,
  /**
   * 数据源存在则报错的模式,意味着当将DF保存到数据源的时候,如果数据已经存在,就会抛出异常
   * @since 1.3.0
   */
  ErrorIfExists,
  /**
   * 忽略模式,意味着在将DF存储到数据源的时候,如果数据存在,保存操作不会保存DF数据,且不会改变现在的数据
   * @since 1.3.0
   */
  Ignore
}
