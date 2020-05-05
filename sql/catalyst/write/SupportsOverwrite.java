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

package org.apache.spark.sql.connector.write;

import org.apache.spark.sql.sources.AlwaysTrue$;
import org.apache.spark.sql.sources.Filter;

/**
 * 写出构建器特征,用于表的重写
 * 使用过滤器对数据进行重写会删除匹配与指定过滤器的数据,且使用数据对其进行替换
 */
public interface SupportsOverwrite extends WriteBuilder, SupportsTruncate {
  /**
   * 配置写出器,用于替换满足过滤器的数据
   * 当前仅当满足过滤条件的时候,行必须要从数据源中删除.过滤器使用的是AND逻辑
   * @param filters filters used to match data to overwrite
   * @return this write builder for method chaining
   */
  WriteBuilder overwrite(Filter[] filters);

  @Override
  default WriteBuilder truncate() {
    return overwrite(new Filter[] { AlwaysTrue$.MODULE$ });
  }
}
