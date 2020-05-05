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

/**
 * 写构建器接口,支持动态分区重写
 * 写出会动态的重写分区,移除每个逻辑分区存在的数据并写入新的数据
 * 这个用于对hive表进行兼容,但是不推荐.相反,使用@SupportsOverwrite 显式地替换
 */
public interface SupportsDynamicOverwrite extends WriteBuilder {
  /**
   * 重写动态分区
   *
   * @return this write builder for method chaining
   */
  WriteBuilder overwriteDynamicPartitions();
}
