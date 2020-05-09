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

package org.apache.spark.sql.connector.write

import org.apache.spark.annotation.{Experimental, Unstable}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.sources.InsertableRelation

/**
  这个特征需要由v1版本的数据源@DataSources实现.这些数据源需要v2版本的数据源进行平衡.@InsertableRelation 仅仅用来
  添加数据.写出构建器@WriteBuilder的其他实例包括@SupportsOverwrite 和@SupportsTruncate. 支持数据添加之外的其他操作.
  这个接口提供spark数据源迁移到数据源v2版本上,且在之后的发行版中会删除
 */
@Experimental
@Unstable
trait V1WriteBuilder extends WriteBuilder {

  /**
   参考一个@InsertableRelation,允许添加DF到指定位置.插入方法需要设置@overwrite=false.数据源需要在@SupportsOverwrite
   或者@SupportsTruncate实现重写方法.
   */
  def buildForV1Write(): InsertableRelation
  /**
    * 下面的方法不能被v1版本的写出构建器实现,超类会抛出异常
    */
  // 获取批次写出器
  override final def buildForBatch(): BatchWrite = super.buildForBatch()

  // 获取流式写出器
  override final def buildForStreaming(): StreamingWrite = super.buildForStreaming()
}
