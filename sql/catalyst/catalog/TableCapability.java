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
 * 表功能
 * 表使用@capabilities() 返回功能集合,每个功能都会提示spark,表示这个表支持某项功能.例如,@BATCH_READ 允许spark
 * 使用批次扫描的方式读取数据
 */
@Experimental
public enum TableCapability {
  /**
   * 批次读取模式
   */
  BATCH_READ,

  /**
   * 微批次读取模式
   */
  MICRO_BATCH_READ,

  /**
   * 连续读取模式
   */
  CONTINUOUS_READ,

  /**
   * 批次写出模式
   * 这个表使用这个模式时,必须支持数据添加,且支持额外的写操作
   * 例如@TRUNCATE 情况,@OVERWRITE_BY_FILTER 过滤器重写,和@OVERWRITE_DYNAMIC 动态重新功能
   */
  BATCH_WRITE,

  /**
   * 流式写出模式
   *
   * 这个表使用这个模式时,必须支持数据添加,且支持额外的写操作
   * 例如@TRUNCATE 情况,@OVERWRITE_BY_FILTER 过滤器重写,和@OVERWRITE_DYNAMIC 动态重新功能
   */
  STREAMING_WRITE,

  /**
   * b表示表可以在一次写操作中清空表。
   */
  TRUNCATE,

  /**
   * 表示在一次写操作中，使用指定的过滤函数去替换存在的数据
   */
  OVERWRITE_BY_FILTER,

  /**
   * 在一次写操作中可以动态替换存在的数据
   */
  OVERWRITE_DYNAMIC,

  /**
   * 一次写操作中可以集接受任意schema
   */
  ACCEPT_ANY_SCHEMA,

  /**
   * 表可以支持添加写操作，使用的是v1的@InsertableRelation 接口
   * 表必须要创建一个@V1WriteBuilder,且需要支持清空,动态重写,以及条件重写的功能
   */
  V1_BATCH_WRITE
}
