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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow

/**
  广播变量模式,用于标识参与广播的元组形式.这个的典型事例就是标识(元组保持不变)和hash(元组转化为相同的hash index型式)
 */
trait BroadcastMode {
  
  // 转换行数据
  def transform(rows: Array[InternalRow]): Any

  def transform(rows: Iterator[InternalRow], sizeHint: Option[Long]): Any
  
  // 规范化广播变量模式
  def canonicalized: BroadcastMode
}

/**
  标识类型的广播变量模式,需要行按照原始的形式进行广播
 */
case object IdentityBroadcastMode extends BroadcastMode {
  // TODO: pack the UnsafeRows into single bytes array.
  override def transform(rows: Array[InternalRow]): Array[InternalRow] = rows

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): Array[InternalRow] = rows.toArray

  override def canonicalized: BroadcastMode = this
}
