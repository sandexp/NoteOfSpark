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

package org.apache.spark.sql.expressions

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
 * 聚合器,用单个联合的以及交换的reduce函数.这个reduce函数可以用于变量所有输入值,且会将其聚合成单个值.如果没有输入,就会返回空值
 * 这个当前假定了至少有一个输入行
 */
private[sql] class ReduceAggregator[T: Encoder](func: (T, T) => T)
  extends Aggregator[T, (Boolean, T), T] {

  // 编码器
  @transient private val encoder = implicitly[Encoder[T]]

  override def zero: (Boolean, T) = (false, null.asInstanceOf[T])

  // 获取缓冲编码器
  override def bufferEncoder: Encoder[(Boolean, T)] =
    ExpressionEncoder.tuple(
      ExpressionEncoder[Boolean](),
      encoder.asInstanceOf[ExpressionEncoder[T]])

  // 获取输出编码器
  override def outputEncoder: Encoder[T] = encoder

  // 聚合函数
  override def reduce(b: (Boolean, T), a: T): (Boolean, T) = {
    if (b._1) {
      (true, func(b._2, a))
    } else {
      (true, a)
    }
  }

  // 合并函数
  override def merge(b1: (Boolean, T), b2: (Boolean, T)): (Boolean, T) = {
    if (!b1._1) {
      b2
    } else if (!b2._1) {
      b1
    } else {
      (true, func(b1._2, b2._2))
    }
  }

  override def finish(reduction: (Boolean, T)): T = {
    if (!reduction._1) {
      throw new IllegalStateException("ReduceAggregator requires at least one input row")
    }
    reduction._2
  }
}
