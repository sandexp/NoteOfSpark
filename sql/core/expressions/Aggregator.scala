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

import org.apache.spark.sql.{Encoder, TypedColumn}
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression

/**
 * 用户自定义聚合的基础类。用于@DataSet的操作，获取组中的所有元素，并将其合并为一个值
  例如下述示例程序
 * {{{
 *   case class Data(i: Int)
 *
 *   val customSummer =  new Aggregator[Data, Int, Int] {
 *     def zero: Int = 0
 *     def reduce(b: Int, a: Data): Int = b + a.i
 *     def merge(b1: Int, b2: Int): Int = b1 + b2
 *     def finish(r: Int): Int = r
 *   }.toColumn()
 *
 *   val ds: Dataset[Data] = ...
 *   val aggregated = ds.select(customSummer)
 * }}}
 *
  这个是基于宽松的聚合策略<a href="https://github.com/twitter/algebird"></a>
 *
 * @tparam IN 输入的聚合类型
 * @tparam BUF reduce的类型
 * @tparam OUT 最终输出类型
 */
abstract class Aggregator[-IN, BUF, OUT] extends Serializable {

  /**
   * 聚合的起始值，需要满足b+zero=b的条件
   */
  def zero: BUF

  /**
   * 将两个值聚合成一个值.考虑到性能的要求,这个函数会对b进行修改,并且返回b,而不会构造一个新的b
   */
  def reduce(b: BUF, a: IN): BUF

  /**
   * 合并两个中间值b1,b2
   */
  def merge(b1: BUF, b2: BUF): BUF

  /**
   * 转换reduce的输出
   */
  def finish(reduction: BUF): OUT

  /**
   * 指定中间值的编码器
   */
  def bufferEncoder: Encoder[BUF]

  /**
   * 使用最终输出值进行编码
   */
  def outputEncoder: Encoder[OUT]

  /**
   * 返回聚合器,作为类型列@TypedColumn ,可以用于数据集操作
   */
  def toColumn: TypedColumn[IN, OUT] = {
    implicit val bEncoder = bufferEncoder
    implicit val cEncoder = outputEncoder

    val expr =
      AggregateExpression(
        TypedAggregateExpression(this),
        Complete,
        isDistinct = false)

    new TypedColumn[IN, OUT](expr, encoderFor[OUT])
  }
}
