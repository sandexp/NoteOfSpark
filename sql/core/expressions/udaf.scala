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

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.types._

/**
 * 用户定义的聚合函数(UDAF)基础类
 */
@Stable
abstract class UserDefinedAggregateFunction extends Serializable {

  /**
    输入类型(schema):
   结构体类型@StructType 代表输入参数的数据类型.例如@UserDefinedAggregateFunction 需要两个输入参数,分别是DoubleType
   和LongType类型.返回的结构体类型类似下述结构
    new StructType()
      .add("doubleInput", DoubleType)
      .add("longInput", LongType)
   这个结构体类型的属性名仅仅用于辨识输入参数.用户可以选择名称,缺标识输入参数
   */
  def inputSchema: StructType

  /**
   结构体类型@StructType 代表聚合缓冲区的数据类型.例如@UserDefinedAggregateFunction有两个参数,分别是DoubleType和
   LongType.返回的结构体类型如下:
    new StructType()
    .add("doubleInput", DoubleType)
    .add("longInput", LongType)
   其中结构体属性的名称用于标识缓冲区值的属性.用户可以选择名称去标识它
   */
  def bufferSchema: StructType

  /**
   * 这个类@UserDefinedAggregateFunction返回的数据类型
   */
  def dataType: DataType

  /**
   * 如果函数时决定性的返回true.给定相同的输入,总是会得到相同的输出
   */
  def deterministic: Boolean

  /**
   初始化给定的聚合缓冲区.
    需要使用合并函数将两个初始缓冲区上使用合并函数
   */
  def initialize(buffer: MutableAggregationBuffer): Unit

  /**
   * 更新给定的聚合缓冲区,使用新的输入数据.每个输入行会被调用一次
   */
  def update(buffer: MutableAggregationBuffer, input: Row): Unit

  /**
   * 合并两个聚合缓冲区,且存储更新的缓冲值到buffer1中.当合并两个部分聚合的数据的时候使用.
   */
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit

  /**
   基于给定的聚合缓冲区,计算@UserDefinedAggregateFunction 的最终结果
   */
  def evaluate(buffer: Row): Any

  /**
    对于这个UDAF创建列,使用给定的列作为输入参数
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = false)
    Column(aggregateExpression)
  }

  /**
   对于这个UDAF创建一个列,使用给定列中的去重值作为输入参数
   */
  @scala.annotation.varargs
  def distinct(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = true)
    Column(aggregateExpression)
  }
}

/**
 代表可变的聚合缓冲区的行
意味着不可以再spark外部继承
 */
@Stable
abstract class MutableAggregationBuffer extends Row {

  /** Update the ith value of this buffer. */
  def update(i: Int, value: Any): Unit
}
