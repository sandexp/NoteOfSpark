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
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{WindowSpec => _, _}

/**
在DF中定义窗口的实用函数
// PARTITION BY country ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
Window.partitionBy("country").orderBy("date")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
// PARTITION BY country ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
 *   Window.partitionBy("country").orderBy("date").rowsBetween(-3, 3)
 注意: 当没有定义排序规则的时候,默认使用无界窗口(rowFrame,unboundedPreceding,unboundedFollowing).当排序定义的时候,
  默认使用增长型窗口(rangeFrame, unboundedPreceding, currentRow)
 */
@Stable
object Window {

  /**
   创建一个窗口@WindowSpec,使用定位的分区
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec = {
    spec.partitionBy(colName, colNames : _*)
  }

  /**
   使用定义的分区创建窗口
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec = {
    spec.partitionBy(cols : _*)
  }

  /**
   使用定义的排序规则创建@WindowSpec
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec = {
    spec.orderBy(colName, colNames : _*)
  }

  /**
   使用定义的排序规则创建@WindowSpec
   */
  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec = {
    spec.orderBy(cols : _*)
  }

  /**
   代表分区的首行的值,等于sql中的UNBOUNDED PRECEDING
   这个用于只读frame的解析
   Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
   */
  def unboundedPreceding: Long = Long.MinValue

  /**
   代表分区的最后一行，等于sql中的UNBOUNDED FOLLOWING
   这个用于只读frame的界限
    Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
   */
  def unboundedFollowing: Long = Long.MaxValue

  /**
   代表当前行的值,可以用于只读frame的界限
   Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
   */
  def currentRow: Long = 0

  /**
   使用定义的frame界限创建一个@WindowSpec,区域范围是[start,end]
   起始和结束指针都是与当前行相关的指针.例如,"0"代表当前行,"-1"意味着之前的行.5意味着当前行的后5行
    推荐用户使用Window.unboundedPreceding,Window.unboundedFollowing和Window.currentRow指定特定的界限值,而非是使用整型值.
   
   行界限基于分区内的行指针.偏移量表示当前行的行数量.例如,给定基于滑动窗口的行下限值为-1,上限值为2.那么索引为5的窗口在4-7直接.
  每次滑动1
    使用示例:
    import org.apache.spark.sql.expressions.Window
    val df = Seq((1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"))
       .toDF("id", "category")
    val byCategoryOrderedById =
       Window.partitionBy('category).orderBy('id).rowsBetween(Window.currentRow, 1)
    df.withColumn("sum", sum('id) over byCategoryOrderedById).show()
   
   *   +---+--------+---+
   *   | id|category|sum|
   *   +---+--------+---+
   *   |  1|       b|  3|
   *   |  2|       b|  5|
   *   |  3|       b|  3|
   *   |  1|       a|  2|
   *   |  1|       a|  3|
   *   |  2|       a|  2|
   *   +---+--------+---+
   * @param start 起始位置.如果这个设置为最小的long值,则表示是无界的
   * @param end 界限结束值,如果设置为最大的long就表示为无界限的.
   */
  // Note: when updating the doc for this method, also update WindowSpec.rowsBetween.
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    spec.rowsBetween(start, end)
  }

  /**
  使用定义的frame界限创建一个@WindowSpec
  start和end都是与当前行相关的.例如,0意味着当前行,-1意味着之前的行,5意味着偏移为5的行
  推荐用户使用Window.unboundedPreceding,Window.unboundedFollowing和Window.currentRow指定特定的界限值,而非是使用整型值.
  基于范围的解析是基于order by表达式的实际值的.偏移量用于修改order by表达式的值.如果当前order by表达式值为10,且下限值为-3.
  结果的下限值就是7.
   *
   * {{{
   *   import org.apache.spark.sql.expressions.Window
   *   val df = Seq((1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"))
   *     .toDF("id", "category")
   *   val byCategoryOrderedById =
   *     Window.partitionBy('category).orderBy('id).rangeBetween(Window.currentRow, 1)
   *   df.withColumn("sum", sum('id) over byCategoryOrderedById).show()
   *
   *   +---+--------+---+
   *   | id|category|sum|
   *   +---+--------+---+
   *   |  1|       b|  3|
   *   |  2|       b|  5|
   *   |  3|       b|  3|
   *   |  1|       a|  4|
   *   |  1|       a|  4|
   *   |  2|       a|  2|
   *   +---+--------+---+
   * }}}
   *
   * @param start 起始位置.如果这个设置为最小的long值,则表示是无界的
   * @param end 界限结束值,如果设置为最大的long就表示为无界限的.
   */
  // Note: when updating the doc for this method, also update WindowSpec.rangeBetween.
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    spec.rangeBetween(start, end)
  }

  private[sql] def spec: WindowSpec = {
    new WindowSpec(Seq.empty, Seq.empty, UnspecifiedFrame)
  }

}

/**
DF中窗口的实用函数
// PARTITION BY country ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
Window.partitionBy("country").orderBy("date")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
// PARTITION BY country ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
 Window.partitionBy("country").orderBy("date").rowsBetween(-3, 3)
 */
@Stable
class Window private()  // So we can see Window in JavaDoc.
