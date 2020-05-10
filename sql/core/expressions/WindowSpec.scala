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
import org.apache.spark.sql.{AnalysisException, Column}
import org.apache.spark.sql.catalyst.expressions._

/**
 窗口指导,定义了分区,排序,和窗口的边界
  使用@Window 的静态方法区成交一个@WindowSpec
 */
@Stable
class WindowSpec private[sql](
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    frame: WindowFrame) {

  /**
   定义在@WindowSpec 中的分区列
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec = {
    partitionBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   定义@WindowSpec 的分区列
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec = {
    new WindowSpec(cols.map(_.expr), orderSpec, frame)
  }

  /**
    定义@WindowSpec 中的排序列
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec = {
    orderBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   定义@WindowSpec 的排序列
   */
  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec = {
    val sortOrder: Seq[SortOrder] = cols.map { col =>
      col.expr match {
        case expr: SortOrder =>
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending)
      }
    }
    new WindowSpec(partitionSpec, sortOrder, frame)
  }

  /**
   定义窗口的上下界。开始和接受都是当前行的指针。0表示当前行，-1表示之前一行，5表示之后的第5行。
   推荐用户使用@Window.unboundedPreceding,@Window.unboundedFollowing，@Window.currentRow.
   示例：
   *   import org.apache.spark.sql.expressions.Window
   *   val df = Seq((1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"))
   *     .toDF("id", "category")
   *   val byCategoryOrderedById =
   *     Window.partitionBy('category).orderBy('id).rowsBetween(Window.currentRow, 1)
   *   df.withColumn("sum", sum('id) over byCategoryOrderedById).show()
   *
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
  // Note: when updating the doc for this method, also update Window.rowsBetween.
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => CurrentRow
      case Long.MinValue => UnboundedPreceding
      case x if Int.MinValue <= x && x <= Int.MaxValue => Literal(x.toInt)
      case x => throw new AnalysisException(s"Boundary start is not a valid integer: $x")
    }

    val boundaryEnd = end match {
      case 0 => CurrentRow
      case Long.MaxValue => UnboundedFollowing
      case x if Int.MinValue <= x && x <= Int.MaxValue => Literal(x.toInt)
      case x => throw new AnalysisException(s"Boundary end is not a valid integer: $x")
    }

    new WindowSpec(
      partitionSpec,
      orderSpec,
      SpecifiedWindowFrame(RowFrame, boundaryStart, boundaryEnd))
  }

  /**
  使用定义的frame界限创建一个@WindowSpec
  start和end都是与当前行相关的.例如,0意味着当前行,-1意味着之前的行,5意味着偏移为5的行
  推荐用户使用Window.unboundedPreceding,Window.unboundedFollowing和Window.currentRow指定特定的界限值,而非是使用整型值.
  基于范围的解析是基于order by表达式的实际值的.偏移量用于修改order by表达式的值.如果当前order by表达式值为10,且下限值为-3.
  结果的下限值就是7.
   *   import org.apache.spark.sql.expressions.Window
   *   val df = Seq((1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"))
   *     .toDF("id", "category")
   *   val byCategoryOrderedById =
   *     Window.partitionBy('category).orderBy('id).rangeBetween(Window.currentRow, 1)
   *   df.withColumn("sum", sum('id) over byCategoryOrderedById).show()
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
   * @param start 起始位置.如果这个设置为最小的long值,则表示是无界的
   * @param end 界限结束值,如果设置为最大的long就表示为无界限的.
   */
  // Note: when updating the doc for this method, also update Window.rangeBetween.
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => CurrentRow
      case Long.MinValue => UnboundedPreceding
      case x => Literal(x)
    }

    val boundaryEnd = end match {
      case 0 => CurrentRow
      case Long.MaxValue => UnboundedFollowing
      case x => Literal(x)
    }

    new WindowSpec(
      partitionSpec,
      orderSpec,
      SpecifiedWindowFrame(RangeFrame, boundaryStart, boundaryEnd))
  }

  /**
   * Converts this [[WindowSpec]] into a [[Column]] with an aggregate expression.
   */
  private[sql] def withAggregate(aggregate: Column): Column = {
    val spec = WindowSpecDefinition(partitionSpec, orderSpec, frame)
    new Column(WindowExpression(aggregate.expr, spec))
  }
}
