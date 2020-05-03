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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, Statistics => V2Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

/**
  * 数据源v2表的逻辑执行计划
 *
 * @param table   这个关系所表示的表
 * @param options 表操作的参数
 * @output 输出的索引引用
 */
case class DataSourceV2Relation(
    table: Table,
    output: Seq[AttributeReference],
    options: CaseInsensitiveStringMap)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  import DataSourceV2Implicits._

  // 获取表名称
  override def name: String = table.name()

  // 确定是否跳过schema的处理
  override def skipSchemaResolution: Boolean = table.supports(TableCapability.ACCEPT_ANY_SCHEMA)

  // 获取简单的字符串
  override def simpleString(maxFields: Int): String = {
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  // 计算表的统计值,测试情况下不可使用
  override def computeStats(): Statistics = {
    if (Utils.isTesting) {
      // when testing, throw an exception if this computeStats method is called because stats should
      // not be accessed before pushing the projection and filters to create a scan. otherwise, the
      // stats are not accurate because they are based on a full table scan of all columns.
      throw new IllegalStateException(
        s"BUG: computeStats called before pushdown on DSv2 relation: $name")
    } else {
      // 不处于测试状态下的时候,需要返回统计数值,因为就算是不好的统计值也是不查询失败要好.
      table.asReadable.newScanBuilder(options) match {
        case r: SupportsReportStatistics =>
          val statistics = r.estimateStatistics()
          DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
        case _ =>
          Statistics(sizeInBytes = conf.defaultSizeInBytes)
      }
    }
  }

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }
}

/** DSv2表(带有scan)的逻辑计划
 * 这个在优化器中使用,用于在转换为物理计划之前将过滤器和执行计划进行叠加.保证了优化器使用的统计值会叠加过滤器和执行计划.
 *
 * @param table a DSv2 [[Table]]
 * @param scan a DSv2 [[Scan]]
 * @param output 这个关系的输出属性
 */
case class DataSourceV2ScanRelation(
    table: Table,
    scan: Scan,
    output: Seq[AttributeReference]) extends LeafNode with NamedRelation {

  // 获取表的名称
  override def name: String = table.name()

  // 获取简单的表述
  override def simpleString(maxFields: Int): String = {
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  // 计算统计值,并返回
  override def computeStats(): Statistics = {
    scan match {
      case r: SupportsReportStatistics =>
        val statistics = r.estimateStatistics()
        DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
      case _ =>
        Statistics(sizeInBytes = conf.defaultSizeInBytes)
    }
  }
}

/**
  * 这个类是@DataSourceV2Relation 的特化,设置流式的bit set为true.
 * 注意,这个计划的读取者是可变的,spark不会对这个机会进行叠加操作,这样比例这个计划的编号.需要在指出如何对流式数据源进行叠加之后,
  *才能进行计划的确认.
  * 构造器参数:
  * output: Seq[Attribute] 输出属性列表
  * scan: Scan 扫描器
  * stream: SparkDataStream spark数据流
  * startOffset: Option[Offset] 起始偏移量
  * endOffset: Option[Offset] 结束偏移量
 */
case class StreamingDataSourceV2Relation(
    output: Seq[Attribute],
    scan: Scan,
    stream: SparkDataStream,
    startOffset: Option[Offset] = None,
    endOffset: Option[Offset] = None)
  extends LeafNode with MultiInstanceRelation {

  // 确定是否为流式数据源
  override def isStreaming: Boolean = true

  // 新建逻辑计划
  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  // 计算统计值,并返回
  override def computeStats(): Statistics = scan match {
    case r: SupportsReportStatistics =>
      val statistics = r.estimateStatistics()
      DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

// 数据源v2的关系
object DataSourceV2Relation {
  def create(table: Table, options: CaseInsensitiveStringMap): DataSourceV2Relation = {
    val output = table.schema().toAttributes
    DataSourceV2Relation(table, output, options)
  }

  def create(table: Table): DataSourceV2Relation = create(table, CaseInsensitiveStringMap.empty)

  /**
    * 将数据源转换为逻辑的统计值
   */
  def transformV2Stats(
      v2Statistics: V2Statistics,
      defaultRowCount: Option[BigInt],
      defaultSizeInBytes: Long): Statistics = {
    val numRows: Option[BigInt] = if (v2Statistics.numRows().isPresent) {
      Some(v2Statistics.numRows().getAsLong)
    } else {
      defaultRowCount
    }
    Statistics(
      sizeInBytes = v2Statistics.sizeInBytes().orElse(defaultSizeInBytes),
      rowCount = numRows)
  }
}
