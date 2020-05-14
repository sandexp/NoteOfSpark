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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.analysis.ViewType
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{DataType, StructType}

/**
  逻辑计划节点,包含了SQL额外的转换信息
  当一个查询有多个实现的时候,用于持有SQL的转换信息.
  例如,CREATE TABLE可以被不同的节点实现，版本分别是v1和v2版本。
  由于必须要转换成具体的逻辑计划，所以转换的逻辑计划没有被解决。
  转换的逻辑计划位于@Catalyst中，这个SQL转换逻辑会被保存在@AbstractSqlParser中
 */
abstract class ParsedStatement extends LogicalPlan {
  // Redact properties and options when parsed nodes are used by generic methods like toString
  override def productIterator: Iterator[Any] = super.productIterator.map {
    case mapArg: Map[_, _] => conf.redactOptions(mapArg)
    case other => other
  }

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty

  final override lazy val resolved = false
}

/**
  创建表的指令,由SQL转换
  这个是元数据才可以使用的指令,不能将数据写入到表中
 */
case class CreateTableStatement(
    tableName: Seq[String],
    tableSchema: StructType,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    ifNotExists: Boolean) extends ParsedStatement

/**
CREATE TABLE AS SELECT指令,由SQL转换过来
 */
case class CreateTableAsSelectStatement(
    tableName: Seq[String],
    asSelect: LogicalPlan,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    ifNotExists: Boolean) extends ParsedStatement {

  override def children: Seq[LogicalPlan] = Seq(asSelect)
}

/**
  CREATE VIEW 表达式,由SQL转换过来
 */
case class CreateViewStatement(
    viewName: Seq[String],
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    viewType: ViewType) extends ParsedStatement

/**
  REPLACE TABLE 指令,由SQL转换过来
  如果表在运行指令之前存在,执行这个表达式会替代元数据,且从表的底层清除行数据
 */
case class ReplaceTableStatement(
    tableName: Seq[String],
    tableSchema: StructType,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    orCreate: Boolean) extends ParsedStatement

/**
  REPLACE TABLE AS SELECT 指令,由SQL转换过来
 */
case class ReplaceTableAsSelectStatement(
    tableName: Seq[String],
    asSelect: LogicalPlan,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    orCreate: Boolean) extends ParsedStatement {

  override def children: Seq[LogicalPlan] = Seq(asSelect)
}


/**
  ALTER TABLE ... ADD COLUMNS 添加列
 */
case class QualifiedColType(name: Seq[String], dataType: DataType, comment: Option[String])

/**
  添加列ALTER TABLE ... ADD COLUMNS,由SQL转换过来
 */
case class AlterTableAddColumnsStatement(
    tableName: Seq[String],
    columnsToAdd: Seq[QualifiedColType]) extends ParsedStatement

/**
  列改变指令ALTER TABLE ... CHANGE COLUMN ,由SQL转换过来
 */
case class AlterTableAlterColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    dataType: Option[DataType],
    comment: Option[String]) extends ParsedStatement

/**
  列重命名ALTER TABLE ... RENAME COLUMN,由SQL转换过来
 */
case class AlterTableRenameColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    newName: String) extends ParsedStatement

/**
  ALTER TABLE ... DROP COLUMNS,由SQL转换过来
 */
case class AlterTableDropColumnsStatement(
    tableName: Seq[String],
    columnsToDrop: Seq[Seq[String]]) extends ParsedStatement

/**
  ALTER TABLE ... SET TBLPROPERTIES 修改表属性,由SQL转换过来
 */
case class AlterTableSetPropertiesStatement(
    tableName: Seq[String],
    properties: Map[String, String]) extends ParsedStatement

/**
  ALTER TABLE ... UNSET TBLPROPERTIES修改表属性,由SQL转换过来
 */
case class AlterTableUnsetPropertiesStatement(
    tableName: Seq[String],
    propertyKeys: Seq[String],
    ifExists: Boolean) extends ParsedStatement

/**
ALTER TABLE ... SET LOCATION 设置表的位置,SQL转换得来
 */
case class AlterTableSetLocationStatement(
    tableName: Seq[String],
    partitionSpec: Option[TablePartitionSpec],
    location: String) extends ParsedStatement

/**
  ALTER TABLE ... RECOVER PARTITIONS 指令,由SQL转换过来
 */
case class AlterTableRecoverPartitionsStatement(
    tableName: Seq[String]) extends ParsedStatement

/**
  添加分区ALTER TABLE ... ADD PARTITION,由SQL转换过来
 */
case class AlterTableAddPartitionStatement(
    tableName: Seq[String],
    partitionSpecsAndLocs: Seq[(TablePartitionSpec, Option[String])],
    ifNotExists: Boolean) extends ParsedStatement

/**
  重命名分区ALTER TABLE ... ADD PARTITION,由SQL转换过来
 */
case class AlterTableRenamePartitionStatement(
    tableName: Seq[String],
    from: TablePartitionSpec,
    to: TablePartitionSpec) extends ParsedStatement

/**
  删除分区指令ALTER TABLE ... DROP PARTITION,由SQL转换过来
 */
case class AlterTableDropPartitionStatement(
    tableName: Seq[String],
    specs: Seq[TablePartitionSpec],
    ifExists: Boolean,
    purge: Boolean,
    retainData: Boolean) extends ParsedStatement

/**
  修改表序列化属性指令ALTER TABLE ... SERDEPROPERTIES,由SQL转换而来
 */
case class AlterTableSerDePropertiesStatement(
    tableName: Seq[String],
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, String]],
    partitionSpec: Option[TablePartitionSpec]) extends ParsedStatement

/**
  修改属性ALTER VIEW ... SET TBLPROPERTIES,由SQL转换而来
 */
case class AlterViewSetPropertiesStatement(
    viewName: Seq[String],
    properties: Map[String, String]) extends ParsedStatement

/**
  修改表属性ALTER VIEW ... UNSET TBLPROPERTIES,由SQL转换而来
 */
case class AlterViewUnsetPropertiesStatement(
    viewName: Seq[String],
    propertyKeys: Seq[String],
    ifExists: Boolean) extends ParsedStatement

/**
  修改查询信息,由SQL转换而来
 */
case class AlterViewAsStatement(
    viewName: Seq[String],
    originalText: String,
    query: LogicalPlan) extends ParsedStatement

/**
  表重命名ALTER TABLE ... RENAME TO,由SQL转换而来
 */
case class RenameTableStatement(
    oldName: Seq[String],
    newName: Seq[String],
    isView: Boolean) extends ParsedStatement

/**
   删除表DROP TABLE指令,用于转换SQL
 */
case class DropTableStatement(
    tableName: Seq[String],
    ifExists: Boolean,
    purge: Boolean) extends ParsedStatement

/**
  删除视图指令,用于SQL转换
 */
case class DropViewStatement(
    viewName: Seq[String],
    ifExists: Boolean) extends ParsedStatement

/**
  描述表信息DESCRIBE TABLE tbl_name,由SQL信息转换过来
 */
case class DescribeTableStatement(
    tableName: Seq[String],
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean) extends ParsedStatement

/**
  命名空间描述DESCRIBE NAMESPACE,由SQL转换过来
 */
case class DescribeNamespaceStatement(
    namespace: Seq[String],
    extended: Boolean) extends ParsedStatement

/**
  表信息描述DESCRIBE TABLE tbl_name col_name,由SQL转换过来
 */
case class DescribeColumnStatement(
    tableName: Seq[String],
    colNameParts: Seq[String],
    isExtended: Boolean) extends ParsedStatement

/**
  插入指令INSERT INTO
 *
 * @param table                the logical plan representing the table.
 * @param query                the logical plan representing data to write to.
 * @param overwrite            overwrite existing table or partitions.
 * @param partitionSpec        a map from the partition key to the partition value (optional).
 *                             If the value is missing, dynamic partition insert will be performed.
 *                             As an example, `INSERT INTO tbl PARTITION (a=1, b=2) AS` would have
 *                             Map('a' -> Some('1'), 'b' -> Some('2')),
 *                             and `INSERT INTO tbl PARTITION (a=1, b) AS ...`
 *                             would have Map('a' -> Some('1'), 'b' -> None).
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                             Only valid for static partitions.
 */
case class InsertIntoStatement(
    table: LogicalPlan,
    partitionSpec: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean) extends ParsedStatement {

  require(overwrite || !ifPartitionNotExists,
    "IF NOT EXISTS is only valid in INSERT OVERWRITE")
  require(partitionSpec.values.forall(_.nonEmpty) || !ifPartitionNotExists,
    "IF NOT EXISTS is only valid with static partitions")

  override def children: Seq[LogicalPlan] = query :: Nil
}

/**
 * 显示表信息
 */
case class ShowTablesStatement(namespace: Option[Seq[String]], pattern: Option[String])
  extends ParsedStatement

/**
  显示表信息
 */
case class ShowTableStatement(
    namespace: Option[Seq[String]],
    pattern: String,
    partitionSpec: Option[TablePartitionSpec])
  extends ParsedStatement

/**
  创建命名空间信息 CREATE NAMESPACE
 */
case class CreateNamespaceStatement(
    namespace: Seq[String],
    ifNotExists: Boolean,
    properties: Map[String, String]) extends ParsedStatement

/**
  删除命名空间信息 DROP NAMESPACE
 */
case class DropNamespaceStatement(
    namespace: Seq[String],
    ifExists: Boolean,
    cascade: Boolean) extends ParsedStatement

/**
  修改表的信息ALTER (DATABASE|SCHEMA|NAMESPACE) ... SET (DBPROPERTIES|PROPERTIES)
 */
case class AlterNamespaceSetPropertiesStatement(
    namespace: Seq[String],
    properties: Map[String, String]) extends ParsedStatement

/**
  修改元数据的位置ALTER (DATABASE|SCHEMA|NAMESPACE) ... SET LOCATION
 */
case class AlterNamespaceSetLocationStatement(
    namespace: Seq[String],
    location: String) extends ParsedStatement

/**
  显示命名空间的描述，由SQL转换过来
 */
case class ShowNamespacesStatement(namespace: Option[Seq[String]], pattern: Option[String])
  extends ParsedStatement

/**
  use表达式
 */
case class UseStatement(isNamespaceSet: Boolean, nameParts: Seq[String]) extends ParsedStatement

/**
  analyze table表达式,由SQL转化而来
 */
case class AnalyzeTableStatement(
    tableName: Seq[String],
    partitionSpec: Map[String, Option[String]],
    noScan: Boolean) extends ParsedStatement

/**
  ANALYZE TABLE FOR COLUMNS表达式,由SQL转换而来
 */
case class AnalyzeColumnStatement(
    tableName: Seq[String],
    columnNames: Option[Seq[String]],
    allColumns: Boolean) extends ParsedStatement {
  require(columnNames.isDefined ^ allColumns, "Parameter `columnNames` or `allColumns` are " +
    "mutually exclusive. Only one of them should be specified.")
}

/**
  REPAIR TABLE表达式,由SQL转换而来
 */
case class RepairTableStatement(tableName: Seq[String]) extends ParsedStatement

/**
  LOAD DATA INTO TABLE
 */
case class LoadDataStatement(
    tableName: Seq[String],
    path: String,
    isLocal: Boolean,
    isOverwrite: Boolean,
    partition: Option[TablePartitionSpec]) extends ParsedStatement

/**
  SHOW CREATE TABLE表达式
 */
case class ShowCreateTableStatement(tableName: Seq[String]) extends ParsedStatement

/**
  CACHE TABLE 表达式
 */
case class CacheTableStatement(
    tableName: Seq[String],
    plan: Option[LogicalPlan],
    isLazy: Boolean,
    options: Map[String, String]) extends ParsedStatement

/**
  UNCACHE TABLE表达式
 */
case class UncacheTableStatement(
    tableName: Seq[String],
    ifExists: Boolean) extends ParsedStatement

/**
  TRUNCATE TABLE表达式
 */
case class TruncateTableStatement(
    tableName: Seq[String],
    partitionSpec: Option[TablePartitionSpec]) extends ParsedStatement

/**
  SHOW PARTITIONS表达式
 */
case class ShowPartitionsStatement(
    tableName: Seq[String],
    partitionSpec: Option[TablePartitionSpec]) extends ParsedStatement

/**
REFRESH TABLE 刷新表
 */
case class RefreshTableStatement(tableName: Seq[String]) extends ParsedStatement

/**
  显示列信息 SHOW COLUMNS
 */
case class ShowColumnsStatement(
    table: Seq[String],
    namespace: Option[Seq[String]]) extends ParsedStatement

/**
SHOW CURRENT NAMESPACE 显示当前的命名空间
 */
case class ShowCurrentNamespaceStatement() extends ParsedStatement

/**
  显示表属性 SHOW TBLPROPERTIES
 */
case class ShowTablePropertiesStatement(
    tableName: Seq[String],
    propertyKey: Option[String]) extends ParsedStatement

/**
DESCRIBE FUNCTION 表达式
 */
case class DescribeFunctionStatement(
    functionName: Seq[String],
    isExtended: Boolean) extends ParsedStatement

/**
  SHOW FUNCTIONS 表达式
 */
case class ShowFunctionsStatement(
    userScope: Boolean,
    systemScope: Boolean,
    pattern: Option[String],
    functionName: Option[Seq[String]]) extends ParsedStatement

/**
  DROP FUNCTION 表达式
 */
case class DropFunctionStatement(
    functionName: Seq[String],
    ifExists: Boolean,
    isTemp: Boolean) extends ParsedStatement
