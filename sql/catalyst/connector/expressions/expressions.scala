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

package org.apache.spark.sql.connector.expressions

import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

/** 逻辑表达式
  * 用逻辑表达书的辅助方法类
 */
private[sql] object LogicalExpressions {
  // 通用转换器,仅仅用于转换多个属性名称,由于仅仅用于属性名称上,所以传输sql conf不会产生影响
  private lazy val parser = new CatalystSqlParser(SQLConf.get)

  // 获取指定@value的字面值
  def literal[T](value: T): LiteralValue[T] = {
    val internalLit = catalyst.expressions.Literal(value)
    literal(value, internalLit.dataType)
  }

  def literal[T](value: T, dataType: DataType): LiteralValue[T] = LiteralValue(value, dataType)

  // 将指定名称@name 转换为命名的引用@NamedReference
  def parseReference(name: String): NamedReference =
    FieldReference(parser.parseMultipartIdentifier(name))

  // 获取多段名称的命名引用@NamedReference
  def reference(nameParts: Seq[String]): NamedReference = FieldReference(nameParts)
  
  // 对指定名称和表达式应用转换方法
  def apply(name: String, arguments: Expression*): Transform = ApplyTransform(name, arguments)

  // 获取指定引用列表@references 的桶转换
  def bucket(numBuckets: Int, references: Array[NamedReference]): BucketTransform =
    BucketTransform(literal(numBuckets, IntegerType), references)

  // 获取指定命名引用的标识转换
  def identity(reference: NamedReference): IdentityTransform = IdentityTransform(reference)

  // 获取时间类转换
  def years(reference: NamedReference): YearsTransform = YearsTransform(reference)

  def months(reference: NamedReference): MonthsTransform = MonthsTransform(reference)

  def days(reference: NamedReference): DaysTransform = DaysTransform(reference)

  def hours(reference: NamedReference): HoursTransform = HoursTransform(reference)
}

/**
  * 可重写转换特性
  *
  * 允许spark在sql分析期间,重写给定的转换引用
 */
sealed trait RewritableTransform extends Transform {
  // 使用新的分析引用创建一个这个转换的副本
  def withReferences(newReferences: Seq[NamedReference]): Transform
}

/**
  * 单列单个转换的基础类
  * 构造器参数:
  *   ref: NamedReference 命名引用
 */
private[sql] abstract class SingleColumnTransform(ref: NamedReference) extends RewritableTransform {

  // 获取命名引用
  def reference: NamedReference = ref

  // 获取命名引用列表
  override def references: Array[NamedReference] = Array(ref)

  // 获取命名引用中的表达式列表
  override def arguments: Array[Expression] = Array(ref)

  // 获取命名引用的描述
  override def describe: String = name + "(" + reference.describe + ")"

  // 描述信息显示
  override def toString: String = describe

  // 使用新的命名引用构建转换
  protected def withNewRef(ref: NamedReference): Transform

  // 使用多个命名引用构建转换函数
  override def withReferences(newReferences: Seq[NamedReference]): Transform = {
    assert(newReferences.length == 1,
      s"Tried rewriting a single column transform (${this}) with multiple references.")
    withNewRef(newReferences.head)
  }
}

/**
  * 桶转换器
  * @param numBuckets 桶数量
  * @param columns 命名引用列
  */
private[sql] final case class BucketTransform(
    numBuckets: Literal[Int],
    columns: Seq[NamedReference]) extends RewritableTransform {

  /*
  参数列表:
    name: String  名称
   */
  override val name: String = "bucket"

  // 获取命名引用列表
  override def references: Array[NamedReference] = {
    arguments.collect { case named: NamedReference => named }
  }

  // 获取表达式参数
  override def arguments: Array[Expression] = numBuckets +: columns.toArray

  // 获取描述信息
  override def describe: String = s"bucket(${arguments.map(_.describe).mkString(", ")})"

  // 信息显示
  override def toString: String = describe

  // 使用新的引用形成转换
  override def withReferences(newReferences: Seq[NamedReference]): Transform = {
    this.copy(columns = newReferences)
  }
}

private[sql] object BucketTransform {
  def unapply(transform: Transform): Option[(Int, NamedReference)] = transform match {
    case NamedTransform("bucket", Seq(
        Lit(value: Int, IntegerType),
        Ref(seq: Seq[String]))) =>
      Some((value, FieldReference(seq)))
    case _ =>
      None
  }
}

/**
  * 应用转换
  * @param name 应用转换名称
  * @param args 表达式列表
  */
private[sql] final case class ApplyTransform(
    name: String,
    args: Seq[Expression]) extends Transform {

  // 获取表达式列表
  override def arguments: Array[Expression] = args.toArray

  // 获取命名引用列表
  override def references: Array[NamedReference] = {
    arguments.collect { case named: NamedReference => named }
  }

  // 获取描述信息
  override def describe: String = s"$name(${arguments.map(_.describe).mkString(", ")})"

  override def toString: String = describe
}

/**
  * 任意字面量的简便描述
 */
private object Lit {
  def unapply[T](literal: Literal[T]): Some[(T, DataType)] = {
    Some((literal.value, literal.dataType))
  }
}

/**
  * 任务命名引用的简单提取方法
 */
private object Ref {
  def unapply(named: NamedReference): Some[Seq[String]] = {
    Some(named.fieldNames)
  }
}

/**
  * 任意转换器的简便提取方法
 */
private[sql] object NamedTransform {
  def unapply(transform: Transform): Some[(String, Seq[Expression])] = {
    Some((transform.name, transform.arguments))
  }
}

// 标识符提取转换类
private[sql] final case class IdentityTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "identity"
  override def describe: String = ref.describe
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object IdentityTransform {
  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("identity", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}
// 日期类转换类
private[sql] final case class YearsTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "years"
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object YearsTransform {
  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("years", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class MonthsTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "months"
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object MonthsTransform {
  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("months", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class DaysTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "days"
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object DaysTransform {
  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("days", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class HoursTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override val name: String = "hours"
  override protected def withNewRef(ref: NamedReference): Transform = this.copy(ref)
}

private[sql] object HoursTransform {
  def unapply(transform: Transform): Option[FieldReference] = transform match {
    case NamedTransform("hours", Seq(Ref(parts))) =>
      Some(FieldReference(parts))
    case _ =>
      None
  }
}

private[sql] final case class LiteralValue[T](value: T, dataType: DataType) extends Literal[T] {
  override def describe: String = {
    if (dataType.isInstanceOf[StringType]) {
      s"'$value'"
    } else {
      s"$value"
    }
  }
  override def toString: String = describe
}

/* 属性引用
  构造器参数:
    parts: Seq[String] 多段参数列表
 */
private[sql] final case class FieldReference(parts: Seq[String]) extends NamedReference {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
  override def fieldNames: Array[String] = parts.toArray
  override def describe: String = parts.quoted
  override def toString: String = describe
}

private[sql] object FieldReference {
  def apply(column: String): NamedReference = {
    LogicalExpressions.parseReference(column)
  }
}
