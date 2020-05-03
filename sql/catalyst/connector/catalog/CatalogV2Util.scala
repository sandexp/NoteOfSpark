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

package org.apache.spark.sql.connector.catalog

import java.util
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, UnresolvedV2Relation}
import org.apache.spark.sql.catalyst.plans.logical.AlterTable
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

/*
v2版本的目录工具类
 */
private[sql] object CatalogV2Util {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  /**
    *对一个map使用属性改变，返回其结果
   */
  def applyNamespaceChanges(
      properties: Map[String, String],
      changes: Seq[NamespaceChange]): Map[String, String] = {
    applyNamespaceChanges(properties.asJava, changes).asScala.toMap
  }

  /**
    * 对一个java的map进行属性的改变，且返回其结果
   */
  def applyNamespaceChanges(
      properties: util.Map[String, String],
      changes: Seq[NamespaceChange]): util.Map[String, String] = {
    // 1. 有指定的@properties 复制一个新的map
    val newProperties = new util.HashMap[String, String](properties)
    
    // 2. 对复制出的map进行属性的增删
    changes.foreach {
      case set: NamespaceChange.SetProperty =>
        newProperties.put(set.property, set.value)

      case unset: NamespaceChange.RemoveProperty =>
        newProperties.remove(unset.property)

      case _ =>
      // ignore non-property changes
    }
    // 3. 返回新的map,作为属性表
    Collections.unmodifiableMap(newProperties)
  }

  /**
    * 对于map应用表变化,并返回改变之后的值
   */
  def applyPropertiesChanges(
      properties: Map[String, String],
      changes: Seq[TableChange]): Map[String, String] = {
    applyPropertiesChanges(properties.asJava, changes).asScala.toMap
  }

  /**
   * Apply properties changes to a Java map and return the result.
   */
  def applyPropertiesChanges(
      properties: util.Map[String, String],
      changes: Seq[TableChange]): util.Map[String, String] = {
    val newProperties = new util.HashMap[String, String](properties)

    changes.foreach {
      case set: SetProperty =>
        newProperties.put(set.property, set.value)

      case unset: RemoveProperty =>
        newProperties.remove(unset.property)

      case _ =>
      // ignore non-property changes
    }

    Collections.unmodifiableMap(newProperties)
  }

  /**
    * 对schema进行表变化@changes,返回变换后的schema
   * Apply schema changes to a schema and return the result.
   */
  def applySchemaChanges(schema: StructType, changes: Seq[TableChange]): StructType = {
    // 1. 变化@changes迭代地对schema进行处理,常用的操作包括,添加列,重命名列,更新列类型,更新列内容,删除列的操作
    changes.foldLeft(schema) { (schema, change) =>
      change match {
        case add: AddColumn =>
          add.fieldNames match {
            case Array(name) =>
              val newField = StructField(name, add.dataType, nullable = add.isNullable)
              Option(add.comment) match {
                case Some(comment) =>
                  schema.add(newField.withComment(comment))
                case _ =>
                  schema.add(newField)
              }

            case names =>
              replace(schema, names.init, parent => parent.dataType match {
                case parentType: StructType =>
                  val field = StructField(names.last, add.dataType, nullable = add.isNullable)
                  val newParentType = Option(add.comment) match {
                    case Some(comment) =>
                      parentType.add(field.withComment(comment))
                    case None =>
                      parentType.add(field)
                  }

                  Some(StructField(parent.name, newParentType, parent.nullable, parent.metadata))

                case _ =>
                  throw new IllegalArgumentException(s"Not a struct: ${names.init.last}")
              })
          }

        case rename: RenameColumn =>
          replace(schema, rename.fieldNames, field =>
            Some(StructField(rename.newName, field.dataType, field.nullable, field.metadata)))

        case update: UpdateColumnType =>
          replace(schema, update.fieldNames, field => {
            if (!update.isNullable && field.nullable) {
              throw new IllegalArgumentException(
                s"Cannot change optional column to required: $field.name")
            }
            Some(StructField(field.name, update.newDataType, update.isNullable, field.metadata))
          })

        case update: UpdateColumnComment =>
          replace(schema, update.fieldNames, field =>
            Some(field.withComment(update.newComment)))

        case delete: DeleteColumn =>
          replace(schema, delete.fieldNames, _ => None)

        case _ =>
          // ignore non-schema changes
          schema
      }
    }
  }

  // 使用更新函数,对指定数据名称进行替换
  private def replace(
      struct: StructType,
      fieldNames: Seq[String],
      update: StructField => Option[StructField]): StructType = {
    // 1. 获取属性列表@fieldNames 表头在结构体的位置指针
    val pos = struct.getFieldIndex(fieldNames.head)
        .getOrElse(throw new IllegalArgumentException(s"Cannot find field: ${fieldNames.head}"))
    // 2. 根据指针位置获取结构体的属性值
    val field = struct.fields(pos)
    // 3. 获取更新之后的值
    val replacement: Option[StructField] = (fieldNames.tail, field.dataType) match {
      case (Seq(), _) =>
        update(field)

      case (names, struct: StructType) =>
        val updatedType: StructType = replace(struct, names, update)
        Some(StructField(field.name, updatedType, field.nullable, field.metadata))

      case (Seq("key"), map @ MapType(keyType, _, _)) =>
        val updated = update(StructField("key", keyType, nullable = false))
            .getOrElse(throw new IllegalArgumentException(s"Cannot delete map key"))
        Some(field.copy(dataType = map.copy(keyType = updated.dataType)))

      case (Seq("key", names @ _*), map @ MapType(keyStruct: StructType, _, _)) =>
        Some(field.copy(dataType = map.copy(keyType = replace(keyStruct, names, update))))

      case (Seq("value"), map @ MapType(_, mapValueType, isNullable)) =>
        val updated = update(StructField("value", mapValueType, nullable = isNullable))
            .getOrElse(throw new IllegalArgumentException(s"Cannot delete map value"))
        Some(field.copy(dataType = map.copy(
          valueType = updated.dataType,
          valueContainsNull = updated.nullable)))

      case (Seq("value", names @ _*), map @ MapType(_, valueStruct: StructType, _)) =>
        Some(field.copy(dataType = map.copy(valueType = replace(valueStruct, names, update))))

      case (Seq("element"), array @ ArrayType(elementType, isNullable)) =>
        val updated = update(StructField("element", elementType, nullable = isNullable))
            .getOrElse(throw new IllegalArgumentException(s"Cannot delete array element"))
        Some(field.copy(dataType = array.copy(
          elementType = updated.dataType,
          containsNull = updated.nullable)))

      case (Seq("element", names @ _*), array @ ArrayType(elementStruct: StructType, _)) =>
        Some(field.copy(dataType = array.copy(elementType = replace(elementStruct, names, update))))

      case (names, dataType) =>
        throw new IllegalArgumentException(
          s"Cannot find field: ${names.head} in ${dataType.simpleString}")
    }

    // 4. 对处于更新位置的属性进行更新,并返回新的属性列表
    val newFields = struct.fields.zipWithIndex.flatMap {
      case (_, index) if pos == index =>
        replacement
      case (other, _) =>
        Some(other)
    }
    // 5. 返回新的结构体类型
    new StructType(newFields)
  }

  // 根据指定的目录@catalog和标识符@ident 加载表数据
  def loadTable(catalog: CatalogPlugin, ident: Identifier): Option[Table] =
    try {
      // 先将目录转换为表目录,在通过表目录转化成表
      Option(catalog.asTableCatalog.loadTable(ident))
    } catch {
      case _: NoSuchTableException => None
      case _: NoSuchDatabaseException => None
      case _: NoSuchNamespaceException => None
    }

  // 根据指定的目录@catalog 和标识符@ident 加载关系
  def loadRelation(catalog: CatalogPlugin, ident: Identifier): Option[NamedRelation] = {
    loadTable(catalog, ident).map(DataSourceV2Relation.create)
  }

  // 确定指定目录@cataloglog是否为会话目录
  def isSessionCatalog(catalog: CatalogPlugin): Boolean = {
    catalog.name().equalsIgnoreCase(CatalogManager.SESSION_CATALOG_NAME)
  }

  /*
  转换表属性
  输入参数:
    properties: Map[String, String] 属性表
    options: Map[String, String]  参数表
    location: Option[String] 所属位置表
    comment: Option[String] 注释信息表
    provider: String 所提供的信息
   */
  def convertTableProperties(
      properties: Map[String, String],
      options: Map[String, String],
      location: Option[String],
      comment: Option[String],
      provider: String): Map[String, String] = {
    // 1. 未提供必要的位置相关信息,抛出异常,只要配置了path属性,则不需要设置执行位置
    if (options.contains("path") && location.isDefined) {
      throw new AnalysisException(
        "LOCATION and 'path' in OPTIONS are both used to indicate the custom table path, " +
          "you can only specify one of them.")
    }
    
    // 2. 未提供相关的注释信息,抛出异常
    if ((options.contains(TableCatalog.PROP_COMMENT)
      || properties.contains(TableCatalog.PROP_COMMENT)) && comment.isDefined) {
      throw new AnalysisException(
        s"COMMENT and option/property '${TableCatalog.PROP_COMMENT}' " +
          s"are both used to set the table comment, you can only specify one of them.")
    }

    // 3. 未提供相关的必要信息,抛出异常
    if (options.contains(TableCatalog.PROP_PROVIDER)
      || properties.contains(TableCatalog.PROP_PROVIDER)) {
      throw new AnalysisException(
        "USING and option/property 'provider' are both used to set the provider implementation, " +
          "you can only specify one of them.")
    }

    // 4. 获取非路径信息
    val filteredOptions = options.filterKeys(_ != "path")
    
    // 5. 创建一个表,用于存放TBLPROPERTIES和OPTIONS信息(除了位置信息)
    val tableProperties = new mutable.HashMap[String, String]()
    tableProperties ++= properties
    tableProperties ++= filteredOptions
    
    // 6. 将USING,LOCATION,COMMENT的子句信息转换到表属性中
    tableProperties += (TableCatalog.PROP_PROVIDER -> provider)
    comment.map(text => tableProperties += (TableCatalog.PROP_COMMENT -> text))
    location.orElse(options.get("path")).map(
      loc => tableProperties += (TableCatalog.PROP_LOCATION -> loc))

    tableProperties.toMap
  }

  /*
  创建修改表
  输入参数:
    originalNameParts: Seq[String]  原始命名部分
    catalog: CatalogPlugin  目录插件
    tableName: Seq[String]  表名称
    changes: Seq[TableChange] 表变化
   返回参数
      AlterTable  v2类型表,修改表的逻辑执行计划
   */
  def createAlterTable(
      originalNameParts: Seq[String],
      catalog: CatalogPlugin,
      tableName: Seq[String],
      changes: Seq[TableChange]): AlterTable = {
    // 将没有了转换为表目录
    val tableCatalog = catalog.asTableCatalog
    // 将表名称转换为标识符
    val ident = tableName.asIdentifier
    // 处理v2表的关系
    val unresolved = UnresolvedV2Relation(originalNameParts, tableCatalog, ident)
    // 生成修改表的逻辑执行计划
    AlterTable(tableCatalog, ident, unresolved, changes)
  }
}
