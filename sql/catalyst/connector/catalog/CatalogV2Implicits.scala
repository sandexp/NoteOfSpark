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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.connector.expressions.{BucketTransform, IdentityTransform, LogicalExpressions, Transform}
import org.apache.spark.sql.types.StructType

/**
* 使用v2版本的目录插件@CatalogPlugin 的辅助转换类
 */
private[sql] object CatalogV2Implicits {
  import LogicalExpressions._

  /*
  分区类型辅助器
  构造器参数:
    partitionType: StructType 分区类型
   */
  implicit class PartitionTypeHelper(partitionType: StructType) {
    // 将分区类型转换为转换函数类型@Transform
    def asTransforms: Array[Transform] = {
      partitionType.names.map(col => identity(reference(Seq(col)))).toArray
    }
  }
  
  /*
  桶辅助器
  构造器参数:
    spec: BucketSpec  桶指定类型
   */
  implicit class BucketSpecHelper(spec: BucketSpec) {
    
    // 将桶转换为桶转换函数类型@BucketTransform
    def asTransform: BucketTransform = {
      // 注意,如果桶带有了参与排序的列,那么不能对其进行转换
      if (spec.sortColumnNames.nonEmpty) {
        throw new AnalysisException(
          s"Cannot convert bucketing with sort columns to a transform: $spec")
      }
      // 将桶转换为桶转换函数
      val references = spec.bucketColumnNames.map(col => reference(Seq(col)))
      bucket(spec.numBuckets, references.toArray)
    }
  }
  
  /*
    转换辅助器
    构造器参数:
      transforms 转换器列表
   */
  implicit class TransformHelper(transforms: Seq[Transform]) {
    
    // 转换为分区列
    def asPartitionColumns: Seq[String] = {
      // 1. 将转换器分割成有id转换器和无id转换器
      val (idTransforms, nonIdTransforms) = transforms.partition(_.isInstanceOf[IdentityTransform])
      
      // 2. 如果存在有无id转换器,那么这次转换必定会失败,并抛出异常@AnalysisException
      if (nonIdTransforms.nonEmpty) {
        throw new AnalysisException("Transforms cannot be converted to partition columns: " +
            nonIdTransforms.map(_.describe).mkString(", "))
      }

      // 3. 将有id转换器映射为单列单转换器,如果属性数量大于1,则转换也会失败
      idTransforms.map(_.asInstanceOf[IdentityTransform]).map(_.reference).map { ref =>
        val parts = ref.fieldNames
        if (parts.size > 1) {
          throw new AnalysisException(s"Cannot partition by nested column: $ref")
        } else {
          parts(0)
        }
      }
    }
  }

  /*
  目录辅助器
  构造器参数:
    plugin: CatalogPlugin 目录插件
   */
  implicit class CatalogHelper(plugin: CatalogPlugin) {
    
    // 将目录转换为表目录
    def asTableCatalog: TableCatalog = plugin match {
      case tableCatalog: TableCatalog =>
        tableCatalog
      case _ =>
        throw new AnalysisException(s"Cannot use catalog ${plugin.name}: not a TableCatalog")
    }

    // 转换为带有命名空间的目录
    def asNamespaceCatalog: SupportsNamespaces = plugin match {
      case namespaceCatalog: SupportsNamespaces =>
        namespaceCatalog
      case _ =>
        throw new AnalysisException(
          s"Cannot use catalog ${plugin.name}: does not support namespaces")
    }
  }

  /*
  命名空间辅助器
  构造器参数:
    namespace: Array[String]  命名空间列表
    
   */
  implicit class NamespaceHelper(namespace: Array[String]) {
    // 表示命名空间信息(合并)
    def quoted: String = namespace.map(quote).mkString(".")
  }

  /*
  标识辅助器
    ident: Identifier 标识器
   */
  implicit class IdentifierHelper(ident: Identifier) {
    // 命名空间表示
    def quoted: String = {
      if (ident.namespace.nonEmpty) {
        ident.namespace.map(quote).mkString(".") + "." + quote(ident.name)
      } else {
        quote(ident.name)
      }
    }

    // 将标识符@ident 转换为 [命名空间]:[名称]的形式
    def asMultipartIdentifier: Seq[String] = ident.namespace :+ ident.name
  }

  /*
  多部分标识符辅助器
  构造器参数:
    parts: Seq[String] 标识符列表
   */
  implicit class MultipartIdentifierHelper(parts: Seq[String]) {
    if (parts.isEmpty) {
      throw new AnalysisException("multi-part identifier cannot be empty.")
    }

    // 将标识符列表转换为标识符
    def asIdentifier: Identifier = Identifier.of(parts.init.toArray, parts.last)

    // 将标识符列表转化为表标识符
    def asTableIdentifier: TableIdentifier = parts match {
      case Seq(tblName) => TableIdentifier(tblName)
      case Seq(dbName, tblName) => TableIdentifier(tblName, Some(dbName))
      case _ =>
        throw new AnalysisException(
          s"$quoted is not a valid TableIdentifier as it has more than 2 name parts.")
    }

    // 标识符信息显示
    def quoted: String = parts.map(quote).mkString(".")
  }

  // 目录信息显示
  private def quote(part: String): String = {
    if (part.contains(".") || part.contains("`")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }
}
