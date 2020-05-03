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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
  * 用于封装目录查找函数和相关的提取函数的特征
 */
private[sql] trait LookupCatalog extends Logging {

  protected val catalogManager: CatalogManager

  /**
    * 获取当前的目录集合
   */
  def currentCatalog: CatalogPlugin = catalogManager.currentCatalog

  /**
    *对标识符和目录的抓取器
   * 如果标识符中没有设置目录的话,这个不会代替默认的目录
   */
  private object CatalogAndMultipartIdentifier {
    //抓取目录和标识符信息
    def unapply(parts: Seq[String]): Some[(Option[CatalogPlugin], Seq[String])] = parts match {
      case Seq(_) =>
        Some((None, parts))
      case Seq(catalogName, tail @ _*) =>
        try {
          Some((Some(catalogManager.catalog(catalogName)), tail))
        } catch {
          case _: CatalogNotFoundException =>
            Some((None, parts))
        }
    }
  }

  /**
    * 从多个部分的标识符中抓取会话目录和标识符
   */
  object SessionCatalogAndIdentifier {
    def unapply(parts: Seq[String]): Option[(CatalogPlugin, Identifier)] = parts match {
      case CatalogAndIdentifier(catalog, ident) if CatalogV2Util.isSessionCatalog(catalog) =>
        Some(catalog, ident)
      case _ => None
    }
  }

  /**
    * 从多段标识符中抓取非会话目录和标识符
   */
  object NonSessionCatalogAndIdentifier {
    def unapply(parts: Seq[String]): Option[(CatalogPlugin, Identifier)] = parts match {
      case CatalogAndIdentifier(catalog, ident) if !CatalogV2Util.isSessionCatalog(catalog) =>
        Some(catalog, ident)
      case _ => None
    }
  }

  /**
    * 从带有当前目录的多段名称中提取目录和命名空间信息。目录会优先于命名空间
   */
  object CatalogAndNamespace {
    def unapply(nameParts: Seq[String]): Some[(CatalogPlugin, Seq[String])] = {
      assert(nameParts.nonEmpty)
      try {
        Some((catalogManager.catalog(nameParts.head), nameParts.tail))
      } catch {
        case _: CatalogNotFoundException =>
          Some((currentCatalog, nameParts))
      }
    }
  }

  /**
    * 从带有当前目录的多段名称中，提取目录和标识符信息。目录优先于标识符，但是对于单个部分的名称，标识符优先于目录名称
   */
  object CatalogAndIdentifier {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

    private val globalTempDB = SQLConf.get.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)

    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Identifier)] = {
      assert(nameParts.nonEmpty)
      if (nameParts.length == 1) {
        Some((currentCatalog, Identifier.of(Array(), nameParts.head)))
      } else if (nameParts.head.equalsIgnoreCase(globalTempDB)) {
        // Conceptually global temp views are in a special reserved catalog. However, the v2 catalog
        // API does not support view yet, and we have to use v1 commands to deal with global temp
        // views. To simplify the implementation, we put global temp views in a special namespace
        // in the session catalog. The special namespace has higher priority during name resolution.
        // For example, if the name of a custom catalog is the same with `GLOBAL_TEMP_DATABASE`,
        // this custom catalog can't be accessed.
        Some((catalogManager.v2SessionCatalog, nameParts.asIdentifier))
      } else {
        try {
          Some((catalogManager.catalog(nameParts.head), nameParts.tail.asIdentifier))
        } catch {
          case _: CatalogNotFoundException =>
            Some((currentCatalog, nameParts.asIdentifier))
        }
      }
    }
  }

  /**
    * 从多段标识符中抓取合法的表标识符
   *
   * For legacy support only. Please use [[CatalogAndIdentifier]] instead on DSv2 code paths.
   */
  object AsTableIdentifier {
    def unapply(parts: Seq[String]): Option[TableIdentifier] = parts match {
      case CatalogAndMultipartIdentifier(None, names)
          if CatalogV2Util.isSessionCatalog(currentCatalog) =>
        names match {
          case Seq(name) =>
            Some(TableIdentifier(name))
          case Seq(database, name) =>
            Some(TableIdentifier(name, Some(database)))
          case _ =>
            None
        }
      case _ =>
        None
    }
  }

  /**
    * 对于临时视图来说，在没有目录的情况下，从多段标识符中抓取表标识符
   */
  object AsTemporaryViewIdentifier {
    def unapply(parts: Seq[String]): Option[TableIdentifier] = parts match {
      case CatalogAndMultipartIdentifier(None, Seq(table)) =>
        Some(TableIdentifier(table))
      case CatalogAndMultipartIdentifier(None, Seq(database, table)) =>
        Some(TableIdentifier(table, Some(database)))
      case _ =>
        None
    }
  }
}
