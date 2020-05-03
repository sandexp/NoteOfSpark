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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.internal.SQLConf


/** 线程安全的目录插件@CatalogPlugin 管理器。可以定位所有注册的日志，允许调用者按照名称查找日志。
 * 但是仍旧有许多的指令(例如,ANALYZE TABLE)不支持v2版本的目录操作API.它们会忽略当前的目录,且去绑定v1版本的会话目录@SessionCatalog.
 * 为了避免同时在@SessionCatalog和@CatalogManger 中进行定位.在当前目录是会话目录的时候,使用目录管理器@CatalogManager 对当前会话
  * 目录@SessionCatalog的数据库进行读写.
  * TODO 所有1的命令都需要从当前目录中查找.@SessionCatalog 不需要定位当前的数据库
  * 构造器参数:
  *   conf SQL配置信息
  *   defaultSessionCatalog 默认会话日志插件(v2版本)
  *   v1SessionCatalog v1版本的会话目录
  * 参数列表:
  *   catalogs 目录注册表(提供目录名称--> 目录插件的映射)
  *   _currentNamespace 当前命名空间列表
  *   _currentCatalogName 当前目录名称
 */

private[sql]
class CatalogManager(
    conf: SQLConf,
    defaultSessionCatalog: CatalogPlugin,
    val v1SessionCatalog: SessionCatalog) extends Logging {
  import CatalogManager.SESSION_CATALOG_NAME
  
  private val catalogs = mutable.HashMap.empty[String, CatalogPlugin]
  
   /**
    查询指定目录名称@name的日志插件信息@CatalogPlugin,使用缓存机制,如果@catalogs中没有,则会通过sql配置加载
   */
  def catalog(name: String): CatalogPlugin = synchronized {
    if (name.equalsIgnoreCase(SESSION_CATALOG_NAME)) {
      v2SessionCatalog
    } else {
      catalogs.getOrElseUpdate(name, Catalogs.load(name, conf))
    }
  }

  /*
    加载v2版本的会话目录信息@CatalogPlugin.如果是目录扩展内容,则需要对其进行授权
   */
  private def loadV2SessionCatalog(): CatalogPlugin = {
    Catalogs.load(SESSION_CATALOG_NAME, conf) match {
      case extension: CatalogExtension =>
        extension.setDelegateCatalog(defaultSessionCatalog)
        extension
      case other => other
    }
  }

  /**
    * 如果指定了@V2_SESSION_CATALOG 配置,尝试去实例化用户指定的v2版本的会话目录.否则返回默认的会话目录即可
   *  这个目录是v2版本的目录,即@CatalogPlugin,这个可以授权给v1版本的会话目录.当目录需要标识的时候,但是又需要v2版本目录API时候进行
    *  授权使用.当资源实现继承了v2类型的@TableProvider API的时候,且没有列举在可靠的配置上(spark.sql.sources.write.useV1SourceList)
    *  的时候使用
   */
  private[sql] def v2SessionCatalog: CatalogPlugin = {
    // 如果查找到v2版本会话目录的配置实现,将其转换为v2版本的@CatalogPlugin,如果缓存中不存在,则需要进行手动加载
    conf.getConf(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION).map { customV2SessionCatalog =>
      try {
        catalogs.getOrElseUpdate(SESSION_CATALOG_NAME, loadV2SessionCatalog())
      } catch {
        case NonFatal(_) =>
          logError(
            "Fail to instantiate the custom v2 session catalog: " + customV2SessionCatalog)
          defaultSessionCatalog
      }
    }.getOrElse(defaultSessionCatalog)
  }

  // 获取默认目录插件@CatalogPlugin 的默认命名空间
  private def getDefaultNamespace(c: CatalogPlugin) = c match {
    case c: SupportsNamespaces => c.defaultNamespace()
    case _ => Array.empty[String]
  }
  
  private var _currentNamespace: Option[Array[String]] = None
  
  // 获取当前的命名空间
  def currentNamespace: Array[String] = synchronized {
    _currentNamespace.getOrElse {
      if (currentCatalog.name() == SESSION_CATALOG_NAME) {
        Array(v1SessionCatalog.getCurrentDatabase)
      } else {
        getDefaultNamespace(currentCatalog)
      }
    }
  }

  // 设置当前的命名空间
  def setCurrentNamespace(namespace: Array[String]): Unit = synchronized {
    if (currentCatalog.name() == SESSION_CATALOG_NAME) {
      if (namespace.length != 1) {
        throw new NoSuchNamespaceException(namespace)
      }
      v1SessionCatalog.setCurrentDatabase(namespace.head)
    } else {
      _currentNamespace = Some(namespace)
    }
  }

  private var _currentCatalogName: Option[String] = None

  // 获取当前v2版本下的目录@CatalogPlugin
  def currentCatalog: CatalogPlugin = synchronized {
    catalog(_currentCatalogName.getOrElse(conf.getConf(SQLConf.DEFAULT_CATALOG)))
  }

  // 设置当前的目录为指定@catalogName
  def setCurrentCatalog(catalogName: String): Unit = synchronized {
    // `setCurrentCatalog` is noop if it doesn't switch to a different catalog.
    if (currentCatalog.name() != catalogName) {
      _currentCatalogName = Some(catalogName)
      _currentNamespace = None
      // Reset the current database of v1 `SessionCatalog` when switching current catalog, so that
      // when we switch back to session catalog, the current namespace definitely is ["default"].
      v1SessionCatalog.setCurrentDatabase(SessionCatalog.DEFAULT_DATABASE)
    }
  }

  // 清空所有注册的目录,仅仅在测试的时候使用
  private[sql] def reset(): Unit = synchronized {
    catalogs.clear()
    _currentNamespace = None
    _currentCatalogName = None
    v1SessionCatalog.setCurrentDatabase(SessionCatalog.DEFAULT_DATABASE)
  }
}

private[sql] object CatalogManager {
  // 会话目录名称
  val SESSION_CATALOG_NAME: String = "spark_catalog"
}
