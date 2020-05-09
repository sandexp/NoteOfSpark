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

package org.apache.spark.sql.catalog

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

/**
 * spark的数据目录接口.可以使用SparkSession.catalog获取
 */
@Stable
abstract class Catalog {

  /**
   * 返回当前会话中的默认数据库
   */
  def currentDatabase: String

  /**
   * 设置当前会话中的默认数据库为指定@dbName
   */
  def setCurrentDatabase(dbName: String): Unit

  /**
   * 返回所有会话中的数据库列表
   */
  def listDatabases(): Dataset[Database]

  /**
   * 返回当前数据库中表/视图的列表.这个包含了临时的视图
   */
  def listTables(): Dataset[Table]

  /**
   * 获取指定数据库中的表/视图列表,包含所有的临时视图
   */
  @throws[AnalysisException]("database does not exist")
  def listTables(dbName: String): Dataset[Table]

  /**
   * 返回当前数据库中的函数名称,包含所有的临时函数
   */
  def listFunctions(): Dataset[Function]

  /**
   * 返回当前数据库中注册的函数列表,包括所有的临时函数
   */
  @throws[AnalysisException]("database does not exist")
  def listFunctions(dbName: String): Dataset[Function]

  /**
   * 返回给定表/视图/临时视图的列的列表信息
   */
  @throws[AnalysisException]("table does not exist")
  def listColumns(tableName: String): Dataset[Column]

  /**
   * 返回给定数据块的给定表/视图的列的列表信息
   */
  @throws[AnalysisException]("database or table does not exist")
  def listColumns(dbName: String, tableName: String): Dataset[Column]

  /**
   * 获取给定名称的数据块,如果找不到会抛出异常
   */
  @throws[AnalysisException]("database does not exist")
  def getDatabase(dbName: String): Database

  /**
   * 获取指定名称的表/视图,这个表可以是临时的视图或者是表/视图.如果找不到会抛出异常
   */
  @throws[AnalysisException]("table does not exist")
  def getTable(tableName: String): Table

  /**
   * 使用指定的数据库名称和表名称获取表/视图.如果表找不到会抛出异常.
   */
  @throws[AnalysisException]("database or table does not exist")
  def getTable(dbName: String, tableName: String): Table

  /**
   * 获取给定名称的函数.如果函数可以是临时函数,找不到的时候会抛出异常
   */
  @throws[AnalysisException]("function does not exist")
  def getFunction(functionName: String): Function

  /**
   * 获取指定名称的函数.当函数找不到的时候会抛出异常
   */
  @throws[AnalysisException]("database or function does not exist")
  def getFunction(dbName: String, functionName: String): Function

  /**
   * 确定指定数据库是否存在
   */
  def databaseExists(dbName: String): Boolean

  /**
   * 检查指定的表是否存在,可以是临时的视图或者是表/视图
   */
  def tableExists(tableName: String): Boolean

  /**
   * 检查指定数据库中的指定表是否存在
   */
  def tableExists(dbName: String, tableName: String): Boolean

  /**
   * 检查指定函数是否存在,可以是临时的函数
   */
  def functionExists(functionName: String): Boolean

  /**
   * 检查指定数据库下的指定函数是否存在
   */
  def functionExists(dbName: String, functionName: String): Boolean

  /**
   * 由给定的路径创建相应的@DataFrame.使用@spark.sql.sources.default 的默认数据源
   */
  def createTable(tableName: String, path: String): DataFrame

  /**
   * 由给定的路径和给定的数据源创建DF
   */
  def createTable(tableName: String, path: String, source: String): DataFrame

  /**
   * 基于给定的数据集创建DF
   */
  def createTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    createTable(tableName, source, options.asScala.toMap)
  }

  /**
   * (Scala-specific)
   * 基于数据源中的数据集以及相关参数创建DF
   */
  def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame

  /**
   * 同上,这里可以指定数据表的schema结构
   */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    createTable(tableName, source, schema, options.asScala.toMap)
  }

  /**
  同上,这里可以指定数据表的schema结构
   */
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame

  /**
    删除本地临时视图,如果视图之前被缓存了,会被解除缓存.本地临时视图是会话级别的.上面周期是会话的生命周期.
    当会话结束的时候会自动结束.不会连接到任何一个数据库上.注意到在spark 2.0版本这个方法返回类型为Unit,到2.1版本为boolean类型
    如果删除成功返回true
   */
  def dropTempView(viewName: String): Boolean

  /**
    删除全局的临时视图.如果视图之前被缓存,则需要解除缓存
    全局临时视图是跨会话的.什么周期是spark的应用.当应用停止的时候会自动停止.与全局临时表存在有连续.对于全局临时视图必须使用全限定名
    例如`SELECT * FROM global_temp.view1`.
   */
  def dropGlobalTempView(viewName: String): Boolean

  /**
   恢复表目录中的所有分区,仅仅在分区表中工作,不能是视图
   */
  def recoverPartitions(tableName: String): Unit

  /**
    如果当前表缓存在内存中则返回true
   */
  def isCached(tableName: String): Boolean

  /**
    将指定的表缓存在内存中
   */
  def cacheTable(tableName: String): Unit

  /**
   使用指定的存储等级缓存指定的表
   */
  def cacheTable(tableName: String, storageLevel: StorageLevel): Unit


  /**
   将指定的表从内存缓存中移除
   */
  def uncacheTable(tableName: String): Unit

  /**
   移除内存缓存中的所有缓存表
   */
  def clearCache(): Unit

  /**
   刷新给定表的缓存数据和元数据信息.由于性能信息,sparkSQL或者外部数据源可以缓存表的元数据(例如数据块的位置).用于可以调用这些函数
    去取消缓存.
   */
  def refreshTable(tableName: String): Unit

  /**
   刷新所有缓存数据集@DataSet中包含给定路径的数据.
   */
  def refreshByPath(path: String): Unit
}
