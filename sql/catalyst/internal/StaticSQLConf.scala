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

package org.apache.spark.sql.internal

import java.util.Locale

import org.apache.spark.util.Utils


/**
  * SQL统计信息配置,这是一个跨会话的,可变的spark配置.外部用户可以通过@SparkSession.conf 查看统计sql的配置,但是不能够对其修改.
 */
object StaticSQLConf {

  import SQLConf.buildStaticConf
  /*
  参数列表:
    WAREHOUSE_PATH 数据仓库路径(默认数据存储位置)
    CATALOG_IMPLEMENTATION  目录实现(内存/hive)
    GLOBAL_TEMP_DATABASE  全局临时的数据库
    SCHEMA_STRING_LENGTH_THRESHOLD schema长度容量(hive元数据库中存储的最大容量)
    FILESOURCE_TABLE_RELATION_CACHE_SIZE  表名->缓冲区最大的缓冲区大小
    CODEGEN_CACHE_MAX_ENTRIES 缓存最大数量
    DEBUG_MODE debug模式
   */
  val WAREHOUSE_PATH = buildStaticConf("spark.sql.warehouse.dir")
    .doc("The default location for managed databases and tables.")
    .stringConf
    .createWithDefault(Utils.resolveURI("spark-warehouse").toString)

  val CATALOG_IMPLEMENTATION = buildStaticConf("spark.sql.catalogImplementation")
    .internal()
    .stringConf
    .checkValues(Set("hive", "in-memory"))
    .createWithDefault("in-memory")

  val GLOBAL_TEMP_DATABASE = buildStaticConf("spark.sql.globalTempDatabase")
    .internal()
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .createWithDefault("global_temp")

  // This is used to control when we will split a schema's JSON string to multiple pieces
  // in order to fit the JSON string in metastore's table property (by default, the value has
  // a length restriction of 4000 characters, so do not use a value larger than 4000 as the default
  // value of this property). We will split the JSON string of a schema to its length exceeds the
  // threshold. Note that, this conf is only read in HiveExternalCatalog which is cross-session,
  // that's why this conf has to be a static SQL conf.
  val SCHEMA_STRING_LENGTH_THRESHOLD =
    buildStaticConf("spark.sql.sources.schemaStringLengthThreshold")
      .doc("The maximum length allowed in a single cell when " +
        "storing additional schema information in Hive's metastore.")
      .internal()
      .intConf
      .createWithDefault(4000)

  val FILESOURCE_TABLE_RELATION_CACHE_SIZE =
    buildStaticConf("spark.sql.filesourceTableRelationCacheSize")
      .internal()
      .doc("The maximum size of the cache that maps qualified table names to table relation plans.")
      .intConf
      .checkValue(cacheSize => cacheSize >= 0, "The maximum size of the cache must not be negative")
      .createWithDefault(1000)

  val CODEGEN_CACHE_MAX_ENTRIES = buildStaticConf("spark.sql.codegen.cache.maxEntries")
      .internal()
      .doc("When nonzero, enable caching of generated classes for operators and expressions. " +
        "All jobs share the cache that can use up to the specified number for generated classes.")
      .intConf
      .checkValue(maxEntries => maxEntries >= 0, "The maximum must not be negative")
      .createWithDefault(100)

  val CODEGEN_COMMENTS = buildStaticConf("spark.sql.codegen.comments")
    .internal()
    .doc("When true, put comment in the generated code. Since computing huge comments " +
      "can be extremely expensive in certain cases, such as deeply-nested expressions which " +
      "operate over inputs with wide schemas, default is false.")
    .booleanConf
    .createWithDefault(false)

  // When enabling the debug, Spark SQL internal table properties are not filtered out; however,
  // some related DDL commands (e.g., ANALYZE TABLE and CREATE TABLE LIKE) might not work properly.
  val DEBUG_MODE = buildStaticConf("spark.sql.debug")
    .internal()
    .doc("Only used for internal debugging. Not all functions are supported when it is enabled.")
    .booleanConf
    .createWithDefault(false)
  
  /*
  参数列表:
    HIVE_THRIFT_SERVER_SINGLESESSION 设置为true的时候,hive的thrift服务器在单会话模式下运行.JDBC/ODBC共享临时视图,函数注册,
    sql配置的信息.默认为false
    SPARK_SESSION_EXTENSIONS sparkSession的扩展,是一个逗号分割的列表
    QUERY_EXECUTION_LISTENERS 查询执行监听器
    STREAMING_QUERY_LISTENERS 流式查询监听器
    UI_RETAINED_EXECUTIONS sparkUI留存的执行数量
    BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD 广播变量最大容量
    SQL_EVENT_TRUNCATE_LENGTH sql事件删除的长度(超出这个值就会清除这个sql)
    SQL_LEGACY_SESSION_INIT_WITH_DEFAULTS sql合法的初始化大小
    DEFAULT_URL_STREAM_HANDLER_FACTORY_ENABLED 设置为true,则支持添加jar到HDFS上
   */

  val HIVE_THRIFT_SERVER_SINGLESESSION =
    buildStaticConf("spark.sql.hive.thriftServer.singleSession")
      .doc("When set to true, Hive Thrift server is running in a single session mode. " +
        "All the JDBC/ODBC connections share the temporary views, function registries, " +
        "SQL configuration and the current database.")
      .booleanConf
      .createWithDefault(false)

  val SPARK_SESSION_EXTENSIONS = buildStaticConf("spark.sql.extensions")
    .doc("A comma-separated list of classes that implement " +
      "Function1[SparkSessionExtensions, Unit] used to configure Spark Session extensions. The " +
      "classes must have a no-args constructor. If multiple extensions are specified, they are " +
      "applied in the specified order. For the case of rules and planner strategies, they are " +
      "applied in the specified order. For the case of parsers, the last parser is used and each " +
      "parser can delegate to its predecessor. For the case of function name conflicts, the last " +
      "registered function name is used.")
    .stringConf
    .toSequence
    .createOptional

  val QUERY_EXECUTION_LISTENERS = buildStaticConf("spark.sql.queryExecutionListeners")
    .doc("List of class names implementing QueryExecutionListener that will be automatically " +
      "added to newly created sessions. The classes should have either a no-arg constructor, " +
      "or a constructor that expects a SparkConf argument.")
    .stringConf
    .toSequence
    .createOptional

  val STREAMING_QUERY_LISTENERS = buildStaticConf("spark.sql.streaming.streamingQueryListeners")
    .doc("List of class names implementing StreamingQueryListener that will be automatically " +
      "added to newly created sessions. The classes should have either a no-arg constructor, " +
      "or a constructor that expects a SparkConf argument.")
    .stringConf
    .toSequence
    .createOptional

  val UI_RETAINED_EXECUTIONS =
    buildStaticConf("spark.sql.ui.retainedExecutions")
      .doc("Number of executions to retain in the Spark UI.")
      .intConf
      .createWithDefault(1000)

  val BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD =
    buildStaticConf("spark.sql.broadcastExchange.maxThreadThreshold")
      .internal()
      .doc("The maximum degree of parallelism to fetch and broadcast the table. " +
        "If we encounter memory issue like frequently full GC or OOM when broadcast table " +
        "we can decrease this number in order to reduce memory usage. " +
        "Notice the number should be carefully chosen since decreasing parallelism might " +
        "cause longer waiting for other broadcasting. Also, increasing parallelism may " +
        "cause memory problem.")
      .intConf
      .checkValue(thres => thres > 0 && thres <= 128, "The threshold must be in [0,128].")
      .createWithDefault(128)

  val SQL_EVENT_TRUNCATE_LENGTH = buildStaticConf("spark.sql.event.truncate.length")
    .doc("Threshold of SQL length beyond which it will be truncated before adding to " +
      "event. Defaults to no truncation. If set to 0, callsite will be logged instead.")
    .intConf
    .checkValue(_ >= 0, "Must be set greater or equal to zero")
    .createWithDefault(Int.MaxValue)

  val SQL_LEGACY_SESSION_INIT_WITH_DEFAULTS =
    buildStaticConf("spark.sql.legacy.sessionInitWithConfigDefaults")
      .doc("Flag to revert to legacy behavior where a cloned SparkSession receives SparkConf " +
        "defaults, dropping any overrides in its parent SparkSession.")
      .booleanConf
      .createWithDefault(false)

  val DEFAULT_URL_STREAM_HANDLER_FACTORY_ENABLED =
    buildStaticConf("spark.sql.defaultUrlStreamHandlerFactory.enabled")
      .doc("When true, set FsUrlStreamHandlerFactory to support ADD JAR against HDFS locations")
      .internal()
      .booleanConf
      .createWithDefault(true)
}
