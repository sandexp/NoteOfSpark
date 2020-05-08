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
package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.parquet.Log;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.Serializable;
import java.util.logging.Handler;
import java.util.logging.Logger;

// JUL日志重定向器,parquet-mr版本小于1.8是使用@SLF4JBridgeHandler,1.9之后的版本直接使用@SLF4J
final class ParquetLogRedirector implements Serializable {
  /**
   * 客户端类需要持有这个实例的引用,保证重定向的发生.这个对于序列化的类来说很重要.这里属性的构造器被忽略了.
   */
  static final ParquetLogRedirector INSTANCE = new ParquetLogRedirector();

  // JUL loggers must be held by a strong reference, otherwise they may get destroyed by GC.
  // However, the root JUL logger used by Parquet isn't properly referenced.  Here we keep
  // references to loggers in both parquet-mr <= 1.6 and 1.7/1.8
  /**
   * JUL 日志处理器必须使用强引用持有,否则会被gc掉.但是跟JUL日志处理器(Parquet)没有合适的设置引用.这里保持引用到
   * 1.6(1.7/1.8)版本的parquet-mr.
   */
  private static final Logger apacheParquetLogger =
    Logger.getLogger(Log.class.getPackage().getName());
  private static final Logger parquetLogger = Logger.getLogger("parquet");

  static {
    // For parquet-mr 1.7 and 1.8, which are under `org.apache.parquet` namespace.
    try {
      Class.forName(Log.class.getName());
      redirect(Logger.getLogger(Log.class.getPackage().getName()));
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }

    // For parquet-mr 1.6.0 and lower versions bundled with Hive, which are under `parquet`
    // namespace.
    try {
      Class.forName("parquet.Log");
      redirect(Logger.getLogger("parquet"));
    } catch (Throwable t) {
      // SPARK-9974: com.twitter:parquet-hadoop-bundle:1.6.0 is not packaged into the assembly
      // when Spark is built with SBT. So `parquet.Log` may not be found.  This try/catch block
      // should be removed after this issue is fixed.
    }
  }

  private ParquetLogRedirector() {
  }

  // 重定向日志
  private static void redirect(Logger logger) {
    // 删除原有的日志处理器@logger
    for (Handler handler : logger.getHandlers()) {
      logger.removeHandler(handler);
    }
    logger.setUseParentHandlers(false);
    // 设置新的日志处理器
    logger.addHandler(new SLF4JBridgeHandler());
  }
}
