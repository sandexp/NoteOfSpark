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

import java.util.{Map => JMap}

import org.apache.spark.TaskContext
import org.apache.spark.internal.config.{ConfigEntry, ConfigProvider, ConfigReader}

/**
  * 只读的SQL配置,会有执行器上的任务创建.从本地属性读取配置,可以从driver端传递到执行器端
  * context: TaskContext  任务上下文
 */
class ReadOnlySQLConf(context: TaskContext) extends SQLConf {

  /*
  参数列表:
    settings: JMap[String, String] 设置参数表
    reader: ConfigReader  配置读取器
   */
  @transient override val settings: JMap[String, String] = {
    context.getLocalProperties.asInstanceOf[JMap[String, String]]
  }

  @transient override protected val reader: ConfigReader = {
    new ConfigReader(new TaskContextConfigProvider(context))
  }

  // 设置配置信息(并带有参数检查),不支持的操作
  override protected def setConfWithCheck(key: String, value: String): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  // 解除设置,不支持
  override def unsetConf(key: String): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  // 解除设置,不支持
  override def unsetConf(entry: ConfigEntry[_]): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  // 清除配置,不支持
  override def clear(): Unit = {
    throw new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  // 配置克隆,不支持
  override def clone(): SQLConf = {
    throw new UnsupportedOperationException("Cannot clone/copy ReadOnlySQLConf.")
  }

  override def copy(entries: (ConfigEntry[_], Any)*): SQLConf = {
    throw new UnsupportedOperationException("Cannot clone/copy ReadOnlySQLConf.")
  }
}

class TaskContextConfigProvider(context: TaskContext) extends ConfigProvider {
  override def get(key: String): Option[String] = Option(context.getLocalProperty(key))
}
