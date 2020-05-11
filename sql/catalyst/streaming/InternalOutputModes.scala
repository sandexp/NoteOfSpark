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

package org.apache.spark.sql.catalyst.streaming

import java.util.Locale

import org.apache.spark.sql.streaming.OutputMode

/**
 内部输出模式
 */
private[sql] object InternalOutputModes {

  /**
    添加模式
    这个输出模式中,流式DF/DS的新建行会被写入到sink中.输出模式可以用于查询中,不包含任何的聚合
   */
  case object Append extends OutputMode

  /**
    完成模式
    输出模式,所有流式DF/DS的行在存在有更新的情况下,才会被写入到sink中.输出模式仅仅可以用于包含聚合的查询中
   */
  case object Complete extends OutputMode

  /**
    更新模式
    流式DF/DS中的行在每次有更新的时候会被写入到sink中.如果查询不包含聚合,相当于添加模式
   */
  case object Update extends OutputMode


  def apply(outputMode: String): OutputMode = {
    outputMode.toLowerCase(Locale.ROOT) match {
      case "append" =>
        OutputMode.Append
      case "complete" =>
        OutputMode.Complete
      case "update" =>
        OutputMode.Update
      case _ =>
        throw new IllegalArgumentException(s"Unknown output mode $outputMode. " +
          "Accepted output modes are 'append', 'complete', 'update'")
    }
  }
}
