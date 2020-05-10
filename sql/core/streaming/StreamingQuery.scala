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

package org.apache.spark.sql.streaming

import java.util.UUID
import java.util.concurrent.TimeoutException

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.SparkSession

/**
查询的处理，新数据到达的时候，后台连续指向。所有方法都是线程安全的.
 */
@Evolving
trait StreamingQuery {

  /**
   获取查询的名称,如果用户没有指定则为空.这个名称可以通过@org.apache.spark.sql.streaming.DataStreamWriter 指定。
   例如：
      dataframe.writeStream.queryName("query").start()
    这个名称如果设置的话，在激活查询中是唯一的
   */
  def name: String

  /**
    获取查询的唯一id，这个查询可以通过持久化数据中恢复。也就是说，这个id当一个查询首次启动的时候查询，且重启的时候id不变。
   */
  def id: UUID

  /**
   获取运行查询的唯一id。也就是说，每次启动/重启的时候,查询会生成一个唯一id.也就是说重启的时候id是相同的但是runId是不同的
   */
  def runId: UUID

  /**
   获取当前的sparkSession
   */
  def sparkSession: SparkSession

  /**
    如果当前查询处于运行状态,返回true
   */
  def isActive: Boolean

  /**
   如果查询由于异常终止则返回@StreamingQueryException
   */
  def exception: Option[StreamingQueryException]

  /**
    返回当前查询的状态
   */
  def status: StreamingQueryStatus

  /**
    返回这个查询最近更新的@StreamingQueryProgress,每个流每次的更新的数量由@spark.sql.streaming.numRecentProgressUpdates
    控制
   */
  def recentProgress: Array[StreamingQueryProgress]

  /**
    返回流式查询最小更新的@StreamingQueryProgress
   */
  def lastProgress: StreamingQueryProgress

  /**
    等待当前查询的结束,或者使用@query.stop() 进行停职.如果查询异常停止,那么就会抛出异常.
    如果查询终止了,所有这个方法的子调用会立即返回,或是是抛出异常.
    @throws StreamingQueryException 表示查询由于异常终止
   */
  @throws[StreamingQueryException]
  def awaitTermination(): Unit

  /**
    等待当前查询的结束,或者使用@query.stop() 进行终止。如果查询异常终止，那么就会抛出异常。否则，会返回查询是否终止(在
    指定的超时时间内).
    如果查询已经终止,所有这个方法的自调用会立即返回true.或者是抛出异常.
    @throws StreamingQueryException 是否异常终止
   */
  @throws[StreamingQueryException]
  def awaitTermination(timeoutMs: Long): Boolean

  /**
   阻塞到所有数据源中的key用数据处理完毕,并且提交到sink出.这个方法是用于测试的.注意当数据是连续的时候,这个方法会永久阻塞.
    此外,这个方法仅仅保证阻塞到数据同步添加到@org.apache.spark.sql.execution.streaming.Source为止
   */
  def processAllAvailable(): Unit

  /**
  如果运行的话,停止查询的执行.这个会在查询结束或者超时的时候终止.
  默认情况下会无限阻塞,可以通过@spark.sql.streaming.stopTimeout 配置超时时间。超时时间为0或者负数会视作无限阻塞。
  如果抛出了@TimeoutException,用户可以重试这个流.如果存在问题,建议kill spark应用.
   */
  @throws[TimeoutException]
  def stop(): Unit

  /**
    打印物理执行计划到控制台上，用于debug
   */
  def explain(): Unit

  /**
    打印物理计划到控制台上，用于debug
   * @param extended 是否进行额外的解释
   */
  def explain(extended: Boolean): Unit
}
