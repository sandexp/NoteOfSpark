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

import org.apache.spark.annotation.Evolving
import org.apache.spark.scheduler.SparkListenerEvent

/**
  监听与流式查询@StreamingQuery 相关的事件。
  注意: 方法是线程不安全的,因为可能被不同线程调用
 */
@Evolving
abstract class StreamingQueryListener {

  import StreamingQueryListener._

  /**
    回调函数
    在查询开始的时候调用
    注意: 这个是同步调用的,也就是说,@onQueryStart 会在DataStreamWriter.start()之前在所有监听器上调用
   */
  def onQueryStarted(event: QueryStartedEvent): Unit

  /**
    回调函数
    当有状态更新的时候调用
    这个方法是异步的，流式查询@StreamingQuery 状态会在方法调用结束处理。因此，流式查询在处理事件的时候会被改变。
    可以发现当处理@QueryProgressEvent的时候可以发现流式查询@StreamingQuery被终止
   */
  def onQueryProgress(event: QueryProgressEvent): Unit

  /**
    回调函数
    流式查询结束的时候调用
   */
  def onQueryTerminated(event: QueryTerminatedEvent): Unit
}


/**
流式监听器，定义了各种监听对象
 */
@Evolving
object StreamingQueryListener {

  /**
    流式查询监听器的基本类型
   */
  @Evolving
  trait Event extends SparkListenerEvent

  /**
   代表查询开始的事件
   * @param id 重启可恢复的全局唯一id
   * @param runId 重启会变动的全局唯一id
   * @param name 用户指定的查询名称
   * @since 2.1.0
   */
  @Evolving
  class QueryStartedEvent private[sql](
      val id: UUID,
      val runId: UUID,
      val name: String) extends Event

  /**
    流式查询中更新的事件
   * @param progress 流式查询处理的更新
   */
  @Evolving
  class QueryProgressEvent private[sql](val progress: StreamingQueryProgress) extends Event

  /**
   终止查询的事件
   * @param id 全局唯一查询id，重启可以恢复
   * @param runId 全局唯一查询id，重启会重新赋值
   * @param exception 查询终止时的异常，当为null时表示正常执行完毕
   * @since 2.1.0
   */
  @Evolving
  class QueryTerminatedEvent private[sql](
      val id: UUID,
      val runId: UUID,
      val exception: Option[String]) extends Event
}
