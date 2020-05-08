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

package org.apache.spark.sql.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.execution.streaming.ContinuousTrigger;
import org.apache.spark.sql.execution.streaming.OneTimeTrigger$;
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * 触发器
 * 这个触发器的政策用于预测流式查询@StreamingQuery 查询频率
 */
@Evolving
public class Trigger {

  /**
   * 一个触发协议,周期性地按照一定的时间间隔进行处理.如果时间间隔@interval是0,查询会尽可能的快速执行
   */
  public static Trigger ProcessingTime(long intervalMs) {
      return ProcessingTimeTrigger.create(intervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * 一个触发协议,周期性地按照一定的时间间隔进行处理.如果时间间隔@interval是0,查询会尽可能的快速执行
   * 执行示例:
   * {{{
   *    import java.util.concurrent.TimeUnit
   *    df.writeStream().trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
   * }}}
   */
  public static Trigger ProcessingTime(long interval, TimeUnit timeUnit) {
      return ProcessingTimeTrigger.create(interval, timeUnit);
  }

  /**
   * 一个触发协议,周期性地按照一定的时间间隔进行处理.如果@durat持续时间@duration为0,查询会尽可能的快速执行
   *
   * {{{
   *    import scala.concurrent.duration._
   *    df.writeStream.trigger(Trigger.ProcessingTime(10.seconds))
   * }}}
   */
  public static Trigger ProcessingTime(Duration interval) {
      return ProcessingTimeTrigger.apply(interval);
  }

  /**
   * 一个触发协议,周期性地按照一定的时间间隔进行处理.如果时间间隔@interval是0,查询会尽可能的快速执行
   * {{{
   *    df.writeStream.trigger(Trigger.ProcessingTime("10 seconds"))
   * }}}
   */
  public static Trigger ProcessingTime(String interval) {
      return ProcessingTimeTrigger.apply(interval);
  }

  /**
   * 一个触发器,可以处理流式查询的一个批次数据,然后终止查询
   */
  public static Trigger Once() {
    return OneTimeTrigger$.MODULE$;
  }

  /**
   * 一个触发器,可以周期性的处理流式数据,在指定的时间间隔异步地设置检查点
   */
  public static Trigger Continuous(long intervalMs) {
    return ContinuousTrigger.apply(intervalMs);
  }

  /**
   * 这个触发器可以连续的处理流式数据,按照指定的周期异步设置检查点
   * {{{
   *    import java.util.concurrent.TimeUnit
   *    df.writeStream.trigger(Trigger.Continuous(10, TimeUnit.SECONDS))
   * }}}
   */
  public static Trigger Continuous(long interval, TimeUnit timeUnit) {
    return ContinuousTrigger.create(interval, timeUnit);
  }

  /**
   * 连续处理流式数据的触发器,按照指定的时间间隔设置检查点
   * {{{
   *    import scala.concurrent.duration._
   *    df.writeStream.trigger(Trigger.Continuous(10.seconds))
   * }}}
   */
  public static Trigger Continuous(Duration interval) {
    return ContinuousTrigger.apply(interval);
  }

  /**
   * 连续地处理流式数据的触发器,按照指定的时间间隔设置检查点
   * {{{
   *    df.writeStream.trigger(Trigger.Continuous("10 seconds"))
   * }}}
   */
  public static Trigger Continuous(String interval) {
    return ContinuousTrigger.apply(interval);
  }
}
