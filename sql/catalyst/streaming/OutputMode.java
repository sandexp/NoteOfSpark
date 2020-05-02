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
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes;

/**
 * 输出模式,描述了什么数据在流式的@DataFrame/Dataset出现新数据的时候,可以写出到流式的sink中.
 */
@Evolving
public class OutputMode {

  /**
   * 输出模式: 添加
   * 只有在流式的@DataFrame/Dataset 中有新的行数据写出到sink中时使用的输出模式。输出模式可以仅仅用在查询中，
   * 不会包括任何的聚合情况。
   */
  public static OutputMode Append() {
    return InternalOutputModes.Append$.MODULE$;
  }

  /**
   * 输出模式: 完成
   * 在流式的@DataFrame/Dataset 中所有的行数据每次更新都写出到sink中。这个输出模式仅仅在查询中不包含聚合操作的时候进行。
   */
  public static OutputMode Complete() {
    return InternalOutputModes.Complete$.MODULE$;
  }

  /**
   * 输出模式: 更新
   * 当流式@DataFrame/Dataset 行数据进行更新的时候，行信息在更新之后会被写出到sink中。如果查询中不包含聚合操作，等同于Append模式。
   */
  public static OutputMode Update() {
    return InternalOutputModes.Update$.MODULE$;
  }
}
