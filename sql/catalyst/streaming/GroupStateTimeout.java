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
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.plans.logical.*;

/**
 * 代表数据集操作的超时类型,其中包括@mapGroupsWithState和@flatMapGroupsWithState.详情参考@GroupState的细节
 */
@Experimental
@Evolving
public class GroupStateTimeout {

  /**
   * 基于处理时间的超时,超时的持续时间可以给每个组进行设置.这里主要通过调用@GroupState.setTimeoutDuration()方法
   */
  public static GroupStateTimeout ProcessingTimeTimeout() {
    return ProcessingTimeTimeout$.MODULE$;
  }

  /**
   * 基于事件时间的超时，事件时间的时间戳可以给每个组进行设置，主要是调用@GroupState.setTimeoutTimestamp()的方法。
   * 此外,可以定义查询的标志位,使用了@Dataset.withWatermark进行标志位设置.当标记位提前到到组的时间戳之前的时候,且组没有
   * 检索到任何数据,那么组就会超时,
   */
  public static GroupStateTimeout EventTimeTimeout() { return EventTimeTimeout$.MODULE$; }

  // 无延时
  public static GroupStateTimeout NoTimeout() { return NoTimeout$.MODULE$; }

}
