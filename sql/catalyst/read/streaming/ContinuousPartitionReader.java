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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.PartitionReader;

/**
 * 分区读取器@PartitionReader的变形,用于连续的流式处理
 */
@Evolving
public interface ContinuousPartitionReader<T> extends PartitionReader<T> {

  /**
   * 获取当前记录的偏移量,或者如果没有读取记录的时候设置启动偏移量
   * 执行引擎会使用方法@get调用这个方法,用于去追踪当前的偏移量.当一个点(epoch)结束的时候,每个分区的上一条记录的偏移量会作为重启的
   * 检查点存储起来.
   */
  PartitionOffset getOffset();
}
