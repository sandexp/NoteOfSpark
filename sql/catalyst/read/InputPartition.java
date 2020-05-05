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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;

import java.io.Serializable;

/**
 * 输入分区的序列化实现,这些分区由Batch调用@planInputPartitions 返回,且是流处理对于的部分.
 *
 * 注意到输入分区@InputPartition 会被序列化并且被发送到执行器上.分区读取器@PartitionReader 会由@PartitionReader 进行创建,
 * 或者由@PartitionReaderFactory调用@createColumnarReader(InputPartition)。输入分区必须序列化，但是分区读取器@PartitionReader
 * 不需要序列化
 */
@Evolving
public interface InputPartition extends Serializable {

  /**
   * 输入分区读取器的偏好位置列表,偏好位置的选择是取决于在哪里执行更加快速.但是spark不保证输入分区读取器一定在这些位置上运行.
   * 实现需要保证可以在任何位置进行运行,这个位置信息是一个字符串,代表着主机名称.
   * 注意到当主机名称不能够被spark识别的时候,会被忽略,不会出现在位置列表中.默认返回值是空的字符串数组,这个意味着输入分区的读取器
   * 没有位置偏好选择.
   * 如果这个方法执行失败,那么动作就会失败,spark job就不会提交执行
   */
  default String[] preferredLocations() {
    return new String[0];
  }
}
