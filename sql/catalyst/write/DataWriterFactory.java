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

package org.apache.spark.sql.connector.write;

import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.Serializable;

/**
 * 数据写出器@DataWriter 的工厂类,由批量写出器BatchWrite 创建,用于在执行器端创建和初始数据写出器@DataWriter
 * 注意,写出器工厂会被序列化,并发送到执行器端,当数据写出器在执行器端创建的时候,且进行写出的时候.这些接口必须被序列化,但是
 * 数据写出器@DataWriter不需要初始化.
 */
@Evolving
public interface DataWriterFactory extends Serializable {

  /**
   * 返回数据写出器,用于进行写出工作.注意,spark可以重用相同的数据对象系列,为了获取更好的性能.数据写出器需要进行防备式备份.例如在
   * 缓冲数据之前进行数据拷贝.
   * 如果方法执行失败,spark写出人物就会失败,且进行重试(有上限).
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *                    Usually Spark processes many RDD partitions at the same time,
   *                    implementations should use the partition id to distinguish writers for
   *                    different partitions.
   * @param taskId The task id returned by {@link TaskContext#taskAttemptId()}. Spark may run
   *               multiple tasks for the same partition (due to speculation or task failures,
   *               for example).
   */
  DataWriter<InternalRow> createWriter(int partitionId, long taskId);
}
