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

package org.apache.spark.sql.connector.write.streaming;

import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;

import java.io.Serializable;

/**
 * 数据写出器的工厂类.用于在执行器端创建和初始化数据写出器@DataWriter
 * 注意,写出器工厂会被序列化且会被发送给执行器,数据写出器会在执行器上创建,且进行真正的写操作.所以接口必须被序列化.
 */
@Evolving
public interface StreamingDataWriterFactory extends Serializable {

  /**
   * 创建数据写出器.注意spark会重用相同的数据对象(当发送数据到数据写出器上的时候).为了获取更好的性能.数据写出器应当进行防御式的副本策略.
   * 例如在缓冲操作之前,对数据进行拷贝.
   *
   * 如果方法失败,spark会进行重试的容错措施.
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *                    Usually Spark processes many RDD partitions at the same time,
   *                    implementations should use the partition id to distinguish writers for
   *                    different partitions.
   * @param taskId The task id returned by {@link TaskContext#taskAttemptId()}. Spark may run
   *               multiple tasks for the same partition (due to speculation or task failures,
   *               for example).
   * @param epochId A monotonically increasing id for streaming queries that are split in to
   *                discrete periods of execution.
   */
  DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId);
}
