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

import org.apache.spark.annotation.Evolving;

/**
 * 批次写出器
 * 这个接口定义了如何批量写出数据到数据源中
 *
 * 写出过程:
 * 1. 由写出工厂类创建一个写出器@createBatchWriterFactory,对其尽序列化,并发送到输入RDD的所有分区上
 * 2. 对于每个分区,创建一个数据写出器,且使用写出器写出分区数据.如果所有数据都成功写入,则调用提交方法进行job提交.如果发送异常
 * 则会调用@abort()放弃提交
 * 3. 如果所有写出器都成功执行,则会@commit(WriterCommitMessage[]),如果有一个写出器失败，则会放弃任务的执行
 *
 * spark不提供重试job的功能，需要手动地清除数据.
 */
@Evolving
public interface BatchWrite {

  /**
   * 创建批次写出器,写出器会被序列化并发送大执行器上
   */
  DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info);

  /**
   * 确认spark是否需要使用提交协调器,去保证一个分区至多一个任务提交
   */
  default boolean useCommitCoordinator() {
    return true;
  }

  /**
   * 从成功的数据写出器中处理提交消息(提交成功的回调)
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort(WriterCommitMessage[])} would be called.
   */
  default void onDataWriterCommit(WriterCommitMessage message) {}

  /**
   * 使用指定的提交消息列表提交这个写出的job.提交消息有成功的写出器组成.
   * 如果执行失败.就会放弃任务.
   * 注意到推测执行可能导致多个任务运行在一个分区上.默认情况下,spark使用提交协调器满足一个时刻只有一个任务提交.
   * 这个方法的实现可以关闭这个行为.在关闭情况下,就会发生这种情况
   */
  void commit(WriterCommitMessage[] messages);

  /**
   * 放弃任务的执行,这里放弃执行时尽最大努力的.
   */
  void abort(WriterCommitMessage[] messages);
}
