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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * 这个接口定义了在流式查询中如何去写出数据到数据源中
 *
 * 写过程包括:
 * 1. 通过@createStreamingWriterFactory 创建写出器工厂,序列化并发送输入数据(RDD)的所有分区
 * 2. 对于每个分区的顶点(epoch),创建数据写出器,且写出节点的数据.如果所有数据成功写入,则数据写出器会提交@commit 操作.
 * 如果执行期间存在有移除,则会抛出异常.
 * 3. 如果所有分区的顶点都被提交,则会调用提交方法.否则则会放弃任务的执行.
 *
 * spark虽然提供重试写任务,但是不提供写job.所以用户需要手动重试
 */
@Evolving
public interface StreamingWrite {

  /**
   * 创建写出器工厂,这个会被序列化,并发送到执行器上.如果这里失败,就不会提交任何job
   *
   * @param info Information about the RDD that will be written to this data writer
   */
  StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info);

  /**
   * 提交指定节点的job,如果所有数据都写出成功,则会提交任务.
   * 如果方法执行失败,则会调用@abort 放弃执行.
   * 执行引擎可以在同一个顶点上(epoch)调用提交操作多次.为了支持仅仅消费一次语义,实现必须保证对同一个顶点的多次提交时幂等的.
   */
  void commit(long epochId, WriterCommitMessage[] messages);

  /**
   * 由于写出失败或者提交是啊比,放弃job的执行.如果这个方法执行失败,底层数据源需要手动地进行清除.
   * 除非是由于提交失败的原因,否则给定的消息会有一些空槽,因为仅仅一部分写出器在放弃执行之前提交了.或者一些写出器虽然提交,但是提交的消息
   * 没有到达驱动器端.所以对于数据源来说,仅仅是尽最大努力的情况数据写出器留存数据.
   */
  void abort(long epochId, WriterCommitMessage[] messages);
}
