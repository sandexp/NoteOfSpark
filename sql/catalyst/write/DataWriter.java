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

import java.io.Closeable;
import java.io.IOException;

/**
 * 由@DataWriterFactory 返回的数据写出器,用于对输入RDD写出数据
 * 一个spark人物有专用的数据写出器,所以不需要线程安全考虑.写出方法@write对于每个输入RDD分区的记录会被调用.
 * 如果一条记录写失败,则会执行@abort方法.剩余的记录就不会被处理.如果所有记录都成功执行,就会调用@commit方法进行提交.
 *
 * 一旦数据写出器使用@commit或者@abort 方法成功返回.spark会调用@close方法,使得数据写出器进行资源清理.在调用@close方法之后,
 * 生命周期就会结束.spark不会对其进行使用.
 *
 * 如果数据写出器成功执行,写出提交的消息@WriterCommitMessage 会被发送到driver端,且会传递BatchWrite,让其对这些消息进行提交.
 * 如果数据写出失败,就会发送异常到驱动器端.且spark会对其进行重试.
 * 在每次尝试中,数据写出器工厂创建的数据写出器,接受的任务id不同.spark会调用BatchWrite的@abort方法在尝试上限到达的时候进行放弃执行.
 *
 * 处理重试机制之外,spark会运行推测任务执行,这种情况会在写出任务执行需要花费很长时间时才会采取.与尝试任务不同,这些会一个接着一个的运行.
 * 推测任务会同时的进行.可能输入RDD分区有多个数据写出器,这些写出器有不同的任务id,这些id同时运行.数据源需要确保这些数据写出器不会冲突.
 * 实现的方法可以在驱动器之间进行沟通.用于保证某一个时刻这些数据写出器中只有一个可以成功提交.
 *
 * 注意,现在泛型T指的是@InternalRow
 */
@Evolving
public interface DataWriter<T> extends Closeable {

  /**
   * 写出一条记录
   *
   * 如果方法失败,会调用@abort方法,且这个数据写出器被认作是失败的
   */
  void write(T record) throws IOException;

  /**
   * 在所有记录写出成功之后提交所有的写出器.返回一个提交的消息.
   * 写出的数据仅仅在提交之后才会对数据源读取器可见.意味着这个方法会隐藏写出的数据,且询问驱动器的@BatchWrite
   * 通过方法@WriterCommitMessage 进行最终提交
   *
   * 如果方法执行失败,会执行抛弃方法
   */
  WriterCommitMessage commit() throws IOException;

  /**
   * 如果执行失败则抛弃写出器.实现需要清除已经写入的数据.
   * 这个方法会仅仅在记录写出失败时候调用,或者提交失败也可能调用
   * 如果方法失败,底层数据源有需要清理的数据,但是但是清理的数据不应当对于数据源读取器可见
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  void abort() throws IOException;
}
