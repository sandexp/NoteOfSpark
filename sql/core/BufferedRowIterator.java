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

package org.apache.spark.sql.execution;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.LinkedList;

/**
 * 缓存行迭代器
 *
 * 迭代器接口,用于从生产函数中拉取输出,用于多个操作者
 * 参数列表
 * @currentRows 当前行的列表列表
 * @unsafeRow unsafe的行(当输出中没有列的时候使用)
 * @startTimeNs 起始时间
 * @partitionIndex 分区索引
 *
 */
public abstract class BufferedRowIterator {
  protected LinkedList<InternalRow> currentRows = new LinkedList<>();
  protected UnsafeRow unsafeRow = new UnsafeRow(0);
  private long startTimeNs = System.nanoTime();

  protected int partitionIndex = -1;

  // 确认是否包含下一行数据
  public boolean hasNext() throws IOException {
    if (currentRows.isEmpty()) {
      processNext();
    }
    return !currentRows.isEmpty();
  }

  // 获取当前行
  public InternalRow next() {
    return currentRows.remove();
  }

  /**
   * 获取持续的时间(从对象创建到调用时刻的时间).这个对象代表一个pipeline.所以这个是pipeline运行时长的度量
   */
  public long durationMs() {
    return (System.nanoTime() - startTimeNs) / (1000 * 1000);
  }

  /**
   * 由内部行数据初始化迭代器列表
   */
  public abstract void init(int index, Iterator<InternalRow>[] iters);

  /*
   * Attributes of the following four methods are public. Thus, they can be also accessed from
   * methods in inner classes. See SPARK-23598
   */
  /**
   * 将一行数据添加到当前的行列表中
   */
  public void append(InternalRow row) {
    currentRows.add(row);
  }

  /**
   * 确定迭代器是否需要定时,如果返回true,调用者需要停止循环(停止条件为:当前行列表为空)
   */
  public boolean shouldStop() {
    return !currentRows.isEmpty();
  }

  /**
   * 对于当前任务,峰值执行内存+=size
   * Increase the peak execution memory for current task.
   */
  public void incPeakExecutionMemory(long size) {
    TaskContext.get().taskMetrics().incPeakExecutionMemory(size);
  }

  /**
   * 处理输入,直到有一个行可以作为输出(当前行)
   * 调用之后,如果行数据为空,意味着没有行数据了
   */
  protected abstract void processNext() throws IOException;
}
