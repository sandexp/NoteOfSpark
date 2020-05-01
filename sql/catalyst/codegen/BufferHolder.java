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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;

/**
 * 名称: 缓冲占位器
 * 这是一个辅助类，可以关联非安全行的数据缓冲区，数据缓冲区可以增长和字段重新指向非安全的行
 * 这个类可以用于构建一次通过的非安全行写出程序。数据会直接写到数据缓冲区中，且不需要其他副本。每个写出程序只有一个这个类的实例。
 * 所以内存段/数据缓冲区可以被重复使用.注意到每个到达的记录,需要调用缓冲占位器@BufferHolder的重置方法.才可以写出记录且重用数据缓冲区.
 */
final class BufferHolder {

  /**
   * 属性列表:
   * @ARRAY_MAX 数组最大大小默认为 Integer.MAX_VALUE - 15
   * @buffer 缓冲数组
   * @cursor 字节对齐量
   * @row 非安全的行
   * @fixedSize 指定的大小
   */
  private static final int ARRAY_MAX = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

  // buffer is guarantee to be word-aligned since UnsafeRow assumes each field is word-aligned.
  private byte[] buffer;
  private int cursor = Platform.BYTE_ARRAY_OFFSET;
  private final UnsafeRow row;
  private final int fixedSize;

  /**
   *
   * 构造器类方法:
   * 初始化容量为64
   */
  BufferHolder(UnsafeRow row) {
    this(row, 64);
  }

  /**
   *
   *
   */
  BufferHolder(UnsafeRow row, int initialSize) {
    /*
    1. 计算字节集合的长度(字节):
    长度len = (属性数量+63)/64*8
    其中属性数量是int类型的值
     */
    int bitsetWidthInBytes = UnsafeRow.calculateBitSetWidthInBytes(row.numFields());
    if (row.numFields() > (ARRAY_MAX - initialSize - bitsetWidthInBytes) / 8) {
      throw new UnsupportedOperationException(
        "Cannot create BufferHolder for input UnsafeRow because there are " +
          "too many fields (number of fields: " + row.numFields() + ")");
    }
    /*
     * 2. 初始化固定缓冲大小
     * 这个固定缓冲不是用于存储数据的,而是用于存储数据的统计值信息的,这里需要将其放入到缓冲区中,作为缓冲区的一部分
     *
     * 长度len=字节集合的长度+ 行的属性数量(int类型)*8
     * 注意: 一个int类型就站8个位
     */
    this.fixedSize = bitsetWidthInBytes + 8 * row.numFields();
    // 3. 缓冲区的容量为初始化缓冲区大小和固定缓冲大小之和
    // 有2的论述可知,这个缓冲区的大小为数据缓冲量+ 统计数值缓冲量
    int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(fixedSize + initialSize);
    // 4. 分配缓冲区内存
    this.buffer = new byte[roundedSize];
    // 5. 初始化行数据
    this.row = row;
    // 6. 设置行数据的起始指针和行数据的大小,用于确定行数据的位置
    this.row.pointTo(buffer, buffer.length);
  }

  /**
   * 对缓冲区进行扩容,扩容至少@neededSize大小,且知道行数据到缓冲区中
   */
  void grow(int neededSize) {
    // 1. 输入非法扩容值,抛出异常
    if (neededSize < 0) {
      throw new IllegalArgumentException(
        "Cannot grow BufferHolder by size " + neededSize + " because the size is negative");
    }
    if (neededSize > ARRAY_MAX - totalSize()) {
      throw new IllegalArgumentException(
        "Cannot grow BufferHolder by size " + neededSize + " because the size after growing " +
          "exceeds size limitation " + ARRAY_MAX);
    }
    // 2. 确定扩容之后,行数据的长度
    final int length = totalSize() + neededSize;
    if (buffer.length < length) {
      /*
       3. 扩容策略,为最少需要容量@length的2倍,如果这个长度足够大,则直接取值为@ARRAY_MAX
       注意到这里扩容的频率不会特别高,因为缓冲区是可以重用的
      */
      int newLength = length < ARRAY_MAX / 2 ? length * 2 : ARRAY_MAX;
      int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(newLength);
      // 4. 分配新的缓冲区内存
      final byte[] tmp = new byte[roundedSize];
      Platform.copyMemory(
        buffer,
        Platform.BYTE_ARRAY_OFFSET,
        tmp,
        Platform.BYTE_ARRAY_OFFSET,
        totalSize());
      // 5. 重新执向新的缓冲区
      buffer = tmp;
      // 6. 重新指向新的行位置
      row.pointTo(buffer, buffer.length);
    }
  }

  // 获取缓冲区
  byte[] getBuffer() {
    return buffer;
  }

  // 获取缓冲区指针
  int getCursor() {
    return cursor;
  }

  // 指针移动+val
  void increaseCursor(int val) {
    cursor += val;
  }

  // 重置缓冲区(重新设置缓冲区指针,指针到0数据位置处,此时只有统计数据在缓冲区中)
  void reset() {
    cursor = Platform.BYTE_ARRAY_OFFSET + fixedSize;
  }

  // 获取全部数据占用的位数量
  int totalSize() {
    return cursor - Platform.BYTE_ARRAY_OFFSET;
  }
}
