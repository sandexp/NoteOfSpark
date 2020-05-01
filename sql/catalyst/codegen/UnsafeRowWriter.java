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
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.bitset.BitSetMethods;

/**
 * 非安全行写出器:
 * 这是一个辅助类,用于写出数据到全局的行缓冲区中,使用@UnsafeRow格式.
 * 这个类会记住行缓冲区起始写入的偏移量位置,且写入的时候会移动指针.如果新的数据到来的话,(如果是外层写出器就是输入记录,如果是内部写出器
 * 的话则是嵌入式的结构体).写出之前需要调用@UnsafeRowWriter.resetRowWriter 进行重置,更新起始偏移量@startingOffset.清除空位.
 * 注意如果是外部写出器,就意味着重视需要从全局缓冲区的起始写入.不需要更新@startingOffset 仅仅需要在写出新数据之前调用@zeroOutNullBytes
 * 方法
 */
public final class UnsafeRowWriter extends UnsafeWriter {

  /**
   * @row 非安全的行
   * @nullBitsSize 空位的大小
   * @fixedSize 给定的长度
   */
  private final UnsafeRow row;

  private final int nullBitsSize;
  private final int fixedSize;

  public UnsafeRowWriter(int numFields) {
    this(new UnsafeRow(numFields));
  }

  public UnsafeRowWriter(int numFields, int initialBufferSize) {
    this(new UnsafeRow(numFields), initialBufferSize);
  }

  public UnsafeRowWriter(UnsafeWriter writer, int numFields) {
    this(null, writer.getBufferHolder(), numFields);
  }

  private UnsafeRowWriter(UnsafeRow row) {
    this(row, new BufferHolder(row), row.numFields());
  }

  private UnsafeRowWriter(UnsafeRow row, int initialBufferSize) {
    this(row, new BufferHolder(row, initialBufferSize), row.numFields());
  }

  /**
   * 初始化行写出器
   */
  private UnsafeRowWriter(UnsafeRow row, BufferHolder holder, int numFields) {
    super(holder);
    // 1. 初始化行
    this.row = row;
    // 2. 计算空位大小(即没有数据时候的大小)
    this.nullBitsSize = UnsafeRow.calculateBitSetWidthInBytes(numFields);
    // 3. 设定缓冲区的总大小
    this.fixedSize = nullBitsSize + 8 * numFields;
    // 4. 设定起始指针
    this.startingOffset = cursor();
  }

  /**
   * 使用缓冲占位器收集的大小更新行的总大小,返回行数据
   */
  public UnsafeRow getRow() {
    row.setTotalSize(totalSize());
    return row;
  }

  /**
   * 重置起始偏移量,根据的是当前行缓冲区的当前指针,且清除了空位.在写一条新的结构体到行缓冲区之前的时候会被调用
   */
  public void resetRowWriter() {
    // 1. 设置起始偏移量
    this.startingOffset = cursor();
    // 2. 对全局缓冲区进行扩容,保证有足够的空间写出定长的数据
    grow(fixedSize);
    // 3. 移动指针位置
    increaseCursor(fixedSize);
    // 4. 清除空位
    zeroOutNullBytes();
  }

  /**
   * 清除空位,这个需要在将行数据写入到行缓冲区之前进行调用
   */
  public void zeroOutNullBytes() {
    for (int i = 0; i < nullBitsSize; i += 8) {
      Platform.putLong(getBuffer(), startingOffset + i, 0L);
    }
  }

  // 确定指定索引处是否为控制
  public boolean isNullAt(int ordinal) {
    return BitSetMethods.isSet(getBuffer(), startingOffset, ordinal);
  }

  // 设置指定索引@ordinal处的值为null
  public void setNullAt(int ordinal) {
    BitSetMethods.set(getBuffer(), startingOffset, ordinal);
    write(ordinal, 0L);
  }
  // 设置指定索引@ordinal处的值为null
  @Override
  public void setNull1Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull2Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull4Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull8Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  // 获取指定索引处元素的偏移量
  public long getFieldOffset(int ordinal) {
    return startingOffset + nullBitsSize + 8L * ordinal;
  }

  // 写出指定的值@value
  @Override
  public void write(int ordinal, boolean value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0L);
    writeBoolean(offset, value);
  }

  @Override
  public void write(int ordinal, byte value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0L);
    writeByte(offset, value);
  }

  @Override
  public void write(int ordinal, short value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0L);
    writeShort(offset, value);
  }

  @Override
  public void write(int ordinal, int value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0L);
    writeInt(offset, value);
  }

  @Override
  public void write(int ordinal, long value) {
    writeLong(getFieldOffset(ordinal), value);
  }

  @Override
  public void write(int ordinal, float value) {
    final long offset = getFieldOffset(ordinal);
    writeLong(offset, 0);
    writeFloat(offset, value);
  }

  @Override
  public void write(int ordinal, double value) {
    writeDouble(getFieldOffset(ordinal), value);
  }

  // 十进制移除处理
  @Override
  public void write(int ordinal, Decimal input, int precision, int scale) {
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      // make sure Decimal object has the same scale as DecimalType
      if (input != null && input.changePrecision(precision, scale)) {
        write(ordinal, input.toUnscaledLong());
      } else {
        setNullAt(ordinal);
      }
    } else {
      // grow the global buffer before writing data.
      holder.grow(16);

      // always zero-out the 16-byte buffer
      Platform.putLong(getBuffer(), cursor(), 0L);
      Platform.putLong(getBuffer(), cursor() + 8, 0L);

      // Make sure Decimal object has the same scale as DecimalType.
      // Note that we may pass in null Decimal object to set null for it.
      if (input == null || !input.changePrecision(precision, scale)) {
        BitSetMethods.set(getBuffer(), startingOffset, ordinal);
        // keep the offset for future update
        setOffsetAndSize(ordinal, 0);
      } else {
        final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
        final int numBytes = bytes.length;
        assert numBytes <= 16;

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor(), numBytes);
        setOffsetAndSize(ordinal, bytes.length);
      }

      // move the cursor forward.
      increaseCursor(16);
    }
  }
}
