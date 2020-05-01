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

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * 写出器,基础的抽象类
 */
public abstract class UnsafeWriter {
  // Keep internal buffer holder
  /**
   * 属性列表:
   * @holder 缓冲区占用器
   * @startingOffset 起始偏移量
   */
  protected final BufferHolder holder;
  protected int startingOffset;

  protected UnsafeWriter(BufferHolder holder) {
    this.holder = holder;
  }

  // 有缓冲占位器授予的祖先方法
  public final BufferHolder getBufferHolder() {
    return holder;
  }

  // 获取缓冲区
  public final byte[] getBuffer() {
    return holder.getBuffer();
  }

  // 重置缓冲区
  public final void reset() {
    holder.reset();
  }

  // 获取缓冲区的总大小
  public final int totalSize() {
    return holder.totalSize();
  }

  // 缓冲区扩容
  public final void grow(int neededSize) {
    holder.grow(neededSize);
  }

  // 获取缓冲区的指针
  public final int cursor() {
    return holder.getCursor();
  }

  // 增加缓冲区指针
  public final void increaseCursor(int val) {
    holder.increaseCursor(val);
  }

  // 从之前的指针设置偏移量和大小
  public final void setOffsetAndSizeFromPreviousCursor(int ordinal, int previousCursor) {
    setOffsetAndSize(ordinal, previousCursor, cursor() - previousCursor);
  }
  // 设置偏移量大小
  protected void setOffsetAndSize(int ordinal, int size) {
    setOffsetAndSize(ordinal, cursor(), size);
  }

  protected void setOffsetAndSize(int ordinal, int currentCursor, int size) {
    final long relativeOffset = currentCursor - startingOffset;
    final long offsetAndSize = (relativeOffset << 32) | (long)size;

    write(ordinal, offsetAndSize);
  }

  protected final void zeroOutPaddingBytes(int numBytes) {
    if ((numBytes & 0x07) > 0) {
      Platform.putLong(getBuffer(), cursor() + ((numBytes >> 3) << 3), 0L);
    }
  }

  public abstract void setNull1Bytes(int ordinal);
  public abstract void setNull2Bytes(int ordinal);
  public abstract void setNull4Bytes(int ordinal);
  public abstract void setNull8Bytes(int ordinal);

  public abstract void write(int ordinal, boolean value);
  public abstract void write(int ordinal, byte value);
  public abstract void write(int ordinal, short value);
  public abstract void write(int ordinal, int value);
  public abstract void write(int ordinal, long value);
  public abstract void write(int ordinal, float value);
  public abstract void write(int ordinal, double value);
  public abstract void write(int ordinal, Decimal input, int precision, int scale);

  public final void write(int ordinal, UTF8String input) {
    writeUnalignedBytes(ordinal, input.getBaseObject(), input.getBaseOffset(), input.numBytes());
  }

  public final void write(int ordinal, byte[] input) {
    write(ordinal, input, 0, input.length);
  }

  public final void write(int ordinal, byte[] input, int offset, int numBytes) {
    writeUnalignedBytes(ordinal, input, Platform.BYTE_ARRAY_OFFSET + offset, numBytes);
  }

  // 写出非对其的字节
  private void writeUnalignedBytes(
      int ordinal,
      Object baseObject,
      long baseOffset,
      int numBytes) {
    // 1. 确定需要的容量
    final int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
    // 2. 扩容
    grow(roundedSize);
    // 3. 清除用于对齐字节
    zeroOutPaddingBytes(numBytes);
    // 4. 对需要写出的对象进行复制
    Platform.copyMemory(baseObject, baseOffset, getBuffer(), cursor(), numBytes);
    // 5. 设置偏移量并写出
    setOffsetAndSize(ordinal, numBytes);
    // 6. 移动指针位置
    increaseCursor(roundedSize);
  }

  // 写出时间间隔类型
  public final void write(int ordinal, CalendarInterval input) {
    // 1. 定量扩容到16
    grow(16);

    if (input == null) {
      BitSetMethods.set(getBuffer(), startingOffset, ordinal);
    } else {
      // 写出月,日和ms属性
      Platform.putInt(getBuffer(), cursor(), input.months);
      Platform.putInt(getBuffer(), cursor() + 4, input.days);
      Platform.putLong(getBuffer(), cursor() + 8, input.microseconds);
    }
    // 反转空间,这样之后就可以对其进行更新
    setOffsetAndSize(ordinal, 16);
    // 移动指针
    increaseCursor(16);
  }

  public final void write(int ordinal, UnsafeRow row) {
    writeAlignedBytes(ordinal, row.getBaseObject(), row.getBaseOffset(), row.getSizeInBytes());
  }

  public final void write(int ordinal, UnsafeMapData map) {
    writeAlignedBytes(ordinal, map.getBaseObject(), map.getBaseOffset(), map.getSizeInBytes());
  }

  public final void write(UnsafeArrayData array) {
    // Unsafe arrays both can be written as a regular array field or as part of a map. This makes
    // updating the offset and size dependent on the code path, this is why we currently do not
    // provide an method for writing unsafe arrays that also updates the size and offset.
    int numBytes = array.getSizeInBytes();
    grow(numBytes);
    Platform.copyMemory(
            array.getBaseObject(),
            array.getBaseOffset(),
            getBuffer(),
            cursor(),
            numBytes);
    increaseCursor(numBytes);
  }

  // 写出对齐的数据
  private void writeAlignedBytes(
      int ordinal,
      Object baseObject,
      long baseOffset,
      int numBytes) {
    // 1. 进行扩容
    grow(numBytes);
    // 2. 将需要写出的对象复制一个副本
    Platform.copyMemory(baseObject, baseOffset, getBuffer(), cursor(), numBytes);
    // 3. 设置偏移量并写出
    setOffsetAndSize(ordinal, numBytes);
    // 4. 移动写出指针
    increaseCursor(numBytes);
  }

  protected final void writeBoolean(long offset, boolean value) {
    Platform.putBoolean(getBuffer(), offset, value);
  }

  protected final void writeByte(long offset, byte value) {
    Platform.putByte(getBuffer(), offset, value);
  }

  protected final void writeShort(long offset, short value) {
    Platform.putShort(getBuffer(), offset, value);
  }

  protected final void writeInt(long offset, int value) {
    Platform.putInt(getBuffer(), offset, value);
  }

  protected final void writeLong(long offset, long value) {
    Platform.putLong(getBuffer(), offset, value);
  }

  protected final void writeFloat(long offset, float value) {
    Platform.putFloat(getBuffer(), offset, value);
  }

  protected final void writeDouble(long offset, double value) {
    Platform.putDouble(getBuffer(), offset, value);
  }
}
