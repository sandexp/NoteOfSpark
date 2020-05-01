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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;

import static org.apache.spark.sql.catalyst.expressions.UnsafeArrayData.calculateHeaderPortionInBytes;

/**
 * 非安全的数组写出器:
 * 这是一个辅助类,用于写出数据到全局行缓冲区中,使用@UnsafeArrayData 的格式
 */
public final class UnsafeArrayWriter extends UnsafeWriter {

  /**
   * 属性列表:
   * @numElements 数组元素数量
   * @elementSize 数组元素大小
   * @headerInBytes 字节header大小
   */
  private int numElements;
  private int elementSize;
  private int headerInBytes;

  // 断言指定索引,是否是合法的数组索引
  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numElements : "index (" + index + ") should < " + numElements;
  }

  public UnsafeArrayWriter(UnsafeWriter writer, int elementSize) {
    super(writer.getBufferHolder());
    this.elementSize = elementSize;
  }


  /**
   * 初始化指定数量的数组
   */
  public void initialize(int numElements) {
    // 需要8个字节在header中存储元素数量
    /*
      1. 初始化元素数量,header的字节数量,以及起始的偏移量
     */
    this.numElements = numElements;
    this.headerInBytes = calculateHeaderPortionInBytes(numElements);
    this.startingOffset = cursor();

    /*
      2. 确定需要扩容的数据大小num为:
        num=elementSize * numElements
     */
    int fixedPartInBytes =
      ByteArrayMethods.roundNumberOfBytesToNearestWord(elementSize * numElements);
    /*
      3. 对于header和指定大小的数据进行缓冲区的全局扩容
      需要扩展的容量cap为
      cap= header字节数量@headerInBytes + 需要扩展的数据大小@fixedPartInBytes
     */
    holder.grow(headerInBytes + fixedPartInBytes);

    // 4. 写出元素数量,并且情况数据部分的内容,仅仅保留header部分
    Platform.putLong(getBuffer(), startingOffset, numElements);

    // 5. header字节的处理,将8个位置都写为0
    for (int i = 8; i < headerInBytes; i += 8) {
      Platform.putLong(getBuffer(), startingOffset + i, 0L);
    }

    // 6. 填充数据部分为0(字节)
    for (int i = elementSize * numElements; i < fixedPartInBytes; i++) {
      Platform.putByte(getBuffer(), startingOffset + headerInBytes + i, (byte) 0);
    }
    // 7. 移动数组指针到新的位置(这条记录的末尾)
    increaseCursor(headerInBytes + fixedPartInBytes);
  }

  // 计算指定位置@ordinal 的偏移量
  private long getElementOffset(int ordinal) {
    return startingOffset + headerInBytes + ordinal * (long) elementSize;
  }

  // 设置指定索引处@ordinal 的值为空
  private void setNullBit(int ordinal) {
    assertIndexIsValid(ordinal);
    BitSetMethods.set(getBuffer(), startingOffset + 8, ordinal);
  }

  // 设置指定索引处@ordinal 的值为null,并写出0(byte)
  @Override
  public void setNull1Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    writeByte(getElementOffset(ordinal), (byte)0);
  }

  // 设置指定索引处@ordinal 的值为null,并写出0(short)
  @Override
  public void setNull2Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    writeShort(getElementOffset(ordinal), (short)0);
  }

  // 设置指定索引处@ordinal 的值为null,并写出0(int)
  @Override
  public void setNull4Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    writeInt(getElementOffset(ordinal), 0);
  }

  // 设置指定索引处@ordinal 的值为null,并写出0(long)
  @Override
  public void setNull8Bytes(int ordinal) {
    setNullBit(ordinal);
    // put zero into the corresponding field when set null
    writeLong(getElementOffset(ordinal), 0);
  }

  public void setNull(int ordinal) { setNull8Bytes(ordinal); }

  // 在指定索引处写出指定的bool类型值@value
  @Override
  public void write(int ordinal, boolean value) {
    assertIndexIsValid(ordinal);
    writeBoolean(getElementOffset(ordinal), value);
  }

  // 在指定索引处写出指定的byte类型值为@value
  @Override
  public void write(int ordinal, byte value) {
    assertIndexIsValid(ordinal);
    writeByte(getElementOffset(ordinal), value);
  }

  // 在指定索引处写出指定的short类型值为@value
  @Override
  public void write(int ordinal, short value) {
    assertIndexIsValid(ordinal);
    writeShort(getElementOffset(ordinal), value);
  }

  // 在指定索引处写出指定的int类型值为@value
  @Override
  public void write(int ordinal, int value) {
    assertIndexIsValid(ordinal);
    writeInt(getElementOffset(ordinal), value);
  }

  // 在指定索引处写出指定的long类型值为@value
  @Override
  public void write(int ordinal, long value) {
    assertIndexIsValid(ordinal);
    writeLong(getElementOffset(ordinal), value);
  }

  // 在指定索引处写出指定的float类型值@value
  @Override
  public void write(int ordinal, float value) {
    assertIndexIsValid(ordinal);
    writeFloat(getElementOffset(ordinal), value);
  }

  // 在指定索引处写出指定的double类型值@value
  @Override
  public void write(int ordinal, double value) {
    assertIndexIsValid(ordinal);
    writeDouble(getElementOffset(ordinal), value);
  }

  @Override
  public void write(int ordinal, Decimal input, int precision, int scale) {
    // make sure Decimal object has the same scale as DecimalType
    assertIndexIsValid(ordinal);
    if (input != null && input.changePrecision(precision, scale)) {
      if (precision <= Decimal.MAX_LONG_DIGITS()) {
        write(ordinal, input.toUnscaledLong());
      } else {
        // 处理十进制数溢出的问题
        final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
        final int numBytes = bytes.length;
        assert numBytes <= 16;
        int roundedSize = ByteArrayMethods.roundNumberOfBytesToNearestWord(numBytes);
        holder.grow(roundedSize);

        zeroOutPaddingBytes(numBytes);

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, getBuffer(), cursor(), numBytes);
        setOffsetAndSize(ordinal, numBytes);

        // move the cursor forward with 8-bytes boundary
        increaseCursor(roundedSize);
      }
    } else {
      setNull(ordinal);
    }
  }
}
