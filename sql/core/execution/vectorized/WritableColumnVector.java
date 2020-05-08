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
package org.apache.spark.sql.execution.vectorized;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * 这个类定义了列式向量表的写出API.支持所有类型,且包含所有put的API,同时支持批量put版本
 * 批量版本是优选.
 * 容量: 数据是密集型存储的,但是数组不是定容量的.需要调用reserve()去确认存在有足够的空间去添加元素.
 * 意味着put方法是不会对这个进行检查的
 * 一个可写的列式向量表一旦创建,需要被认作为不可变的.换句话说,在读取之后调用put是不合法的,除非使用reset重置
 * 可写列式向量表可以重用
 */
public abstract class WritableColumnVector extends ColumnVector {

  /**
   * 重置列式向量表,用于写操作.当前存储值不会可用
   */
  public void reset() {
    if (isConstant) return;

    if (childColumns != null) {
      for (ColumnVector c: childColumns) {
        ((WritableColumnVector) c).reset();
      }
    }
    elementsAppended = 0;
    if (numNulls > 0) {
      putNotNulls(0, capacity);
      numNulls = 0;
    }
  }

  // 关闭列式向量表
  @Override
  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    if (dictionaryIds != null) {
      dictionaryIds.close();
      dictionaryIds = null;
    }
    dictionary = null;
  }

  // 预留指定的内存空间
  public void reserve(int requiredCapacity) {
    if (requiredCapacity < 0) {
      throwUnsupportedException(requiredCapacity, null);
    } else if (requiredCapacity > capacity) {
      // 确定新的容量
      int newCapacity = (int) Math.min(MAX_CAPACITY, requiredCapacity * 2L);
      if (requiredCapacity <= newCapacity) {
        try {
          // 如果需求的容量小于新容量,则内部预留新的容量的存储空间
          reserveInternal(newCapacity);
        } catch (OutOfMemoryError outOfMemoryError) {
          throwUnsupportedException(requiredCapacity, outOfMemoryError);
        }
      } else {
        throwUnsupportedException(requiredCapacity, null);
      }
    }
  }

  private void throwUnsupportedException(int requiredCapacity, Throwable cause) {
    String message = "Cannot reserve additional contiguous bytes in the vectorized reader (" +
        (requiredCapacity >= 0 ? "requested " + requiredCapacity + " bytes" : "integer overflow") +
        "). As a workaround, you can reduce the vectorized reader batch size, or disable the " +
        "vectorized reader, or disable " + SQLConf.BUCKETING_ENABLED().key() + " if you read " +
        "from bucket table. For Parquet file format, refer to " +
        SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().key() +
        " (default " + SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().defaultValueString() +
        ") and " + SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key() + "; for ORC file format, " +
        "refer to " + SQLConf.ORC_VECTORIZED_READER_BATCH_SIZE().key() +
        " (default " + SQLConf.ORC_VECTORIZED_READER_BATCH_SIZE().defaultValueString() +
        ") and " + SQLConf.ORC_VECTORIZED_READER_ENABLED().key() + ".";
    throw new RuntimeException(message, cause);
  }

  @Override
  public boolean hasNull() {
    return numNulls > 0;
  }

  @Override
  public int numNulls() { return numNulls; }

  /**
   * 返回行编号的目录id
   * 当@represents 代码目录列表的时候会被调用.有相应的方法使用.参考spark-16928
   */
  public abstract int getDictId(int rowId);

  /**
   * 当前列的目录
   * 如果非空,需要对其编码
   * If it's not null, will be used to decode the value in getXXX().
   */
  protected Dictionary dictionary;

  /**
   * 重用目录列的id列表
   */
  protected WritableColumnVector dictionaryIds;

  /**
   * 如果目录有一个目录则返回true
   */
  public boolean hasDictionary() { return this.dictionary != null; }

  /**
   * 返回目录类别的底层结构
   */
  public WritableColumnVector getDictionaryIds() {
    return dictionaryIds;
  }

  /**
   * 更新目录信息
   */
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  /**
   * 对于目录类别,预留指定大小的空间
   */
  public WritableColumnVector reserveDictionaryIds(int capacity) {
    if (dictionaryIds == null) {
      dictionaryIds = reserveNewColumn(capacity, DataTypes.IntegerType);
    } else {
      dictionaryIds.reset();
      dictionaryIds.reserve(capacity);
    }
    return dictionaryIds;
  }

  /**
   * 保证有足够的空间去存储指定容量的元素.也就是说,put方法必须对于所有rowId起效
   */
  protected abstract void reserveInternal(int capacity);


  /**
   各种插入值,添加值,获取值的方法
   *
   */
  /**
   * Sets null/not null to the value at rowId.
   */
  public abstract void putNotNull(int rowId);
  public abstract void putNull(int rowId);

  /**
   * Sets null/not null to the values at [rowId, rowId + count).
   */
  public abstract void putNulls(int rowId, int count);
  public abstract void putNotNulls(int rowId, int count);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putBoolean(int rowId, boolean value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putBooleans(int rowId, int count, boolean value);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putByte(int rowId, byte value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putBytes(int rowId, int count, byte value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putBytes(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putShort(int rowId, short value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putShorts(int rowId, int count, short value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putShorts(int rowId, int count, short[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 2]) to [rowId, rowId + count)
   * The data in src must be 2-byte platform native endian shorts.
   */
  public abstract void putShorts(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putInt(int rowId, int value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putInts(int rowId, int count, int value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putInts(int rowId, int count, int[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be 4-byte platform native endian ints.
   */
  public abstract void putInts(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be 4-byte little endian ints.
   */
  public abstract void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putLong(int rowId, long value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putLongs(int rowId, int count, long value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putLongs(int rowId, int count, long[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
   * The data in src must be 8-byte platform native endian longs.
   */
  public abstract void putLongs(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src + srcIndex, src + srcIndex + count * 8) to [rowId, rowId + count)
   * The data in src must be 8-byte little endian longs.
   */
  public abstract void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putFloat(int rowId, float value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putFloats(int rowId, int count, float value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putFloats(int rowId, int count, float[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be ieee formatted floats in platform native endian.
   */
  public abstract void putFloats(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  public abstract void putDouble(int rowId, double value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  public abstract void putDoubles(int rowId, int count, double value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  public abstract void putDoubles(int rowId, int count, double[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
   * The data in src must be ieee formatted doubles in platform native endian.
   */
  public abstract void putDoubles(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Puts a byte array that already exists in this column.
   */
  public abstract void putArray(int rowId, int offset, int length);

  /**
   * Sets values from [value + offset, value + offset + count) to the values at rowId.
   */
  public abstract int putByteArray(int rowId, byte[] value, int offset, int count);
  public final int putByteArray(int rowId, byte[] value) {
    return putByteArray(rowId, value, 0, value.length);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      // TODO: best perf?
      byte[] bytes = getBinary(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  public void putDecimal(int rowId, Decimal value, int precision) {
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      putInt(rowId, (int) value.toUnscaledLong());
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      putLong(rowId, value.toUnscaledLong());
    } else {
      BigInteger bigInteger = value.toJavaBigDecimal().unscaledValue();
      putByteArray(rowId, bigInteger.toByteArray());
    }
  }

  public void putInterval(int rowId, CalendarInterval value) {
    getChild(0).putInt(rowId, value.months);
    getChild(1).putInt(rowId, value.days);
    getChild(2).putLong(rowId, value.microseconds);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) return null;
    if (dictionary == null) {
      return arrayData().getBytesAsUTF8String(getArrayOffset(rowId), getArrayLength(rowId));
    } else {
      byte[] bytes = dictionary.decodeToBinary(dictionaryIds.getDictId(rowId));
      return UTF8String.fromBytes(bytes);
    }
  }

  /**
   * Gets the values of bytes from [rowId, rowId + count), as a UTF8String.
   * This method is similar to {@link ColumnVector#getBytes(int, int)}, but can save data copy as
   * UTF8String is used as a pointer.
   */
  protected abstract UTF8String getBytesAsUTF8String(int rowId, int count);

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    if (dictionary == null) {
      return arrayData().getBytes(getArrayOffset(rowId), getArrayLength(rowId));
    } else {
      return dictionary.decodeToBinary(dictionaryIds.getDictId(rowId));
    }
  }

  /**
   * Append APIs. These APIs all behave similarly and will append data to the current vector.  It
   * is not valid to mix the put and append APIs. The append APIs are slower and should only be
   * used if the sizes are not known up front.
   * In all these cases, the return value is the rowId for the first appended element.
   */
  public final int appendNull() {
    assert (!(dataType() instanceof StructType)); // Use appendStruct()
    reserve(elementsAppended + 1);
    putNull(elementsAppended);
    return elementsAppended++;
  }

  public final int appendNotNull() {
    reserve(elementsAppended + 1);
    putNotNull(elementsAppended);
    return elementsAppended++;
  }

  public final int appendNulls(int count) {
    assert (!(dataType() instanceof StructType));
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putNulls(elementsAppended, count);
    elementsAppended += count;
    return result;
  }

  public final int appendNotNulls(int count) {
    assert (!(dataType() instanceof StructType));
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putNotNulls(elementsAppended, count);
    elementsAppended += count;
    return result;
  }

  public final int appendBoolean(boolean v) {
    reserve(elementsAppended + 1);
    putBoolean(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendBooleans(int count, boolean v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putBooleans(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendByte(byte v) {
    reserve(elementsAppended + 1);
    putByte(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendBytes(int count, byte v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putBytes(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendBytes(int length, byte[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putBytes(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendShort(short v) {
    reserve(elementsAppended + 1);
    putShort(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendShorts(int count, short v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putShorts(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendShorts(int length, short[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putShorts(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendInt(int v) {
    reserve(elementsAppended + 1);
    putInt(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendInts(int count, int v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putInts(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendInts(int length, int[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putInts(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendLong(long v) {
    reserve(elementsAppended + 1);
    putLong(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendLongs(int count, long v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putLongs(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendLongs(int length, long[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putLongs(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendFloat(float v) {
    reserve(elementsAppended + 1);
    putFloat(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendFloats(int count, float v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putFloats(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendFloats(int length, float[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putFloats(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendDouble(double v) {
    reserve(elementsAppended + 1);
    putDouble(elementsAppended, v);
    return elementsAppended++;
  }

  public final int appendDoubles(int count, double v) {
    reserve(elementsAppended + count);
    int result = elementsAppended;
    putDoubles(elementsAppended, count, v);
    elementsAppended += count;
    return result;
  }

  public final int appendDoubles(int length, double[] src, int offset) {
    reserve(elementsAppended + length);
    int result = elementsAppended;
    putDoubles(elementsAppended, length, src, offset);
    elementsAppended += length;
    return result;
  }

  public final int appendByteArray(byte[] value, int offset, int length) {
    int copiedOffset = arrayData().appendBytes(length, value, offset);
    reserve(elementsAppended + 1);
    putArray(elementsAppended, copiedOffset, length);
    return elementsAppended++;
  }

  public final int appendArray(int length) {
    reserve(elementsAppended + 1);
    putArray(elementsAppended, arrayData().elementsAppended, length);
    return elementsAppended++;
  }

  /**
   * Appends a NULL struct. This *has* to be used for structs instead of appendNull() as this
   * recursively appends a NULL to its children.
   * We don't have this logic as the general appendNull implementation to optimize the more
   * common non-struct case.
   */
  public final int appendStruct(boolean isNull) {
    if (isNull) {
      // This is the same as appendNull but without the assertion for struct types
      reserve(elementsAppended + 1);
      putNull(elementsAppended);
      elementsAppended++;
      for (WritableColumnVector c: childColumns) {
        if (c.type instanceof StructType) {
          c.appendStruct(true);
        } else {
          c.appendNull();
        }
      }
    } else {
      appendNotNull();
    }
    return elementsAppended;
  }

  // `WritableColumnVector` puts the data of array in the first child column vector, and puts the
  // array offsets and lengths in the current column vector.
  @Override
  public final ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) return null;
    return new ColumnarArray(arrayData(), getArrayOffset(rowId), getArrayLength(rowId));
  }

  // `WritableColumnVector` puts the key array in the first child column vector, value array in the
  // second child column vector, and puts the offsets and lengths in the current column vector.
  @Override
  public final ColumnarMap getMap(int rowId) {
    if (isNullAt(rowId)) return null;
    return new ColumnarMap(getChild(0), getChild(1), getArrayOffset(rowId), getArrayLength(rowId));
  }

  public WritableColumnVector arrayData() {
    return childColumns[0];
  }

  public abstract int getArrayLength(int rowId);

  public abstract int getArrayOffset(int rowId);

  @Override
  public WritableColumnVector getChild(int ordinal) { return childColumns[ordinal]; }

  /**
   * Returns the elements appended.
   */
  public final int getElementsAppended() { return elementsAppended; }

  /**
   * Marks this column as being constant.
   */
  public final void setIsConstant() { isConstant = true; }

  /**
   * 行的最大数量
   * Maximum number of rows that can be stored in this column.
   */
  protected int capacity;

  /**
   * 当前列的最大容量上限
   */
  @VisibleForTesting
  protected int MAX_CAPACITY = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

  /**
   * 列中空值的数量,这个是对读取器的优化,可以跳过空值
   */
  protected int numNulls;

  /**
   * 如果value是定长的则返回true,否则返回false
   */
  protected boolean isConstant;

  /**
   * 每个数组大小的默认长度,按照需求增长
   */
  protected static final int DEFAULT_ARRAY_LENGTH = 4;

  /**
   * 添加数据的时候,当前的写指针
   */
  protected int elementsAppended;

  /**
   * 如果有嵌入式类,则指向子列
   */
  protected WritableColumnVector[] childColumns;

  /**
   * Reserve a new column.
   */
  protected abstract WritableColumnVector reserveNewColumn(int capacity, DataType type);

  protected boolean isArray() {
    return type instanceof ArrayType || type instanceof BinaryType || type instanceof StringType ||
      DecimalType.isByteArrayDecimalType(type);
  }

  /**
   * Sets up the common state and also handles creating the child columns if this is a nested
   * type.
   */
  protected WritableColumnVector(int capacity, DataType type) {
    super(type);
    this.capacity = capacity;

    if (isArray()) {
      DataType childType;
      int childCapacity = capacity;
      if (type instanceof ArrayType) {
        childType = ((ArrayType)type).elementType();
      } else {
        childType = DataTypes.ByteType;
        childCapacity *= DEFAULT_ARRAY_LENGTH;
      }
      this.childColumns = new WritableColumnVector[1];
      this.childColumns[0] = reserveNewColumn(childCapacity, childType);
    } else if (type instanceof StructType) {
      StructType st = (StructType)type;
      this.childColumns = new WritableColumnVector[st.fields().length];
      for (int i = 0; i < childColumns.length; ++i) {
        this.childColumns[i] = reserveNewColumn(capacity, st.fields()[i].dataType());
      }
    } else if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      this.childColumns = new WritableColumnVector[2];
      this.childColumns[0] = reserveNewColumn(capacity, mapType.keyType());
      this.childColumns[1] = reserveNewColumn(capacity, mapType.valueType());
    } else if (type instanceof CalendarIntervalType) {
      // Three columns. Months as int. Days as Int. Microseconds as Long.
      this.childColumns = new WritableColumnVector[3];
      this.childColumns[0] = reserveNewColumn(capacity, DataTypes.IntegerType);
      this.childColumns[1] = reserveNewColumn(capacity, DataTypes.IntegerType);
      this.childColumns[2] = reserveNewColumn(capacity, DataTypes.LongType);
    } else {
      this.childColumns = null;
    }
  }
}
