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
package org.apache.spark.sql.vectorized;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * 列式向量表
 * 这个接口代表了spark的内存列式数据.这个接口定义了主要的api,使用这些api可以获取数据,同时可以获取批次版本信息.
 * 批次版本信息一般是快速的,优选的.
 * 大多数API会使用行编号@rowId 作为参数.对于列式向量表@ColumnVector 中,批次的起始行号为0.
 * spark仅仅根据列式向量表@ColumnVector 的数据类型调用@get方法.例如,如果是int类型,spark回去调用@getInt方法或者是
 * 方法@getInts(int,int).
 * 列式向两边支持所有的数据类型,为了处理嵌入式的类型,列式向量表存在有子节点,且是一个树状结构.请参考@getStruct 方法和@getArray方法
 * 以及@getMap 方法.
 * 列式向量表@ColumnVector在数据加载的过程中都是可以重用的,这样用于避免重复地分配内存.
 * 列式向量表@ColumnVector 意味着CPU效率的最大化,但是不会降低存储的覆盖.实现需要优先计算效率,其次是存储效率.因为
 * 列式向量表@ColumnVector在数据加载过程中可以重新使用.占用存储的大小,相对来说不需要考虑.
 */
@Evolving
public abstract class ColumnVector implements AutoCloseable {

  /**
   * 返回列式向量表的数据类型
   */
  public final DataType dataType() { return type; }

  /**
   *清空列向量表的内存,列式向量表在清空之后不可用
   *
   * This overwrites `AutoCloseable.close` to remove the `throws` clause, as column vector is
   * in-memory and we don't expect any exception to happen during closing.
   */
  @Override
  public abstract void close();

  /**
   * 确定列式向量表中是否包含空值
   */
  public abstract boolean hasNull();

  /**
   * 返回列式向量表中空值的数量
   */
  public abstract int numNulls();

  /**
   * 确定是否指定行的value值为空
   */
  public abstract boolean isNullAt(int rowId);


  /**
   * 返回指定行@rowId的bool类型值
   */
  public abstract boolean getBoolean(int rowId);

  /**
   * 获取指定行@rowId开始的指定数量@count的bool值
   */
  public boolean[] getBooleans(int rowId, int count) {
    boolean[] res = new boolean[count];
    for (int i = 0; i < count; i++) {
      res[i] = getBoolean(rowId + i);
    }
    return res;
  }

  /**
   * Returns the byte type value for rowId. The return value is undefined and can be anything,
   * if the slot for rowId is null.
   */
  public abstract byte getByte(int rowId);

  /**
   * Gets byte type values from [rowId, rowId + count). The return values for the null slots
   * are undefined and can be anything.
   */
  public byte[] getBytes(int rowId, int count) {
    byte[] res = new byte[count];
    for (int i = 0; i < count; i++) {
      res[i] = getByte(rowId + i);
    }
    return res;
  }

  /**
   * Returns the short type value for rowId. The return value is undefined and can be anything,
   * if the slot for rowId is null.
   */
  public abstract short getShort(int rowId);

  /**
   * Gets short type values from [rowId, rowId + count). The return values for the null slots
   * are undefined and can be anything.
   */
  public short[] getShorts(int rowId, int count) {
    short[] res = new short[count];
    for (int i = 0; i < count; i++) {
      res[i] = getShort(rowId + i);
    }
    return res;
  }

  // 获取指定行(行范围)内的value数据
  /**
   * Returns the int type value for rowId. The return value is undefined and can be anything,
   * if the slot for rowId is null.
   */
  public abstract int getInt(int rowId);

  /**
   * Gets int type values from [rowId, rowId + count). The return values for the null slots
   * are undefined and can be anything.
   */
  public int[] getInts(int rowId, int count) {
    int[] res = new int[count];
    for (int i = 0; i < count; i++) {
      res[i] = getInt(rowId + i);
    }
    return res;
  }

  /**
   * Returns the long type value for rowId. The return value is undefined and can be anything,
   * if the slot for rowId is null.
   */
  public abstract long getLong(int rowId);

  /**
   * Gets long type values from [rowId, rowId + count). The return values for the null slots
   * are undefined and can be anything.
   */
  public long[] getLongs(int rowId, int count) {
    long[] res = new long[count];
    for (int i = 0; i < count; i++) {
      res[i] = getLong(rowId + i);
    }
    return res;
  }

  /**
   * Returns the float type value for rowId. The return value is undefined and can be anything,
   * if the slot for rowId is null.
   */
  public abstract float getFloat(int rowId);

  /**
   * Gets float type values from [rowId, rowId + count). The return values for the null slots
   * are undefined and can be anything.
   */
  public float[] getFloats(int rowId, int count) {
    float[] res = new float[count];
    for (int i = 0; i < count; i++) {
      res[i] = getFloat(rowId + i);
    }
    return res;
  }

  /**
   * Returns the double type value for rowId. The return value is undefined and can be anything,
   * if the slot for rowId is null.
   */
  public abstract double getDouble(int rowId);

  /**
   * Gets double type values from [rowId, rowId + count). The return values for the null slots
   * are undefined and can be anything.
   */
  public double[] getDoubles(int rowId, int count) {
    double[] res = new double[count];
    for (int i = 0; i < count; i++) {
      res[i] = getDouble(rowId + i);
    }
    return res;
  }

  /**
   * Returns the struct type value for rowId. If the slot for rowId is null, it should return null.
   *
   * To support struct type, implementations must implement {@link #getChild(int)} and make this
   * vector a tree structure. The number of child vectors must be same as the number of fields of
   * the struct type, and each child vector is responsible to store the data for its corresponding
   * struct field.
   */
  public final ColumnarRow getStruct(int rowId) {
    if (isNullAt(rowId)) return null;
    return new ColumnarRow(this, rowId);
  }

  /**
   * Returns the array type value for rowId. If the slot for rowId is null, it should return null.
   *
   * To support array type, implementations must construct an {@link ColumnarArray} and return it in
   * this method. {@link ColumnarArray} requires a {@link ColumnVector} that stores the data of all
   * the elements of all the arrays in this vector, and an offset and length which points to a range
   * in that {@link ColumnVector}, and the range represents the array for rowId. Implementations
   * are free to decide where to put the data vector and offsets and lengths. For example, we can
   * use the first child vector as the data vector, and store offsets and lengths in 2 int arrays in
   * this vector.
   */
  public abstract ColumnarArray getArray(int rowId);

  /**
   * Returns the map type value for rowId. If the slot for rowId is null, it should return null.
   *
   * In Spark, map type value is basically a key data array and a value data array. A key from the
   * key array with a index and a value from the value array with the same index contribute to
   * an entry of this map type value.
   *
   * To support map type, implementations must construct a {@link ColumnarMap} and return it in
   * this method. {@link ColumnarMap} requires a {@link ColumnVector} that stores the data of all
   * the keys of all the maps in this vector, and another {@link ColumnVector} that stores the data
   * of all the values of all the maps in this vector, and a pair of offset and length which
   * specify the range of the key/value array that belongs to the map type value at rowId.
   */
  public abstract ColumnarMap getMap(int ordinal);

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  public abstract Decimal getDecimal(int rowId, int precision, int scale);

  /**
   * Returns the string type value for rowId. If the slot for rowId is null, it should return null.
   * Note that the returned UTF8String may point to the data of this column vector, please copy it
   * if you want to keep it after this column vector is freed.
   */
  public abstract UTF8String getUTF8String(int rowId);

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  public abstract byte[] getBinary(int rowId);

  /**
   * Returns the calendar interval type value for rowId. If the slot for rowId is null, it should
   * return null.
   *
   * In Spark, calendar interval type value is basically two integer values representing the number
   * of months and days in this interval, and a long value representing the number of microseconds
   * in this interval. An interval type vector is the same as a struct type vector with 3 fields:
   * `months`, `days` and `microseconds`.
   *
   * To support interval type, implementations must implement {@link #getChild(int)} and define 3
   * child vectors: the first child vector is an int type vector, containing all the month values of
   * all the interval values in this vector. The second child vector is an int type vector,
   * containing all the day values of all the interval values in this vector. The third child vector
   * is a long type vector, containing all the microsecond values of all the interval values in this
   * vector.
   */
  public final CalendarInterval getInterval(int rowId) {
    if (isNullAt(rowId)) return null;
    final int months = getChild(0).getInt(rowId);
    final int days = getChild(1).getInt(rowId);
    final long microseconds = getChild(2).getLong(rowId);
    return new CalendarInterval(months, days, microseconds);
  }

  // 获取指定索引处的子列式向量表@ColumnVector
  public abstract ColumnVector getChild(int ordinal);

  /**
   * 当前列的数据类型
   */
  protected DataType type;

  /**
   * 设置这个列式向量表的数据类型
   */
  protected ColumnVector(DataType type) {
    this.type = type;
  }
}
