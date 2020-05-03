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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * 有向(树形)列式向量表.
 * 当前不支持日期类型和map数据类型
 *
 * 参数列表:
 * @accessor 有向向量表存储器
 * @childColumns 有向向量表的子列
 */
public final class ArrowColumnVector extends ColumnVector {

  private final ArrowVectorAccessor accessor;
  private ArrowColumnVector[] childColumns;

  // 确定有向向量表中是否含有空值
  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  // 获取有向向量表空值的数量
  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }


  // 关闭有向列式向量表
  @Override
  public void close() {
    // 1. 置空子列
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    // 2. 关闭当前列
    accessor.close();
  }

  // 获取指定行@rowId的数据
  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getBinary(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException();
  }

  // 获取指定索引@ordinal处的子列信息
  @Override
  public ArrowColumnVector getChild(int ordinal) { return childColumns[ordinal]; }

  public ArrowColumnVector(ValueVector vector) {
    super(ArrowUtils.fromArrowField(vector.getField()));

    if (vector instanceof BitVector) {
      accessor = new BooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      accessor = new ByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      accessor = new ShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      accessor = new IntAccessor((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      accessor = new FloatAccessor((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      accessor = new TimestampAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new ArrayAccessor(listVector);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      accessor = new StructAccessor(structVector);

      childColumns = new ArrowColumnVector[structVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new ArrowColumnVector(structVector.getVectorById(i));
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  // 有向向量表的根节点
  private abstract static class ArrowVectorAccessor {

    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  // 各种类型的存储器(底层存储单元为向量表)
  private static class BooleanAccessor extends ArrowVectorAccessor {

    // 向量表
    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  private static class ByteAccessor extends ArrowVectorAccessor {

    private final TinyIntVector accessor;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class ShortAccessor extends ArrowVectorAccessor {

    private final SmallIntVector accessor;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class IntAccessor extends ArrowVectorAccessor {

    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class LongAccessor extends ArrowVectorAccessor {

    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class FloatAccessor extends ArrowVectorAccessor {

    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DoubleAccessor extends ArrowVectorAccessor {

    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DecimalAccessor extends ArrowVectorAccessor {

    private final DecimalVector accessor;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {

    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
          stringResult.buffer.memoryAddress() + stringResult.start,
          stringResult.end - stringResult.start);
      }
    }
  }

  private static class BinaryAccessor extends ArrowVectorAccessor {

    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static class DateAccessor extends ArrowVectorAccessor {

    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class TimestampAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroTZVector accessor;

    TimestampAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class ArrayAccessor extends ArrowVectorAccessor {

    private final ListVector accessor;
    private final ArrowColumnVector arrayData;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
      this.arrayData = new ArrowColumnVector(vector.getDataVector());
    }

    @Override
    final boolean isNullAt(int rowId) {
      // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (accessor.getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }

    @Override
    final ColumnarArray getArray(int rowId) {
      ArrowBuf offsets = accessor.getOffsetBuffer();
      int index = rowId * ListVector.OFFSET_WIDTH;
      int start = offsets.getInt(index);
      int end = offsets.getInt(index + ListVector.OFFSET_WIDTH);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * Access struct values in a ArrowColumnVector doesn't use this accessor. Instead, it uses
   * getStruct() method defined in the parent class. Any call to "get" method in this class is a
   * bug in the code.
   *
   */
  private static class StructAccessor extends ArrowVectorAccessor {

    StructAccessor(StructVector vector) {
      super(vector);
    }
  }
}
