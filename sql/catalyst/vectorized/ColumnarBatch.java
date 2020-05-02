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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 列批次
 *
 * 这个类包装了多个列向量表@ColumnVector,用作只能的行表.提供批量的行视图,这样spark就可以按照行获取数据.
 * 这个类的实例,可以在整个数据加载过程中进行重新使用.
 */
@Evolving
public final class ColumnarBatch implements AutoCloseable {
  /**
   * 属性列表
   * @numRows 行数量
   * @columns 列向量组
   * @row 列批次的行数据,可以使用getRow获取
   */
  private int numRows;
  private final ColumnVector[] columns;
  private final ColumnarBatchRow row;

  /**
   * 调用去关闭这个批次的所有列,在调用之后,就不能获取数据了.必须在最后调用,用于清除分配的内存.
   */
  @Override
  public void close() {
    for (ColumnVector c: columns) {
      c.close();
    }
  }

  /**
   * 返回这个批次的迭代器,用于对行的迭代
   */
  public Iterator<InternalRow> rowIterator() {
    final int maxRows = numRows;
    final ColumnarBatchRow row = new ColumnarBatchRow(columns);
    return new Iterator<InternalRow>() {
      // 行id编号,默认指向0处
      int rowId = 0;

      // 确定是否存在下一行数据
      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      // 获取当前行的数据
      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        row.rowId = rowId++;
        return row;
      }
      // 迭代器不支持移除元素的操作
      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * 设置批次行数量
   */
  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }

  /**
   * 返回这个批次的列的数量
   */
  public int numCols() { return columns.length; }

  /**
   * 返回读取的行数,包括其中过滤掉的行
   */
  public int numRows() { return numRows; }

  /**
   * 返回指定索引处的列
   */
  public ColumnVector column(int ordinal) { return columns[ordinal]; }

  /**
   * 返回批次中在指定行@rowId处的行数据,返回的行可以反复调用
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  public InternalRow getRow(int rowId) {
    assert(rowId >= 0 && rowId < numRows);
    row.rowId = rowId;
    return row;
  }


  public ColumnarBatch(ColumnVector[] columns) {
    this(columns, 0);
  }

  /**
   * 从存在的列向量表中,创建新的批次
   * @param columns 批次的列数量
   * @param numRows 批次的行数量
   */
  public ColumnarBatch(ColumnVector[] columns, int numRows) {
    this.columns = columns;
    this.numRows = numRows;
    this.row = new ColumnarBatchRow(columns);
  }
}

/**
 * 列式存储的批量行类
 *
 * 内部类,包装了列向量表@ColumnVector,用于提供行视图
 * 参数列表:
 * @rowId 行编号
 * @columns 列向量表
 */
class ColumnarBatchRow extends InternalRow {
  public int rowId;
  private final ColumnVector[] columns;

  ColumnarBatchRow(ColumnVector[] columns) {
    this.columns = columns;
  }

  // 获取列的数量(即行中属性的数量)
  @Override
  public int numFields() { return columns.length; }

  // 复制行数据
  @Override
  public InternalRow copy() {
    GenericInternalRow row = new GenericInternalRow(columns.length);
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = columns[i].dataType();
        if (dt instanceof BooleanType) {
          row.setBoolean(i, getBoolean(i));
        } else if (dt instanceof ByteType) {
          row.setByte(i, getByte(i));
        } else if (dt instanceof ShortType) {
          row.setShort(i, getShort(i));
        } else if (dt instanceof IntegerType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof LongType) {
          row.setLong(i, getLong(i));
        } else if (dt instanceof FloatType) {
          row.setFloat(i, getFloat(i));
        } else if (dt instanceof DoubleType) {
          row.setDouble(i, getDouble(i));
        } else if (dt instanceof StringType) {
          row.update(i, getUTF8String(i).copy());
        } else if (dt instanceof BinaryType) {
          row.update(i, getBinary(i));
        } else if (dt instanceof DecimalType) {
          DecimalType t = (DecimalType)dt;
          row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (dt instanceof DateType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof TimestampType) {
          row.setLong(i, getLong(i));
        } else {
          throw new RuntimeException("Not implemented. " + dt);
        }
      }
    }
    return row;
  }

  // 获取行中指定列的数据
  @Override
  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int ordinal) { return columns[ordinal].isNullAt(rowId); }

  @Override
  public boolean getBoolean(int ordinal) { return columns[ordinal].getBoolean(rowId); }

  @Override
  public byte getByte(int ordinal) { return columns[ordinal].getByte(rowId); }

  @Override
  public short getShort(int ordinal) { return columns[ordinal].getShort(rowId); }

  @Override
  public int getInt(int ordinal) { return columns[ordinal].getInt(rowId); }

  @Override
  public long getLong(int ordinal) { return columns[ordinal].getLong(rowId); }

  @Override
  public float getFloat(int ordinal) { return columns[ordinal].getFloat(rowId); }

  @Override
  public double getDouble(int ordinal) { return columns[ordinal].getDouble(rowId); }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return columns[ordinal].getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return columns[ordinal].getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return columns[ordinal].getBinary(rowId);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return columns[ordinal].getInterval(rowId);
  }

  @Override
  public ColumnarRow getStruct(int ordinal, int numFields) {
    return columns[ordinal].getStruct(rowId);
  }

  @Override
  public ColumnarArray getArray(int ordinal) {
    return columns[ordinal].getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return columns[ordinal].getMap(rowId);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType t = (DecimalType) dataType;
      return getDecimal(ordinal, t.precision(), t.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType)dataType).fields().length);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else {
      throw new UnsupportedOperationException("Datatype not supported " + dataType);
    }
  }

  @Override
  public void update(int ordinal, Object value) { throw new UnsupportedOperationException(); }

  @Override
  public void setNullAt(int ordinal) { throw new UnsupportedOperationException(); }
}
