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

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;

import java.io.IOException;
import java.util.Arrays;
import java.util.TimeZone;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase.ValuesReaderIntIterator;
import static org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase.createRLEIterator;

/**
 * 单个列的解码器,用于返回value值
 * @valuesRead 需要读取的value数量
 * @endOfPageValueCount 表示当前页尾的值
 * @dictionary 字典(如果字典参与编码)
 * @maxDefLevel 列的最大定义等级
 * @totalValueCount 列的value总数
 * @pageValueCount 当前页的value总数
 * @pageReader 页读取器
 * @descriptor 列描述器
 * @originalType 原始类型
 * @convertTz 时区
 * @UTC UTC时区
 */
public class VectorizedColumnReader {
  private long valuesRead;
  private long endOfPageValueCount;
  private final Dictionary dictionary;
  private final int maxDefLevel;

  /**
   * 确定当前页的字典是否编码
   */
  private boolean isCurrentPageDictionaryEncoded;

  /**
   * 重复/定义/value 读取器
   */
  private SpecificParquetRecordReaderBase.IntIterator repetitionLevelColumn;
  private SpecificParquetRecordReaderBase.IntIterator definitionLevelColumn;
  private ValuesReader dataColumn;

  // Only set if vectorized decoding is true. This is used instead of the row by row decoding
  // with `definitionLevelColumn`.
  private VectorizedRleValuesReader defColumn;
  private final long totalValueCount;
  private int pageValueCount;
  private final PageReader pageReader;
  private final ColumnDescriptor descriptor;
  private final OriginalType originalType;
  private final TimeZone convertTz;
  private static final TimeZone UTC = DateTimeUtils.TimeZoneUTC();

  public VectorizedColumnReader(
      ColumnDescriptor descriptor,
      OriginalType originalType,
      PageReader pageReader,
      TimeZone convertTz) throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.convertTz = convertTz;
    this.originalType = originalType;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
        this.isCurrentPageDictionaryEncoded = true;
      } catch (IOException e) {
        throw new IOException("could not decode the dictionary for " + descriptor, e);
      }
    } else {
      this.dictionary = null;
      this.isCurrentPageDictionaryEncoded = false;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new IOException("totalValueCount == 0");
    }
  }

  /**
   * 移动到下一个value,如果下一个value非空则返回false
   */
  private boolean next() throws IOException {
    if (valuesRead >= endOfPageValueCount) {
      if (valuesRead >= totalValueCount) {
        // How do we get here? Throw end of stream exception?
        return false;
      }
      readPage();
    }
    ++valuesRead;
    return definitionLevelColumn.nextInt() == maxDefLevel;
  }

  /**
   * 从列读取所有总计value到列中
   * 输入参数
   * @total 总计值
   * @column 列向量表
   */
  void readBatch(int total, WritableColumnVector column) throws IOException {
    int rowId = 0;
    WritableColumnVector dictionaryIds = null;
    // 1. 获取目录列表
    if (dictionary != null) {
      // SPARK-16334: We only maintain a single dictionary per row batch, so that it can be used to
      // decode all previous dictionary encoded pages if we ever encounter a non-dictionary encoded
      // page.
      dictionaryIds = column.reserveDictionaryIds(total);
    }
    while (total > 0) {
      // 2. 计算这个页中剩余的value数量(val= 页尾指针 - 需要读取的value数量)
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      // 3. 剩余value为0,则全都读取当前页value
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }
      // 4. 确定类型名称
      int num = Math.min(total, leftInPage);
      PrimitiveType.PrimitiveTypeName typeName =
        descriptor.getPrimitiveType().getPrimitiveTypeName();

      if (isCurrentPageDictionaryEncoded) {
        // 5. 读取并且对目录id类别进行解码
        defColumn.readIntegers(
            num, dictionaryIds, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
        // 6. @TIMESTAMP_MILLIS 使用int 64编码，不能够懒加载去解码，因为需要传递value给进程
        if (column.hasDictionary() || (rowId == 0 &&
            (typeName == PrimitiveType.PrimitiveTypeName.INT32 ||
            (typeName == PrimitiveType.PrimitiveTypeName.INT64 &&
              originalType != OriginalType.TIMESTAMP_MILLIS) ||
            typeName == PrimitiveType.PrimitiveTypeName.FLOAT ||
            typeName == PrimitiveType.PrimitiveTypeName.DOUBLE ||
            typeName == PrimitiveType.PrimitiveTypeName.BINARY))) {
          // 7. 列向量表支持目录的懒编码,这样就可以设置目录信息,如果rowId不为0则不能进行该操作且这个列不能包含子目录
          column.setDictionary(new ParquetDictionary(dictionary));
        } else {
          // 8. 如果没有目录,或者类型不存在,则需要将数据读取到列中
          decodeDictionaryIds(rowId, num, column, dictionaryIds);
        }
      } else {
        // 9. 对于没有编码的处理
        if (column.hasDictionary() && rowId != 0) {
          // 10. 这个批次已经含有目录编码值,但是新页不存在.这个批次不支持目录混合.需要自己进行目录解码
          decodeDictionaryIds(0, rowId, column, column.getDictionaryIds());
        }
        // 11. 将列的目录置空,并读取行批次数据
        column.setDictionary(null);
        switch (typeName) {
          case BOOLEAN:
            readBooleanBatch(rowId, num, column);
            break;
          case INT32:
            readIntBatch(rowId, num, column);
            break;
          case INT64:
            readLongBatch(rowId, num, column);
            break;
          case INT96:
            readBinaryBatch(rowId, num, column);
            break;
          case FLOAT:
            readFloatBatch(rowId, num, column);
            break;
          case DOUBLE:
            readDoubleBatch(rowId, num, column);
            break;
          case BINARY:
            readBinaryBatch(rowId, num, column);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            readFixedLenByteArrayBatch(
              rowId, num, column, descriptor.getPrimitiveType().getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + typeName);
        }
      }
      // 12. 更新度量值(读取的value数量)
      valuesRead += num;
      // 移动行指针
      rowId += num;
      total -= num;
    }
  }

  // 确定是否需要转换时间戳
  private boolean shouldConvertTimestamps() {
    return convertTz != null && !convertTz.equals(UTC);
  }

  /**
   * 辅助函数,用于构建对parquet的schema格式不匹配的异常
   */
  private SchemaColumnConvertNotSupportedException constructConvertNotSupportedException(
      ColumnDescriptor descriptor,
      WritableColumnVector column) {
    return new SchemaColumnConvertNotSupportedException(
      Arrays.toString(descriptor.getPath()),
      descriptor.getPrimitiveType().getPrimitiveTypeName().toString(),
      column.dataType().catalogString());
  }

  /**
   * 将指定数量@num的value读取到列中,可以有目录列表和目录对value进行编码
   * 输入参数:
   * @rowId 行编号
   * @num 读取value的数量
   * @column 列式向量表
   * @dictionaryIds 目录列表
   */
  private void decodeDictionaryIds(
      int rowId,
      int num,
      WritableColumnVector column,
      WritableColumnVector dictionaryIds) {
    // 1. 读取行号row --> row+num的value值,将其存储到列中(包括各种类型的参数)
    switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
      case INT32:
        if (column.dataType() == DataTypes.IntegerType ||
            DecimalType.is32BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putInt(i, dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.ByteType) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putByte(i, (byte) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.ShortType) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putShort(i, (short) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      case INT64:
        if (column.dataType() == DataTypes.LongType ||
            DecimalType.is64BitDecimalType(column.dataType()) ||
            originalType == OriginalType.TIMESTAMP_MICROS) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putLong(i, dictionary.decodeToLong(dictionaryIds.getDictId(i)));
            }
          }
        } else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putLong(i,
                DateTimeUtils.fromMillis(dictionary.decodeToLong(dictionaryIds.getDictId(i))));
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      case FLOAT:
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            column.putFloat(i, dictionary.decodeToFloat(dictionaryIds.getDictId(i)));
          }
        }
        break;

      case DOUBLE:
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            column.putDouble(i, dictionary.decodeToDouble(dictionaryIds.getDictId(i)));
          }
        }
        break;
      case INT96:
        if (column.dataType() == DataTypes.TimestampType) {
          if (!shouldConvertTimestamps()) {
            for (int i = rowId; i < rowId + num; ++i) {
              if (!column.isNullAt(i)) {
                Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                column.putLong(i, ParquetRowConverter.binaryToSQLTimestamp(v));
              }
            }
          } else {
            for (int i = rowId; i < rowId + num; ++i) {
              if (!column.isNullAt(i)) {
                Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                long rawTime = ParquetRowConverter.binaryToSQLTimestamp(v);
                long adjTime = DateTimeUtils.convertTz(rawTime, convertTz, UTC);
                column.putLong(i, adjTime);
              }
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;
      case BINARY:
        // TODO: this is incredibly inefficient as it blows up the dictionary right here. We
        // need to do this better. We should probably add the dictionary data to the ColumnVector
        // and reuse it across batches. This should mean adding a ByteArray would just update
        // the length and offset.
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
            column.putByteArray(i, v.getBytes());
          }
        }
        break;
      case FIXED_LEN_BYTE_ARRAY:
        // DecimalType written in the legacy mode
        if (DecimalType.is32BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putInt(i, (int) ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (DecimalType.is64BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putLong(i, ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putByteArray(i, v.getBytes());
            }
          }
        } else {
          throw constructConvertNotSupportedException(descriptor, column);
        }
        break;

      default:
        throw new UnsupportedOperationException(
          "Unsupported type: " + descriptor.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  /**
   * 读取所有读取批次的函数,从列读取器@columnReader读取指定数量@value到列中.保证这个数量小于当前页中value的数量
   * 包括各种类型的数据
   */

  private void readBooleanBatch(int rowId, int num, WritableColumnVector column)
      throws IOException {
    if (column.dataType() != DataTypes.BooleanType) {
      throw constructConvertNotSupportedException(descriptor, column);
    }
    defColumn.readBooleans(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
  }

  private void readIntBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.IntegerType || column.dataType() == DataTypes.DateType ||
        DecimalType.is32BitDecimalType(column.dataType())) {
      defColumn.readIntegers(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ByteType) {
      defColumn.readBytes(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ShortType) {
      defColumn.readShorts(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readLongBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    if (column.dataType() == DataTypes.LongType ||
        DecimalType.is64BitDecimalType(column.dataType()) ||
        originalType == OriginalType.TIMESTAMP_MICROS) {
      defColumn.readLongs(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i, DateTimeUtils.fromMillis(dataColumn.readLong()));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readFloatBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: support implicit cast to double?
    if (column.dataType() == DataTypes.FloatType) {
      defColumn.readFloats(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readDoubleBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.DoubleType) {
      defColumn.readDoubles(
          num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readBinaryBatch(int rowId, int num, WritableColumnVector column) throws IOException {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (column.dataType() == DataTypes.StringType || column.dataType() == DataTypes.BinaryType
            || DecimalType.isByteArrayDecimalType(column.dataType())) {
      defColumn.readBinarys(num, column, rowId, maxDefLevel, data);
    } else if (column.dataType() == DataTypes.TimestampType) {
      if (!shouldConvertTimestamps()) {
        for (int i = 0; i < num; i++) {
          if (defColumn.readInteger() == maxDefLevel) {
            // Read 12 bytes for INT96
            long rawTime = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
            column.putLong(rowId + i, rawTime);
          } else {
            column.putNull(rowId + i);
          }
        }
      } else {
        for (int i = 0; i < num; i++) {
          if (defColumn.readInteger() == maxDefLevel) {
            // Read 12 bytes for INT96
            long rawTime = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
            long adjTime = DateTimeUtils.convertTz(rawTime, convertTz, UTC);
            column.putLong(rowId + i, adjTime);
          } else {
            column.putNull(rowId + i);
          }
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  private void readFixedLenByteArrayBatch(
      int rowId,
      int num,
      WritableColumnVector column,
      int arrayLen) {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (DecimalType.is32BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putInt(rowId + i,
              (int) ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is64BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i,
              ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putByteArray(rowId + i, data.readBinary(arrayLen).getBytes());
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw constructConvertNotSupportedException(descriptor, column);
    }
  }

  // 读取一个数据页的内容,这里提供了两个版本数据页的读取方式(v1/v2)
  private void readPage() {
    DataPage page = pageReader.readPage();
    // TODO: Why is this a visitor?
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        try {
          readPageV1(dataPageV1);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Void visit(DataPageV2 dataPageV2) {
        try {
          readPageV2(dataPageV2);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  // 初始化数据读取器
  private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in) throws IOException {
    // 1. 设置页尾指针指向= 当前页的value总数+ 需要读取的value数量
    this.endOfPageValueCount = valuesRead + pageValueCount;
    // 2. 设置数据类,确定当前页目录编码状态
    if (dataEncoding.usesDictionary()) {
      // 这种情况下使用了数据目录,需要对数据页进行编码
      this.dataColumn = null;
      if (dictionary == null) {
        throw new IOException(
            "could not read page in col " + descriptor +
                " as the dictionary was missing for encoding " + dataEncoding);
      }
      @SuppressWarnings("deprecation")
      Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
      if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedRleValuesReader();
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      // 这种情况下不需要对数据进行编码
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedPlainValuesReader();
      this.isCurrentPageDictionaryEncoded = false;
    }

    // 3. 设置数据列的起始位置
    try {
      dataColumn.initFromPage(pageValueCount, in);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  private void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader;

    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new VectorizedRleValuesReader(bitWidth);
    dlReader = this.defColumn;
    this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
    this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
    try {
      BytesInput bytes = page.getBytes();
      ByteBufferInputStream in = bytes.toInputStream();
      rlReader.initFromPage(pageValueCount, in);
      dlReader.initFromPage(pageValueCount, in);
      initDataReader(page.getValueEncoding(), in);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    this.repetitionLevelColumn = createRLEIterator(descriptor.getMaxRepetitionLevel(),
        page.getRepetitionLevels(), descriptor);

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    // do not read the length from the stream. v2 pages handle dividing the page bytes.
    this.defColumn = new VectorizedRleValuesReader(bitWidth, false);
    this.definitionLevelColumn = new ValuesReaderIntIterator(this.defColumn);
    this.defColumn.initFromPage(
        this.pageValueCount, page.getDefinitionLevels().toInputStream());
    try {
      initDataReader(page.getDataEncoding(), page.getData().toInputStream());
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
}
