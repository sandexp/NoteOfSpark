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

package org.apache.spark.sql.types;

import org.apache.spark.annotation.Stable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 获取/创建指定的数据类型，用户需要使用单例对象和这个类提供的工厂方法
 * 类型参数:
 * @StringType 字符串类型
 * @BinaryType 二进制类型
 * @BooleanType bool类型
 * @DateType 数据类型
 * @TimestampType 时间戳类型
 * @CalendarIntervalType 日期类型
 * @DoubleType double类型
 * @FloatType float类型
 * @ByteType 字节类型
 * @IntegerType integer类型
 * @LongType long类型
 * @NullType 空值类型
 * @ShortType short类型
 */
@Stable
public class DataTypes {
  /**
   * Gets the StringType object.
   */
  public static final DataType StringType = StringType$.MODULE$;

  /**
   * Gets the BinaryType object.
   */
  public static final DataType BinaryType = BinaryType$.MODULE$;

  /**
   * Gets the BooleanType object.
   */
  public static final DataType BooleanType = BooleanType$.MODULE$;

  /**
   * Gets the DateType object.
   */
  public static final DataType DateType = DateType$.MODULE$;

  /**
   * Gets the TimestampType object.
   */
  public static final DataType TimestampType = TimestampType$.MODULE$;

  /**
   * Gets the CalendarIntervalType object.
   */
  public static final DataType CalendarIntervalType = CalendarIntervalType$.MODULE$;

  /**
   * Gets the DoubleType object.
   */
  public static final DataType DoubleType = DoubleType$.MODULE$;

  /**
   * Gets the FloatType object.
   */
  public static final DataType FloatType = FloatType$.MODULE$;

  /**
   * Gets the ByteType object.
   */
  public static final DataType ByteType = ByteType$.MODULE$;

  /**
   * Gets the IntegerType object.
   */
  public static final DataType IntegerType = IntegerType$.MODULE$;

  /**
   * Gets the LongType object.
   */
  public static final DataType LongType = LongType$.MODULE$;

  /**
   * Gets the ShortType object.
   */
  public static final DataType ShortType = ShortType$.MODULE$;

  /**
   * Gets the NullType object.
   */
  public static final DataType NullType = NullType$.MODULE$;

  /**
   * 创建指定类型的数组类型.
   */
  public static ArrayType createArrayType(DataType elementType) {
    if (elementType == null) {
      throw new IllegalArgumentException("elementType should not be null.");
    }
    return new ArrayType(elementType, true);
  }

  /**
   * Creates an ArrayType by specifying the data type of elements ({@code elementType}) and
   * whether the array contains null values ({@code containsNull}).
   */
  public static ArrayType createArrayType(DataType elementType, boolean containsNull) {
    if (elementType == null) {
      throw new IllegalArgumentException("elementType should not be null.");
    }
    return new ArrayType(elementType, containsNull);
  }

  /**
   * 创建十进制类型,通过指定精确度和范围
   */
  public static DecimalType createDecimalType(int precision, int scale) {
    return DecimalType$.MODULE$.apply(precision, scale);
  }

  /**
   * 使用默认的精确度和容量范围创建十进制类型的值.
   */
  public static DecimalType createDecimalType() {
    return DecimalType$.MODULE$.USER_DEFAULT();
  }

  /**
   * 创建map类型,通过指定key的数据类型和value的数据备选.其中@valueContainsNull 的属性为true
   */
  public static MapType createMapType(DataType keyType, DataType valueType) {
    if (keyType == null) {
      throw new IllegalArgumentException("keyType should not be null.");
    }
    if (valueType == null) {
      throw new IllegalArgumentException("valueType should not be null.");
    }
    return new MapType(keyType, valueType, true);
  }

  /**
   * 创建map类型,通过指定key和value的类型.且指定是否包含空值@valueContainsNull
   */
  public static MapType createMapType(
      DataType keyType,
      DataType valueType,
      boolean valueContainsNull) {
    if (keyType == null) {
      throw new IllegalArgumentException("keyType should not be null.");
    }
    if (valueType == null) {
      throw new IllegalArgumentException("valueType should not be null.");
    }
    return new MapType(keyType, valueType, valueContainsNull);
  }

  /**
   * 通过指定名称@name,数据类型@dataType和是否可以为空值@nullable 创建结构体参数
   */
  public static StructField createStructField(
      String name,
      DataType dataType,
      boolean nullable,
      Metadata metadata) {
    if (name == null) {
      throw new IllegalArgumentException("name should not be null.");
    }
    if (dataType == null) {
      throw new IllegalArgumentException("dataType should not be null.");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("metadata should not be null.");
    }
    return new StructField(name, dataType, nullable, metadata);
  }

  /**
   * 使用空的元数据创建结构体属性
   */
  public static StructField createStructField(String name, DataType dataType, boolean nullable) {
    return createStructField(name, dataType, nullable, (new MetadataBuilder()).build());
  }

  /**
   * 使用指定的属性列表创建结构体类型
   */
  public static StructType createStructType(List<StructField> fields) {
    return createStructType(fields.toArray(new StructField[fields.size()]));
  }

  /**
   * 使用指定的属性数组创建结构体类型
   */
  public static StructType createStructType(StructField[] fields) {
    if (fields == null) {
      throw new IllegalArgumentException("fields should not be null.");
    }
    Set<String> distinctNames = new HashSet<>();
    for (StructField field : fields) {
      if (field == null) {
        throw new IllegalArgumentException(
          "fields should not contain any null.");
      }

      distinctNames.add(field.name());
    }
    if (distinctNames.size() != fields.length) {
      throw new IllegalArgumentException("fields should have distinct names.");
    }

    return StructType$.MODULE$.apply(fields);
  }
}
