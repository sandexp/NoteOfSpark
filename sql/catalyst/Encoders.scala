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

package org.apache.spark.sql

import java.lang.reflect.Modifier

import scala.reflect.{classTag, ClassTag}
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast}
import org.apache.spark.sql.catalyst.expressions.objects.{DecodeUsingSerializer, EncodeUsingSerializer}
import org.apache.spark.sql.types._

/**
 * 编码器的创建方法
 *
 */
object Encoders {

  /**
    * 可为空的bool类型编码器
   */
  def BOOLEAN: Encoder[java.lang.Boolean] = ExpressionEncoder()

  /**
   * 可为空的byte类型编码器
   */
  def BYTE: Encoder[java.lang.Byte] = ExpressionEncoder()

  /**
   * 可为空的short类型编码器
   */
  def SHORT: Encoder[java.lang.Short] = ExpressionEncoder()

  /**
   * 可为空的int类型编码器
   */
  def INT: Encoder[java.lang.Integer] = ExpressionEncoder()

  /**
   * 可为空的long类型编码器
   */
  def LONG: Encoder[java.lang.Long] = ExpressionEncoder()

  /**
   * 可为空的float类型编码器
   */
  def FLOAT: Encoder[java.lang.Float] = ExpressionEncoder()

  /**
    * 可为空的double类型编码器
   */
  def DOUBLE: Encoder[java.lang.Double] = ExpressionEncoder()

  /**
    * 可为空的string类型编码器
   */
  def STRING: Encoder[java.lang.String] = ExpressionEncoder()

  /**
    * 可为空的decimal类型编码器
   */
  def DECIMAL: Encoder[java.math.BigDecimal] = ExpressionEncoder()

  /**
    * 可为空的date类型编码器
   */
  def DATE: Encoder[java.sql.Date] = ExpressionEncoder()

  /**
    * 可为空的localdate类型编码器
   */
  def LOCALDATE: Encoder[java.time.LocalDate] = ExpressionEncoder()

  /**
    * 可为空的timestamp类型编码器
   */
  def TIMESTAMP: Encoder[java.sql.Timestamp] = ExpressionEncoder()

  /**
    * 可为空的java.time.Instant类型编码器
   */
  def INSTANT: Encoder[java.time.Instant] = ExpressionEncoder()

  /**
    * 可为空的BINARY 类型编码器
   */
  def BINARY: Encoder[Array[Byte]] = ExpressionEncoder()

  /**
    * 创建java bean的编码器,类型T必须是共有的
   * 支持下述类型的bean:
   *  - 基本数据类型: boolean, int, double, etc.
   *  - 包装数据类型: Boolean, Integer, Double, etc.
   *  - String
   *  - java.math.BigDecimal, java.math.BigInteger
   *  - 时间相关类型: java.sql.Date, java.sql.Timestamp, java.time.LocalDate, java.time.Instant
   *  - 集合类型: only array and java.util.List currently, map support is in progress
   *  - nested java bean.
   */
  def bean[T](beanClass: Class[T]): Encoder[T] = ExpressionEncoder.javaBean(beanClass)

  /**
    * Kryo类型的编码器
   */
  def kryo[T: ClassTag]: Encoder[T] = genericSerializer(useKryo = true)

  /**
   * Creates an encoder that serializes objects of type T using Kryo.
   * This encoder maps T into a single byte array (binary) field.
   *
   * T must be publicly accessible.
   *
   * @since 1.6.0
   */
  def kryo[T](clazz: Class[T]): Encoder[T] = kryo(ClassTag[T](clazz))

  /**
    * java序列化类的编码器
    * 注意: 这个及其低效，仅仅用作最后手段
   */
  def javaSerialization[T: ClassTag]: Encoder[T] = genericSerializer(useKryo = false)

  /**
   * Creates an encoder that serializes objects of type T using generic Java serialization.
   * This encoder maps T into a single byte array (binary) field.
   *
   * T must be publicly accessible.
   *
   * @note This is extremely inefficient and should only be used as the last resort.
   *
   * @since 1.6.0
   */
  def javaSerialization[T](clazz: Class[T]): Encoder[T] = javaSerialization(ClassTag[T](clazz))

  /** Throws an exception if T is not a public class. */
  private def validatePublicClass[T: ClassTag](): Unit = {
    if (!Modifier.isPublic(classTag[T].runtimeClass.getModifiers)) {
      throw new UnsupportedOperationException(
        s"${classTag[T].runtimeClass.getName} is not a public class. " +
          "Only public classes are supported.")
    }
  }

  /** A way to construct encoders using generic serializers. */
  private def genericSerializer[T: ClassTag](useKryo: Boolean): Encoder[T] = {
    if (classTag[T].runtimeClass.isPrimitive) {
      throw new UnsupportedOperationException("Primitive types are not supported.")
    }

    validatePublicClass[T]()

    ExpressionEncoder[T](
      objSerializer =
        EncodeUsingSerializer(
          BoundReference(0, ObjectType(classOf[AnyRef]), nullable = true), kryo = useKryo),
      objDeserializer =
        DecodeUsingSerializer[T](
          Cast(GetColumnByOrdinal(0, BinaryType), BinaryType),
          classTag[T],
          kryo = useKryo),
      clsTag = classTag[T]
    )
  }

  // 元组类型
  /**
   * An encoder for 2-ary tuples.
   *
   * @since 1.6.0
   */
  def tuple[T1, T2](
    e1: Encoder[T1],
    e2: Encoder[T2]): Encoder[(T1, T2)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2))
  }

  /**
   * An encoder for 3-ary tuples.
   *
   * @since 1.6.0
   */
  def tuple[T1, T2, T3](
    e1: Encoder[T1],
    e2: Encoder[T2],
    e3: Encoder[T3]): Encoder[(T1, T2, T3)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2), encoderFor(e3))
  }

  /**
   * An encoder for 4-ary tuples.
   *
   * @since 1.6.0
   */
  def tuple[T1, T2, T3, T4](
    e1: Encoder[T1],
    e2: Encoder[T2],
    e3: Encoder[T3],
    e4: Encoder[T4]): Encoder[(T1, T2, T3, T4)] = {
    ExpressionEncoder.tuple(encoderFor(e1), encoderFor(e2), encoderFor(e3), encoderFor(e4))
  }

  /**
   * An encoder for 5-ary tuples.
   *
   * @since 1.6.0
   */
  def tuple[T1, T2, T3, T4, T5](
    e1: Encoder[T1],
    e2: Encoder[T2],
    e3: Encoder[T3],
    e4: Encoder[T4],
    e5: Encoder[T5]): Encoder[(T1, T2, T3, T4, T5)] = {
    ExpressionEncoder.tuple(
      encoderFor(e1), encoderFor(e2), encoderFor(e3), encoderFor(e4), encoderFor(e5))
  }
  
  // scala类型
  /**
   * An encoder for Scala's product type (tuples, case classes, etc).
   * @since 2.0.0
   */
  def product[T <: Product : TypeTag]: Encoder[T] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive int type.
   * @since 2.0.0
   */
  def scalaInt: Encoder[Int] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive long type.
   * @since 2.0.0
   */
  def scalaLong: Encoder[Long] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive double type.
   * @since 2.0.0
   */
  def scalaDouble: Encoder[Double] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive float type.
   * @since 2.0.0
   */
  def scalaFloat: Encoder[Float] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive byte type.
   * @since 2.0.0
   */
  def scalaByte: Encoder[Byte] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive short type.
   * @since 2.0.0
   */
  def scalaShort: Encoder[Short] = ExpressionEncoder()

  /**
   * An encoder for Scala's primitive boolean type.
   * @since 2.0.0
   */
  def scalaBoolean: Encoder[Boolean] = ExpressionEncoder()

}
