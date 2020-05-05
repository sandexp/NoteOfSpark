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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;

import java.io.Closeable;
import java.io.IOException;

/**
 * 由分区读取器工厂创建的分区读取器,用于输出RDD分区的数据
 * 注意,当前泛型T仅仅可以是行式数据@InternalRow 作为正常的数据源.或者是列批次@ColumnarBatch 作为列式数据源.
 */
@Evolving
public interface PartitionReader<T> extends Closeable {

  /**
   * 处理当前记录,如果没有记录则会返回false
   */
  boolean next() throws IOException;

  /**
   * 返回当前记录,这个方法需要返回值,直到调用了@next方法
   */
  T get();
}
