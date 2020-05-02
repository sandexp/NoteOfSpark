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

import org.apache.spark.sql.catalyst.util.MapData;

/**
 * 列式向量表的map抽象
 * 参数列表:
 * @keys key的列式数组
 * @values value的列式数组
 * @length map的大小
 */
public final class ColumnarMap extends MapData {
  private final ColumnarArray keys;
  private final ColumnarArray values;
  private final int length;

  public ColumnarMap(ColumnVector keys, ColumnVector values, int offset, int length) {
    this.length = length;
    this.keys = new ColumnarArray(keys, offset, length);
    this.values = new ColumnarArray(values, offset, length);
  }

  // 获取元素的数量
  @Override
  public int numElements() { return length; }

  // 获取key的列式数组
  @Override
  public ColumnarArray keyArray() {
    return keys;
  }

  // 获取map的列式数组
  @Override
  public ColumnarArray valueArray() {
    return values;
  }

  // map拷贝
  @Override
  public ColumnarMap copy() {
    throw new UnsupportedOperationException();
  }
}
