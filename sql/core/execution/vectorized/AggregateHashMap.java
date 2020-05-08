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
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.LongType;

/**
 * 这个是一个仅仅可以添加kv的hashmap的一种解释性实现.可以作为缓存的结构,从而获取快速的查找性能.尤其是进行聚合操作的时候.这个
 * 可以在@HashAggregate中进行审查,用于加速聚合操作.
 * 这个有一个2的n次方的数组构成,用于index的查找和列批次的查找.列批次存储了kv对,index查找依赖于线性探查法(尝试次数最小).
 * 使用一种开销小的hash函数,这个会对查找提升很快.但是使用线性探测法和低开销的hash函数也会使得强健性降低.
 */
public class AggregateHashMap {

  /**
   * @columnVectors 列向量表
   * @aggBufferRow 聚合的缓存行(可变列的行)
   * @buckets 桶
   * @numBuckets 桶数量
   * @numRows 行数量
   * @maxSteps 最大步长
   */
  private OnHeapColumnVector[] columnVectors;
  private MutableColumnarRow aggBufferRow;
  private int[] buckets;
  private int numBuckets;
  private int numRows = 0;
  private int maxSteps = 3;

  /**
   * @DEFAULT_CAPACITY 默认容量
   * @DEFAULT_LOAD_FACTOR 默认加载因子
   * @DEFAULT_MAX_STEPS 默认最大步长
   */
  private static int DEFAULT_CAPACITY = 1 << 16;
  private static double DEFAULT_LOAD_FACTOR = 0.25;
  private static int DEFAULT_MAX_STEPS = 3;

  public AggregateHashMap(StructType schema, int capacity, double loadFactor, int maxSteps) {

    // We currently only support single key-value pair that are both longs
    assert (schema.size() == 2 && schema.fields()[0].dataType() == LongType &&
        schema.fields()[1].dataType() == LongType);

    // capacity should be a power of 2
    assert (capacity > 0 && ((capacity & (capacity - 1)) == 0));

    this.maxSteps = maxSteps;
    numBuckets = (int) (capacity / loadFactor);
    columnVectors = OnHeapColumnVector.allocateColumns(capacity, schema);
    aggBufferRow = new MutableColumnarRow(columnVectors);
    buckets = new int[numBuckets];
    Arrays.fill(buckets, -1);
  }

  public AggregateHashMap(StructType schema) {
    this(schema, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_MAX_STEPS);
  }

  // 对指定key进行查找或者是插入
  public MutableColumnarRow findOrInsert(long key) {
    int idx = find(key);
    if (idx != -1 && buckets[idx] == -1) {
      columnVectors[0].putLong(numRows, key);
      columnVectors[1].putLong(numRows, 0);
      buckets[idx] = numRows++;
    }
    aggBufferRow.rowId = buckets[idx];
    return aggBufferRow;
  }

  @VisibleForTesting
  public int find(long key) {
    long h = hash(key);
    int step = 0;
    int idx = (int) h & (numBuckets - 1);
    while (step < maxSteps) {
      // Return bucket index if it's either an empty slot or already contains the key
      if (buckets[idx] == -1) {
        return idx;
      } else if (equals(idx, key)) {
        return idx;
      }
      idx = (idx + 1) & (numBuckets - 1);
      step++;
    }
    // Didn't find it
    return -1;
  }

  // 简易的hash函数 h(key)=key
  private long hash(long key) {
    return key;
  }

  private boolean equals(int idx, long key1) {
    return columnVectors[0].getLong(buckets[idx]) == key1;
  }
}
