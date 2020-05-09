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

package org.apache.spark.sql.execution;

import java.io.IOException;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.map.BytesToBytesMap;

/**
 * 定长的unsafe聚合的hashmap
 * 基于unsafe的hashmap,用于对归并操作获取更好的性能,参与聚合的值是定长的
 * 这个map支持最大20亿的key
 */
public final class UnsafeFixedWidthAggregationMap {

  /**
   * 空的聚合缓冲区,使用unsafe行@UnsafeRow的格式进行编码.当插入新的key到map的时候,会拷贝这个buffer,且将其作为value进行使用
   */
  private final byte[] emptyAggregationBuffer;
  // 聚合缓冲区的schema
  private final StructType aggregationBufferSchema;
  // 分组key的schema
  private final StructType groupingKeySchema;

  /**
   * unsafe行的编码形式
   */
  private final UnsafeProjection groupingKeyProjection;

  /**
   * 字节数组key --> 字节数组value的hashmap
   */
  private final BytesToBytesMap map;

  /**
   * 重用指针,指向当前的聚合缓冲区
   */
  private final UnsafeRow currentAggregationBuffer;

  /**
   * 如果unsafe定长聚合map支持带有指定schema的聚合缓冲区的话返回true
   */
  public static boolean supportsAggregationBufferSchema(StructType schema) {
    for (StructField field: schema.fields()) {
      if (!UnsafeRow.isMutable(field.dataType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param emptyAggregationBuffer the default value for new keys (a "zero" of the agg. function)
   * @param aggregationBufferSchema the schema of the aggregation buffer, used for row conversion.
   * @param groupingKeySchema the schema of the grouping key, used for row conversion.
   * @param taskContext the current task context.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param pageSizeBytes the data page size, in bytes; limits the maximum record size.
   */
  public UnsafeFixedWidthAggregationMap(
      InternalRow emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      TaskContext taskContext,
      int initialCapacity,
      long pageSizeBytes) {
    this.aggregationBufferSchema = aggregationBufferSchema;
    this.currentAggregationBuffer = new UnsafeRow(aggregationBufferSchema.length());
    this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
    this.groupingKeySchema = groupingKeySchema;
    this.map = new BytesToBytesMap(
      taskContext.taskMemoryManager(), initialCapacity, pageSizeBytes);

    // Initialize the buffer for aggregation value
    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();

    // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
    // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
    // does not fully consume the aggregation map's output (e.g. aggregate followed by limit).
    taskContext.addTaskCompletionListener(context -> {
      free();
    });
  }

  /**
   * 获取聚合缓冲区.为了提升效率,所有这个方法的调用会返回相同的对象,如果不能分配其他的内存,这个返回会返回null
   */
  public UnsafeRow getAggregationBuffer(InternalRow groupingKey) {
    final UnsafeRow unsafeGroupingKeyRow = this.groupingKeyProjection.apply(groupingKey);

    return getAggregationBufferFromUnsafeRow(unsafeGroupingKeyRow);
  }

  // 从指定的unsafe 行数据中获取聚合缓冲区
  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow key) {
    return getAggregationBufferFromUnsafeRow(key, key.hashCode());
  }

  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow key, int hash) {
    // Probe our map using the serialized key
    final BytesToBytesMap.Location loc = map.lookup(
      key.getBaseObject(),
      key.getBaseOffset(),
      key.getSizeInBytes(),
      hash);
    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      boolean putSucceeded = loc.append(
        key.getBaseObject(),
        key.getBaseOffset(),
        key.getSizeInBytes(),
        emptyAggregationBuffer,
        Platform.BYTE_ARRAY_OFFSET,
        emptyAggregationBuffer.length
      );
      if (!putSucceeded) {
        return null;
      }
    }

    // Reset the pointer to point to the value that we just stored or looked up:
    currentAggregationBuffer.pointTo(
      loc.getValueBase(),
      loc.getValueOffset(),
      loc.getValueLength()
    );
    return currentAggregationBuffer;
  }

  /**
   * Returns an iterator over the keys and values in this map. This uses destructive iterator of
   * BytesToBytesMap. So it is illegal to call any other method on this map after `iterator()` has
   * been called.
   *
   * For efficiency, each call returns the same object.
   */
  public KVIterator<UnsafeRow, UnsafeRow> iterator() {
    return new KVIterator<UnsafeRow, UnsafeRow>() {

      private final BytesToBytesMap.MapIterator mapLocationIterator =
        map.destructiveIterator();
      private final UnsafeRow key = new UnsafeRow(groupingKeySchema.length());
      private final UnsafeRow value = new UnsafeRow(aggregationBufferSchema.length());

      @Override
      public boolean next() {
        if (mapLocationIterator.hasNext()) {
          final BytesToBytesMap.Location loc = mapLocationIterator.next();
          key.pointTo(
            loc.getKeyBase(),
            loc.getKeyOffset(),
            loc.getKeyLength()
          );
          value.pointTo(
            loc.getValueBase(),
            loc.getValueOffset(),
            loc.getValueLength()
          );
          return true;
        } else {
          return false;
        }
      }

      @Override
      public UnsafeRow getKey() {
        return key;
      }

      @Override
      public UnsafeRow getValue() {
        return value;
      }

      @Override
      public void close() {
        // Do nothing.
      }
    };
  }

  /**
   * 返回当前峰值内存使用量,按照字节为单位
   */
  public long getPeakMemoryUsedBytes() {
    return map.getPeakMemoryUsedBytes();
  }

  /**
   * 释放这个map的内存,这个操作是幂等性的,可以多次调用
   */
  public void free() {
    map.free();
  }

  /**
   * 获取平均桶列表的迭代器
   *
   * Gets the average bucket list iterations per lookup in the underlying `BytesToBytesMap`.
   */
  public double getAvgHashProbeBucketListIterations() {
    return map.getAvgHashProbeBucketListIterations();
  }

  /**
   * 就地对map的记录进行排序,将其溢写到磁盘,并且返回@UnsafeKVExternalSorter
   * 注意到map在插入新的记录的时候会重置,且返回的排序器不能够插入新的记录
   */
  public UnsafeKVExternalSorter destructAndCreateExternalSorter() throws IOException {
    return new UnsafeKVExternalSorter(
      groupingKeySchema,
      aggregationBufferSchema,
      SparkEnv.get().blockManager(),
      SparkEnv.get().serializerManager(),
      map.getPageSizeBytes(),
      (int) SparkEnv.get().conf().get(
        package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD()),
      map);
  }
}
