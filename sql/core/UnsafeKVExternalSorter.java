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

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.sql.catalyst.expressions.BaseOrdering;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.unsafe.sort.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * unsafe的KV外部排序器
 * 用于对外部kv型记录进行排序.key和value都是@UnsafeRows
 * 注意这个类允许传递@BytesToBytesMap 进行排序
 * 参数列表:
 * @keySchema key的schema
 * @valueSchema value的schema
 * @prefixComputer 前缀
 * @sorter 外部排序器
 */
public final class UnsafeKVExternalSorter {

  private final StructType keySchema;
  private final StructType valueSchema;
  private final UnsafeExternalRowSorter.PrefixComputer prefixComputer;
  private final UnsafeExternalSorter sorter;

  public UnsafeKVExternalSorter(
      StructType keySchema,
      StructType valueSchema,
      BlockManager blockManager,
      SerializerManager serializerManager,
      long pageSizeBytes,
      int numElementsForSpillThreshold) throws IOException {
    this(keySchema, valueSchema, blockManager, serializerManager, pageSizeBytes,
      numElementsForSpillThreshold, null);
  }

  public UnsafeKVExternalSorter(
      StructType keySchema,
      StructType valueSchema,
      BlockManager blockManager,
      SerializerManager serializerManager,
      long pageSizeBytes,
      int numElementsForSpillThreshold,
      @Nullable BytesToBytesMap map) throws IOException {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    final TaskContext taskContext = TaskContext.get();

    prefixComputer = SortPrefixUtils.createPrefixGenerator(keySchema);
    PrefixComparator prefixComparator = SortPrefixUtils.getPrefixComparator(keySchema);
    BaseOrdering ordering = GenerateOrdering.create(keySchema);
    Supplier<RecordComparator> comparatorSupplier =
      () -> new KVComparator(ordering, keySchema.length());
    boolean canUseRadixSort = keySchema.length() == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(keySchema.apply(0));

    TaskMemoryManager taskMemoryManager = taskContext.taskMemoryManager();

    if (map == null) {
      sorter = UnsafeExternalSorter.create(
        taskMemoryManager,
        blockManager,
        serializerManager,
        taskContext,
        comparatorSupplier,
        prefixComparator,
        (int) (long) SparkEnv.get().conf().get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE()),
        pageSizeBytes,
        numElementsForSpillThreshold,
        canUseRadixSort);
    } else {
      // During spilling, the pointer array in `BytesToBytesMap` will not be used, so we can borrow
      // that and use it as the pointer array for `UnsafeInMemorySorter`.
      LongArray pointerArray = map.getArray();
      // `BytesToBytesMap`'s pointer array is only guaranteed to hold all the distinct keys, but
      // `UnsafeInMemorySorter`'s pointer array need to hold all the entries. Since
      // `BytesToBytesMap` can have duplicated keys, here we need a check to make sure the pointer
      // array can hold all the entries in `BytesToBytesMap`.
      // The pointer array will be used to do in-place sort, which requires half of the space to be
      // empty. Note: each record in the map takes two entries in the pointer array, one is record
      // pointer, another is key prefix. So the required size of pointer array is `numRecords * 4`.
      // TODO: It's possible to change UnsafeInMemorySorter to have multiple entries with same key,
      // so that we can always reuse the pointer array.
      if (map.numValues() > pointerArray.size() / 4) {
        // Here we ask the map to allocate memory, so that the memory manager won't ask the map
        // to spill, if the memory is not enough.
        pointerArray = map.allocateArray(map.numValues() * 4L);
      }

      // Since the pointer array(either reuse the one in the map, or create a new one) is guaranteed
      // to be large enough, it's fine to pass `null` as consumer because we won't allocate more
      // memory.
      final UnsafeInMemorySorter inMemSorter = new UnsafeInMemorySorter(
        null,
        taskMemoryManager,
        comparatorSupplier.get(),
        prefixComparator,
        pointerArray,
        canUseRadixSort);

      // We cannot use the destructive iterator here because we are reusing the existing memory
      // pages in BytesToBytesMap to hold records during sorting.
      // The only new memory we are allocating is the pointer/prefix array.
      BytesToBytesMap.MapIterator iter = map.iterator();
      final int numKeyFields = keySchema.size();
      UnsafeRow row = new UnsafeRow(numKeyFields);
      while (iter.hasNext()) {
        final BytesToBytesMap.Location loc = iter.next();
        final Object baseObject = loc.getKeyBase();
        final long baseOffset = loc.getKeyOffset();

        // Get encoded memory address
        // baseObject + baseOffset point to the beginning of the key data in the map, but that
        // the KV-pair's length data is stored in the word immediately before that address
        MemoryBlock page = loc.getMemoryPage();
        long address = taskMemoryManager.encodePageNumberAndOffset(page, baseOffset - 8);

        // Compute prefix
        row.pointTo(baseObject, baseOffset, loc.getKeyLength());
        final UnsafeExternalRowSorter.PrefixComputer.Prefix prefix =
          prefixComputer.computePrefix(row);

        inMemSorter.insertRecord(address, prefix.value, prefix.isNull);
      }

      sorter = UnsafeExternalSorter.createWithExistingInMemorySorter(
        taskMemoryManager,
        blockManager,
        serializerManager,
        taskContext,
        comparatorSupplier,
        prefixComparator,
        (int) (long) SparkEnv.get().conf().get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE()),
        pageSizeBytes,
        numElementsForSpillThreshold,
        inMemSorter);

      // reset the map, so we can re-use it to insert new records. the inMemSorter will not used
      // anymore, so the underline array could be used by map again.
      map.reset();
    }
  }

  /**
   * 将一条kv信息插入到排序器中.如果排序器没有足够的内容去容纳这条记录,排序器会对内存中的记录进行排序,将部分记录写出,
   * 并重新分配内存给新的记录
   */
  public void insertKV(UnsafeRow key, UnsafeRow value) throws IOException {
    final UnsafeExternalRowSorter.PrefixComputer.Prefix prefix =
      prefixComputer.computePrefix(key);
    sorter.insertKVRecord(
      key.getBaseObject(), key.getBaseOffset(), key.getSizeInBytes(),
      value.getBaseObject(), value.getBaseOffset(), value.getSizeInBytes(),
      prefix.value, prefix.isNull);
  }

  /**
   * 将另一个@UnsafeKVExternalSorter合并到当前的排序器中
   */
  public void merge(UnsafeKVExternalSorter other) throws IOException {
    sorter.merge(other.sorter);
  }

  /**
   * 返回经过排序的迭代器,调用者需要调用@cleanupResources 情况消费完毕的迭代器
   */
  public KVSorterIterator sortedIterator() throws IOException {
    try {
      final UnsafeSorterIterator underlying = sorter.getSortedIterator();
      if (!underlying.hasNext()) {
        // Since we won't ever call next() on an empty iterator, we need to clean up resources
        // here in order to prevent memory leaks.
        cleanupResources();
      }
      return new KVSorterIterator(underlying);
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }

  /**
   * 获取已经溢写到磁盘的字节总数
   */
  public long getSpillSize() {
    return sorter.getSpillSize();
  }

  /**
   * 获取峰值内存使用量
   */
  public long getPeakMemoryUsedBytes() {
    return sorter.getPeakMemoryUsedBytes();
  }

  /**
   * 标记当前内存页不可以被使用,因此分配新的内存页,或者是将当前页的内容进行溢写
   */
  @VisibleForTesting
  void closeCurrentPage() {
    sorter.closeCurrentPage();
  }

  /**
   * 释放当前排序器的内存数据结构,情况所有溢写的文件
   */
  public void cleanupResources() {
    sorter.cleanupResources();
  }

  private static final class KVComparator extends RecordComparator {
    private final BaseOrdering ordering;
    private final UnsafeRow row1;
    private final UnsafeRow row2;

    KVComparator(BaseOrdering ordering, int numKeyFields) {
      this.row1 = new UnsafeRow(numKeyFields);
      this.row2 = new UnsafeRow(numKeyFields);
      this.ordering = ordering;
    }

    // 记录比较逻辑
    @Override
    public int compare(
        Object baseObj1,
        long baseOff1,
        int baseLen1,
        Object baseObj2,
        long baseOff2,
        int baseLen2) {
      // Note that since ordering doesn't need the total length of the record, we just pass 0
      // into the row.
      row1.pointTo(baseObj1, baseOff1 + 4, 0);
      row2.pointTo(baseObj2, baseOff2 + 4, 0);
      return ordering.compare(row1, row2);
    }
  }

  public class KVSorterIterator extends KVIterator<UnsafeRow, UnsafeRow> {
    private UnsafeRow key = new UnsafeRow(keySchema.size());
    private UnsafeRow value = new UnsafeRow(valueSchema.size());
    private final UnsafeSorterIterator underlying;

    private KVSorterIterator(UnsafeSorterIterator underlying) {
      this.underlying = underlying;
    }

    @Override
    public boolean next() throws IOException {
      try {
        if (underlying.hasNext()) {
          underlying.loadNext();

          Object baseObj = underlying.getBaseObject();
          long recordOffset = underlying.getBaseOffset();
          int recordLen = underlying.getRecordLength();

          // Note that recordLen = keyLen + valueLen + 4 bytes (for the keyLen itself)
          int keyLen = Platform.getInt(baseObj, recordOffset);
          int valueLen = recordLen - keyLen - 4;
          key.pointTo(baseObj, recordOffset + 4, keyLen);
          value.pointTo(baseObj, recordOffset + 4 + keyLen, valueLen);

          return true;
        } else {
          key = null;
          value = null;
          cleanupResources();
          return false;
        }
      } catch (IOException e) {
        cleanupResources();
        throw e;
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
      cleanupResources();
    }
  }
}
