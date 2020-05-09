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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;
import scala.collection.Iterator;
import scala.math.Ordering;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * unsafe 的外部行排序器
 */
public final class UnsafeExternalRowSorter {

  // 如果为正数,将会将记录溢写到磁盘上(按照指定的频率),仅仅在测试环境使用
  private int testSpillFrequency = 0;
  // 需要插入的行数量
  private long numRowsInserted = 0;
  // schema类型
  private final StructType schema;
  // 前缀计算器
  private final UnsafeExternalRowSorter.PrefixComputer prefixComputer;
  // 排序器
  private final UnsafeExternalSorter sorter;

  // 确定资源是否被释放,清除工作之后,再使用迭代器调用就会返回false.下游操作会触发资源的情况.
  private boolean isReleased = false;

  /**
   * 前缀计算器
   */
  public abstract static class PrefixComputer {

    /**
     * 前缀
     * @value 前缀value值,当isNull为true的时候为空前缀值
     * @isNull 前缀是否为空
     */
    public static class Prefix {
      public long value;
      public boolean isNull;
    }

    /**
     * 对于给定的行计算前缀.为了提升效率,返回的对象可以重用
     */
    public abstract Prefix computePrefix(InternalRow row);
  }

  public static UnsafeExternalRowSorter createWithRecordComparator(
      StructType schema,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      UnsafeExternalRowSorter.PrefixComputer prefixComputer,
      long pageSizeBytes,
      boolean canUseRadixSort) throws IOException {
    return new UnsafeExternalRowSorter(schema, recordComparatorSupplier, prefixComparator,
      prefixComputer, pageSizeBytes, canUseRadixSort);
  }

  public static UnsafeExternalRowSorter create(
      StructType schema,
      Ordering<InternalRow> ordering,
      PrefixComparator prefixComparator,
      UnsafeExternalRowSorter.PrefixComputer prefixComputer,
      long pageSizeBytes,
      boolean canUseRadixSort) throws IOException {
    Supplier<RecordComparator> recordComparatorSupplier =
      () -> new RowComparator(ordering, schema.length());
    return new UnsafeExternalRowSorter(schema, recordComparatorSupplier, prefixComparator,
      prefixComputer, pageSizeBytes, canUseRadixSort);
  }

  private UnsafeExternalRowSorter(
      StructType schema,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      UnsafeExternalRowSorter.PrefixComputer prefixComputer,
      long pageSizeBytes,
      boolean canUseRadixSort) {
    this.schema = schema;
    this.prefixComputer = prefixComputer;
    final SparkEnv sparkEnv = SparkEnv.get();
    final TaskContext taskContext = TaskContext.get();
    sorter = UnsafeExternalSorter.create(
      taskContext.taskMemoryManager(),
      sparkEnv.blockManager(),
      sparkEnv.serializerManager(),
      taskContext,
      recordComparatorSupplier,
      prefixComparator,
      (int) (long) sparkEnv.conf().get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE()),
      pageSizeBytes,
      (int) SparkEnv.get().conf().get(
        package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD()),
      canUseRadixSort
    );
  }

  /**
   * 按照指定的频率@frequency 进行强制溢写,仅仅在测试环境可用
   */
  @VisibleForTesting
  void setTestSpillFrequency(int frequency) {
    assert frequency > 0 : "Frequency must be positive";
    testSpillFrequency = frequency;
  }

  /**
   * 插入指定行@row
   * @param row 需要插入的行
   * @throws IOException
   */
  public void insertRow(UnsafeRow row) throws IOException {
    // 1. 计算插入行的前缀信息
    final PrefixComputer.Prefix prefix = prefixComputer.computePrefix(row);
    // 2. 根据给出行,计算出这条行记录的起始偏移量,大小,以及基础值信息,将其插入到外部排序器@sorter中
    sorter.insertRecord(
      row.getBaseObject(),
      row.getBaseOffset(),
      row.getSizeInBytes(),
      prefix.value,
      prefix.isNull
    );
    // 3. 更新行插入信息的度量值
    numRowsInserted++;
    // 4. 需要的情况下进行溢写(只有测试情况下才会)
    if (testSpillFrequency > 0 && (numRowsInserted % testSpillFrequency) == 0) {
      sorter.spill();
    }
  }

  /**
   * 获取峰值内存使用量
   */
  public long getPeakMemoryUsage() {
    return sorter.getPeakMemoryUsedBytes();
  }

  /**
   * 获取排序的时间(ns)
   */
  public long getSortTimeNanos() {
    return sorter.getSortTimeNanos();
  }

  // 清理排序器中的数据
  public void cleanupResources() {
    isReleased = true;
    sorter.cleanupResources();
  }

  /**
   * 行排序函数
   * @return 排序完成的行数据
   */
  public Iterator<InternalRow> sort() throws IOException {
    try {
      // 1. 获取排序完成的迭代器
      final UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();
      // 2. 如果没有行数据,则清空所有资源
      if (!sortedIterator.hasNext()) {
        // Since we won't ever call next() on an empty iterator, we need to clean up resources
        // here in order to prevent memory leaks.
        cleanupResources();
      }
      //3. 获取行迭代器,用于获取行数据
      return new RowIterator() {

        /**
         * @numFields schema中属性的数量
         * @row unsafe行
         */
        private final int numFields = schema.length();
        private UnsafeRow row = new UnsafeRow(numFields);

        // 将行的指针指向下一个行的位置
        @Override
        public boolean advanceNext() {
          try {
            if (!isReleased && sortedIterator.hasNext()) {
              sortedIterator.loadNext();
              row.pointTo(
                  sortedIterator.getBaseObject(),
                  sortedIterator.getBaseOffset(),
                  sortedIterator.getRecordLength());
              // Here is the initial bug fix in SPARK-9364: the bug fix of use-after-free bug
              // when returning the last row from an iterator. For example, in
              // [[GroupedIterator]], we still use the last row after traversing the iterator
              // in `fetchNextGroupIterator`
              if (!sortedIterator.hasNext()) {
                row = row.copy(); // so that we don't have dangling pointers to freed page
                cleanupResources();
              }
              return true;
            } else {
              row = null; // so that we don't keep references to the base object
              return false;
            }
          } catch (IOException e) {
            cleanupResources();
            // Scala iterators don't declare any checked exceptions, so we need to use this hack
            // to re-throw the exception:
            Platform.throwException(e);
          }
          throw new RuntimeException("Exception should have been re-thrown in next()");
        }

        // 获取当前行
        @Override
        public UnsafeRow getRow() { return row; }

      }.toScala();
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }

  public Iterator<InternalRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException {
    while (inputIterator.hasNext()) {
      insertRow(inputIterator.next());
    }
    return sort();
  }

  /**
   * 行比较器
   */
  private static final class RowComparator extends RecordComparator {
    // 行排序规则
    private final Ordering<InternalRow> ordering;
    // 比较行1
    private final UnsafeRow row1;
    // 比较行2
    private final UnsafeRow row2;

    RowComparator(Ordering<InternalRow> ordering, int numFields) {
      this.row1 = new UnsafeRow(numFields);
      this.row2 = new UnsafeRow(numFields);
      this.ordering = ordering;
    }

    // 行比较逻辑
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
      row1.pointTo(baseObj1, baseOff1, 0);
      row2.pointTo(baseObj2, baseOff2, 0);
      return ordering.compare(row1, row2);
    }
  }
}
