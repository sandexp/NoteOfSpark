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

package org.apache.spark.sql.execution

import java.util.NoSuchElementException

import org.apache.spark.sql.catalyst.InternalRow

/**
  * 内部迭代器接口,可以代表更加精确的API
 * scala迭代器的重要部分就是@hasNext和@next的施压。
 */
abstract class RowIterator {
  /**
    * 迭代器移动一行，如果迭代器没有行了就返回false。否则返回true。新的行数据可以通过方法@getRow检索
   */
  def advanceNext(): Boolean

  /**
    * 从迭代器中检索行数据。这个方法是幂等的。在@advanceNext返回false调用时非法的。
   */
  def getRow: InternalRow

  /**
    * 将行式迭代器转换为scala的迭代器
   */
  def toScala: Iterator[InternalRow] = new RowIteratorToScala(this)
}

object RowIterator {
  // 由scala迭代器转换为行式迭代器
  def fromScala(scalaIter: Iterator[InternalRow]): RowIterator = {
    scalaIter match {
      case wrappedRowIter: RowIteratorToScala => wrappedRowIter.rowIter
      case _ => new RowIteratorFromScala(scalaIter)
    }
  }
}

/**
  * 行式迭代器的scala形式
  * @param rowIter  行式迭代器
  */
private final class RowIteratorToScala(val rowIter: RowIterator) extends Iterator[InternalRow] {
  /*
  参数列表:
  hasNextWasCalled: Boolean   是否hasnext已经调用
  _hasNext: Boolean 是否包含下一个元素
   */
  private [this] var hasNextWasCalled: Boolean = false
  private [this] var _hasNext: Boolean = false
  override def hasNext: Boolean = {
    // Idempotency:
    if (!hasNextWasCalled) {
      _hasNext = rowIter.advanceNext()
      hasNextWasCalled = true
    }
    _hasNext
  }
  // 获取当前元素,并移动迭代器指针(通过行式迭代器移动)
  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    hasNextWasCalled = false
    rowIter.getRow
  }
}

private final class RowIteratorFromScala(scalaIter: Iterator[InternalRow]) extends RowIterator {
  private[this] var _next: InternalRow = null
  override def advanceNext(): Boolean = {
    if (scalaIter.hasNext) {
      _next = scalaIter.next()
      true
    } else {
      _next = null
      false
    }
  }
  override def getRow: InternalRow = _next
  override def toScala: Iterator[InternalRow] = scalaIter
}
