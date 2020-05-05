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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;

/**
 * 构建批次写出器@BatchWrite 的接口.实现了可以混合其他接口,用于支持不同方式写出到数据源的功能.
 * 除非是被混合接口修改,批次写出器@BatchWrite 添加数据的时候不会影响存在的数据.
 */
@Evolving
public interface WriteBuilder {

  /**
   * 从spark传递一个查询编号@queryId 给数据源.查询编号是查询的唯一标识符,可能有多个查询同时运行,或者是多个查询同时重启和暂停.
   *
   * 返回一个构建器,携带有查询编号@queryId.默认情况下返回本类对象
   */
  default WriteBuilder withQueryId(String queryId) {
    return this;
  }

  /**
   * spark传递输入数据的schema到数据源中
   *
   * @return a new builder with the `schema`. By default it returns `this`, which means the given
   *         `schema` is ignored. Please override this method to take the `schema`.
   */
  default WriteBuilder withInputDataSchema(StructType schema) {
    return this;
  }

  /**
   * 返回一个批次写出器@BatchWrite,这个写出器将数据写出到批次源中.默认情况下抛出异常,数据源必须重新这个方法,用于提供这个实现.
   * 如果表提供了批量写入的功能,则必须要实现这个方法
   */
  default BatchWrite buildForBatch() {
    throw new UnsupportedOperationException(getClass().getName() +
      " does not support batch write");
  }

  /**
   * 返回一个流式写出器@StreamingWrite,将数据写出到流式源中.默认情况下,这个方法会抛出异常,数据源必须重新这个方法,用于提供实现.
   * 如果表需要流式写出功能,则必须对其进行实现
   */
  default StreamingWrite buildForStreaming() {
    throw new UnsupportedOperationException(getClass().getName() +
      " does not support streaming write");
  }
}
