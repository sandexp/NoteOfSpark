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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;

/**
 * 通过微批次流@MicroBatchStream 或者连续流@ContinuousStream处理的抽象实现
 * 在执行期间,数据源的偏移量必须被记录,且用作重启的检查点.每个数据源需要提供偏移量的实现,数据源可以使用这个实现去重构数据流中的位置
 * 到已经被处理的位置.
 */
@Evolving
public abstract class Offset {
    /**
     * 偏移量的json序列化的实现,用于将偏移量保存到偏移量日志中.
     * 注意: 假定json的标识与偏移量的内容相等
     *
     * A JSON-serialized representation of an Offset that is
     * used for saving offsets to the offset log.
     * Note: We assume that equivalent/equal offsets serialize to
     * identical JSON strings.
     *
     * @return JSON 串
     */
    public abstract String json();

    /**
     * 判断两个偏移量是否相等
     *
     * Equality based on JSON string representation. We leverage the
     * JSON representation for normalization between the Offset's
     * in deserialized and serialized representations.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Offset) {
            return this.json().equals(((Offset) obj).json());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.json().hashCode();
    }

    @Override
    public String toString() {
        return this.json();
    }
}
