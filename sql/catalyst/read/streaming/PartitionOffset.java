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

import java.io.Serializable;

/**
 * 在流式处理中,用于每个分区的偏移量,连续的读取器实现会提高去合并全局偏移量的方法.这个偏移量必须要是可以序列化的
 *
 * Used for per-partition offsets in continuous processing. ContinuousReader implementations will
 * provide a method to merge these into a global Offset.
 *
 * These offsets must be serializable.
 */
@Evolving
public interface PartitionOffset extends Serializable {
}
