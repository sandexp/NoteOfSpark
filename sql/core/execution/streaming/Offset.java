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

package org.apache.spark.sql.execution.streaming;

/**
 * 偏移量
 * 这个类是@org.apache.spark.sql.connector.read.streaming.Offset的子类,这个是内部系统的类,且弃用了.新的流式数据源实现需要使用
 * v2版本的API.这个长期使用.
 * 这个类今后版本可能会移除
 */
public abstract class Offset extends org.apache.spark.sql.connector.read.streaming.Offset {}
