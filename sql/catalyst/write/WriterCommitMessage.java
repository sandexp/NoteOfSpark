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

import java.io.Serializable;

/**
 * 由数据写出器@DataWriter 使用提交方法@commit 返回的提交消息。会被发送会driver端.
 * 这是一个空接口,数据源需要自定义字节的消息类,且在执行器端生成消息的时候使用这个接口,并且在driver端出来消息.
 */
@Evolving
public interface WriterCommitMessage extends Serializable {}
