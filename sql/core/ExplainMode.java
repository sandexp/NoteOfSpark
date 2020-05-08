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
package org.apache.spark.sql;

import org.apache.spark.annotation.Unstable;

/**
 * 解释模式
 * 用于只读执行计划(逻辑/物理)的输出格式,用于debug
 */
@Unstable
public enum ExplainMode {
  /**
   * 简单模式,意味着打印DataFrame的执行计划的时候,需要将物理计划打印到控制台上
   * @since 3.0.0
   */
  Simple,
  /**
   * 扩展模式,当打印DF的执行计划的时候,物理执行计划和逻辑执行计划都会打印到控制台上
   * @since 3.0.0
   */
  Extended,
  /**
   *
   * 依赖模式,当打印DF的执行计划的时候,如果生成的代码可用,物理执行计划和生成的代码会打印在控制台上
   * @since 3.0.0
   */
  Codegen,
  /**
   *
   * 成本模式,意味着打印一个DF的信息的时候,如果计划节点的统计数值可以获取到,逻辑计划和统计值会打印到控制台上
   * @since 3.0.0
   */
  Cost,
  /**
   * 格式化模式意味着当打印DF的信息的时候,解释的输出会分成两个部分,分别是物理计划和节点信息.
   * @since 3.0.0
   */
  Formatted
}
