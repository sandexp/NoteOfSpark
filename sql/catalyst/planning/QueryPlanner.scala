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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
  查询计划处理器
  给定逻辑计划@LogicalPlan,返回一个逻辑计划列表,这个逻辑计划可以用于执行.
  如果策略没有使用给定的逻辑操作,那么会返回一个空列表
 */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {

  /**
    返回逻辑计划的占位符,这个占位符用于执行计划操作.这个占位符会被查询计划处理器自动填充,使用可以的执行策略.
   */
  protected def planLater(plan: LogicalPlan): PhysicalPlan

  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}

/**

  查询计划处理器,抽象类,用于将逻辑计划@LogicalPlan 转化为物理计划.
  子类需要指定@GenericStrategy 列表,每个策略可以返回物理执行计划.如果给定的策略不能处理树中剩余的操作符,会调用@planLater方法，
  这个方法会返回一个可以被@collected 的对象，且将其使用可以的策略填充进去
  
 * TODO: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER...
 *       PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
 *
 * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
 */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  // 执行策略列表,可以被计划处理器使用
  def strategies: Seq[GenericStrategy[PhysicalPlan]]
  
  // 将指定的逻辑计划转化为物理计划
  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    // 收集物理计划候选者
    val candidates = strategies.iterator.flatMap(_(plan))
    // 候选者中可能包含@planLater 的占位符,所以尝试使用子计划对其进行替换
    val plans = candidates.flatMap { candidate =>
      // 收集候选物理计划中的占位符信息
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // 物理计划候选者中不包含占位符,直接将其作为物理计划即可
        Iterator(candidate)
      } else {
        // 将逻辑计划标记为@planLater 且代替占位符
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // 先完成子计划(递归)的逻辑计划
            val childPlans = this.plan(logicalPlan)

            // 替换当前计划中的占位符
            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) => childPlan
                }
              }
            }
        }
      }
    }
    // 对生成的计划进行优化
    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

  // 使用@planLater 的策略收集占位符标记
  protected def collectPlaceholders(plan: PhysicalPlan): Seq[(PhysicalPlan, LogicalPlan)]

  // 优化效率低的计划,组织组合展开的问题
  protected def prunePlans(plans: Iterator[PhysicalPlan]): Iterator[PhysicalPlan]
}
