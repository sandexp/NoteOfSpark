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

package org.apache.spark.sql.catalyst.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

// 规则执行器
object RuleExecutor {
  protected val queryExecutionMeter = QueryExecutionMetering()

  // 运行指定规则的统计值
  def dumpTimeSpent(): String = {
    queryExecutionMeter.dumpTimeSpent()
  }

  // 重置运行指定规则的统计值
  def resetMetrics(): Unit = {
    queryExecutionMeter.resetMetrics()
  }
}

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   规则的执行策略,表名执行的最大数量.如果执行在最大迭代次数之前,到达指定的数量,会停止
   */
  abstract class Strategy { def maxIterations: Int }

  // 只运行一次的策略,是幂等性的
  case object Once extends Strategy { val maxIterations = 1 }

  /**
    运行到指定点或者是最大迭代次数停止,无论先满足哪个条件都会停止.
    特别地,FixedPoint(1)表示只运行一次
   */
  case class FixedPoint(maxIterations: Int) extends Strategy

  /**
    * 规则的批次
    * @param name 批次名称
    * @param strategy 批次的策略
    * @param rules 规则
    */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  // 定义规则批次序列,可以在实现中重写
  protected def batches: Seq[Batch]

  // 幂等性检测中的黑名单批次
  protected val blacklistedOnceBatches: Set[String] = Set.empty

  /**
    定义检测函数.可以给结构化的计划进行检测.例如,在优化器中的每个规则检查是否计划可以解决.所以可以捕捉无效的计划.在给定的计划没有穿的
    结构化整合的检查的时候,检测函数会返回false.
   */
  protected def isPlanIntegral(plan: TreeType): Boolean = true

  /**
   检查重新优化后,计划是否保持一致的函数
   */
  private def checkBatchIdempotence(batch: Batch, plan: TreeType): Unit = {
    val reOptimized = batch.rules.foldLeft(plan) { case (p, rule) => rule(p) }
    // 如果计划发生了改变,会抛出异常
    if (!plan.fastEquals(reOptimized)) {
      val message =
        s"""
           |Once strategy's idempotence is broken for batch ${batch.name}
           |${sideBySide(plan.treeString, reOptimized.treeString).mkString("\n")}
          """.stripMargin
      throw new TreeNodeException(reOptimized, message, null)
    }
  }

  /**
   执行子类定义的规则批次,且对于每个规则使用定位器去定位时间信息
   */
  def executeAndTrack(plan: TreeType, tracker: QueryPlanningTracker): TreeType = {
    QueryPlanningTracker.withTracker(tracker) {
      execute(plan)
    }
  }

  /**
    执行由子类定义的批次,这个批次会被序列化的执行1,使用定义的执行策略.在每个批次中,规则也是序列化的执行的
    @plan 使用执行的计划
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    val queryExecutionMetrics = RuleExecutor.queryExecutionMeter
    val planChangeLogger = new PlanChangeLogger()
    val tracker: Option[QueryPlanningTracker] = QueryPlanningTracker.get

    // 1. 对初始输入进行结构化完整性检测
    if (!isPlanIntegral(plan)) {
      val message = "The structural integrity of the input plan is broken in " +
        s"${this.getClass.getName.stripSuffix("$")}."
      throw new TreeNodeException(plan, message, null)
    }

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // 2. 运行到指定的点为止(或者是策略指定的最大迭代次数,循环进行)
      while (continue) {
        // 3. 获取当前的执行计划
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            val effective = !result.fastEquals(plan)

            if (effective) {
              queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName)
              queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime)
              planChangeLogger.logRule(rule.ruleName, plan, result)
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)

            // Record timing information using QueryPlanningTracker
            tracker.foreach(_.recordRuleInvocation(rule.ruleName, runTime, effective))

            // Run the structural integrity checker against the plan after each rule.
            if (!isPlanIntegral(result)) {
              val message = s"After applying rule ${rule.ruleName} in batch ${batch.name}, " +
                "the structural integrity of the plan is broken."
              throw new TreeNodeException(result, message, null)
            }

            result
        }
        // 4. 确定是否超出策略指定的最大迭代次数
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}"
            if (Utils.isTesting) {
              // 测试情况不允许多次迭代
              throw new TreeNodeException(curPlan, message, null)
            } else {
              // 超出一次迭代,则打印出相关的日志信息
              logWarning(message)
            }
          }
          // 检查单批次的幂等性
          if (batch.strategy == Once &&
            Utils.isTesting && !blacklistedOnceBatches.contains(batch.name)) {
            checkBatchIdempotence(batch, curPlan)
          }
          // 超出最大迭代次数,则停止执行计划的执行
          continue = false
        }

        // 如果是最后一个执行计划则停止运行
        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }
      // 5, 打印执行批次的信息
      planChangeLogger.logBatch(batch.name, batchStartPlan, curPlan)
    }

    curPlan
  }
  
  /**
      logLevel  日志等级
      logRules  日志规则
      logBatches  日志批次
    */
  private class PlanChangeLogger {

    private val logLevel = SQLConf.get.optimizerPlanChangeLogLevel

    private val logRules = SQLConf.get.optimizerPlanChangeRules.map(Utils.stringToSeq)

    private val logBatches = SQLConf.get.optimizerPlanChangeBatches.map(Utils.stringToSeq)

    // 规则信息聚合
    def logRule(ruleName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
      if (logRules.isEmpty || logRules.get.contains(ruleName)) {
        def message(): String = {
          s"""
             |=== Applying Rule ${ruleName} ===
             |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
           """.stripMargin
        }

        logBasedOnLevel(message)
      }
    }
    
    // 批次信息记录
    def logBatch(batchName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
      if (logBatches.isEmpty || logBatches.get.contains(batchName)) {
        def message(): String = {
          if (!oldPlan.fastEquals(newPlan)) {
            s"""
               |=== Result of Batch ${batchName} ===
               |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
          """.stripMargin
          } else {
            s"Batch ${batchName} has no effect."
          }
        }

        logBasedOnLevel(message)
      }
    }

    // 等级信息记录
    private def logBasedOnLevel(f: => String): Unit = {
      logLevel match {
        case "TRACE" => logTrace(f)
        case "DEBUG" => logDebug(f)
        case "INFO" => logInfo(f)
        case "WARN" => logWarning(f)
        case "ERROR" => logError(f)
        case _ => logTrace(f)
      }
    }
  }
}
