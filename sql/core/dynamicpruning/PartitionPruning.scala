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

package org.apache.spark.sql.dynamicpruning

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 分区移除:
 动态分区移除优化是基于join操作的类型和选择性进行的.在查询优化期间,使用过滤器在分区表上插入一条谓词.
 DPP的基本原理就是,使用其他的过滤器插入一条副本子查询.需要满足如下条件:
  1. 需要删除的表使用join key的方式进行分区
  2. join操作是下述类型,inner, left semi,left outer,right outer操作
为了在广播变量中开启分区移除功能,使用自定义的分区移除@DynamicPruning 语法,在查询计划中,当join类型可知的时候,使用下述原理
  1. 如果join是广播变量的hash join,可以使用广播变量的重用值替换副本子查询
  2. 否则,如果如果分区移除的开销大于运行子查询两次的开销,则保持子查询
  3. 否则移除子查询
 */
object PartitionPruning extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * 获取给定分区的分区表的扫描情况,返回形式以逻辑计划形式表示
   */
  def getPartitionTableScan(a: Expression, plan: LogicalPlan): Option[LogicalRelation] = {
    //1. 获取表达式和逻辑计划的原始信息
    val srcInfo: Option[(Expression, LogicalPlan)] = findExpressionAndTrackLineageDown(a, plan)
    srcInfo.flatMap {
      case (resExp, l: LogicalRelation) =>
        l.relation match {
          // 表达式和逻辑计划关系是处于hadoop上的,则对其进行处理
          case fs: HadoopFsRelation =>
            val partitionColumns = AttributeSet(
              l.resolve(fs.partitionSchema, fs.sparkSession.sessionState.analyzer.resolver))
            if (resExp.references.subsetOf(partitionColumns)) {
              return Some(l)
            } else {
              None
            }
          case _ => None
        }
      case _ => None
    }
  }

  /**
   插入一个动态分区移除的谓词,在join的一侧使用过滤器,用于join的另一侧
    1. 可以在查询计划期间辨识过滤器,可以自定义@DynamicPruning 表达式(使用普通的in表达式)
    2. 插入一个标记位,这个标记位表示子查询是否重要,且无论join策略如何,都需要运行
      pruningKey 需要修剪的key的表达式
      pruningPlan 需要修剪的逻辑计划
      filteringKey  key过滤表达式
      filteringPlan 逻辑计划表达式
      joinKeys  参与join的表达式列表
      hasBenefit  是否有价值
   */
  private def insertPredicate(
      pruningKey: Expression,
      pruningPlan: LogicalPlan,
      filteringKey: Expression,
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression],
      hasBenefit: Boolean): LogicalPlan = {
    //1. 驱动是否需要重用
    val reuseEnabled = SQLConf.get.dynamicPartitionPruningReuseBroadcast
    // 2. 确定过滤的key在joinkey表达式列表中的位置
    val index = joinKeys.indexOf(filteringKey)
    // 3. 根据重用和使用的价值决定逻辑计划到底是哪个
    if (hasBenefit || reuseEnabled) {
      // insert a DynamicPruning wrapper to identify the subquery during query planning
      Filter(
        DynamicPruningSubquery(
          pruningKey,
          filteringPlan,
          joinKeys,
          index,
          !hasBenefit),
        pruningPlan)
    } else {
      // abort dynamic partition pruning
      pruningPlan
    }
  }

  /**
   给定一个估测的过滤器比例,假定分区修剪有益处(即在分区计划在过滤之后大于另一端join操作的字节大小的情况下).使用列统计值
    对过滤比例进行过滤,否则使用配置值@spark.sql.optimizer.joinFilterRatio
        partExpr 分区表达式
        partPlan 分区逻辑计划
        otherExpr 其他表达式
        otherPlan 其他逻辑计划
   */
  private def pruningHasBenefit(
      partExpr: Expression,
      partPlan: LogicalPlan,
      otherExpr: Expression,
      otherPlan: LogicalPlan): Boolean = {
    // 获取给定表属性的去重统计
    def distinctCounts(attr: Attribute, plan: LogicalPlan): Option[BigInt] = {
      plan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
    }
    // 1. 当CBO状态丢失,但是有可以选择的谓词时,获取默认的过滤比例
    val fallbackRatio = SQLConf.get.dynamicPartitionPruningFallbackFilterRatio
    
    // 2. 获取基于join类型和列统计值的过滤比例
    val filterRatio = (partExpr.references.toList, otherExpr.references.toList) match {
      // filter out expressions with more than one attribute on any side of the operator
      case (leftAttr :: Nil, rightAttr :: Nil)
        if SQLConf.get.dynamicPartitionPruningUseStats =>
          // get the CBO stats for each attribute in the join condition
          val partDistinctCount = distinctCounts(leftAttr, partPlan)
          val otherDistinctCount = distinctCounts(rightAttr, otherPlan)
          val availableStats = partDistinctCount.isDefined && partDistinctCount.get > 0 &&
            otherDistinctCount.isDefined
          if (!availableStats) {
            fallbackRatio
          } else if (partDistinctCount.get.toDouble <= otherDistinctCount.get.toDouble) {
            // there is likely an estimation error, so we fallback
            fallbackRatio
          } else {
            1 - otherDistinctCount.get.toDouble / partDistinctCount.get.toDouble
          }
      case _ => fallbackRatio
    }

    // 3. 获取修剪的开销,这个开销等于所有关系扫描的总字节大小
    val overhead = otherPlan.collectLeaves().map(_.stats.sizeInBytes).sum.toFloat
    // 4. 确认修剪是否有益处的方式就是: 过滤字节数量与开销字节数量的大小关系比较
    filterRatio * partPlan.stats.sizeInBytes.toFloat > overhead.toFloat
  }

  
  /**
    确定表达式是否可以选择的
   */
  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case Like(_, _, _) => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case _ => false
  }

  /**
    确定指定的逻辑计划是否有过滤器谓词
   */
  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.find {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }.isDefined
  }

  /**
   确定是否可以对于join key操作移除分区.过滤器侧需要满足两点条件
    1. 不能是流式的
    2. 需要包含可选择的谓词(用于过滤)
   */
  private def hasPartitionPruningFilter(plan: LogicalPlan): Boolean = {
    !plan.isStreaming && hasSelectivePredicate(plan)
  }

  // 确定指定的join是否可向左移除(Inner | LeftSemi | RightOuter)
  private def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }
  
  // 确定指定的join类型@joinType 是否可向右移除(inner/LeftOuter)
  private def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftOuter => true
    case _ => false
  }

  private def prune(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      // skip this rule if there's already a DPP subquery on the LHS of a join
      case j @ Join(Filter(_: DynamicPruningSubquery, _), _, _, _, _) => j
      case j @ Join(_, Filter(_: DynamicPruningSubquery, _), _, _, _) => j
      case j @ Join(left, right, joinType, Some(condition), hint) =>
        var newLeft = left
        var newRight = right

        // extract the left and right keys of the join condition
        val (leftKeys, rightKeys) = j match {
          case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _) => (lkeys, rkeys)
          case _ => (Nil, Nil)
        }

        // checks if two expressions are on opposite sides of the join
        def fromDifferentSides(x: Expression, y: Expression): Boolean = {
          def fromLeftRight(x: Expression, y: Expression) =
            !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
              !y.references.isEmpty && y.references.subsetOf(right.outputSet)
          fromLeftRight(x, y) || fromLeftRight(y, x)
        }

        splitConjunctivePredicates(condition).foreach {
          case EqualTo(a: Expression, b: Expression)
              if fromDifferentSides(a, b) =>
            val (l, r) = if (a.references.subsetOf(left.outputSet) &&
              b.references.subsetOf(right.outputSet)) {
              a -> b
            } else {
              b -> a
            }

            // there should be a partitioned table and a filter on the dimension table,
            // otherwise the pruning will not trigger
            var partScan = getPartitionTableScan(l, left)
            if (partScan.isDefined && canPruneLeft(joinType) &&
                hasPartitionPruningFilter(right)) {
              val hasBenefit = pruningHasBenefit(l, partScan.get, r, right)
              newLeft = insertPredicate(l, newLeft, r, right, rightKeys, hasBenefit)
            } else {
              partScan = getPartitionTableScan(r, right)
              if (partScan.isDefined && canPruneRight(joinType) &&
                  hasPartitionPruningFilter(left) ) {
                val hasBenefit = pruningHasBenefit(r, partScan.get, l, left)
                newRight = insertPredicate(r, newRight, l, left, leftKeys, hasBenefit)
              }
            }
          case _ =>
        }
        Join(newLeft, newRight, joinType, Some(condition), hint)
    }
  }

  // 获取逻辑计划
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // Do not rewrite subqueries.
    case s: Subquery if s.correlated => plan
    case _ if !SQLConf.get.dynamicPartitionPruningEnabled => plan
    case _ => prune(plan)
  }
}
