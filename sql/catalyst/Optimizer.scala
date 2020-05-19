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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
  优化器
  所有优化器继承的抽象类，包含标准批次(继承的优化器可以重写)
 */
abstract class Optimizer(catalogManager: CatalogManager)
  extends RuleExecutor[LogicalPlan] {

  // 检查计划的结构完整性(使用在测试模式下),当前在计划准则执行完毕之后进行检测,包括如下内容
  // 1. 是否执行计划已经被处理
  // 2. 仅仅在支持的操作符上持有特殊的表达式
  override protected def isPlanIntegral(plan: LogicalPlan): Boolean = {
    !Utils.isTesting || (plan.resolved &&
      plan.find(PlanHelper.specialExpressionsInUnsupportedOperator(_).nonEmpty).isEmpty)
  }
  
  /**
    * 黑名单批次列表
    */
  override protected val blacklistedOnceBatches: Set[String] =
    Set(
      "PartitionPruning",
      "Extract Python UDFs")

  // 获取指定点的最大迭代次数
  protected def fixedPoint = FixedPoint(SQLConf.get.optimizerMaxIterations)

  /**
    定义优化器的默认规则批次
    这个类的实现需要重写这个方法,且规则批次运行在优化器中.由批次@batches返回
   */
  def defaultBatches: Seq[Batch] = {
    // 获取操作优化集合
    val operatorOptimizationRuleSet =
      Seq(
        // Operator push down
        PushProjectionThroughUnion,
        ReorderJoin,
        EliminateOuterJoin,
        PushDownPredicates,
        PushDownLeftSemiAntiJoin,
        PushLeftSemiLeftAntiThroughJoin,
        LimitPushDown,
        ColumnPruning,
        InferFiltersFromConstraints,
        // 合并操作
        CollapseRepartition,
        CollapseProject,
        CollapseWindow,
        CombineFilters,
        CombineLimits,
        CombineUnions,
        // 常数累加以及长度缩减
        TransposeWindow,
        NullPropagation,
        ConstantPropagation,
        FoldablePropagation,
        OptimizeIn,
        ConstantFolding,
        ReorderAssociativeOperator,
        LikeSimplification,
        BooleanSimplification,
        SimplifyConditionals,
        RemoveDispensableExpressions,
        SimplifyBinaryComparison,
        ReplaceNullWithFalseInPredicate,
        PruneFilters,
        SimplifyCasts,
        SimplifyCaseConversionExpressions,
        RewriteCorrelatedScalarSubquery,
        EliminateSerialization,
        RemoveRedundantAliases,
        RemoveNoopOperators,
        SimplifyExtractValueOps,
        CombineConcats) ++
        extendedOperatorOptimizationRules
    
    // 获取操作优化批次列表
    /*
    包括:
    1. 推测过滤器之前的操作优化
    2. 推测过滤器
    3. 在如此过滤器之后的操作优化器
     */
    val operatorOptimizationBatch: Seq[Batch] = {
      val rulesWithoutInferFiltersFromConstraints =
        operatorOptimizationRuleSet.filterNot(_ == InferFiltersFromConstraints)
      Batch("Operator Optimization before Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) ::
      Batch("Infer Filters", Once,
        InferFiltersFromConstraints) ::
      Batch("Operator Optimization after Inferring Filters", fixedPoint,
        rulesWithoutInferFiltersFromConstraints: _*) :: Nil
    }

    /*
    批次信息
    1. 去重信息
    2. 最终分析信息
    3. Union信息
    4. 优化零极限
    5. 早期本地相关处理(减少rule的遍历处理)
    6. 挂起相关的表达式
    7. 子查询优化
    8. 操作符替代优化
    9. 聚合优化
    10. 早期过滤条件和主属性的过滤
    11. join重排序
    12. 移除排序
    13. 十进制数的优化
    14. 对象表达式优化
    15. 本地相关性处理
    16. 检查笛卡尔积
    17. 重新子查询
    18. 浮点数正常化处理
     */
    val batches = (Batch("Eliminate Distinct", Once, EliminateDistinct) ::
    /*
    在最终分析(Finish Analysis)中的一些规则不是优化准则，且书友分析器中，因此需要修正。
    但是，因为页需要使用分析器去对查询进行规范化处理。因此在分析器中不应该消除子查询，或者计算当前时间
     */
    Batch("Finish Analysis", Once,
      EliminateResolvedHint,
      EliminateSubqueryAliases,
      EliminateView,
      ReplaceExpressions,
      ComputeCurrentTime,
      GetCurrentDatabase(catalogManager),
      RewriteDistinctAggregates,
      ReplaceDeduplicateWithAggregate) :
    /*
    优化器起始于此
    1. 在启动优化准则之前,进行Union合并@CombineUnions 的首次调用,因为其可以降低迭代器的数量且其他准则可以添加或者移除两个union
    操作间的额外操作.
    2. 调用批次的合并操作(其他准则可能有两个独立相连的union操作)
     */
    Batch("Union", Once,
      CombineUnions) ::
    Batch("OptimizeLimitZero", Once,
      OptimizeLimitZero) ::
    /*
    下面的操作会简化逻辑计划,且降低优化器的执行开销.
    例如,操作Filter(LocalRelation)会遍历所有优化器准则，这些准则当其为过滤器(例如InferFiltersFromConstraints)的时候
    会被触发.如果早期运行这个批次,查询会是本地相关的@LocalRelation,且不会触发那么多的准则
     */
    Batch("LocalRelation early", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) ::
    Batch("Pullup Correlated Expressions", Once,
      PullupCorrelatedPredicates) ::
    /*
    子查询批次迭代的使用优化器规则,因此对其保证幂等性是毫无意义的,且会将批次从Once修改为FixedPoint(1)
     */
    Batch("Subquery", FixedPoint(1),
      OptimizeSubqueries) ::
    Batch("Replace Operators", fixedPoint,
      RewriteExceptAll,
      RewriteIntersectAll,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithFilter,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) ::
    Batch("Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) :: Nil ++
    operatorOptimizationBatch) :+
    /*
    这个批次会将过滤条件和主属性(projection)置入扫描节点中.在批次之前,逻辑计划会包含没有汇报状态的节点.
    使用状态的阶段必须在批次之后进行处理
     */
    Batch("Early Filter and Projection Push-Down", Once, earlyScanPushDownRules: _*) :+
    /*
    join重排序:
    AQP的jion代价会在多个线程运行中改变，所以不需要保证在批次中的幂等性。因此使用FixedPoint(1)而不是Once语义
     */
    Batch("Join Reorder", FixedPoint(1),
      CostBasedJoinReorder) :+
    Batch("Eliminate Sorts", Once,
      EliminateSorts) :+
    Batch("Decimal Optimizations", fixedPoint,
      DecimalAggregates) :+
    Batch("Object Expressions Optimization", fixedPoint,
      EliminateMapObjects,
      CombineTypedFilters,
      ObjectSerializerPruning,
      ReassignLambdaVariableID) :+
    Batch("LocalRelation", fixedPoint,
      ConvertToLocalRelation,
      PropagateEmptyRelation) :+
    // 下述操作需要在批次join重排序以及本地相关性处理之后进行
    Batch("Check Cartesian Products", Once,
      CheckCartesianProducts) :+
    Batch("RewriteSubquery", Once,
      RewritePredicateSubquery,
      ColumnPruning,
      CollapseProject,
      RemoveNoopOperators) :+
    // 浮点数的正常化处理,必须在重新子查询@RewriteSubquery 之后处理,这个会创建join
    Batch("NormalizeFloatingNumbers", Once, NormalizeFloatingNumbers)
    // 移除没有rule的批次,这个会当子类没有添加额外的规则的时候发生
    batches.filter(_.rules.nonEmpty)
  }

  /**
    定义不会被优化器排除在外的规则(尽管其可能在SQL配置中的excludedRules设置了,相当于白名单)
    这个类的实现会重新这个方法.这个规则批次最终会运行在优化器上,由批次返回@batches.且规则是默认的规则批次,
    即defaultBatches - (excludedRules - nonExcludableRules)
    这些白名单规则包括:
    1. 去重
    2. 处理的线索信息
    3. 子查询别名规则信息
    4. 视图规则
    5. 替代表达式
    6. 计算当前时间
    7. 获取当前数据库信息
    8. 重新去重的聚合信息
    9. 使用聚合信息替换副本信息
    10. 使用semiJoin替换相交查询
    11. 使用过滤条件替换
    12. 使用AntiJoin进行替换
    13. 全部替换
    14. 替换所有交叉查询
    15. 使用聚合操作替换去重信息
    16. 拉起相关的谓词
    17. 重写相关的标量查询
    18. 浮点数的正常化
   */
  def nonExcludableRules: Seq[String] =
    EliminateDistinct.ruleName ::
      EliminateResolvedHint.ruleName ::
      EliminateSubqueryAliases.ruleName ::
      EliminateView.ruleName ::
      ReplaceExpressions.ruleName ::
      ComputeCurrentTime.ruleName ::
      GetCurrentDatabase(catalogManager).ruleName ::
      RewriteDistinctAggregates.ruleName ::
      ReplaceDeduplicateWithAggregate.ruleName ::
      ReplaceIntersectWithSemiJoin.ruleName ::
      ReplaceExceptWithFilter.ruleName ::
      ReplaceExceptWithAntiJoin.ruleName ::
      RewriteExceptAll.ruleName ::
      RewriteIntersectAll.ruleName ::
      ReplaceDistinctWithAggregate.ruleName ::
      PullupCorrelatedPredicates.ruleName ::
      RewriteCorrelatedScalarSubquery.ruleName ::
      RewritePredicateSubquery.ruleName ::
      NormalizeFloatingNumbers.ruleName :: Nil

  /**
    优化表达式中的所有子查询
   */
  object OptimizeSubqueries extends Rule[LogicalPlan] {
    // 移除指定逻辑执行计划@plan 的顶层排序
    private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
      plan match {
        // 排序类型,则直接取子查询的逻辑计划即可
        case Sort(_, _, child) => child
        // 如果当前逻辑执行计划是一个主属性(不是排序类型),则需要迭代的将其子节点的逻辑计划排序移除
        case Project(fields, child) => Project(fields, removeTopLevelSort(child))
        case other => other
      }
    }
    // 对指定逻辑执行计划@plan进行优化
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case s: SubqueryExpression =>
        // 优化子查询
        val Subquery(newPlan, _) = Optimizer.this.execute(Subquery.fromExpression(s))
        // 在这里,使用优化的子查询计划,这样就可以连接到子查询表达式中.这里可以安全地移除定长排序,这样子查询的元组数据就是无序的了
        s.withNewPlan(removeTopLevelSort(newPlan))
    }
  }

  /**
    重写操作优化规则,用于提供额外的规则信息,用于操作批次优化
   */
  def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = Nil

  /**
    用于对早期的主属性和过滤属性添加额外的规则,将其置于扫描中
   */
  def earlyScanPushDownRules: Seq[Rule[LogicalPlan]] = Nil

  /**
    获取批次信息,即defaultBatches - (excludedRules - nonExcludableRules).这些批次信息是优化器中最终运行的批次信息.
    这个类的实现需要重新默认批次@defaultBatches,且默认的情况下是@nonExcludableRules
   */
  final override def batches: Seq[Batch] = {
    val excludedRulesConf =
      SQLConf.get.optimizerExcludedRules.toSeq.flatMap(Utils.stringToSeq)
    val excludedRules = excludedRulesConf.filter { ruleName =>
      val nonExcludable = nonExcludableRules.contains(ruleName)
      if (nonExcludable) {
        logWarning(s"Optimization rule '${ruleName}' was not excluded from the optimizer " +
          s"because this rule is a non-excludable rule.")
      }
      !nonExcludable
    }
    if (excludedRules.isEmpty) {
      defaultBatches
    } else {
      defaultBatches.flatMap { batch =>
        val filteredRules = batch.rules.filter { rule =>
          val exclude = excludedRules.contains(rule.ruleName)
          if (exclude) {
            logInfo(s"Optimization rule '${rule.ruleName}' is excluded from the optimizer.")
          }
          !exclude
        }
        if (batch.rules == filteredRules) {
          Some(batch)
        } else if (filteredRules.nonEmpty) {
          Some(Batch(batch.name, batch.strategy, filteredRules: _*))
        } else {
          logInfo(s"Optimization batch '${batch.name}' is excluded from the optimizer " +
            s"as all enclosed rules have been excluded.")
          None
        }
      }
    }
  }
}

/**
  移除无用的重复值(去重)
  这个规则需要在@RewriteDistinctAggregates 之前使用
 */
object EliminateDistinct extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions  {
    case ae: AggregateExpression if ae.isDistinct =>
      ae.aggregateFunction match {
        case _: Max | _: Min => ae.copy(isDistinct = false)
        case _ => ae
      }
  }
}

/**
  测试代码中的优化器
  为了保证可扩展性,在抽象优化准则中使用标准规则,而指定的规则会进入到指定的子类
 */
object SimpleTestOptimizer extends SimpleTestOptimizer

class SimpleTestOptimizer extends Optimizer(
  new CatalogManager(
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true),
    FakeV2SessionCatalog,
    new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, new SQLConf())))

/**
  移除查询计划的多余别名信息.多余的别名信息就是不会改变列的名称和元数据的别名信息.且不会重复删除数据
 */
object RemoveRedundantAliases extends Rule[LogicalPlan] {

  /**
    创建属性映射表,属性映射关系为旧属性值 --> 新属性值.这个函数仅仅会返回改变的属性键值对
   */
  private def createAttributeMapping(current: LogicalPlan, next: LogicalPlan)
      : Seq[(Attribute, Attribute)] = {
    current.output.zip(next.output).filterNot {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }

  /**
    当别名信息重复的时候,移除表达式顶层别名信息
    @e 表达式信息
    @blacklist 黑名单属性集合
   */
  private def removeRedundantAlias(e: Expression, blacklist: AttributeSet): Expression = e match {
    // 带有元数据信息的别名新不能够被剥离开,否则元数据信息会丢失.如果别名与属性名称不同,就不会对其进行剥离
    // 获取会改变根逻辑执行计划的输出schema名称
    case a @ Alias(attr: Attribute, name)
      if a.metadata == Metadata.empty &&
        name == attr.name &&
        !blacklist.contains(attr) &&
        !blacklist.contains(a) =>
      attr
    case a => a
  }

  /**
    从逻辑计划以及其子树中,移除重复的别名表达式.指定的黑名单属性集合用于阻止重复属性的移除,这样以对一个join操作的输入数据进行
    重复数据删除
   */
  private def removeRedundantAliases(plan: LogicalPlan, blacklist: AttributeSet): LogicalPlan = {
    plan match {
      // 如果当前@plan 是一个子查询,name不能够移除别名信息,否则就不能产生相同的属性了,即黑名单信息包含了节点(查询计划)的所有信息
      case Subquery(child, correlated) =>
        Subquery(removeRedundantAliases(child, blacklist ++ child.outputSet), correlated)
        
      /*
      Join 操作需要区别对待,因为左/右join不允许使用相同的属性.这里使用黑名单列表,阻止这样情况的发送.这个规则仅仅在属性不属于黑马单
      的时候才会移除
       */
      case Join(left, right, joinType, condition, hint) =>
        // 1. 去除左表的多余别名信息
        val newLeft = removeRedundantAliases(left, blacklist ++ right.outputSet)
        // 2. 去除右表的多余别名信息
        val newRight = removeRedundantAliases(right, blacklist ++ newLeft.outputSet)
        val mapping = AttributeMap(
          createAttributeMapping(left, newLeft) ++
          createAttributeMapping(right, newRight))
        // 3. 更新新的表达式
        val newCondition = condition.map(_.transform {
          case a: Attribute => mapping.getOrElse(a, a)
        })
        Join(newLeft, newRight, joinType, newCondition, hint)
        
      // 其他情况处理
      case _ =>
        // 移除子树的多余别名信息
        val currentNextAttrPairs = mutable.Buffer.empty[(Attribute, Attribute)]
        val newNode = plan.mapChildren { child =>
          val newChild = removeRedundantAliases(child, blacklist)
          currentNextAttrPairs ++= createAttributeMapping(child, newChild)
          newChild
        }

        // Create the attribute mapping. Note that the currentNextAttrPairs can contain duplicate
        // keys in case of Union (this is caused by the PushProjectionThroughUnion rule); in this
        // case we use the the first mapping (which should be provided by the first child).
        val mapping = AttributeMap(currentNextAttrPairs)

        // Create a an expression cleaning function for nodes that can actually produce redundant
        // aliases, use identity otherwise.
        val clean: Expression => Expression = plan match {
          case _: Project => removeRedundantAlias(_, blacklist)
          case _: Aggregate => removeRedundantAlias(_, blacklist)
          case _: Window => removeRedundantAlias(_, blacklist)
          case _ => identity[Expression]
        }

        // Transform the expressions.
        newNode.mapExpressions { expr =>
          clean(expr.transform {
            case a: Attribute => mapping.getOrElse(a, a)
          })
        }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = removeRedundantAliases(plan, AttributeSet.empty)
}

/**
  移除空操作符,空操作符不会进行任何的修改
 */
object RemoveNoopOperators extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // 移除空的主属性(计划)
    case p @ Project(_, child) if child.sameOutput(p) => child

    // 移除空窗口
    case w: Window if w.windowExpressions.isEmpty => w.child
  }
}

/**
 * Pushes down [[LocalLimit]] beneath UNION ALL and beneath the streamed inputs of outer joins.
 */
object LimitPushDown extends Rule[LogicalPlan] {

  // 去除全局的限制值
  private def stripGlobalLimitIfPresent(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case GlobalLimit(_, child) => child
      case _ => plan
    }
  }
  
  /**
    置入本地限制值
    主要是基于限制表达式和逻辑计划中的每个分区的行数有关
    *@limitExp limit表达式
    *@plan 逻辑计划
    */
  private def maybePushLocalLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
    (limitExp, plan.maxRowsPerPartition) match {
      case (IntegerLiteral(newLimit), Some(childMaxRows)) if newLimit < childMaxRows =>
        // 如果子节点在每个分区的最大row上有一个cap(整形字面量的检索位置),
        // 且这个cap大于新的limit,name将本地limit置于cap处
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))

      case (_, None) =>
        // 如果子节点没有cap信息,直接置入LocalLimit
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))

      case _ =>
        plan
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    /*
      对于子节点来说,在UNION ALL操作下,添加额外的limit操作.这个操作没有Limit或者没有Limit的子孙信息(最大行较大)的时候.
      假定不存在任何的Limit 下推规则,启发式操作是无效的.这个规则不能推测最大行的值.
      注意到:  Union意味着Union All,不会重复删除行数据.因此使用这个进行下推操作是具有安全性的.一旦添加了UNION DISTINCT 操作,
      就不能够进行下推操作了.
    */
    case LocalLimit(exp, Union(children)) =>
      LocalLimit(exp, Union(children.map(maybePushLocalLimit(exp, _))))
      
    /*
    在OUTER JOIN的情景下添加额外的限制,对于left outer和right outer join,将限制分别值置入left/right端.
     在FULL OUTER JOIN下进行下推是不安全的.
     需要保证吓退的规则不会引入两端的界限值.
     因此:
     - 如果一侧已经限制了,如果新的limit值比较小,就会在顶层堆放其他limit信息.多余的limit信息可以通过@CombineLimits
     规则进行处理
     */
    case LocalLimit(exp, join @ Join(left, right, joinType, _, _)) =>
      val newJoin = joinType match {
        case RightOuter => join.copy(right = maybePushLocalLimit(exp, right))
        case LeftOuter => join.copy(left = maybePushLocalLimit(exp, left))
        case _ => join
      }
      LocalLimit(exp, newJoin)
  }
}

/**
  通过Union 将主属性(计划)进行下推
  
  将主属性(计划)操作置于union操作的两端
  操作下推是安全的,按照下述格式进行列举:
  Union:
  Union意味着Union All,这个不会重复删除行信息.因此,下推过滤属性和主属性(计划)是安全的.一旦使用了UNION DISTINCT,
  就不能够就那些计划的下推了
 */
object PushProjectionThroughUnion extends Rule[LogicalPlan] with PredicateHelper {

  /**
    获取属性映射表,处理union左右两端的映射关系
   */
  private def buildRewrites(left: LogicalPlan, right: LogicalPlan): AttributeMap[Attribute] = {
    assert(left.output.size == right.output.size)
    AttributeMap(left.output.zip(right.output))
  }

  /**
    重写表达式,这样可以被推到右端(Union/Except).这个方法依赖于输出属性(union/intersect/except)总是等于左边的输出.
   */
  private def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]) = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    } match {
      // Make sure exprId is unique in each child of Union.
      case Alias(child, alias) => Alias(child, alias)()
      case other => other
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // 通过UNION ALL 下推确定的主属性(计划)
    case p @ Project(projectList, Union(children)) =>
      assert(children.nonEmpty)
      if (projectList.forall(_.deterministic)) {
        val newFirstChild = Project(projectList, children.head)
        val newOtherChildren = children.tail.map { child =>
          val rewrites = buildRewrites(children.head, child)
          Project(projectList.map(pushToRight(_, rewrites)), child)
        }
        Union(newFirstChild +: newOtherChildren)
      } else {
        p
      }
  }
}

/**
 申请消除读取查询计划中不需要的列
  由于在过滤器@Filter和@PushPredicatesThroughProject冲突之前添加了主属性(计划),这个规则会移除计划p2.按照如下的格式:
   p1 @ Project(_, Filter(_, p2 @ Project(_, child))) if p2.outputSet.subsetOf(p2.inputSet)
   p2进场会被这个规则插入,且是无用的,p1可以按照任何方式修剪这个列
 */
object ColumnPruning extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = removeProjectBeforeFilter(plan transform {
    // Prunes the unused columns from project list of Project/Aggregate/Expand
    case p @ Project(_, p2: Project) if !p2.outputSet.subsetOf(p.references) =>
      p.copy(child = p2.copy(projectList = p2.projectList.filter(p.references.contains)))
    case p @ Project(_, a: Aggregate) if !a.outputSet.subsetOf(p.references) =>
      p.copy(
        child = a.copy(aggregateExpressions = a.aggregateExpressions.filter(p.references.contains)))
    case a @ Project(_, e @ Expand(_, _, grandChild)) if !e.outputSet.subsetOf(a.references) =>
      val newOutput = e.output.filter(a.references.contains(_))
      val newProjects = e.projections.map { proj =>
        proj.zip(e.output).filter { case (_, a) =>
          newOutput.contains(a)
        }.unzip._1
      }
      a.copy(child = Expand(newProjects, newOutput, grandChild))

    // Prunes the unused columns from child of `DeserializeToObject`
    case d @ DeserializeToObject(_, _, child) if !child.outputSet.subsetOf(d.references) =>
      d.copy(child = prunedChild(child, d.references))

    // Prunes the unused columns from child of Aggregate/Expand/Generate/ScriptTransformation
    case a @ Aggregate(_, _, child) if !child.outputSet.subsetOf(a.references) =>
      a.copy(child = prunedChild(child, a.references))
    case f @ FlatMapGroupsInPandas(_, _, _, child) if !child.outputSet.subsetOf(f.references) =>
      f.copy(child = prunedChild(child, f.references))
    case e @ Expand(_, _, child) if !child.outputSet.subsetOf(e.references) =>
      e.copy(child = prunedChild(child, e.references))
    case s @ ScriptTransformation(_, _, _, child, _)
        if !child.outputSet.subsetOf(s.references) =>
      s.copy(child = prunedChild(child, s.references))

    // prune unrequired references
    case p @ Project(_, g: Generate) if p.references != g.outputSet =>
      val requiredAttrs = p.references -- g.producedAttributes ++ g.generator.references
      val newChild = prunedChild(g.child, requiredAttrs)
      val unrequired = g.generator.references -- p.references
      val unrequiredIndices = newChild.output.zipWithIndex.filter(t => unrequired.contains(t._1))
        .map(_._2)
      p.copy(child = g.copy(child = newChild, unrequiredChildIndex = unrequiredIndices))

    // prune unrequired nested fields
    case p @ Project(projectList, g: Generate) if SQLConf.get.nestedPruningOnExpressions &&
        NestedColumnAliasing.canPruneGenerator(g.generator) =>
      NestedColumnAliasing.getAliasSubMap(projectList ++ g.generator.children).map {
        case (nestedFieldToAlias, attrToAliases) =>
          val newGenerator = g.generator.transform {
            case f: ExtractValue if nestedFieldToAlias.contains(f) =>
              nestedFieldToAlias(f).toAttribute
          }.asInstanceOf[Generator]

          // Defer updating `Generate.unrequiredChildIndex` to next round of `ColumnPruning`.
          val newGenerate = g.copy(generator = newGenerator)

          val newChild = NestedColumnAliasing.replaceChildrenWithAliases(newGenerate, attrToAliases)

          Project(NestedColumnAliasing.getNewProjectList(projectList, nestedFieldToAlias), newChild)
      }.getOrElse(p)

    // Eliminate unneeded attributes from right side of a Left Existence Join.
    case j @ Join(_, right, LeftExistence(_), _, _) =>
      j.copy(right = prunedChild(right, j.references))

    // all the columns will be used to compare, so we can't prune them
    case p @ Project(_, _: SetOperation) => p
    case p @ Project(_, _: Distinct) => p
    // Eliminate unneeded attributes from children of Union.
    case p @ Project(_, u: Union) =>
      if (!u.outputSet.subsetOf(p.references)) {
        val firstChild = u.children.head
        val newOutput = prunedChild(firstChild, p.references).output
        // pruning the columns of all children based on the pruned first child.
        val newChildren = u.children.map { p =>
          val selected = p.output.zipWithIndex.filter { case (a, i) =>
            newOutput.contains(firstChild.output(i))
          }.map(_._1)
          Project(selected, p)
        }
        p.copy(child = u.withNewChildren(newChildren))
      } else {
        p
      }

    // Prune unnecessary window expressions
    case p @ Project(_, w: Window) if !w.windowOutputSet.subsetOf(p.references) =>
      p.copy(child = w.copy(
        windowExpressions = w.windowExpressions.filter(p.references.contains)))

    // Can't prune the columns on LeafNode
    case p @ Project(_, _: LeafNode) => p

    case p @ NestedColumnAliasing(nestedFieldToAlias, attrToAliases) =>
      NestedColumnAliasing.replaceToAliases(p, nestedFieldToAlias, attrToAliases)

    // for all other logical plans that inherits the output from it's children
    // Project over project is handled by the first case, skip it here.
    case p @ Project(_, child) if !child.isInstanceOf[Project] =>
      val required = child.references ++ p.references
      if (!child.inputSet.subsetOf(required)) {
        val newChildren = child.children.map(c => prunedChild(c, required))
        p.copy(child = child.withNewChildren(newChildren))
      } else {
        p
      }
  })

  /** Applies a projection only when the child is producing unnecessary attributes */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if (!c.outputSet.subsetOf(allReferences)) {
      Project(c.output.filter(allReferences.contains), c)
    } else {
      c
    }

  /**
   * The Project before Filter is not necessary but conflict with PushPredicatesThroughProject,
   * so remove it. Since the Projects have been added top-down, we need to remove in bottom-up
   * order, otherwise lower Projects can be missed.
   */
  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p1 @ Project(_, f @ Filter(_, p2 @ Project(_, child)))
      if p2.outputSet.subsetOf(child.outputSet) &&
        // We only remove attribute-only project.
        p2.projectList.forall(_.isInstanceOf[AttributeReference]) =>
      p1.copy(child = f.copy(child = child))
  }
}

/**
   将两个主属性(计划)操作进行合并,合并成一个计划,且使用别名替换.将表达式合并到单个表达式中,用于下述情况:
   1. 当两个主属性操作相邻的时候
   2. 当两个主属性操作存在有LocalLimit/Sample/Repartition 操作,且上层属性包含同样数量的列,这些列相等/或者是别名相等.
    考虑到使用GlobalLimit(LocalLimit)
 */
object CollapseProject extends Rule[LogicalPlan] {
  
  // 进行主属性合并的主函数
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p1 @ Project(_, p2: Project) =>
      if (haveCommonNonDeterministicOutput(p1.projectList, p2.projectList)) {
        p1
      } else {
        p2.copy(projectList = buildCleanedProjectList(p1.projectList, p2.projectList))
      }
    case p @ Project(_, agg: Aggregate) =>
      if (haveCommonNonDeterministicOutput(p.projectList, agg.aggregateExpressions)) {
        p
      } else {
        agg.copy(aggregateExpressions = buildCleanedProjectList(
          p.projectList, agg.aggregateExpressions))
      }
    case Project(l1, g @ GlobalLimit(_, limit @ LocalLimit(_, p2 @ Project(l2, _))))
        if isRenaming(l1, l2) =>
      val newProjectList = buildCleanedProjectList(l1, l2)
      g.copy(child = limit.copy(child = p2.copy(projectList = newProjectList)))
    case Project(l1, limit @ LocalLimit(_, p2 @ Project(l2, _))) if isRenaming(l1, l2) =>
      val newProjectList = buildCleanedProjectList(l1, l2)
      limit.copy(child = p2.copy(projectList = newProjectList))
    case Project(l1, r @ Repartition(_, _, p @ Project(l2, _))) if isRenaming(l1, l2) =>
      r.copy(child = p.copy(projectList = buildCleanedProjectList(l1, p.projectList)))
    case Project(l1, s @ Sample(_, _, _, _, p2 @ Project(l2, _))) if isRenaming(l1, l2) =>
      s.copy(child = p2.copy(projectList = buildCleanedProjectList(l1, p2.projectList)))
  }
  
  // 收集别名属性列表@AttributeMap[Alias]
  private def collectAliases(projectList: Seq[NamedExpression]): AttributeMap[Alias] = {
    AttributeMap(projectList.collect {
      case a: Alias => a.toAttribute -> a
    })
  }

  // 确定是否存在共有的非确定的输出
  private def haveCommonNonDeterministicOutput(
      upper: Seq[NamedExpression], lower: Seq[NamedExpression]): Boolean = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)

    // Collapse upper and lower Projects if and only if their overlapped expressions are all
    // deterministic.
    upper.exists(_.collect {
      case a: Attribute if aliases.contains(a) => aliases(a).child
    }.exists(!_.deterministic))
  }
  
  /**
    * 构建主属性表
    * @param upper  上位计划
    * @param lower  下位计划
    * @return
    */
  private def buildCleanedProjectList(
      upper: Seq[NamedExpression],
      lower: Seq[NamedExpression]): Seq[NamedExpression] = {
    // 创建一个别名映射表，映射关系为别名名称 --> 下层主属性
    // 例如: 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)
    
    /*
    代替低位主属性(计划)的属性值,这样可以安全的移除掉它
    例如: 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
    使用向上转换的方式,防止无限迭代
     */
    val rewrittenUpper = upper.map(_.transformUp {
      case a: Attribute => aliases.getOrElse(a, a)
    })
    // 合并高低位的属性,这样可以切除不需要的别名信息
    rewrittenUpper.map { p =>
      CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
    }
  }

  // 确定两个表达式列表,是否存在有重命名的情况
  private def isRenaming(list1: Seq[NamedExpression], list2: Seq[NamedExpression]): Boolean = {
    list1.length == list2.length && list1.zip(list2).forall {
      case (e1, e2) if e1.semanticEquals(e2) => true
      case (Alias(a: Attribute, _), b) if a.metadata == Metadata.empty && a.name == b.name => true
      case _ => false
    }
  }
}

/**
  合并相邻的分区操作
 */
object CollapseRepartition extends Rule[LogicalPlan] {
  // 进行逻辑计划的合并
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    /*
      情景1: 当分区有子分区或者表达式分区的时候
      当顶级顶点不能够shuffle的时候,但是子节点开启了shuffle.如果最后一个分区数量大的话,范围子节点.否则保持不变.
     */
    case r @ Repartition(_, _, child: RepartitionOperation) => (r.shuffle, child.shuffle) match {
      case (false, true) => if (r.numPartitions >= child.numPartitions) child else r
      case _ => r.copy(child = child.child)
    }
    
    /*
      情景2: 当分区表达式@RepartitionByExpression 含有分区/分区表达式的子节点的时候,可以移除这个子节点
     */
    case r @ RepartitionByExpression(_, child: RepartitionOperation, _) =>
      r.copy(child = child.child)
  }
}

/**
  合并相邻的窗口表达式
  - 如果分区指定和其他指定是相同的,且窗口表达式是幂等的,且是相同的window函数类型,将其合并到父类型中
 */
object CollapseWindow extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case w1 @ Window(we1, ps1, os1, w2 @ Window(we2, ps2, os2, grandChild))
        if ps1 == ps2 && os1 == os2 && w1.references.intersect(w2.windowOutputSet).isEmpty &&
          we1.nonEmpty && we2.nonEmpty &&
          // This assumes Window contains the same type of window expressions. This is ensured
          // by ExtractWindowFunctions.
          WindowFunctionType.functionType(we1.head) == WindowFunctionType.functionType(we2.head) =>
      w1.copy(windowExpressions = we2 ++ we1, child = grandChild)
  }
}

/**
  装置相邻的窗口表达式
  - 如果父窗口表达式的分区指定兼容子窗口表达式的分区指定,则将其进行装置
 * - If the partition spec of the parent Window expression is compatible with the partition spec
 *   of the child window expression, transpose them.
 */
object TransposeWindow extends Rule[LogicalPlan] {
  private def compatibleParititions(ps1 : Seq[Expression], ps2: Seq[Expression]): Boolean = {
    ps1.length < ps2.length && ps2.take(ps1.length).permutations.exists(ps1.zip(_).forall {
      case (l, r) => l.semanticEquals(r)
    })
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case w1 @ Window(we1, ps1, os1, w2 @ Window(we2, ps2, os2, grandChild))
        if w1.references.intersect(w2.windowOutputSet).isEmpty &&
           w1.expressions.forall(_.deterministic) &&
           w2.expressions.forall(_.deterministic) &&
           compatibleParititions(ps1, ps2) =>
      Project(w1.output, Window(we2, ps2, os2, Window(we1, ps1, os1, grandChild)))
  }
}

/**
  由限制条件推测过滤器
  
  由操作符存在的约束条件生成一个额外的过滤器,但是会移除那些已经是操作符条件的或者是操作符子条件表达式的部分.
  这个过滤器会在过滤器操作器中插入到存在表达式中/或者是join操作的任意一侧.
  注意到: 这个优化适用于许多类型的join,主要是有利于inner join和left semi Join
 */
object InferFiltersFromConstraints extends Rule[LogicalPlan]
  with PredicateHelper with ConstraintHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.constraintPropagationEnabled) {
      inferFilters(plan)
    } else {
      plan
    }
  }
  
  /**
    推测过滤条件
    * @param plan 逻辑执行计划
    * @return
    */
  private def inferFilters(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, child) =>
      val newFilters = filter.constraints --
        (child.constraints ++ splitConjunctivePredicates(condition))
      if (newFilters.nonEmpty) {
        Filter(And(newFilters.reduce(And), condition), child)
      } else {
        filter
      }

    case join @ Join(left, right, joinType, conditionOpt, _) =>
      joinType match {
        // For inner join, we can infer additional filters for both sides. LeftSemi is kind of an
        // inner join, it just drops the right side in the final output.
        case _: InnerLike | LeftSemi =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newLeft = inferNewFilter(left, allConstraints)
          val newRight = inferNewFilter(right, allConstraints)
          join.copy(left = newLeft, right = newRight)

        // For right outer join, we can only infer additional filters for left side.
        case RightOuter =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newLeft = inferNewFilter(left, allConstraints)
          join.copy(left = newLeft)

        // For left join, we can only infer additional filters for right side.
        case LeftOuter | LeftAnti =>
          val allConstraints = getAllConstraints(left, right, conditionOpt)
          val newRight = inferNewFilter(right, allConstraints)
          join.copy(right = newRight)

        case _ => join
      }
  }

  // 获取所有限制条件
  private def getAllConstraints(
      left: LogicalPlan,
      right: LogicalPlan,
      conditionOpt: Option[Expression]): Set[Expression] = {
    val baseConstraints = left.constraints.union(right.constraints)
      .union(conditionOpt.map(splitConjunctivePredicates).getOrElse(Nil).toSet)
    baseConstraints.union(inferAdditionalConstraints(baseConstraints))
  }

  // 根据限制条件@constraint 对指定的逻辑计划@plan 进行过滤条件推测.
  private def inferNewFilter(plan: LogicalPlan, constraints: Set[Expression]): LogicalPlan = {
    val newPredicates = constraints
      .union(constructIsNotNullConstraints(constraints, plan.output))
      .filter { c =>
        c.references.nonEmpty && c.references.subsetOf(plan.outputSet) && c.deterministic
      } -- plan.constraints
    if (newPredicates.isEmpty) {
      plan
    } else {
      Filter(newPredicates.reduce(And), plan)
    }
  }
}

/**
  合并所有相邻的Union操作符,形成单个Union操作符
 */
object CombineUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case u: Union => flattenUnion(u, false)
    case Distinct(u: Union) => Distinct(flattenUnion(u, true))
  }

  private def flattenUnion(union: Union, flattenDistinct: Boolean): Union = {
    val stack = mutable.Stack[LogicalPlan](union)
    val flattened = mutable.ArrayBuffer.empty[LogicalPlan]
    while (stack.nonEmpty) {
      stack.pop() match {
        case Distinct(Union(children)) if flattenDistinct =>
          stack.pushAll(children.reverse)
        case Union(children) =>
          stack.pushAll(children.reverse)
        case child =>
          flattened += child
      }
    }
    Union(flattened)
  }
}

/**
   合并两个相邻的过滤器@Filter操作符,使其成为一个,将非冗余的条件合并成一个连词谓词。
 */
object CombineFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally

  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    // The query execution/optimization does not guarantee the expressions are evaluated in order.
    // We only can combine them if and only if both are deterministic.
    case Filter(fc, nf @ Filter(nc, grandChild)) if fc.deterministic && nc.deterministic =>
      (ExpressionSet(splitConjunctivePredicates(fc)) --
        ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
        case Some(ac) =>
          Filter(And(nc, ac), grandChild)
        case None =>
          nf
      }
  }
}

/**
  移除排序操作,可能发生如下情况:
  1. 如果排序顺序是空的,或者排序顺序没有任何引用的情况下
  2. 如果子节点已经排序完成
  3. 如果有另一个排序操作符(由0->n个主属性(计划)/过滤器操作进行分割)
  4. 如果排序操作符在Join操作中,由0->n个主属性(计划)/过滤器进行分割,且join操作是确定的
  5. 如果排序表达式位于groupBy操作中,且由0->n个主属性/过滤器进行分割,集合的函数是顺序不相关的
 */
object EliminateSorts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case s @ Sort(orders, _, child) if orders.isEmpty || orders.exists(_.child.foldable) =>
      val newOrders = orders.filterNot(_.child.foldable)
      if (newOrders.isEmpty) child else s.copy(order = newOrders)
    case Sort(orders, true, child) if SortOrder.orderingSatisfies(child.outputOrdering, orders) =>
      child
    case s @ Sort(_, _, child) => s.copy(child = recursiveRemoveSort(child))
    case j @ Join(originLeft, originRight, _, cond, _) if cond.forall(_.deterministic) =>
      j.copy(left = recursiveRemoveSort(originLeft), right = recursiveRemoveSort(originRight))
    case g @ Aggregate(_, aggs, originChild) if isOrderIrrelevantAggs(aggs) =>
      g.copy(child = recursiveRemoveSort(originChild))
  }

  private def recursiveRemoveSort(plan: LogicalPlan): LogicalPlan = plan match {
    case Sort(_, _, child) => recursiveRemoveSort(child)
    case other if canEliminateSort(other) =>
      other.withNewChildren(other.children.map(recursiveRemoveSort))
    case _ => plan
  }

  private def canEliminateSort(plan: LogicalPlan): Boolean = plan match {
    case p: Project => p.projectList.forall(_.deterministic)
    case f: Filter => f.condition.deterministic
    case _ => false
  }

  private def isOrderIrrelevantAggs(aggs: Seq[NamedExpression]): Boolean = {
    def isOrderIrrelevantAggFunction(func: AggregateFunction): Boolean = func match {
      case _: Min | _: Max | _: Count => true
      // Arithmetic operations for floating-point values are order-sensitive
      // (they are not associative).
      case _: Sum | _: Average | _: CentralMomentAgg =>
        !Seq(FloatType, DoubleType).exists(_.sameType(func.children.head.dataType))
      case _ => false
    }

    def checkValidAggregateExpression(expr: Expression): Boolean = expr match {
      case _: AttributeReference => true
      case ae: AggregateExpression => isOrderIrrelevantAggFunction(ae.aggregateFunction)
      case _: UserDefinedExpression => false
      case e => e.children.forall(checkValidAggregateExpression)
    }

    aggs.forall(checkValidAggregateExpression)
  }
}

/**
  过滤条件的修剪,通过下述的操作进行估量
  1. 如果过滤器估测的结果为true,则将过滤条件淘汰
  2. 在过滤器评测值为false的时候,替换为一个伪空关系
  3. 消除子节点输出中总是为true的条件
 */
object PruneFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) =>
      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
    case Filter(Literal(false, BooleanType), child) =>
      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
    // If any deterministic condition is guaranteed to be true given the constraints on the child's
    // output, remove the condition
    case f @ Filter(fc, p: LogicalPlan) =>
      val (prunedPredicates, remainingPredicates) =
        splitConjunctivePredicates(fc).partition { cond =>
          cond.deterministic && p.constraints.contains(cond)
        }
      if (prunedPredicates.isEmpty) {
        f
      } else if (remainingPredicates.isEmpty) {
        p
      } else {
        val newCond = remainingPredicates.reduce(And)
        Filter(newCond, p)
      }
  }
}

/**
  谓词下推的同意版本(用于正常的操作和join).这个规则提升了谓词下推的性能,用于级联join.
  例如: Filter-Join-Join-Join
  大多数谓词在单个方向进行下推
 */
object PushDownPredicates extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    CombineFilters.applyLocally
      .orElse(PushPredicateThroughNonJoin.applyLocally)
      .orElse(PushPredicateThroughJoin.applyLocally)
  }
}

/**
  通过if操作,置入过滤操作
  1. 操作需要是确定的
  2. 谓词是确定的,且操作符不会改变行的内容
  假定表达式开销是最小的,则启发式功能关闭
 */
object PushPredicateThroughNonJoin extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally
  // 本地调用函数
  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    // SPARK-13473: We can't push the predicate down when the underlying projection output non-
    // deterministic field(s).  Non-deterministic expressions are essentially stateful. This
    // implies that, for a given input row, the output are determined by the expression's initial
    // state and all the input rows processed before. In another word, the order of input rows
    // matters for non-deterministic expressions, while pushing down predicates changes the order.
    // This also applies to Aggregate.
    case Filter(condition, project @ Project(fields, grandChild))
      if fields.forall(_.deterministic) && canPushThroughCondition(grandChild, condition) =>
      val aliasMap = getAliasMap(project)
      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))

    case filter @ Filter(condition, aggregate: Aggregate)
      if aggregate.aggregateExpressions.forall(_.deterministic)
        && aggregate.groupingExpressions.nonEmpty =>
      val aliasMap = getAliasMap(aggregate)

      // For each filter, expand the alias and check if the filter can be evaluated using
      // attributes produced by the aggregate operator's child operator.
      val (candidates, nonDeterministic) =
        splitConjunctivePredicates(condition).partition(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        val replaced = replaceAlias(cond, aliasMap)
        cond.references.nonEmpty && replaced.references.subsetOf(aggregate.child.outputSet)
      }

      val stayUp = rest ++ nonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val replaced = replaceAlias(pushDownPredicate, aliasMap)
        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
        // If there is no more filter to stay up, just eliminate the filter.
        // Otherwise, create "Filter(stayUp) <- Aggregate <- Filter(pushDownPredicate)".
        if (stayUp.isEmpty) newAggregate else Filter(stayUp.reduce(And), newAggregate)
      } else {
        filter
      }

    // Push [[Filter]] operators through [[Window]] operators. Parts of the predicate that can be
    // pushed beneath must satisfy the following conditions:
    // 1. All the expressions are part of window partitioning key. The expressions can be compound.
    // 2. Deterministic.
    // 3. Placed before any non-deterministic predicates.
    case filter @ Filter(condition, w: Window)
      if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))

      val (candidates, nonDeterministic) =
        splitConjunctivePredicates(condition).partition(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        cond.references.subsetOf(partitionAttrs)
      }

      val stayUp = rest ++ nonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val newWindow = w.copy(child = Filter(pushDownPredicate, w.child))
        if (stayUp.isEmpty) newWindow else Filter(stayUp.reduce(And), newWindow)
      } else {
        filter
      }

    case filter @ Filter(condition, union: Union) =>
      // Union could change the rows, so non-deterministic predicate can't be pushed down
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition(_.deterministic)

      if (pushDown.nonEmpty) {
        val pushDownCond = pushDown.reduceLeft(And)
        val output = union.output
        val newGrandChildren = union.children.map { grandchild =>
          val newCond = pushDownCond transform {
            case e if output.exists(_.semanticEquals(e)) =>
              grandchild.output(output.indexWhere(_.semanticEquals(e)))
          }
          assert(newCond.references.subsetOf(grandchild.outputSet))
          Filter(newCond, grandchild)
        }
        val newUnion = union.withNewChildren(newGrandChildren)
        if (stayUp.nonEmpty) {
          Filter(stayUp.reduceLeft(And), newUnion)
        } else {
          newUnion
        }
      } else {
        filter
      }

    case filter @ Filter(condition, watermark: EventTimeWatermark) =>
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition { p =>
        p.deterministic && !p.references.contains(watermark.eventTime)
      }

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduceLeft(And)
        val newWatermark = watermark.copy(child = Filter(pushDownPredicate, watermark.child))
        // If there is no more filter to stay up, just eliminate the filter.
        // Otherwise, create "Filter(stayUp) <- watermark <- Filter(pushDownPredicate)".
        if (stayUp.isEmpty) newWatermark else Filter(stayUp.reduceLeft(And), newWatermark)
      } else {
        filter
      }

    case filter @ Filter(_, u: UnaryNode)
        if canPushThrough(u) && u.expressions.forall(_.deterministic) =>
      pushDownPredicate(filter, u.child) { predicate =>
        u.withNewChildren(Seq(Filter(predicate, u.child)))
      }
  }

  // 获取别名列表信息
  def getAliasMap(plan: Project): AttributeMap[Expression] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).
    AttributeMap(plan.projectList.collect { case a: Alias => (a.toAttribute, a.child) })
  }

  // 获取别名列表信息
  def getAliasMap(plan: Aggregate): AttributeMap[Expression] = {
    // Find all the aliased expressions in the aggregate list that don't include any actual
    // AggregateExpression, and create a map from the alias to the expression
    val aliasMap = plan.aggregateExpressions.collect {
      case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
        (a.toAttribute, a.child)
    }
    AttributeMap(aliasMap)
  }
  
  // 确定是否可以置入
  def canPushThrough(p: UnaryNode): Boolean = p match {
    // Note that some operators (e.g. project, aggregate, union) are being handled separately
    // (earlier in this rule).
    case _: AppendColumns => true
    case _: Distinct => true
    case _: Generate => true
    case _: Pivot => true
    case _: RepartitionByExpression => true
    case _: Repartition => true
    case _: ScriptTransformation => true
    case _: Sort => true
    case _: BatchEvalPython => true
    case _: ArrowEvalPython => true
    case _ => false
  }

  /*
   下推谓词
   @filter: Filter 过滤条件
   @grandchild 子节点(逻辑计划)
   @insertFilter 过滤器插入函数
    */
  private def pushDownPredicate(
      filter: Filter,
      grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
    // Only push down the predicates that is deterministic and all the referenced attributes
    // come from grandchild.
    // TODO: non-deterministic predicates could be pushed through some operators that do not change
    // the rows.
    val (candidates, nonDeterministic) =
      splitConjunctivePredicates(filter.condition).partition(_.deterministic)

    val (pushDown, rest) = candidates.partition { cond =>
      cond.references.subsetOf(grandchild.outputSet)
    }

    val stayUp = rest ++ nonDeterministic

    if (pushDown.nonEmpty) {
      val newChild = insertFilter(pushDown.reduceLeft(And))
      if (stayUp.nonEmpty) {
        Filter(stayUp.reduceLeft(And), newChild)
      } else {
        newChild
      }
    } else {
      filter
    }
  }

  /**
    检查是否可以安全的置入过滤表达式(通过主属性(计划)),通过报纸谓词子查询不会包含相同的属性(因为这些属性会被移动到计划中).
    当集合和谓词子查询有相当的资源的时候会发生
   */
  private def canPushThroughCondition(plan: LogicalPlan, condition: Expression): Boolean = {
    val attributes = plan.outputSet
    val matched = condition.find {
      case s: SubqueryExpression => s.plan.outputSet.intersect(attributes).nonEmpty
      case _ => false
    }
    matched.isEmpty
  }
}

/**
  下推过滤表达式操作,可以通过使用left/right join估测条件值.过滤表达式会移动到@condition中
  使用left/right子查询的属性对condition进行估测,并将其置入到join过滤表达式中
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Splits join condition expressions or filter predicates (on a given join's output) into three
   * categories based on the attributes required to evaluate them. Note that we explicitly exclude
   * non-deterministic (i.e., stateful) condition expressions in canEvaluateInLeft or
   * canEvaluateInRight to prevent pushing these predicates on either side of the join.
   *
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (pushDownCandidates, nonDeterministic) = condition.partition(_.deterministic)
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(expr => expr.references.subsetOf(right.outputSet))

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ nonDeterministic)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform applyLocally

  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition, hint)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)
      joinType match {
        case _: InnerLike =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val (newJoinConditions, others) =
            commonFilterCondition.partition(canEvaluateWithinJoin)
          val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)

          val join = Join(newLeft, newRight, joinType, newJoinCond, hint)
          if (others.nonEmpty) {
            Filter(others.reduceLeft(And), join)
          } else {
            join
          }
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond, hint)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case LeftOuter | LeftExistence(_) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond, hint)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }

    // push down the join filter into sub query scanning if applicable
    case j @ Join(left, right, joinType, joinCondition, hint) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case _: InnerLike | LeftSemi =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, RightOuter, newJoinCond, hint)
        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond, hint)
        case FullOuter => j
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }
  }
}

/**
  合并两个相邻的Limit操作,将其形成为一个,将表达式合并成一个表达式
 */
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case GlobalLimit(le, GlobalLimit(ne, grandChild)) =>
      GlobalLimit(Least(Seq(ne, le)), grandChild)
    case LocalLimit(le, LocalLimit(ne, grandChild)) =>
      LocalLimit(Least(Seq(ne, le)), grandChild)
    case Limit(le, Limit(ne, grandChild)) =>
      Limit(Least(Seq(ne, le)), grandChild)
  }
}

/**
  检查是否有笛卡尔积.如果没有指定交叉join,并且发现了笛卡尔积的情况下会抛出错误.这个可以通过设
  置@CROSS_JOINS_ENABLED=true进行配置.
  这个规则必须在@ReorderJoin 之后,因为每个join操作的条件必须在检查笛卡尔积之前进行收集.如果你使用了
  SELECT * from R, S where R.r = S.s表达式，R和S的join不是一个笛卡尔积.
  其中谓词@R.r = S.s 不是一个join条件(除非是在ReorderJoin 规则下).
  这个规则必须在批次@LocalRelation 之后运行,因为空关系的join不能是笛卡尔积
 */
object CheckCartesianProducts extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Check if a join is a cartesian product. Returns true if
   * there are no join conditions involving references from both left and right.
   */
  def isCartesianProduct(join: Join): Boolean = {
    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)

    conditions match {
      case Seq(Literal.FalseLiteral) | Seq(Literal(null, BooleanType)) => false
      case _ => !conditions.map(_.references).exists(refs =>
        refs.exists(join.left.outputSet.contains) && refs.exists(join.right.outputSet.contains))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.crossJoinEnabled) {
      plan
    } else plan transform {
      case j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, _, _)
        if isCartesianProduct(j) =>
          throw new AnalysisException(
            s"""Detected implicit cartesian product for ${j.joinType.sql} join between logical plans
               |${left.treeString(false).trim}
               |and
               |${right.treeString(false).trim}
               |Join condition is missing or trivial.
               |Either: use the CROSS JOIN syntax to allow cartesian products between these
               |relations, or: enable implicit cartesian products by setting the configuration
               |variable spark.sql.crossJoin.enabled=true"""
            .stripMargin)
    }
}

/**
  对于定长十进制数进行加速聚合,通过在非扩展性的Long值上执行
  对于精度的增长,使用相同的规则,用于扩展输出@DecimalPrecision
 */
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  private val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case we @ WindowExpression(ae @ AggregateExpression(af, _, _, _), _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(we.copy(windowFunction = ae.copy(aggregateFunction = Sum(UnscaledValue(e)))),
            prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr =
            we.copy(windowFunction = ae.copy(aggregateFunction = Average(UnscaledValue(e))))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4), Option(SQLConf.get.sessionLocalTimeZone))

        case _ => we
      }
      case ae @ AggregateExpression(af, _, _, _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(ae.copy(aggregateFunction = Sum(UnscaledValue(e))), prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr = ae.copy(aggregateFunction = Average(UnscaledValue(e)))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4), Option(SQLConf.get.sessionLocalTimeZone))

        case _ => ae
      }
    }
  }
}

/**
  本地关系转换(不需要数据转换)
 * Converts local operations (i.e. ones that don't require data exchange) on `LocalRelation` to
 * another `LocalRelation`.
 */
object ConvertToLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Project(projectList, LocalRelation(output, data, isStreaming))
        if !projectList.exists(hasUnevaluableExpr) =>
      val projection = new InterpretedMutableProjection(projectList, output)
      projection.initialize(0)
      LocalRelation(projectList.map(_.toAttribute), data.map(projection(_).copy()), isStreaming)

    case Limit(IntegerLiteral(limit), LocalRelation(output, data, isStreaming)) =>
      LocalRelation(output, data.take(limit), isStreaming)

    case Filter(condition, LocalRelation(output, data, isStreaming))
        if !hasUnevaluableExpr(condition) =>
      val predicate = Predicate.create(condition, output)
      predicate.initialize(0)
      LocalRelation(output, data.filter(row => predicate.eval(row)), isStreaming)
  }

  private def hasUnevaluableExpr(expr: Expression): Boolean = {
    expr.find(e => e.isInstanceOf[Unevaluable] && !e.isInstanceOf[AttributeReference]).isDefined
  }
}

/**
  使用@Aggregate 代替逻辑上的去重@Distinct操作,例如
  SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2
 */
object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Distinct(child) => Aggregate(child.output, child.output, child)
  }
}

/**
  使用聚合操作代替逻辑上的重复数据删除的操作
 */
object ReplaceDeduplicateWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Deduplicate(keys, child) if !child.isStreaming =>
      val keyExprIds = keys.map(_.exprId)
      val aggCols = child.output.map { attr =>
        if (keyExprIds.contains(attr.exprId)) {
          attr
        } else {
          Alias(new First(attr).toAggregateExpression(), attr.name)(attr.exprId)
        }
      }
      // SPARK-22951: Physical aggregate operators distinguishes global aggregation and grouping
      // aggregations by checking the number of grouping keys. The key difference here is that a
      // global aggregation always returns at least one row even if there are no input rows. Here
      // we append a literal when the grouping key list is empty so that the result aggregate
      // operator is properly treated as a grouping aggregation.
      val nonemptyKeys = if (keys.isEmpty) Literal(1) :: Nil else keys
      Aggregate(nonemptyKeys, aggCols, child)
  }
}

/**
  使用left-semi join代替逻辑上的交集操作
 * {{{
 *   SELECT a1, a2 FROM Tab1 INTERSECT SELECT b1, b2 FROM Tab2
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT SEMI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
 * }}}
 *
  注意到:
  1. 这个规则仅仅对于INTERSECT DISTINCT 可用。对于INTERSECT ALL不可以使用。
  2. 这个规则在数据重复删除之后进行，否则，生成的join条件不正确
 */
object ReplaceIntersectWithSemiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Intersect(left, right, false) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftSemi, joinCond.reduceLeftOption(And), JoinHint.NONE))
  }
}

/**
  使用left-anti Join代替逻辑上的@Except 操作
  SELECT a1, a2 FROM Tab1 EXCEPT SELECT b1, b2 FROM Tab2
  ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT ANTI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
  
  注意：
  1. 这个规则仅仅适用于@EXCEPT DISTINCT 不能使用在@EXCEPT ALL上
  2. 这个规则必须在重复属性删除之后才能进行，否则产生的join会不正确
 */
object ReplaceExceptWithAntiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right, false) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And), JoinHint.NONE))
  }
}

/**
   代替逻辑上的@Except 操作, 使用Union,Aggregate,Generate的混合操作
   输入查询:
 * {{{
 *    SELECT c1 FROM ut1 EXCEPT ALL SELECT c1 FROM ut2
 * }}}
 *
 * 实际写入查询:
 * {{{
 *   SELECT c1
 *   FROM (
 *     SELECT replicate_rows(sum_val, c1)
 *       FROM (
 *         SELECT c1, sum_val
 *           FROM (
 *             SELECT c1, sum(vcol) AS sum_val
 *               FROM (
 *                 SELECT 1L as vcol, c1 FROM ut1
 *                 UNION ALL
 *                 SELECT -1L as vcol, c1 FROM ut2
 *              ) AS union_all
 *            GROUP BY union_all.c1
 *          )
 *        WHERE sum_val > 0
 *       )
 *   )
 * }}}
 */

object RewriteExceptAll extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right, true) =>
      assert(left.output.size == right.output.size)

      val newColumnLeft = Alias(Literal(1L), "vcol")()
      val newColumnRight = Alias(Literal(-1L), "vcol")()
      val modifiedLeftPlan = Project(Seq(newColumnLeft) ++ left.output, left)
      val modifiedRightPlan = Project(Seq(newColumnRight) ++ right.output, right)
      val unionPlan = Union(modifiedLeftPlan, modifiedRightPlan)
      val aggSumCol =
        Alias(AggregateExpression(Sum(unionPlan.output.head.toAttribute), Complete, false), "sum")()
      val aggOutputColumns = left.output ++ Seq(aggSumCol)
      val aggregatePlan = Aggregate(left.output, aggOutputColumns, unionPlan)
      val filteredAggPlan = Filter(GreaterThan(aggSumCol.toAttribute, Literal(0L)), aggregatePlan)
      val genRowPlan = Generate(
        ReplicateRows(Seq(aggSumCol.toAttribute) ++ left.output),
        unrequiredChildIndex = Nil,
        outer = false,
        qualifier = None,
        left.output,
        filteredAggPlan
      )
      Project(left.output, genRowPlan)
  }
}

/**
  代替逻辑上的@Intersect 操作,使用Union,Aggregate以及Generate的混合操作
 * 输入查询 :
 * {{{
 *    SELECT c1 FROM ut1 INTERSECT ALL SELECT c1 FROM ut2
 * }}}
 *
 * 重新查询:
 * {{{
 *   SELECT c1
 *   FROM (
 *        SELECT replicate_row(min_count, c1)
 *        FROM (
 *             SELECT c1, If (vcol1_cnt > vcol2_cnt, vcol2_cnt, vcol1_cnt) AS min_count
 *             FROM (
 *                  SELECT   c1, count(vcol1) as vcol1_cnt, count(vcol2) as vcol2_cnt
 *                  FROM (
 *                       SELECT true as vcol1, null as , c1 FROM ut1
 *                       UNION ALL
 *                       SELECT null as vcol1, true as vcol2, c1 FROM ut2
 *                       ) AS union_all
 *                  GROUP BY c1
 *                  HAVING vcol1_cnt >= 1 AND vcol2_cnt >= 1
 *                  )
 *             )
 *         )
 * }}}
 */
object RewriteIntersectAll extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Intersect(left, right, true) =>
      assert(left.output.size == right.output.size)

      val trueVcol1 = Alias(Literal(true), "vcol1")()
      val nullVcol1 = Alias(Literal(null, BooleanType), "vcol1")()

      val trueVcol2 = Alias(Literal(true), "vcol2")()
      val nullVcol2 = Alias(Literal(null, BooleanType), "vcol2")()

      // Add a projection on the top of left and right plans to project out
      // the additional virtual columns.
      val leftPlanWithAddedVirtualCols = Project(Seq(trueVcol1, nullVcol2) ++ left.output, left)
      val rightPlanWithAddedVirtualCols = Project(Seq(nullVcol1, trueVcol2) ++ right.output, right)

      val unionPlan = Union(leftPlanWithAddedVirtualCols, rightPlanWithAddedVirtualCols)

      // Expressions to compute count and minimum of both the counts.
      val vCol1AggrExpr =
        Alias(AggregateExpression(Count(unionPlan.output(0)), Complete, false), "vcol1_count")()
      val vCol2AggrExpr =
        Alias(AggregateExpression(Count(unionPlan.output(1)), Complete, false), "vcol2_count")()
      val ifExpression = Alias(If(
        GreaterThan(vCol1AggrExpr.toAttribute, vCol2AggrExpr.toAttribute),
        vCol2AggrExpr.toAttribute,
        vCol1AggrExpr.toAttribute
      ), "min_count")()

      val aggregatePlan = Aggregate(left.output,
        Seq(vCol1AggrExpr, vCol2AggrExpr) ++ left.output, unionPlan)
      val filterPlan = Filter(And(GreaterThanOrEqual(vCol1AggrExpr.toAttribute, Literal(1L)),
        GreaterThanOrEqual(vCol2AggrExpr.toAttribute, Literal(1L))), aggregatePlan)
      val projectMinPlan = Project(left.output ++ Seq(ifExpression), filterPlan)

      // Apply the replicator to replicate rows based on min_count
      val genRowPlan = Generate(
        ReplicateRows(Seq(ifExpression.toAttribute) ++ left.output),
        unrequiredChildIndex = Nil,
        outer = false,
        qualifier = None,
        left.output,
        projectMinPlan
      )
      Project(left.output, genRowPlan)
  }
}

/**
  移除分组表达式中的字面量,因为对于结果不会产生影响,且仅仅会使得分组的key变得更大
 */
object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) if grouping.nonEmpty =>
      val newGrouping = grouping.filter(!_.foldable)
      if (newGrouping.nonEmpty) {
        a.copy(groupingExpressions = newGrouping)
      } else {
        // All grouping expressions are literals. We should not drop them all, because this can
        // change the return semantics when the input of the Aggregate is empty (SPARK-17114). We
        // instead replace this by single, easy to hash/sort, literal expression.
        a.copy(groupingExpressions = Seq(Literal(0, IntegerType)))
      }
  }
}

/**
  移除分组表达式中的重复,因为不会影响结果,且仅仅会使得分组key变得更大
 */
object RemoveRepetitionFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) if grouping.size > 1 =>
      val newGrouping = ExpressionSet(grouping).toSeq
      if (newGrouping.size == grouping.size) {
        a
      } else {
        a.copy(groupingExpressions = newGrouping)
      }
  }
}

/**
  使用空的本地关系@LocalRelation 代替全局限制或者是本地限制中的GlobalLimit 0/LocalLimit 0，因为其不会返回数据
 */
object OptimizeLimitZero extends Rule[LogicalPlan] {
  // returns empty Local Relation corresponding to given plan
  private def empty(plan: LogicalPlan) =
    LocalRelation(plan.output, data = Seq.empty, isStreaming = plan.isStreaming)

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Nodes below GlobalLimit or LocalLimit can be pruned if the limit value is zero (0).
    // Any subtree in the logical plan that has GlobalLimit 0 or LocalLimit 0 as its root is
    // semantically equivalent to an empty relation.
    //
    // In such cases, the effects of Limit 0 can be propagated through the Logical Plan by replacing
    // the (Global/Local) Limit subtree with an empty LocalRelation, thereby pruning the subtree
    // below and triggering other optimization rules of PropagateEmptyRelation to propagate the
    // changes up the Logical Plan.
    //
    // Replace Global Limit 0 nodes with empty Local Relation
    case gl @ GlobalLimit(IntegerLiteral(0), _) =>
      empty(gl)

    // Note: For all SQL queries, if a LocalLimit 0 node exists in the Logical Plan, then a
    // GlobalLimit 0 node would also exist. Thus, the above case would be sufficient to handle
    // almost all cases. However, if a user explicitly creates a Logical Plan with LocalLimit 0 node
    // then the following rule will handle that case as well.
    //
    // Replace Local Limit 0 nodes with empty Local Relation
    case ll @ LocalLimit(IntegerLiteral(0), _) =>
      empty(ll)
  }
}
