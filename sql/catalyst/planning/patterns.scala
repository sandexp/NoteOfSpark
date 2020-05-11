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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._

/**
  需要补充的知识
  关于编译原理中谓词的解释和概念
  */
// 操作辅助器
trait OperationHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  // 收集指定的表达式列表@fields
  protected def collectAliases(fields: Seq[Expression]): AttributeMap[Expression] =
    AttributeMap(fields.collect {
      case a: Alias => (a.toAttribute, a.child)
    })

  protected def substitute(aliases: AttributeMap[Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref)
          .map(Alias(_, name)(a.exprId, a.qualifier))
          .getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a)
          .map(Alias(_, a.name)(a.exprId, a.qualifier)).getOrElse(a)
    }
  }
}

/**
 物理操作
  可以匹配任意数量的项目或者是过滤器操作.所有过滤操作都会被收集,且会被顶层操作符完成
 */
object PhysicalOperation extends OperationHelper with PredicateHelper {

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }

  /**
    收集所有确定性的过滤器，代码嵌入和替换是由必要的。下面是两个实例程序
   * Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
    @plan 逻辑计划
      这个逻辑计划包括有命名表达式,表达式列表,逻辑执行计划,表达式属性表组成
   */
  private def collectProjectsAndFilters(plan: LogicalPlan):
      (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, AttributeMap[Expression]) =
    plan match {
      // 处理所有主体属性
      case Project(fields, child) if fields.forall(_.deterministic) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))
      // 处理过滤条件
      case Filter(condition, child) if condition.deterministic =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)
      // 处理线索节点信息
      case h: ResolvedHint =>
        collectProjectsAndFilters(h.child)

      case other =>
        (None, Nil, other, AttributeMap(Seq()))
    }
}

/**
  扫描操作
  物理操作的一种变量@PhysicalOperation,如果主体属性或者是过滤条件具有不确定性,就会匹配这些操作.只要它们满足@CollapseProject
  和@CombineFilters即可
  扫描的返回类型包括
    命名表达式
    表达式列表
    逻辑执行计划
    属性表达式表
 */
object ScanOperation extends OperationHelper with PredicateHelper {
  type ScanReturnType = Option[(Option[Seq[NamedExpression]],
    Seq[Expression], LogicalPlan, AttributeMap[Expression])]

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    collectProjectsAndFilters(plan) match {
      case Some((fields, filters, child, _)) =>
        Some((fields.getOrElse(child.output), filters, child))
      case None => None
    }
  }

  private def hasCommonNonDeterministic(
      expr: Seq[Expression],
      aliases: AttributeMap[Expression]): Boolean = {
    expr.exists(_.collect {
      case a: AttributeReference if aliases.contains(a) => aliases(a)
    }.exists(!_.deterministic))
  }
  
  /**
    收集指定计划@plan的主属性和过滤信息,形成扫描信息
    * @param plan
    * @return
    */
  private def collectProjectsAndFilters(plan: LogicalPlan): ScanReturnType = {
    plan match {
      // 如果逻辑计划是主属性类型,对其进行处理
      case Project(fields, child) =>
        collectProjectsAndFilters(child) match {
          case Some((_, filters, other, aliases)) =>
            // Follow CollapseProject and only keep going if the collected Projects
            // do not have common non-deterministic expressions.
            if (!hasCommonNonDeterministic(fields, aliases)) {
              val substitutedFields =
                fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
              Some((Some(substitutedFields), filters, other, collectAliases(substitutedFields)))
            } else {
              None
            }
          case None => None
        }
      // 如果逻辑计划是过滤器类型,对其进行处理
      case Filter(condition, child) =>
        collectProjectsAndFilters(child) match {
          case Some((fields, filters, other, aliases)) =>
            // Follow CombineFilters and only keep going if the collected Filters
            // are all deterministic and this filter doesn't have common non-deterministic
            // expressions with lower Project.
            if (filters.forall(_.deterministic) &&
              !hasCommonNonDeterministic(Seq(condition), aliases)) {
              val substitutedCondition = substitute(aliases)(condition)
              Some((fields, filters ++ splitConjunctivePredicates(substitutedCondition),
                other, aliases))
            } else {
              None
            }
          case None => None
        }
      // 线索信息处理
      case h: ResolvedHint =>
        collectProjectsAndFilters(h.child)

      case other =>
        Some((None, Nil, other, AttributeMap(Seq())))
    }
  }
}

/**
 寻找满足相等条件的join关系,可以使用equi-join处理
 不安全的相等操作会被转化为join操作(这种情况下使用默认值代替null)
 */
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, leftKeys, rightKeys, condition, leftChild, rightChild, joinHint) */
  /**
     返回类型
     包含下属参数:
     join类型,表达式列表(左key值),表达式列表(右key值),表达式(执行条件),左子节点,右子节点,join线索
    */
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression],
      Option[Expression], LogicalPlan, LogicalPlan, JoinHint)
  
  /**
    拆解join的操作,将其转换为合适的返回类型
    * @param join
    * @return join的返回类型
    */
  def unapply(join: Join): Option[ReturnType] = join match {
	  case Join(left, right, joinType, condition, hint) =>
	  // 寻找equal-join的谓词,这个可以在join之前进行评估,因此作为join的key使用
	  val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
		logDebug(s"Considering join on: $condition")
	  // 确定join的key
      val joinKeys = predicates.flatMap {
		// 左子节点或者由子节点为空,则join key为null
        case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => None
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => Some((l, r)) //满足left join条件
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => Some((r, l)) //满足right join条件
		// 使用默认的值替换空值,有空值的行就可以相互join了
        case EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
          Seq((Coalesce(Seq(l, Literal.default(l.dataType))),
            Coalesce(Seq(r, Literal.default(r.dataType)))),
            (IsNull(l), IsNull(r))
          )
        case EqualNullSafe(l, r) if canEvaluate(l, right) && canEvaluate(r, left) =>
          Seq((Coalesce(Seq(r, Literal.default(r.dataType))),
            Coalesce(Seq(l, Literal.default(l.dataType)))),
            (IsNull(r), IsNull(l))
          )
        case other => None
      }
	  // 其他谓词处理
      val otherPredicates = predicates.filterNot {
        case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => false
        case Equality(l, r) =>
          canEvaluate(l, left) && canEvaluate(r, right) ||
            canEvaluate(l, right) && canEvaluate(r, left)
        case _ => false
      }

      if (joinKeys.nonEmpty) {
        val (leftKeys, rightKeys) = joinKeys.unzip
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And), left, right, hint))
      } else {
        None
      }
  }
}

/**
这个格式,是用于收集过滤器和内部join的形式
  生成图
 *          Filter
 *            |
 *        inner Join
 *          /    \            ---->      (Seq(plan0, plan1, plan2), conditions)
 *      Filter   plan2
 *        |
 *  inner join
 *      /    \
 *   plan0    plan1
  注意当前这个方法仅仅工作在左子树上
 */
object ExtractFiltersAndInnerJoins extends PredicateHelper {

  /**
	将所有的inner join平面化,各个inner join相互相连.返回一个逻辑计划的列表,
	返回左子树的完全join条件列表
	返回类型:
		逻辑计划
		内部链接类型
   */
  def flattenJoin(plan: LogicalPlan, parentJoinType: InnerLike = Inner)
      : (Seq[(LogicalPlan, InnerLike)], Seq[Expression]) = plan match {
    case Join(left, right, joinType: InnerLike, cond, hint) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(left, joinType)
      (plans ++ Seq((right, joinType)), conditions ++
        cond.toSeq.flatMap(splitConjunctivePredicates))
    case Filter(filterCondition, j @ Join(_, _, _: InnerLike, _, hint)) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(j)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))

    case _ => (Seq((plan, parentJoinType)), Seq.empty)
  }

  def unapply(plan: LogicalPlan)
      : Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]
      = plan match {
    case f @ Filter(filterCondition, j @ Join(_, _, joinType: InnerLike, _, hint))
        if hint == JoinHint.NONE =>
      Some(flattenJoin(f))
    case j @ Join(_, _, joinType, _, hint) if hint == JoinHint.NONE =>
      Some(flattenJoin(j))
    case _ => None
  }
}

/**
  当进行物理执行的聚合时的提取器.与逻辑聚合项必须,会进行下述转换
  1. 命名未命名的分组表达式,这样就可以通过聚合来进行引用
  2. 聚合计算与最终结果不同,例如,count+1中的count会被分割成@AggregateExpression
  且最终计算会计算count.resultAttribute + 1
 */
object PhysicalAggregation {
  // groupingExpressions, aggregateExpressions, resultExpressions, child
  type ReturnType =
    (Seq[NamedExpression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
      // A single aggregate expression might appear multiple times in resultExpressions.
      // In order to avoid evaluating an individual aggregate function multiple times, we'll
      // build a set of semantically distinct aggregate expressions and re-write expressions so
      // that they reference the single copy of the aggregate function which actually gets computed.
      // Non-deterministic aggregate expressions are not deduplicated.
      val equivalentAggregateExpressions = new EquivalentExpressions
      val aggregateExpressions = resultExpressions.flatMap { expr =>
        expr.collect {
          // addExpr() always returns false for non-deterministic expressions and do not add them.
          case agg: AggregateExpression
            if !equivalentAggregateExpressions.addExpr(agg) => agg
          case udf: PythonUDF
            if PythonUDF.isGroupedAggPandasUDF(udf) &&
              !equivalentAggregateExpressions.addExpr(udf) => udf
        }
      }

      val namedGroupingExpressions = groupingExpressions.map {
        case ne: NamedExpression => ne -> ne
        // If the expression is not a NamedExpressions, we add an alias.
        // So, when we generate the result of the operator, the Aggregate Operator
        // can directly get the Seq of attributes representing the grouping expressions.
        case other =>
          val withAlias = Alias(other, other.toString)()
          other -> withAlias
      }
      val groupExpressionMap = namedGroupingExpressions.toMap

      // The original `resultExpressions` are a set of expressions which may reference
      // aggregate expressions, grouping column values, and constants. When aggregate operator
      // emits output rows, we will use `resultExpressions` to generate an output projection
      // which takes the grouping columns and final aggregate result buffer as input.
      // Thus, we must re-write the result expressions so that their attributes match up with
      // the attributes of the final result projection's input row:
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transformDown {
          case ae: AggregateExpression =>
            // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
            // so replace each aggregate expression by its corresponding attribute in the set:
            equivalentAggregateExpressions.getEquivalentExprs(ae).headOption
              .getOrElse(ae).asInstanceOf[AggregateExpression].resultAttribute
            // Similar to AggregateExpression
          case ue: PythonUDF if PythonUDF.isGroupedAggPandasUDF(ue) =>
            equivalentAggregateExpressions.getEquivalentExprs(ue).headOption
              .getOrElse(ue).asInstanceOf[PythonUDF].resultAttribute
          case expression =>
            // Since we're using `namedGroupingAttributes` to extract the grouping key
            // columns, we need to replace grouping key expressions with their corresponding
            // attributes. We do not rely on the equality check at here since attributes may
            // differ cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }

      Some((
        namedGroupingExpressions.map(_._2),
        aggregateExpressions,
        rewrittenResultExpressions,
        child))

    case _ => None
  }
}

/**
  当进行窗口的物理执行计划是的提取器.这个提取器输出逻辑窗口的窗口函数
 	输入逻辑窗口必须包含窗口函数的同样类型,这个是由分析器中的@ExtractWindowExpressions准则保证的
 */
object PhysicalWindow {
  // windowFunctionType, windowExpression, partitionSpec, orderSpec, child
  private type ReturnType =
    (WindowFunctionType, Seq[NamedExpression], Seq[Expression], Seq[SortOrder], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case expr @ logical.Window(windowExpressions, partitionSpec, orderSpec, child) =>

      // The window expression should not be empty here, otherwise it's a bug.
      if (windowExpressions.isEmpty) {
        throw new AnalysisException(s"Window expression is empty in $expr")
      }

      val windowFunctionType = windowExpressions.map(WindowFunctionType.functionType)
        .reduceLeft { (t1: WindowFunctionType, t2: WindowFunctionType) =>
          if (t1 != t2) {
            // We shouldn't have different window function type here, otherwise it's a bug.
            throw new AnalysisException(
              s"Found different window function type in $windowExpressions")
          } else {
            t1
          }
        }

      Some((windowFunctionType, windowExpressions, partitionSpec, orderSpec, child))

    case _ => None
  }
}
