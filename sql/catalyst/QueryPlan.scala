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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode, TreeNodeTag}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

// 查询计划
abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

  /**
    当前状态下的SQLConf
   */
  def conf: SQLConf = SQLConf.get

  // 输出属性列表
  def output: Seq[Attribute]

  /**
    当前节点的输出属性集合
   */
  @transient
  lazy val outputSet: AttributeSet = AttributeSet(output)

  /**
    子节点输入到这个操作符的属性集合
   */
  def inputSet: AttributeSet =
    AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output))

  /**
    节点所有属性集合
   */
  def producedAttributes: AttributeSet = AttributeSet.empty

  /**
    所有出现在当前操作符的表达式中的属性集合
    注意到这个集合不会包含显示传递到输出元组中的数据,即@producedAttributes
   * All Attributes that appear in expressions from this operator.  Note that this set does not
   * include attributes that are implicitly referenced by being passed through to the output tuple.
   */
  @transient
  lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(expressions.map(_.references)) -- producedAttributes
  }

  /**
    表达式引用的属性集合,不包含子节点提供的属性
   */
  final def missingInput: AttributeSet = references -- inputSet

  /**
    转换表达式
    使用向下转换@transformExpressionsDown,用于所有查询操作中的表达式.
    用户不需要特定的方向性.如果需要特定的转换,则需要使用@transformExpressionsDown或者@transformExpressionsUp
   * @param rule 操作表达式的转换规则
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDown(rule)
  }

  /**
    向下转换(对于操作符的表达式)
   * @param rule 操作符的规则
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    mapExpressions(_.transformDown(rule))
  }

  /**
   向上转换(对于操作符的表达式)
   * @param rule 操作符的规则
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    mapExpressions(_.transformUp(rule))
  }

  /**
    对于每个查询操作中的表达式使用map函数,返回一个新的查询操作符(基于映射的表达式)
   */
  def mapExpressions(f: Expression => Expression): this.type = {
    var changed = false

    @inline def transformExpression(e: Expression): Expression = {
      val newE = CurrentOrigin.withOrigin(e.origin) {
        f(e)
      }
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpression(e)
      case Some(value) => Some(recursiveTransform(value))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case stream: Stream[_] => stream.map(recursiveTransform).force
      case seq: Iterable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
    返回当前节点运行@transformExpressions 的阶段,且返回其子节点
   */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transform {
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  // 获取所有查询计划操作符中的表达式
  final def expressions: Seq[Expression] = {
    // Recursively find all expressions from a traversable.
    def seqToExpressions(seq: Iterable[Any]): Iterable[Expression] = seq.flatMap {
      case e: Expression => e :: Nil
      case s: Iterable[_] => seqToExpressions(s)
      case other => Nil
    }

    productIterator.flatMap {
      case e: Expression => e :: Nil
      case s: Some[_] => seqToExpressions(s.toSeq)
      case seq: Iterable[_] => seqToExpressions(seq)
      case other => Nil
    }.toSeq
  }

  lazy val schema: StructType = StructType.fromAttributes(output)

  /** Returns the output schema in the tree format. */
  def schemaString: String = schema.treeString

  /** Prints out the schema in the tree format */
  // scalastyle:off println
  def printSchema(): Unit = println(schemaString)
  // scalastyle:on println

  /**
    状态前缀符,打印在计划之前
    使用!表示是无效计划,使用'表示是未处理计划
   */
  protected def statePrefix = if (missingInput.nonEmpty && children.nonEmpty) "!" else ""

  override def simpleString(maxFields: Int): String = statePrefix + super.simpleString(maxFields)

  override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleStringWithNodeId(): String = {
    val operatorId = getTagValue(QueryPlan.OP_ID_TAG).map(id => s"$id").getOrElse("unknown")
    s"$nodeName ($operatorId)".trim
  }

  def verboseStringWithOperatorId(): String = {
    val codegenIdStr =
      getTagValue(QueryPlan.CODEGEN_ID_TAG).map(id => s"[codegen id : $id]").getOrElse("")
    val operatorId = getTagValue(QueryPlan.OP_ID_TAG).map(id => s"$id").getOrElse("unknown")
    s"""
       |($operatorId) $nodeName $codegenIdStr
     """.stripMargin
  }

  /**
    获取当前计划的所有子查询
   */
  def subqueries: Seq[PlanType] = {
    expressions.flatMap(_.collect {
      case e: PlanExpression[_] => e.plan.asInstanceOf[PlanType]
    })
  }

  /**
    返回一个序列,包含使用函数@f的执行结果(执行对象是这个计划的所有元素),同时考虑到子查询
   */
  def collectInPlanAndSubqueries[B](f: PartialFunction[PlanType, B]): Seq[B] =
    (this +: subqueriesAll).flatMap(_.collect(f))

  /**
    获取当前计划的子查询序列,包括子查询的子查询(嵌入式查询)
   */
  def subqueriesAll: Seq[PlanType] = {
    val subqueries = this.flatMap(_.subqueries)
    subqueries ++ subqueries.flatMap(_.subqueriesAll)
  }

  override def innerChildren: Seq[QueryPlan[_]] = subqueries

  /**
   * A private mutable variable to indicate whether this plan is the result of canonicalization.
   * This is used solely for making sure we wouldn't execute a canonicalized plan.
   * See [[canonicalized]] on how this is set.
   */
  @transient private var _isCanonicalizedPlan: Boolean = false

  protected def isCanonicalizedPlan: Boolean = _isCanonicalizedPlan

  /**
    返回一个计划,尽最大努力去将当前结果进行保存并且移除额外的描述信息(例如,大小写敏感信息,交换操作的排序,表达式id等等信息)
    计划(this.canonicalized == other.canonicalized) 总是会进行这样的计算.
    计划节点需要特殊的规范化处理,需要重写@doCanonicalize方法.其需要移除额外的变量信息
   */
  @transient final lazy val canonicalized: PlanType = {
    // 1.进行语句规范化处理
    var plan = doCanonicalize()
    // If the plan has not been changed due to canonicalization, make a copy of it so we don't
    // mutate the original plan's _isCanonicalizedPlan flag.
    if (plan eq this) {
      plan = plan.makeCopy(plan.mapProductIterator(x => x.asInstanceOf[AnyRef]))
    }
    plan._isCanonicalizedPlan = true
    plan
  }

  /**
    当前计划的规范处理
   */
  protected def doCanonicalize(): PlanType = {
    val canonicalizedChildren = children.map(_.canonicalized)
    // 这个值用于标记表达式唯一编号,从0开始
    var id = -1
    mapExpressions {
      case a: Alias =>
        id += 1
        // As the root of the expression, Alias will always take an arbitrary exprId, we need to
        // normalize that for equality testing, by assigning expr id from 0 incrementally. The
        // alias name doesn't matter and should be erased.
        val normalizedChild = QueryPlan.normalizeExpressions(a.child, allAttributes)
        Alias(normalizedChild, "")(ExprId(id), a.qualifier)

      case ar: AttributeReference if allAttributes.indexOf(ar.exprId) == -1 =>
        // Top level `AttributeReference` may also be used for output like `Alias`, we should
        // normalize the epxrId too.
        id += 1
        ar.withExprId(ExprId(id)).canonicalized

      case other => QueryPlan.normalizeExpressions(other, allAttributes)
    }.withNewChildren(canonicalizedChildren)
  }

  /**
    当给定的查询计划返回的结果与当前查询计划的查询结果相同的时候,返回true.
    因为两个给定的集合是否会产生相等的查询值是一个不确定的情况,这个函数返回false是ok的(这时候可能两个的执行结果是相等的)
    这个行为不会影响到执行的正确性,仅仅查询性能会得到提升.但是如果返回true,则查询结果则一定是一致的.
    这个函数使用了一个修改的版本,对额外信息的差异进行了容错.例如,属性命名或者是表达式id的差异.
   */
  final def sameResult(other: PlanType): Boolean = this.canonicalized == other.canonicalized

  /**
    语义hash
    获取当前查询计划计算的语义hash.与标准hash不同,这里需要消除额外(cosmetric)信息的不同
    即进行规范化处理
   */
  final def semanticHash(): Int = canonicalized.hashCode()

  // 获取当前查询计划的所有属性值
  lazy val allAttributes: AttributeSeq = children.flatMap(_.output)
}

// 查询计划
object QueryPlan extends PredicateHelper {
  // 操作ID标记
  val OP_ID_TAG = TreeNodeTag[Int]("operatorId")
  // 代码生成ID标志
  val CODEGEN_ID_TAG = new TreeNodeTag[Int]("wholeStageCodegenId")

  /**
   * Normalize the exprIds in the given expression, by updating the exprId in `AttributeReference`
   * with its referenced ordinal from input attributes. It's similar to `BindReferences` but we
   * do not use `BindReferences` here as the plan may take the expression as a parameter with type
   * `Attribute`, and replace it with `BoundReference` will cause error.
   */
  def normalizeExpressions[T <: Expression](e: T, input: AttributeSeq): T = {
    e.transformUp {
      case s: PlanExpression[QueryPlan[_] @unchecked] =>
        // Normalize the outer references in the subquery plan.
        val normalizedPlan = s.plan.transformAllExpressions {
          case OuterReference(r) => OuterReference(QueryPlan.normalizeExpressions(r, input))
        }
        s.withNewPlan(normalizedPlan)

      case ar: AttributeReference =>
        val ordinal = input.indexOf(ar.exprId)
        if (ordinal == -1) {
          ar
        } else {
          ar.withExprId(ExprId(ordinal))
        }
    }.canonicalized.asInstanceOf[T]
  }

  /**
   * Composes the given predicates into a conjunctive predicate, which is normalized and reordered.
   * Then returns a new sequence of predicates by splitting the conjunctive predicate.
   */
  def normalizePredicates(predicates: Seq[Expression], output: AttributeSeq): Seq[Expression] = {
    if (predicates.nonEmpty) {
      val normalized = normalizeExpressions(predicates.reduce(And), output)
      splitConjunctivePredicates(normalized)
    } else {
      Nil
    }
  }

  /**
    将查询计划转换为字符串形式,并通过给定的函数添加
    @plan 查询计划
    @append 字符串添加函数
    @verbose 信息是否可见
    @addSuffix 是否添加后缀信息
    @maxFields 最大属性数量
    @printOperatorId 是否打印操作Id信息
   */
  def append[T <: QueryPlan[T]](
      plan: => QueryPlan[T],
      append: String => Unit,
      verbose: Boolean,
      addSuffix: Boolean,
      maxFields: Int = SQLConf.get.maxToStringFields,
      printOperatorId: Boolean = false): Unit = {
    try {
      plan.treeString(append, verbose, addSuffix, maxFields, printOperatorId)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }
}
