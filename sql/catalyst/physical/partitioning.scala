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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
  指定元组的表达式共享方式,这个表达式在执行查询的时候会被分发到多台机器,并发执行.
  分发指的是数据分区的内部结点.指的是,其藐视了分区如何通过集群物理机器进行分区的.直到了这个属性会使得一些操作,例如聚合操作
  Aggregate可以获得分区本地操作,而非是全局操作.
 */
sealed trait Distribution {
  /**
    获取这个分发情况(distribution)需要的分区数量.如果设置为none.则可以允许任意数量的分区.
   */
  def requiredNumPartitions: Option[Int]

  /**
    对于当前分发,创建默认的分区策略,这个会在匹配给定分区数量的时候满足分配情况.
   */
  def createPartitioning(numPartitions: Int): Partitioning
}

/**
  分发策略(Distribution) --> 未指定的分发策略
  代表一个分发,对于数据的协定位置没有保障
 */
case object UnspecifiedDistribution extends Distribution {
  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    throw new IllegalStateException("UnspecifiedDistribution does not have default partitioning.")
  }
}

/**
  分发方式 --> 所有的元组
  代表一个分发策略,有单个分区,且所有数据集的所有元组都是经过位置协商的
 */
case object AllTuples extends Distribution {
  // 分区数量 --> 1个
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1, "The default partitioning of AllTuples can only have 1 partition.")
    SinglePartition
  }
}

/**
  代表数据中元组是通过集群进行共享的,在同一个分区中是位置协同的
  @clustering 集群信息列表
  @requiredNumPartitions 需要的分区数量
  分区模式使用的是简单的hash分区方式
 */
case class ClusteredDistribution(
    clustering: Seq[Expression],
    requiredNumPartitions: Option[Int] = None) extends Distribution {
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(requiredNumPartitions.isEmpty || requiredNumPartitions.get == numPartitions,
      s"This ClusteredDistribution requires ${requiredNumPartitions.get} partitions, but " +
        s"the actual number of partitions is $numPartitions.")
    HashPartitioning(clustering, numPartitions)
  }
}

/**
  代表数据,元组可以通过给定的表达式进行元组集群划分.hash函数由@HashPartitioning.partitionIdExpression 定义.
  所以仅仅@HashPartitioning 可以满足分发需求.
  Hash集群分布式集群分布@ClusteredDistribution 的强烈保证.给定一个元组和分区数量,分布严格要求元组属于对于的分区
 */
case class HashClusteredDistribution(
    expressions: Seq[Expression],
    requiredNumPartitions: Option[Int] = None) extends Distribution {
  require(
    expressions != Nil,
    "The expressions for hash of a HashClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(requiredNumPartitions.isEmpty || requiredNumPartitions.get == numPartitions,
      s"This HashClusteredDistribution requires ${requiredNumPartitions.get} partitions, but " +
        s"the actual number of partitions is $numPartitions.")
    HashPartitioning(expressions, numPartitions)
  }
}

/**
  代表元组排序的数据,主要是通过排序的表达式.
  需要进行如下定义:
  - 给定两个邻接分区,第二个分区的所有行必须要大于或者等于首个分区的任意一行,主要是通过`ordering`的表达式进行实现
  换句话说,这个分区需要行通过分区进行排序,但是不必要一定在分区中
 */
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of an OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    // 范围分区策略
    RangePartitioning(ordering, numPartitions)
  }
}

/**
  代表尽管广播到每个阶段的元组数据.当整个元组结合被转换为不同的数据结构的时候非常常用.
 */
case class BroadcastDistribution(mode: BroadcastMode) extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1,
      "The default partitioning of BroadcastDistribution can only have 1 partition.")
    BroadcastPartitioning(mode)
  }
}

/**
  分区策略
  描述操作输出如何在分区间进行分配的.有两个主要的属性
  1. 分区的数量
  2. 是否可以满足给定的分区
 */
trait Partitioning {
  // 获取分区数量
  val numPartitions: Int

  /**
    如果当前分区策略,满足指定的分布@required 的分区schema.当前数据集不需要进行重新的分区.
    例如当前数据集不需要在指定的分布中@required 进行重新分区.(很可能需要在分区中进行重新分配)
    一个分区再分区数量不匹配的情况下,不会满足给定的分布
   */
  final def satisfies(required: Distribution): Boolean = {
    required.requiredNumPartitions.forall(_ == numPartitions) && satisfies0(required)
  }

  /**
    这个是定义分区是否满足分区的实际方法,在进行分区数量检查之后进行
    默认情况下,分区可以满足@UnspecifiedDistribution和@AllTuples(如果仅仅一个分区的话)
    实现可以通过指定的逻辑进行重写
   */
  protected def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case AllTuples => numPartitions == 1
    case _ => false
  }
}

case class UnknownPartitioning(numPartitions: Int) extends Partitioning

/**
  RoundRobin分区策略,通过由随机分区编号开始且按照RoundRobin的方式代表一个分区.
  分区再DF的重分区实现操作的时候使用
 * Represents a partitioning where rows are distributed evenly across output partitions
 * by starting from a random target partition number and distributing rows in a round-robin
 * fashion. This partitioning is used when implementing the DataFrame.repartition() operator.
 */
case class RoundRobinPartitioning(numPartitions: Int) extends Partitioning

// 单分区,分区只有一个
case object SinglePartition extends Partitioning {
  val numPartitions = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case _: BroadcastDistribution => false
    case _ => true
  }
}

/**
  Hash分区,行会通过hash表达式在分区间进行分配.所有的行(表达式计算出的值相等的行)会被分配到同一个分区中
  @expressions: Seq[Expression] 表达式列表
  @numPartitions 分区数量
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {
  // 子表达式
  override def children: Seq[Expression] = expressions
  // 是否可以为空
  override def nullable: Boolean = false
  // 数据类型
  override def dataType: DataType = IntegerType
  
  // 确定是否满足给定的分区@required方式
  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: HashClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case ClusteredDistribution(requiredClustering, _) =>
          expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

  /**
    获取分区ID表达式,这个表达式会生成一个可用的分区ID(0 ->numPartitions)
    采用MurMur3Hash算法
   */
  def partitionIdExpression: Expression = Pmod(new Murmur3Hash(expressions), Literal(numPartitions))
}

/**
  范围分区策略基于`ordering`的总体排序策略将行在分区间进行分割.当数据按照这种方式进行分区的时候,可以保证到:
  给定两个连续的分区,第二个分区行数据必须要大于第一个分区.通过的是`ordering`表达式指定逻辑.
  这里更加严格强烈的方式是`OrderedDistribution(ordering)`,这个操作不会再分区之间进行覆盖
  这个类需要继承表达式的特征，这样表达式的降序转换就会使用在其子类上
  @ordering: Seq[SortOrder] 排序表达式
  @numPartitions 分区数量
 */
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case OrderedDistribution(requiredOrdering) =>
          // If `ordering` is a prefix of `requiredOrdering`:
          //   Let's say `ordering` is [a, b] and `requiredOrdering` is [a, b, c]. According to the
          //   RangePartitioning definition, any [a, b] in a previous partition must be smaller
          //   than any [a, b] in the following partition. This also means any [a, b, c] in a
          //   previous partition must be smaller than any [a, b, c] in the following partition.
          //   Thus `RangePartitioning(a, b)` satisfies `OrderedDistribution(a, b, c)`.
          //
          // If `requiredOrdering` is a prefix of `ordering`:
          //   Let's say `ordering` is [a, b, c] and `requiredOrdering` is [a, b]. According to the
          //   RangePartitioning definition, any [a, b, c] in a previous partition must be smaller
          //   than any [a, b, c] in the following partition. If there is a [a1, b1] from a previous
          //   partition which is larger than a [a2, b2] from the following partition, then there
          //   must be a [a1, b1 c1] larger than [a2, b2, c2], which violates RangePartitioning
          //   definition. So it's guaranteed that, any [a, b] in a previous partition must not be
          //   greater(i.e. smaller or equal to) than any [a, b] in the following partition. Thus
          //   `RangePartitioning(a, b, c)` satisfies `OrderedDistribution(a, b)`.
          val minSize = Seq(requiredOrdering.size, ordering.size).min
          requiredOrdering.take(minSize) == ordering.take(minSize)
        case ClusteredDistribution(requiredClustering, _) =>
          ordering.map(_.child).forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }
}

/**
  分区集合
  可以用于描述1分区物理操作输出的schema。经常用于多个子节点的操作。这种情况下，集合中的分区描述了操作输出分区的方式。
  例如，对于join操作，两个操作表为A和B。使用条件A.key1 = B.key2.
  假定使用了Hash分区的Schema。有两个分区可以用于描述join操作分区的方式。分别是HashPartitioning(A.key1)和
  HashPartitioning(B.key2).对于集合中分区不相等的不需要这么做，对于外连接比较有用。
 */
case class PartitioningCollection(partitionings: Seq[Partitioning])
  extends Expression with Partitioning with Unevaluable {

  require(
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")

  override def children: Seq[Expression] = partitionings.collect {
    case expr: Expression => expr
  }

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override val numPartitions = partitionings.map(_.numPartitions).distinct.head

  /**
   * Returns true if any `partitioning` of this collection satisfies the given
   * [[Distribution]].
   */
  override def satisfies0(required: Distribution): Boolean =
    partitionings.exists(_.satisfies(required))

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }
}

/**
  广播变量分区策略
  代表行内容收集，转换，广播的分区。可以用于集群中每个节点
 */
case class BroadcastPartitioning(mode: BroadcastMode) extends Partitioning {
  override val numPartitions: Int = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case BroadcastDistribution(m) if m == mode => true
    case _ => false
  }
}
