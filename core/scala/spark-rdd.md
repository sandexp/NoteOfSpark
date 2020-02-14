## **spark-rdd**

---

1.  [PeriodicRDDCheckpointer.scala](# PeriodicRDDCheckpointer)
2.  [AsyncRDDActions.scala](# AsyncRDDActions)
3.  [BinaryFileRDD.scala](# BinaryFileRDD)
4.  [BlockRDD.scala](# BlockRDD)
5.  [CartesianRDD.scala](# CartesianRDD)
6.  [CheckpointRDD.scala](# CheckpointRDD)
7.  [coalescer-public](# coalescer-public)
8.  [CoalescedRDD.scala](# CoalescedRDD)
9.  [CoGroupedRDD.scala](CoGroupedRDD)
10.  [DoubleRDDFunctions.scala](# DoubleRDDFunctions)
11.  [EmptyRDD.scala](# EmptyRDD)
12.  [HadoopRDD.scala](# HadoopRDD)
13.  [InputFileBlockHolder.scala](# InputFileBlockHolder)
14.  [JdbcRDD.scala](# JdbcRDD)
15.  [LocalCheckpointRDD.scala](# LocalCheckpointRDD)
16.  [LocalRDDCheckpointData.scala](# LocalRDDCheckpointData)
17.  [MapPartitionsRDD.scala](# MapPartitionsRDD)
18.  [NewHadoopRDD.scala](# NewHadoopRDD)
19.  [OrderedRDDFunctions.scala](# OrderedRDDFunctions)
20.  [PairRDDFunctions.scala](# PairRDDFunctions)
21.  [ParallelCollectionRDD.scala](# ParallelCollectionRDD)
22.  [PartitionerAwareUnionRDD.scala](# PartitionerAwareUnionRDD)
23.  [PartitionPruningRDD.scala](# PartitionPruningRDD)
24.  [PartitionwiseSampledRDD.scala](# PartitionwiseSampledRDD)
25.  [PipedRDD.scala](# PipedRDD)
26.  [RDD.scala](# RDD)
27.  [RDDBarrier.scala](# RDDBarrier)
28.  [RDDCheckpointData.scala](# RDDCheckpointData)
29.  [RDDOperationScope.scala](# RDDOperationScope)
30.  [ReliableCheckpointRDD.scala](# ReliableCheckpointRDD)
31.  [ReliableRDDCheckpointData.scala](# ReliableRDDCheckpointData)
32.  [SequenceFileRDDFunctions.scala](# SequenceFileRDDFunctions)
33.  [ShuffledRDD.scala](# ShuffledRDD)
34.  [SubtractedRDD.scala](# SubtractedRDD)
35.  [UnionRDD.scala](# UnionRDD)
36.  [WholeTextFileRDD.scala](# WholeTextFileRDD)
37.  [ZippedPartitionsRDD.scala](# ZippedPartitionsRDD)
38.  [ZippedWithIndexRDD.scala](# ZippedWithIndexRDD)
39.  [基础拓展](# 基础拓展)

---

#### PeriodicRDDCheckpointer

```markdown
这个类帮助持久化和给RDD设置检查点,特殊地,它会自动处理持久化和检查点(可选),以及解除持久化和移除检查点文件.
当RDD创建的时候,,且RDD没有被实体化之前,用户调用@updates() 方法.在更新@PeriodicRDDCheckpointer 之后,用户需要实体化RDD,从而确保持久化和检查点确实的发生.
当调用@update() 时,进行了下述动作:
1. 持久化新的RDD(如果没有被持久化),将其放入持久化RDD队列中
2. 从持久化RDD队列中解除持久化,直到其中含有至多3个持久化RDD.
3. 如果使用检查点且到达检查点的周期
+ 设置新的RDD检查点,将其置入检查点RDD队列中
+ 移除旧的检查点RDD
注意:
1. 这个类不应该被拷贝(如果拷贝,就会出现与应当需要设置检查点的RDD起冲突)
2. 这个类在后边的RDD检查点设置完毕之后,才会移除RDD检查点文件

使用示例:
{{{
	# 持久化队列至多存放3个元素
	val (rdd1, rdd2, rdd3, ...) = ...
	val cp = new PeriodicRDDCheckpointer(2, sc) // 设置检查点周期为2
	cp.update(rdd1)
    rdd1.count(); // 持久化RDD1
    cp.update(rdd2)
    rdd2.count();	// 持久化RDD2 RDD1检查点 RDD2
    cp.update(rdd3)
    rdd3.count(); // 持久化RDD3 RDD2 RDD1 检查点 RDD2
    cp.update(rdd4)
    rdd4.count(); // 持久化 RDD4,3,2 检查点 RDD4
    cp.update(rdd5)
    rdd5.count(); // 持久化 RDD 5,4,3 检查点 RDD4
}}}
```

```scala
private[spark] class PeriodicRDDCheckpointer[T](
    checkpointInterval: Int,
    sc: SparkContext)
extends PeriodicCheckpointer[RDD[T]](checkpointInterval, sc) {
    构造器参数:
        checkpointInterval	检查点周期
        sc	spark应用上下文
    操作集:
    def checkpoint(data: RDD[T]): Unit = data.checkpoint()
    功能: 对指定RDD设置检查点
    data.checkpoint()
    
    def isCheckpointed(data: RDD[T]): Boolean = data.isCheckpointed
    功能: 检查指定RDD是否设置了检查点
    
    def persist(data: RDD[T]): Unit
    功能:  持久化指定RDD(如果当前RDD没有持久化)
    if (data.getStorageLevel == StorageLevel.NONE) {
      data.persist()
    }
    
    def unpersist(data: RDD[T]): Unit = data.unpersist()
    功能: 解除指定RDD的持久化
    
    def getCheckpointFiles(data: RDD[T]): Iterable[String]
    功能: 获取指定RDD@data 的检查点文件
}
```

#### AsyncRDDActions

```scala
class AsyncRDDActions[T: ClassTag](self: RDD[T]) extends Serializable with Logging {
    介绍: 根据实现版本的不同,设置了一组异步RDD动作
    操作集:
    def countAsync(): FutureAction[Long]
    功能: 异步计数动作
    val= self.withScope { // 内部为RDD执行体代码
        val totalCount = new AtomicLong // 设置计数值
        self.context.submitJob(
          self, // 设置提交RDD为自己
          (iter: Iterator[T]) => { //设置分区 RDD处理函数,这里为简单的累加
            var result = 0L
            while (iter.hasNext) {
              result += 1L
              iter.next()
            }
            result
          },
          Range(0, self.partitions.length), // 设置任务分区提交范围
          (index: Int, data: Long) => totalCount.addAndGet(data), //通过提交job将计算结果累加到累加器中
          totalCount.get())// 设置结果获取函数
  	}
    
    def collectAsync(): FutureAction[Seq[T]]
    功能: 异步收集计算结果
    val= self.withScope {
        val results = new Array[Array[T]](self.partitions.length) // 设置结果列表
        self.context.submitJob[T, Array[T], Seq[T]](self,  // 设置提交RDD为自己
                                                _.toArray, // 设置RDD处理函数,为列表展开
                                                Range(0, self.partitions.length),// 设置分区提交范围
                                                (index, data) => results(index) = data, // 结果处理
                                                results.flatten.toSeq) // 设置结果形式
  	}
    
    def foreachAsync(f: T => Unit): FutureAction[Unit]
    功能: 异步遍历
    输入参数: f 清理函数
    1. 获取清理函数结果
    val cleanF = self.context.clean(f)
    2. 在所有分区中提交清理过程,并返回清理结果
    self.context.submitJob[T, Unit, Unit](self, _.foreach(cleanF), Range(0, self.partitions.length),
      (index, data) => (), ())
    
    def foreachPartitionAsync(f: Iterator[T] => Unit): FutureAction[Unit] 
    功能: RDD各个分区执行指定函数 f
    val= self.context.submitJob[T, Unit, Unit](self, f, Range(0, self.partitions.length),
      (index, data) => (), ())
    
    def continue(partsScanned: Int)(implicit jobSubmitter: JobSubmitter): Future[Seq[T]]
    功能: 用于递归触发job，用于扫描分区，直到请求的元素被检索到为止。或者扫描的分区扫描结束。这里实现了非阻塞，异步地处理每个job的结果，且使用任务(future)上的回调来触发下一个job.
    输入参数:
    	partsScanned	扫描区域数量
    	jobSubmitter	任务提交器
    if (results.size >= num || partsScanned >= totalParts) {// 任务执行完毕,返回结果数据列表
        Future.successful(results.toSeq)
      } else {
        //需要尝试的分区数量,大于@totalParts 是合法的,因为执行会覆盖所有分区
        var numPartsToTry = 1L
        if (partsScanned > 0) {
            // 在前面的迭代器中没有找到对应的行乘以4进行重试,否则修改需要尝试的次数并加上50%的估算值,在最后也会对其进行核算
          if (results.size == 0) {
            numPartsToTry = partsScanned * 4L
          } else {
              // 修改需要尝试的次数
            numPartsToTry = Math.max(1,
              (1.5 * num * partsScanned / results.size).toInt - partsScanned)
            numPartsToTry = Math.min(numPartsToTry, partsScanned * 4L)
          }
        }
        // 获取剩余的数据量
        val left = num - results.size
        // 获取扫描的top K 列表
        val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
        // 设置结果存储的数据结构
        val buf = new Array[Array[T]](p.size)
        self.context.setCallSite(callSite)
        self.context.setLocalProperties(localProperties)
        val job = jobSubmitter.submitJob(self, // 提交指获取任务
          (it: Iterator[T]) => it.take(left).toArray,//处理过程(获取left个元素)
          p, // 扫描分区范围
          (index: Int, data: Array[T]) => buf(index) = data, // 结果处理函数
          ())
        job.flatMap { _ =>
          buf.foreach(results ++= _.take(num - results.size)) // 添加执行结果
          continue(partsScanned + p.size) // 使用回调触发下一个job
        }
      }
    
    def takeAsync(num: Int): FutureAction[Seq[T]]
    功能: 异步获取first K问题
    1. 获取调用地址和本地属性
    val callSite = self.context.getCallSite
    val localProperties = self.context.getLocalProperties
    2. 设置结果集
    implicit val executionContext = AsyncRDDActions.futureExecutionContext
    val results = new ArrayBuffer[T]
    val totalParts = self.partitions.length
    val= new ComplexFutureAction[Seq[T]](continue(0)(_))
}
```

```scala
private object AsyncRDDActions {
    属性:
    #name @futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("AsyncRDDActions-future", 128))
    执行器任务线程池
}
```

#### BinaryFileRDD

```scala
private[spark] class BinaryFileRDD[T](
    @transient private val sc: SparkContext,
    inputFormatClass: Class[_ <: StreamFileInputFormat[T]],
    keyClass: Class[String],
    valueClass: Class[T],
    conf: Configuration,
    minPartitions: Int)
extends NewHadoopRDD[String, T](sc, inputFormatClass, keyClass, valueClass, conf) {
    介绍: 二进制文件RDD
    构造器参数:
        sc	spark应用上下文
        inputFormatClass	输入类型
        keyClass	key类型
        valueClass	value类型
        conf	spark配置
        minPartitions	最小分区
    操作集:
    def getPartitions: Array[Partition] 
    功能: 获取分区列表
    1. 设置最小分区数量
    val conf = getConf
    // 展示目录状态的线程数量
    conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString) 
    2. 设置输入流
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = new JobContextImpl(conf, jobId)
    inputFormat.setMinPartitions(sc, jobContext, minPartitions)
    3. 获取分片列表
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    4. 将分配内容存入结果列表中
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    val= result
}
```

#### BlockRDD

```scala
private[spark] class BlockRDD[T: ClassTag](sc: SparkContext, @transient val blockIds: Array[BlockId])
extends RDD[T](sc, Nil) {
    介绍: 数据块RDD
    构造器参数:
        sc	spark上下文配置
        blockIds	数据块标识符列表
    属性:
    #name @_locations = BlockManager.blockIdsToLocations(blockIds, SparkEnv.get) transient lazy
    	数据块位置映射表
    #name @_isValid = true volatile 是否可以使用
    操作集:
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    0. 可用性断言
    assertValid()
    1. 获取分区列表
    val= (0 until blockIds.length).map { i =>
      new BlockRDDPartition(blockIds(i), i).asInstanceOf[Partition]
    }.toArray
    
    def compute(split: Partition, context: TaskContext): Iterator[T]
    功能: 获取数据块的数据
    0. 可用性校验
    assertValid()
    1. 从本地/远端获取数据块
    val blockManager = SparkEnv.get.blockManager
    2. 获取数据块标识符
    val blockId = split.asInstanceOf[BlockRDDPartition].blockId
    3. 获取数据块数据对应的迭代器
    val= blockManager.get[T](blockId) match {
      case Some(block) => block.data.asInstanceOf[Iterator[T]]
      case None =>
        throw new Exception(s"Could not compute split, block $blockId of RDD $id not found")
    }
    
    def getPreferredLocations(split: Partition): Seq[String]
    功能: 获取指定分区@split 所对应的数据块位置信息
    assertValid()
    val= _locations(split.asInstanceOf[BlockRDDPartition].blockId)
    
    def isValid: Boolean = _isValid
    功能: 确定可用性
    
    def assertValid(): Unit
    功能: 可用性断言
    if (!isValid) {
      throw new SparkException(
        "Attempted to use %s after its blocks have been removed!".format(toString))
    }
    
    def getBlockIdLocations(): Map[BlockId, Seq[String]] = _locations
    功能: 获取数据块映射表
    
    def removeBlocks(): Unit
    功能: 移除块RDD所所对应的数据块，操作不可逆
    blockIds.foreach { blockId =>
      sparkContext.env.blockManager.master.removeBlock(blockId)
    }
    _isValid = false
}
```

```scala
private[spark] class BlockRDDPartition(val blockId: BlockId, idx: Int) extends Partition {
   构造器属性:
    #name @blockId	数据块标识符
    val index = idx 	编号
}
```

#### CartesianRDD

```scala
private[spark] class CartesianPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {
    介绍: 笛卡尔分区
    构造器参数:
        idx	分区编号
        rdd1	rdd1
        rdd2	rdd2
        s1Index rdd1索引
        s2Index	rdd2索引
    属性:
    #name @s1 = rdd1.partitions(s1Index)	RDD1分区列表
    #name @s2 = rdd2.partitions(s2Index)	RDD2分区列表
    #name @index: Int = idx	索引
    
    操作集:
    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit
    功能: 写出非静态非transient的本类属性到输出流
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
}
```

```scala
private[spark]
class CartesianRDD[T: ClassTag, U: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[T],
    var rdd2 : RDD[U])
extends RDD[(T, U)](sc, Nil) with Serializable {
    构造器参数:
        sc spark应用配置集
        rdd1	RDD1
        rdd2	RDD2	
    属性:
    #name @numPartitionsInRdd2 = rdd2.partitions.length	RDD2分区数量
    操作集:
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    1. 创建笛卡尔积维度的结果数组
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    2. 设置笛卡尔RDD分区数组的内容
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    val= array
    
    def getPreferredLocations(split: Partition): Seq[String]
    功能: 获取指定分区的位置描述信息
    1. 获取当前笛卡尔分区
    val currSplit = split.asInstanceOf[CartesianPartition]
    2. 获取分区位置描述列表
    val= (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
    
    def getDependencies: Seq[Dependency[_]]
    功能: 获取依赖列表(都是窄依赖)
    val= List(
        new NarrowDependency(rdd1) {
          def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
        },
        new NarrowDependency(rdd2) {
          def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
        }
      )
    
    def clearDependencies(): Unit
    功能: 清理依赖
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    
    def compute(split: Partition, context: TaskContext): Iterator[(T, U)]
    功能: 计算指定分区的数据内容(是一个二元组)
    1. 获取当前分片
    val currSplit = split.asInstanceOf[CartesianPartition]
    2. 获取当前分区的内容
    val= for (x <- rdd1.iterator(currSplit.s1, context);
         y <- rdd2.iterator(currSplit.s2, context)) yield (x, y)
}
```

#### CheckpointRDD

```scala
class CheckpointRDDPartition(val index: Int) extends Partition
介绍: 检查点RDD分区,用于恢复检查点数据
```

```scala
private[spark] abstract class CheckpointRDD[T: ClassTag](sc: SparkContext) extends RDD[T](sc, Nil) {
    介绍: 用于从存储中恢复检查点RDD
    操作集:
    def doCheckpoint(): Unit = { }
    def checkpoint(): Unit = { }
    def localCheckpoint(): this.type = this
    注: 检查点RDD不需要再次进行检查处理了
    
    def compute(p: Partition, tc: TaskContext): Iterator[T] = ???
    功能: 计算指定分区的数据内容
    
    def getPartitions: Array[Partition] = ???
    功能: 获取分区列表
}
```

#### coalescer-public

```scala
@DeveloperApi
trait PartitionCoalescer {
    介绍: 分区聚合器,定义了如何聚合指定的RDD
    操作集:
    def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup]
    功能: 聚合指定RDD的分区
    输入参数:
        maxPartitions	聚合后最大分区数量
        parent	分区聚合的父级RDD
    返回参数:
    	分区组@PartitionGroup 列表
}
```

```scala
@DeveloperApi
class PartitionGroup(val prefLoc: Option[String] = None) {
    构造器参数:
    prefLoc	分区组的优先位置
    属性:
    #name @partitions = mutable.ArrayBuffer[Partition]()	分区列表
    操作集:
    def numPartitions: Int = partitions.size
    功能: 获取分区数量
}
```

#### CoalescedRDD

#### CoGroupedRDD

```scala
private[spark] case class NarrowCoGroupSplitDep(
    @transient rdd: RDD[_],
    @transient splitIndex: Int,
    var split: Partition
) extends Serializable {
    介绍: 组合分片窄依赖,对于RDD和分片索引的引用是暂时的,因为冗余的信息存储在@CoGroupedRDD 实例中.由于@CoGroupedRDD和@CoGroupPartition 是分开序列化的,如果RDD和分片索引不是@transient 那么在任务关闭的时候就会添加2次.
    构造器参数:
        rdd	RDD(transient 使得序列化之后不需要再次创建这个RDD)
        splitIndex	分片索引(transient)
        split	分区
    操作集:
    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit
    功能: 写出非transient 和非静态状态下的对象
    Utils.tryOrIOException {
        // 任务序列化时需要更新父分区的应用
        split = rdd.partitions(splitIndex)
        oos.defaultWriteObject()
    }
}
```

```scala
private[spark] class CoGroupPartition(
    override val index: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
extends Partition with Serializable {
    介绍: @CoGroupedRdd 窄依赖的存储信息
    构造器参数:
        index	分区编号
        narrowDeps	窄依赖
    操作集:
    def hashCode(): Int = index
    功能: hash函数
    
    def equals(other: Any): Boolean = super.equals(other)
    功能: 判断相等的逻辑
}
```

```scala
@DeveloperApi
class CoGroupedRDD[K: ClassTag](
    @transient var rdds: Seq[RDD[_ <: Product2[K, _]]],
    part: Partitioner)
extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {
    介绍: 这个RDD描述了与父级RDD的关系,对于父RDD的每个key来说,结果RDD包含携带有该keyvalue值的元组.
    输入参数:
        rdds	父RDD列表
        part	用于分区shuffle输出的分区器
    注意: 这是一个外部API,建议使用RDD.cogroup 而不是直接将其进行实例化
    (k, a) cogroup (k, b) 的结果为 k -> Array(ArrayBuffer as, ArrayBuffer bs)
    属性:
    #name @serializer: Serializer = SparkEnv.get.serializer	序列化器
    #name @partitioner: Some[Partitioner] = Some(part)	分区器
    type CoGroup = CompactBuffer[Any] // 每个buffer是一个CoGroup 使用紧凑缓冲区在小数据量下快速聚合
    type CoGroupValue = (Any, Int) // Int表示依赖编号
    type CoGroupCombiner = Array[CoGroup] // CoGroup组合器
    操作集:
    def setSerializer(serializer: Serializer): CoGroupedRDD[K]
    功能: 设置RDD shuffle的序列化类型,null时采用默认设置@spark.serializer
    this.serializer = serializer
    val= this
    
    def getDependencies: Seq[Dependency[_]] 
    功能: 获取依赖列表
    val= rdds.map { rdd: RDD[_] =>
      // 检查当前分区是否存在,存在则说明是一个窄依赖,否则为一个宽依赖
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, CoGroupCombiner](
          rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
      }
    }
    
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    1. 创建结果列表维度
    val array = new Array[Partition](part.numPartitions)
    2. 设置分区列表每个位置的值
    for (i <- 0 until array.length) {
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    
    def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])]
    功能: 计算指定分区的数据
    1. 获取当前分区的@CoGroupPartition 实例
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.length // 获取RDD数量(与父RDD相连列表长度)\
    2. 获取RDD迭代器列表
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    // 设置迭代器内容(包括RDD迭代器,依赖信息)
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
        // 窄依赖处理
      case oneToOneDependency: OneToOneDependency[Product2[K, Any]] @unchecked =>
        val dependencyPartition = split.narrowDeps(depNum).get.split
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context)
        rddIterators += ((it, depNum))
		// 非窄依赖处理
      case shuffleDependency: ShuffleDependency[_, _, _] =>
        val metrics = context.taskMetrics().createTempShuffleReadMetrics()
        val it = SparkEnv.get.shuffleManager
          .getReader(
            shuffleDependency.shuffleHandle, split.index, split.index + 1, context, metrics)
          .read()
        rddIterators += ((it, depNum))
    }
    3. 创建外部映射,并将迭代器信息添加到映射表中
    val map = createExternalMap(numRdds)
    for ((it, depNum) <- rddIterators) {
      map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum))))
    }
    4. 更新度量信息
    context.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    val= new InterruptibleIterator(context,
      map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
    
    def clearDependencies(): Unit
    功能: 清除依赖
    super.clearDependencies()
    rdds = null
    
    def createExternalMap(numRdds: Int)
    : ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner]
    功能: 创建外部只添加类型的映射表
    1. 获取创建合并器函数
    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      newCombiner(value._2) += value._1
      newCombiner
    }
    2. 获取合并函数
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
      combiner(value._2) += value._1
      combiner
    }
    3. 获取归并合并器函数
    val mergeCombiners: (CoGroupCombiner, CoGroupCombiner) => CoGroupCombiner =
      (combiner1, combiner2) => {
        var depNum = 0
        while (depNum < numRdds) {
          combiner1(depNum) ++= combiner2(depNum)
          depNum += 1
        }
        combiner1
      }
    val= new ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner](
      createCombiner, mergeValue, mergeCombiners)
}
```

#### DoubleRDDFunctions

```scala
class DoubleRDDFunctions(self: RDD[Double]) extends Logging with Serializable {
    介绍: Double类型RDD的外部函数
    构造器参数:
    	self	double类型RDD
    操作集:
    def sum(): Double
    功能: 获取RDD的聚合结果
    val= self.withScope { self.fold(0.0)(_ + _) }
    
    def stats(): StatCounter
    功能: 获取RDD的状态计数器(包括平均值,方差,计数值)
    val= self.withScope { 
        self.mapPartitions(nums => Iterator(StatCounter(nums))).reduce((a, b) => a.merge(b)) }
    
    def mean(): Double
    功能: 获取RDD元素的平均值
    val= self.withScope { stats().mean }
    
    def variance(): Double = self.withScope { stats().variance }
    功能: 获取RDD元素的方差
    
    def stdev(): Double = self.withScope { stats().stdev }
    功能: 获取RDD元素的标准差
    
    def sampleStdev(): Double = self.withScope { stats().sampleStdev }
    功能: 获取采样标准差
    
    def sampleVariance(): Double = self.withScope { stats().sampleVariance }
    功能: 获取采样方差
    
    @Since("2.1.0")
    def popStdev(): Double = self.withScope { stats().popStdev }
    功能: 计算群体标准偏差
    
    @Since("2.1.0")
    def popVariance(): Double = self.withScope { stats().popVariance }
    功能: 计算群体方差
    
    def meanApprox(timeout: Long,confidence: Double = 0.95): PartialResult[BoundedDouble] 
    功能: 在指定时限@timeout 内计算平均值估计值,置信值为@confidence 
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new MeanEvaluator(self.partitions.length, confidence)
    val= self.context.runApproximateJob(self, processPartition, evaluator, timeout)
    
    def sumApprox(timeout: Long,confidence: Double = 0.95): PartialResult[BoundedDouble]
    功能: 在指定时限内,计算和的估算值,置信值为@confidence
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new SumEvaluator(self.partitions.length, confidence)
    val= self.context.runApproximateJob(self, processPartition, evaluator, timeout)
    
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double]
    功能: 根据最大最小值,以及步长计算分布序列
    val span = max - min
    Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
    
    def histogram(buckets: Array[Double],evenBuckets: Boolean = false): Array[Long]
    功能:
    根据给定的桶数据信息，计算其分布对于采样序列<1,10,20,50>
    可以分成<1,10>>,<10,20>>,<20,50>几个区间.在输入端点,分布情况为 1,0,1.
    注意: 如果你的分布是均衡的,比如说<0,10,20,30>,你可以使用O(logn)时间复杂度去插入一个元素.如果你设置@evenBuckets=true,桶必须至少包含两个元素,必须排序且不包含重复元素.所有NaN做同一处理.
    0. 元素数量校验
    if (buckets.length < 2) {
      throw new IllegalArgumentException("buckets array must have at least two elements")
    }
    1. 获取分桶函数
    val bucketFunction = if (evenBuckets) {
      fastBucketFunction(buckets.head, buckets.last, buckets.length - 1) _
    } else {
      basicBucketFunction _
    }
    2. 生成分布序列
    if (self.partitions.length == 0) {
      new Array[Long](buckets.length - 1)
    } else {
      // 需要处理分区分布的合并
      self.mapPartitions(histogramPartition(bucketFunction)).reduce(mergeCounters)
    }
    
    def mergeCounters(a1: Array[Long], a2: Array[Long]): Array[Long] 
    功能: 合并两个序列
    a1.indices.foreach(i => a1(i) += a2(i))
    val= a1
    
    def fastBucketFunction(min: Double, max: Double, count: Int)(e: Double): Option[Int]
    功能: 快速分桶函数(在常数的时间复杂度时间内分桶),返回桶的数量
    if (e.isNaN || e < min || e > max) {
        None
      } else {
        val bucketNumber = (((e - min) / (max - min)) * count).toInt
        Some(math.min(bucketNumber, count - 1))
      }
    
    def basicBucketFunction(e: Double): Option[Int]
    功能: 基本分桶函数,二分查找,时间复杂度(O(nlog n))
    val location = java.util.Arrays.binarySearch(buckets, e)
    if (location < 0) {
        val insertionPoint = -location-1
        if (insertionPoint > 0 && insertionPoint < buckets.length) {
          Some(insertionPoint-1)
        } else {
          None
        }
    } else if (location < buckets.length - 1) {
        Some(location)
      } else {
        Some(location - 1)
      }
    
    def histogramPartition(bucketFunction: (Double) => Option[Int])(iter: Iterator[Double]):
        Iterator[Array[Long]]
    功能: 获取分布分区,分布函数计算给定分区的部分分布.提供的分区函数@bucketFunction 当没有桶的时候返回None可以指定特定的桶,并将其保存,使用的时间复杂度为O(n log n)
    val counters = new Array[Long](buckets.length - 1)
      while (iter.hasNext) {
        bucketFunction(iter.next()) match {
          case Some(x: Int) => counters(x) += 1
          case _ => // No-Op
        }
      }
    val= Iterator(counters)
    
        
    def histogram(bucketCount: Int): (Array[Double], Array[Long]) 
    功能: 计算数据分布情况,比如说最小值为0最大值为100,那么结果的两个桶分别是[0,50),和[50,100].桶计数值@bucketCount至少要是1.如果RDD中包含无穷(NaN),就会抛出异常.当最大最小值相等,则返回一个桶.
    1. 计算最大最小值
    val (max: Double, min: Double) = self.mapPartitions { items =>
      Iterator(
        items.foldRight((Double.NegativeInfinity, Double.PositiveInfinity)
        )((e: Double, x: (Double, Double)) => (x._1.max(e), x._2.min(e))))
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }
    2. 最大最小值合法性检查
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity ) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
    }
    3. 获取分布序列
    val range = if (min != max) {
      customRange(min, max, bucketCount)
    } else {
      List(min, min)
    }                           
    4. 获取分布结果
    val buckets = range.toArray
    val= (buckets, histogram(buckets, true))
}
```

#### EmptyRDD

```scala
private[spark] class EmptyRDD[T: ClassTag](sc: SparkContext) extends RDD[T](sc, Nil) {
    介绍: 这是一个没有分区和元素的RDD
    操作集:
    def getPartitions: Array[Partition] = Array.empty 
    功能: 获取分区列表
    
    def compute(split: Partition, context: TaskContext): Iterator[T] =
    	throw new UnsupportedOperationException("empty RDD")
    功能: 不支持计算分区数据
}
```

#### HadoopRDD

#### InputFileBlockHolder

```scala
private[spark] object InputFileBlockHolder {
    介绍: 输入数据块持有者,拥有当前spark任务的文件名称列表,使用在@HadoopRDD,@FileScanRDD，@NewHadoopRDD，以及SparkSQL的@InputFileName函数中。
    属性：
    #name @inputBlock #type @InheritableThreadLocal[AtomicReference[FileBlock]] 输入数据块
    val= new InheritableThreadLocal[AtomicReference[FileBlock]] {
      override protected def initialValue(): AtomicReference[FileBlock] =
        new AtomicReference(new FileBlock)
    }
    当前读取文件名称的线程变量,用于SparkSQL的@InputFileName 函数
    操作集:
    def getInputFilePath: UTF8String = inputBlock.get().get().filePath
    功能: 获取输入文件路径
    
    def getStartOffset: Long = inputBlock.get().get().startOffset
    功能: 获取输入文件初始偏移量
    
    def getLength: Long = inputBlock.get().get().length
    功能: 获取输入文件的长度
    
    def set(filePath: String, startOffset: Long, length: Long): Unit
    功能: 设置本地线程输入数据块
    require(filePath != null, "filePath cannot be null")
    require(startOffset >= 0, s"startOffset ($startOffset) cannot be negative")
    require(length >= -1, s"length ($length) cannot be smaller than -1")
    inputBlock.get().set(new FileBlock(UTF8String.fromString(filePath), startOffset, length))
    
    def unset(): Unit = inputBlock.remove()
    功能: 清除输入文件块
    
    def initialize(): Unit = inputBlock.get()
    功能: 通过明确的获取值来初始化本地线程
    
    内部类:
    private class FileBlock(val filePath: UTF8String, val startOffset: Long, val length: Long) {
        介绍: 文件数据块,输入文件信息的包装
        构造器参数:
            filePath	文件路径
            startOffset	起始偏移量
            length	文件长度
        def this() {
          this(UTF8String.fromString(""), -1, -1)
        }
      }
}
```

#### JdbcRDD

```scala
private[spark] class JdbcPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  	介绍: JDBC分区
    构造器参数:
    	id	分区编号
    	lower	下限值
    	upper	上限值
    override def index: Int = idx
    功能: 获取分区编号
}
```

```markdown
介绍:
	JDBC RDD,在JDBC连接上执行sql查询并读取结果.使用案例请参考@JdbcRDDSuite
	构造器参数:
	getConnection	获取连接函数,RDD需要注意对链接的拆除
	sql	sql执行内容
    注意: 查询必须包含两个占位符例如:
    ```sql
    	select title, author from books where ? <= id and id <= ?
    ```
	lowerBound	第一个占位符的下限值
	upperBound	第二个占位符的上限值
	numPartitions	分区数量
	mapRow	将单个执行结果@ResultSet 映射成单行需求结果类型(类型转换函数)
```

```scala
class JdbcRDD[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
extends RDD[T](sc, Nil) with Logging {
    操作集:
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    val length = BigInt(1) + upperBound - lowerBound
    (0 until numPartitions).map { i =>
      val start = lowerBound + ((i * length) / numPartitions)
      val end = lowerBound + (((i + 1) * length) / numPartitions) - 1
      new JdbcPartition(i, start.toLong, end.toLong)
    }.toArray
    
    def close(): Unit
    功能: 关闭JDBC连接
    try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }

	def getNext(): T 
	功能: 获取记录值,并移动迭代器指针
	if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
    }
    
    def compute(thePart: Partition, context: TaskContext): Iterator[T]
    功能: 计算指定分区@thePart 
    1. 添加时间完成的监听事件
    context.addTaskCompletionListener[Unit]{ context => closeIfNeeded() }
    2. 获取分区内容
    val part = thePart.asInstanceOf[JdbcPartition]
    3. 获取连接和执行描述
    val conn = getConnection()
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    4. 设定获取记录数量
    val url = conn.getMetaData.getURL
    if (url.startsWith("jdbc:mysql:")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
    } else {
      stmt.setFetchSize(100)
    }
    5. 设置占位符信息,并执行查询
    stmt.setLong(1, part.lower)
    stmt.setLong(2, part.upper)
    val rs = stmt.executeQuery()
}
```

```scala
object JdbcRDD {
    操作集:
    def resultSetToObjectArray(rs: ResultSet): Array[Object]
    功能: 将结果集转化为对象数组
    val= Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
    
    def create[T](
      sc: JavaSparkContext,
      connectionFactory: ConnectionFactory,
      sql: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      mapRow: JFunction[ResultSet, T]): JavaRDD[T] 
    功能: 创建JavaRDD,这个RDD可以执行SQL,读取执行结果
    输入参数:
    	sc	spark上下文
    	connectionFactory	数据库连接池
    	sql	sql指令
    	lowerBound	首个占位符下界
    	upperBound	第二个占位符上界
    	numPartitions	分区数量
    	mapRow	结果映射函数
    val jdbcRDD = new JdbcRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      lowerBound,
      upperBound,
      numPartitions,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)
    val= new JavaRDD[T](jdbcRDD)(fakeClassTag)
    
    def create(
      sc: JavaSparkContext,
      connectionFactory: ConnectionFactory,
      sql: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int): JavaRDD[Array[Object]]
    功能: 创建JavaRDD,用于执行sql并返回结果,但是不能自定义映射函数
    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }
    val= create(sc, connectionFactory, sql, lowerBound, upperBound, numPartitions, mapRow)
}
```

#### LocalCheckpointRDD

```markdown
介绍:
 	这是一个伪检查点RDD,用于在失败时候提供有效的错误信息
 	这个就是一个简单的占位符,因为原始检查点RDD需要完全缓存.只有当执行器失败或者用户解除持久化原始RDD.spark才会重新计算这个@CheckpointRDD.当这个情况发生时,必须要记录错误信息.
```

```scala
private[spark] class LocalCheckpointRDD[T: ClassTag](
    sc: SparkContext,
    rddId: Int,
    numPartitions: Int)
extends CheckpointRDD[T](sc) {
    构造器属性:
        sc	spark上下文
        rddId	rdd编号
        numPartitions 分区数量
    构造器:
    def this(rdd: RDD[T]) {
        this(rdd.context, rdd.id, rdd.partitions.length)
      }
    功能: 构造指定RDD的伪检查点RDD
    
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    val= (0 until numPartitions).toArray.map { i => new CheckpointRDDPartition(i) }
    
    def compute(partition: Partition, context: TaskContext): Iterator[T]
    功能: 计算指定分区的数据内容,抛出异常表名相关数据块找不到.只有原始RDD解除持久化或者执行器丢失才会执行
    val= throw new SparkException(
      s"Checkpoint block ${RDDBlockId(rddId, partition.index)} not found! Either the executor " +
      s"that originally checkpointed this partition is no longer alive, or the original RDD is " +
      s"unpersisted. If this problem persists, you may consider using `rdd.checkpoint()` " +
      s"instead, which is slower than local checkpointing but more fault-tolerant.")
}
```

#### LocalRDDCheckpointData

```markdown
介绍:
	spark缓存层上实现的检查点技术.本地检查点通过跳过存储RDD数据到可信且运行出现错误的这个过程,实现了容错性.(使用不缓存提高容错性).相反,数据写到了本地,短暂生存在各个执行器中的块管理器.当构建了一个长的RDD任务图(且需要经常删除)是相当有效的.(例如 graphX).
```

```scala
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
extends RDDCheckpointData[T](rdd) with Logging {
    介绍: 本地RDD检查点数据
    操作集:
    def doCheckpoint(): CheckpointRDD[T] 
    功能: 进行检查操作,确保RDD完全缓存,之后分区可以被恢复
    0. 断言可以使用磁盘存取
    assume(level.useDisk, s"Storage level $level is not appropriate for local checkpointing")
    1. 计算丢失分区
    val action = (tc: TaskContext, iterator: Iterator[T]) => Utils.getIteratorSize(iterator)
    val missingPartitionIndices = rdd.partitions.map(_.index).filter { i =>
      !SparkEnv.get.blockManager.master.contains(RDDBlockId(rdd.id, i))
    }
    2. 执行任务,恢复丢失分区
    if (missingPartitionIndices.nonEmpty) {
      rdd.sparkContext.runJob(rdd, action, missingPartitionIndices)
    }
    val= new LocalCheckpointRDD[T](rdd)
}
```

```scala
private[spark] object LocalRDDCheckpointData {
    属性:
    #name @DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK	默认存储等级
    操作集:
    def transformStorageLevel(level: StorageLevel): StorageLevel
    功能: 转化为使用磁盘的某种存储等级
    val= StorageLevel(useDisk = true, level.useMemory, level.deserialized, level.replication)
}
```

#### MapPartitionsRDD

```scala
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
extends RDD[U](prev) {
    介绍: 在父RDD的每个分区上使用函数,进而生成新的RDD
    构造器参数:
    	prev	父RDD
    	r	输出映射函数  (TaskContext, Int, Iterator[T]) => Iterator[U]
    	preservesPartitioning	输入函数是否位置分区器(除了prev是键值对RDD,否则都是false,且这种状态下不会修改key)
    	isFromBarrier	是否RDD由@RDDBarrier 转换而来
    	isOrderSensitive	是否对排序敏感
    属性:
    #name @partitioner = if (preservesPartitioning) firstParent[T].partitioner else None	
    	分区器(只有在保持分区的情况下才存在,否则为空)
    #name @isBarrier_ #type @Boolean transient lazy	是否存在边界
    	val= isFromBarrier || dependencies.exists(_.rdd.isBarrier())
    操作集:
    def getPartitions: Array[Partition] = firstParent[T].partitions
    功能: 获取分区列表
    
    def compute(split: Partition, context: TaskContext): Iterator[U]
    功能: 计算分区内容
    
    def clearDependencies(): Unit
    功能: 清除依赖
    super.clearDependencies()
    prev = null
    
    def getOutputDeterministicLevel: DeterministicLevel 
    功能: 获取输出确定等级
    val= if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
}
```

#### NewHadoopRDD

#### OrderedRDDFunctions

```markdown
介绍:
	排序RDD函数,RDD kv对上额外的可用函数,key是可以排序的.对于所有基本类型都是可以排序的,用户可以定义需要的排序方式,或者覆盖默认排序.
```

```scala
class OrderedRDDFunctions[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag] @DeveloperApi() (
    self: RDD[P])
extends Logging with Serializable {
    构造器参数:
    self	RDD
    #name @ordering = implicitly[Ordering[K]]	排序类型
    操作集:
    def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)] = self.withScope
    功能: 按照key进行排序,每个分区都包含key的一部分.在结果RDD上调用@collect 和@save,会返回排序好的记录(在save操作中,会写出到多个part-X文件中,且按照顺序排序)
    1. 获取返回分区器
    val part = new RangePartitioner(numPartitions, self, ascending)
    2. 返回排序完成的RDD
    val= new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
    
    def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] 
    功能: 重组分区,且分区内部局部排序.这个操作比先@repartition 重组,再排序要快.因为分开操作会使得中间结果落到shuffle机构中.
    val= new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
    
    def filterByRange(lower: K, upper: K): RDD[P]
    功能: 返回元素返回在指定返回内的RDD,如果RDD使用@RangePartitioner 排序,这个操作可以更高效,仅仅需要扫描相关分区.否则,标准过滤器会应用到所有分区上
    1. 获取需要过滤的RDD
    val rddToFilter: RDD[P] = self.partitioner match {
      case Some(rp: RangePartitioner[K, V]) =>
        val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndicies.contains)
      case _ =>
        self
    }
    2. 过滤范围内元素
    rddToFilter.filter { case (k, v) => inRange(k) }
    
    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)
    功能: 确认指定值k 是否在范围内
}
```

#### PairRDDFunctions

#### ParallelCollectionRDD

```scala
private[spark] class ParallelCollectionPartition[T: ClassTag](
    var rddId: Long,
    var slice: Int,
    var values: Seq[T]
) extends Partition with Serializable{
    介绍: 并行集合分区
    构造器参数:
        rddId	RDD编号
        slice	切片大小
        values	数据序列
    操作集:
    def iterator: Iterator[T] = values.iterator
    功能: 获取数据的迭代器
    
    def hashCode(): Int = (41 * (41 + rddId) + slice).toInt
    功能: 获取当前对象的hash值
    
    def equals(other: Any): Boolean
    功能: 判断两个对象是否相等
    
    def index: Int = slice
    功能: 获取当前分片索引
    
    @throws(classOf[IOException])
    def writeObject(out: ObjectOutputStream): Unit
    功能: 写出指定对象@out
    1. 获取序列化器
    val sfactory = SparkEnv.get.serializer
    2. 写出当前RDD信息
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject() // 默认写出
      case _ =>
        out.writeLong(rddId)
        out.writeInt(slice)
        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser)(_.writeObject(values))
    }
    
    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit
    功能: 读取指定对象@in
    1. 获取序列化器
    val sfactory = SparkEnv.get.serializer
    2. 读取当前RDD配置
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        rddId = in.readLong()
        slice = in.readInt()
        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser)(ds => values = ds.readObject[Seq[T]]())
    }
}
```

```scala
private[spark] class ParallelCollectionRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]])
extends RDD[T](sc, Nil) {
    介绍: 并行集合RDD
    构造器参数:
    	sc	spark应用上下文
    	data	数据序列
    	numSlices	分片数量
    	locationPrefs	位置映射表
    操作集:
    def getPartitions: Array[Partition]
    功能: 获取分区列表信息
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    val= slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
    
    def compute(s: Partition, context: TaskContext): Iterator[T]
    功能: 计算当前分区的数据
    val= new InterruptibleIterator(context, s.asInstanceOf[ParallelCollectionPartition[T]].iterator)
    
    def getPreferredLocations(s: Partition): Seq[String]=locationPrefs.getOrElse(s.index, Nil)
    功能: 获取分区映射表信息
}
```

```scala
private object ParallelCollectionRDD {
    操作集:
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)]
    功能: 计算分片开始和截止位置信息
    val= (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    
    def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] 
    功能: 将一个集合分割成多个子集合，另一个特例就是范围集合。对分片进行加密可以降低内存开销。使得RDD代表大量数据更加的高效。如果集合是一个包含的返回，则使用包含的范围作为最后一个分片。
    0. 分片数量校验
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
    1. 获取分片完成的数据表
    val= seq match {
      case r: Range => // 普通范围类
        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          }
          else {
            new Range(r.start + start * r.step, r.start + end * r.step, r.step)
          }
        }.toSeq.asInstanceOf[Seq[Seq[T]]]
      case nr: NumericRange[_] =>
        // 数字范围(Long Int Double Float...)
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices
      case _ => // 其他类别
        val array = seq.toArray // 这里为了下面操作避免O(n^2)的展开
        positions(array.length, numSlices).map { case (start, end) =>
            array.slice(start, end).toSeq
        }.toSeq
    }
}
```

#### PartitionerAwareUnionRDD

```scala
private[spark]
class PartitionerAwareUnionRDDPartition(
    @transient val rdds: Seq[RDD[_]],
    override val index: Int
) extends Partition {
	介绍: 代表@PartitionerAwareUnionRDD 分区,维护对于父RDD的分区列表
    构造器参数:
        rdds	RDD列表
        index	索引
    属性:
    #name @parents = rdds.map(_.partitions(index)).toArray	属性RDD
    操作集:
    def hashCode(): Int = index
    功能: 计算分区RDD
    
    def equals(other: Any): Boolean = super.equals(other)
    功能: 判断相等逻辑
    
    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit
    功能: 写出默认属性
    parents = rdds.map(_.partitions(index)).toArray // 先更新父RDD列表
    oos.defaultWriteObject()
}
```

```scala
private[spark] class PartitionerAwareUnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]]
) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
    介绍: 这个RDD可以容纳有同一个分区器下的多个RDD,且将其连接成为一个RDD.而且会保持分区器.所以m个RDD和p个分区会被合并到一个RDD,同时含有p个分区和RDD.联合RDD每个分区的偏向地址是父RDD的偏向地址.
    构造器属性:
        sc	spark上下文
        rdds rdd列表
    属性:
    #name @partitioner = rdds.head.partitioner	分区器
    参数断言:
    require(rdds.nonEmpty)
    require(rdds.forall(_.partitioner.isDefined))
    require(rdds.flatMap(_.partitioner).toSet.size == 1,
            "Parent RDDs have different partitioners: " + rdds.flatMap(_.partitioner))
    操作集:
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    val numPartitions = partitioner.get.numPartitions
    (0 until numPartitions).map { index =>
      new PartitionerAwareUnionRDDPartition(rdds, index) // 同一个分区器中的RDD会合并
    }.toArray
    
    def compute(s: Partition, context: TaskContext): Iterator[T]
    功能: 计算分区数据
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
    
    def clearDependencies(): Unit
    功能: 清除依赖
    super.clearDependencies()
    rdds = null
    
    def currPrefLocs(rdd: RDD[_], part: Partition): Seq[String]
    功能: 获取当前偏向地址
    
    def getPreferredLocations(s: Partition): Seq[String]
    功能: 获取指定分区的偏向地址(大多是父RDD地址列表)
    1. 获取父RDD
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents
    2. 获取父RDD位置
    val locations = rdds.zip(parentPartitions).flatMap {
      case (rdd, part) =>
        val parentLocations = currPrefLocs(rdd, part)
        parentLocations
    }
    3. 获取当前分区偏向地址
    val location = if (locations.isEmpty) {
      None
    } else {
        // 计算出现最多的地址
      Some(locations.groupBy(x => x).maxBy(_._2.length)._1)
    }
    val= location.toSeq
}
```

#### PartitionPruningRDD

```scala
private[spark] class PartitionPruningRDDPartition(idx: Int, val parentSplit: Partition)
extends Partition {
  介绍: 分区修剪RDD分区
  构造器参数:
    idx	分区编号
    parentSplit	父分区
  override val index = idx
}
```

```scala
rivate[spark] class PruneDependency[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean)
extends NarrowDependency[T](rdd) {
    介绍: 分区修剪依赖,表示修剪后的RDD与父RDD之间的关系.,子RDD包含父RDD的分区子集
    构造器参数:
    	rdd	RDD
    	partitionFilterFunc	分区过滤函数
    属性:
    #name @partitions #type @Array[Partition]	分区列表
    val= rdd.partitions
    .filter(s => partitionFilterFunc(s.index)).zipWithIndex
    .map { case(split, idx) => new PartitionPruningRDDPartition(idx, split) : Partition }
    
    操作集:
    def getParents(partitionId: Int): List[Int]
    功能: 获取指定分区的父分区编号列表
    val= List(partitions(partitionId).asInstanceOf[PartitionPruningRDDPartition].parentSplit.index)
}
```

```scala
@DeveloperApi
class PartitionPruningRDD[T: ClassTag](
    prev: RDD[T],
    partitionFilterFunc: Int => Boolean)
extends RDD[T](prev.context, List(new PruneDependency(prev, partitionFilterFunc))) {
    介绍: 分区分割RDD
    构造器属性:
    	prev	分割前RDD
    	partitionFilterFunc	分区过滤函数
    操作集:
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    val= dependencies.head.asInstanceOf[PruneDependency[T]].partitions
    
    def compute(split: Partition, context: TaskContext): Iterator[T]
    功能: 计算分区数据
    val= firstParent[T].iterator(
      split.asInstanceOf[PartitionPruningRDDPartition].parentSplit, context)
}
```

```scala
@DeveloperApi
object PartitionPruningRDD {
    操作集:
    def create[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean): PartitionPruningRDD[T]
    功能: 创建分区修剪RDD
    val= new PartitionPruningRDD[T](rdd, partitionFilterFunc)(rdd.elementClassTag)
}
```

#### PartitionwiseSampledRDD

```markdown
介绍:
	分区智能采样RDD,对于父RDD的每个分区,用户指定随机采样器@RandomSampler 去获取一个分区的随机采样值.给定随机种子,保证可以获取不同的值.
	构造器参数:
		prev	采样RDD
		sampler	随机采样器
		preservesPartitioning	是否保持父RDD的分区
		seed	随机种子
		T 	输入RDD类型
		U	采样RDD类型
```

```scala
private[spark] class PartitionwiseSampledRDD[T: ClassTag, U: ClassTag](
    prev: RDD[T],
    sampler: RandomSampler[T, U],
    preservesPartitioning: Boolean,
    @transient private val seed: Long = Utils.random.nextLong)
extends RDD[U](prev) {
    属性:
    #name @partitioner = if (preservesPartitioning) prev.partitioner else None	分区器
    操作集:
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    val random = new Random(seed)
    firstParent[T].partitions.map(x => new PartitionwiseSampledRDDPartition(x, random.nextLong()))
    
    def getPreferredLocations(split: Partition): Seq[String] 
    功能: 获取偏向的位置
    val= firstParent[T].preferredLocations(split.asInstanceOf[PartitionwiseSampledRDDPartition].prev)
    
    def compute(splitIn: Partition, context: TaskContext): Iterator[U]
    功能: 计算分区数据
    val split = splitIn.asInstanceOf[PartitionwiseSampledRDDPartition]
    val thisSampler = sampler.clone
    thisSampler.setSeed(split.seed)
    val= thisSampler.sample(firstParent[T].iterator(split.prev, context))
    
    def getOutputDeterministicLevel
    功能: 获取输出等级
    val= if (prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
}
```

#### PipedRDD

#### RDD

#### RDDBarrier

```scala
@Experimental
@Since("2.4.0")
class RDDBarrier[T: ClassTag] private[spark] (rdd: RDD[T]) {
    介绍: 在一个界限stage上包装一个RDD.使得spark运行这个stage的任务.
    @Experimental
    @Since("2.4.0")
    def mapPartitions[S: ClassTag](
        f: Iterator[T] => Iterator[S],
        preservesPartitioning: Boolean = false): RDD[S]
    功能: 获取分区列表
    val cleanedF = rdd.sparkContext.clean(f)
    val= new MapPartitionsRDD(
      rdd,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning,
      isFromBarrier = true
    )
    
    @Experimental
    @Since("3.0.0")
    def mapPartitionsWithIndex[S: ClassTag](
        f: (Int, Iterator[T]) => Iterator[S],
        preservesPartitioning: Boolean = false): RDD[S]
    功能: 返回一个RDD,在RDD所有分区上使用了函数@f ,且对原始RDD的分区进行追踪,所有任务都在这个界限stage上运行.
    val cleanedF = rdd.sparkContext.clean(f)
    val= new MapPartitionsRDD(
      rdd,
      (_: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning,
      isFromBarrier = true
    )
}
```

#### RDDCheckpointData

```scala
private[spark] object CheckpointState extends Enumeration {
    介绍: 检查点机制中,管理RDD状态管理
  type CheckpointState = Value
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}
```

```scala
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
extends Serializable {
    介绍: 这个类中包含所有RDD检查点的信息,每个实例都会连接到一个RDD上.管理相关RDD的检查点过程,通过提供的状态来管理检查点的状态.
    属性:
    #name @cpState = Initialized	检查点状态
    #name @cpRDD: Option[CheckpointRDD[T]] = None	对应的检查点RDD
    操作集:
    def isCheckpointed: Boolean = RDDCheckpointData.synchronized { cpState == Checkpointed }
    功能: 确认当前RDD是否被检查
    
    def doCheckpoint(): CheckpointRDD[T]
    功能: 实体化RDD,并持久化,子类重写
    
    def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }
    功能: 返回包含检查点数据的RDD
    
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    val=  = RDDCheckpointData.synchronized { cpRDD.map(_.partitions).getOrElse { Array.empty } }
    
    final def checkpoint(): Unit
    功能: 实体化RDD并对内容进行持久化
    1. 多线程下保护同一个RDD原子修改检查点状态位@cpState
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }
    2. 实体化RDD,并更新状态
    val newRDD = doCheckpoint()
    RDDCheckpointData.synchronized {
      cpRDD = Some(newRDD)
      cpState = Checkpointed
      rdd.markCheckpointed()
    }
}
```

```scala
private[spark] object RDDCheckpointData
介绍: 用于全局同步检查点操作的锁
```

#### RDDOperationScope

#### ReliableCheckpointRDD

#### ReliableRDDCheckpointData

#### SequenceFileRDDFunctions

#### ShuffledRDD

#### SubtractedRDD

#### UnionRDD

#### WholeTextFileRDD

#### ZippedPartitionsRDD

#### ZippedWithIndexRDD

#### 基础拓展