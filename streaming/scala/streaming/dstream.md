1. [ConstantInputDStream.scala](# ConstantInputDStream)
2. [DStream.scala](# DStream)
3. [DStreamCheckpointData.scala](DStreamCheckpointData)
4. [FileInputDStream.scala](# FileInputDStream)
5. [FilteredDStream.scala](# FilteredDStream)
6. [FlatMappedDStream.scala](# FlatMappedDStream)
7. [FlatMapValuedDStream.scala](# FlatMapValuedDStream)
8. [ForEachRDD.scala](# ForEachRDD)
9. [GlommedDStream.scala](# GlommedDStream)
10. [InputDStream.scala](# InputDStream)
11. [MapPartitionedDStream.scala](# MapPartitionedDStream)
12. [MappedDStream.scala](# MappedDStream)

---

#### ConstantInputDStream

```scala
class ConstantInputDStream[T: ClassTag](_ssc: StreamingContext, rdd: RDD[T])
extends InputDStream[T](_ssc) {
    介绍: 输入流,每次执行总是返回同一个RDD,用于测试
    构造器参数:
    	_ssc	streaming上下文
    	rdd	RDD
    初始化操作:
    require(rdd != null,
            "parameter rdd null is illegal, which will lead to NPE in 
            the following transformation")
    功能: RDD校验
    操作集:
    def start(): Unit = {}
    def stop(): Unit = {}
    功能: 常数输入流的起始/停止
    
    def compute(validTime: Time): Option[RDD[T]]
    功能: 计算指定时间的RDD(为常数)
    val= Some(rdd)
}
```

#### DStream

```markdown
介绍:
 	离散流,spark streaming的基本抽象,是连续RDD序列(同一个类型的RDD),代表着连续流类型的数据(参考spark core中对RDD的描述).离散流既可以使用@StreamingContext从存活的RDD中创建(例如,TCP socket,Kafka).或者可以从已经存在的离散流中创建,使用`map`,`window`或者`reduceByKeyAndWindow`创建.当spark streaming程序创建的时候,每个离散流周期性的产生RDD,或者从父离散流中转换RDD.
    这个类中包含离散流的基本操作,比如`map`,`filter`,和`windows`.此外@PairDStreamFunctions中包含kv对的离散流的操作,比如`groupByKeyAndWindow`和`join`.这个操作可以在任何离散流的kv对中自动获取.
 	离散流内部使用下述基本属性描述:
 	1. 当前离散流所依赖的离散流
 	2. 离散流所生成的时间间隔
 	3. 每个时间间隔用于生成RDD的函数
```

```scala
abstract class DStream[T: ClassTag] (
    @transient private[streaming] var ssc: StreamingContext
  ) extends Serializable with Logging {
    属性:
    #name @generatedRDDs = new HashMap[Time, RDD[T]]()	生成RDD表
    #name @zeroTime: Time = null	离散流零时间
    #name @rememberDuration: Duration = null	离散流记忆周期
    #name @storageLevel: StorageLevel = StorageLevel.NONE	存储等级
    #name @mustCheckpoint = false	是否需要设置检查点
    #name @checkpointDuration: Duration = null	检查点周期
    #name @checkpointData = new DStreamCheckpointData(this)	检查点数据
    #name @restoredFromCheckpointData = false	是否可以从检查点中恢复
    #name @graph: DStreamGraph = null	离散流式图
    #name @creationSite = DStream.getCreationSite()	创建指令调用位置
    #name @baseScope: Option[String]	基础作用域
    val= Option(ssc.sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY))
    操作集:
    def slideDuration: Duration
    功能: 获取滑动周期
    
    def dependencies: List[DStream[_]]
    功能: 获取依赖列表
    
    def compute(validTime: Time): Option[RDD[T]]
    功能: 在指定时间生成RDD
    
    def isInitialized = zeroTime != null
    功能: 确定离散流是否开启
    
    def parentRememberDuration = rememberDuration
    功能: 获取父级记忆周期
    
    def context: StreamingContext = ssc
    功能: 获取streaming上下文
    
    def makeScope(time: Time): Option[RDDOperationScope]
    功能: 在同一批次同一个离散流操作中创建组RDD的作用域,每个离散流可以创建多个作用域,每个作用域可能与其他离散流共享.分别使用离散流操作创建不同的作用域.例如,dstream.map(...).map(...)在批次内创建了两个作用域.
    val= baseScope.map { bsJson =>
      val formattedBatchTime = UIUtils.formatBatchTime(
        time.milliseconds, ssc.graph.batchDuration.milliseconds, showYYYYMMSS = false)
      val bs = RDDOperationScope.fromJson(bsJson)
      val baseName = bs.name // e.g. countByWindow, "kafka stream [0]"
      val scopeName =
        if (baseName.length > 10) {
          s"$baseName\n@ $formattedBatchTime"
        } else {
          s"$baseName @ $formattedBatchTime"
        }
      val scopeId = s"${bs.id}_${time.milliseconds}"
      new RDDOperationScope(scopeName, id = scopeId)
    }
    
    def persist(level: StorageLevel): DStream[T]
    功能: 使用指定的存储等级持久化离散流RDD
    if (this.isInitialized) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of a DStream after streaming context has started")
    }
    this.storageLevel = level
    val= this
    
    def persist(): DStream[T] = persist(StorageLevel.MEMORY_ONLY_SER)
    功能: 内存持久化
    
    def cache(): DStream[T] = persist()
    功能: 缓存到内存中
    
    def checkpoint(interval: Duration): DStream[T]
    功能: 开启周期性的检查点操作(这里是一次检查点操作)
    if (isInitialized) {	
      throw new UnsupportedOperationException(
        "Cannot change checkpoint interval of a DStream 
        after streaming context has started")
    }
    persist()
    checkpointDuration = interval
    val= this
    
    def initialize(time: Time): Unit
    功能: 以指定时间为零时间,初始化离散流
    1. 设置零时间
    if (zeroTime != null && zeroTime != time) {
      throw new SparkException(s"ZeroTime is already initialized to $zeroTime"
        + s", cannot initialize it again to $time")
    }
    zeroTime = time
    2. 设置检查点时间间隔(10s)
    if (mustCheckpoint && checkpointDuration == null) {
      checkpointDuration = slideDuration * math.ceil(Seconds(10) / slideDuration).toInt
      logInfo(s"Checkpoint interval automatically set to $checkpointDuration")
    }
    3. 设置记忆周期的最小值
    var minRememberDuration = slideDuration
    if (checkpointDuration != null && minRememberDuration <= checkpointDuration) {
      minRememberDuration = checkpointDuration * 2
    }
    if (rememberDuration == null || rememberDuration < minRememberDuration) {
      rememberDuration = minRememberDuration
    }
    4. 初始化依赖
    dependencies.foreach(_.initialize(zeroTime))
    
    def validateAtInit(): Unit 
    功能: 初始化时验证参数的合法性
    ssc.getState() match {
      case StreamingContextState.INITIALIZED =>
        // 合法
      case StreamingContextState.ACTIVE =>
        throw new IllegalStateException(
          "Adding new inputs, transformations, and output operations after " +
            "starting a context is not supported")
      case StreamingContextState.STOPPED =>
        throw new IllegalStateException(
          "Adding new inputs, transformations, and output operations after " +
            "stopping a context is not supported")
    }
    
    def validateAtStart(): Unit
    功能: 离散流启动时参数校验
    require(rememberDuration != null, "Remember duration is set to null")
    require(
      !mustCheckpoint || checkpointDuration != null,
      s"The checkpoint interval for ${this.getClass.getSimpleName} has not been set." +
        " Please use DStream.checkpoint() to set the interval."
    )
    require(
     	checkpointDuration == null || context.sparkContext.checkpointDir.isDefined,
      	"The checkpoint directory has not been set. Please set it by
        StreamingContext.checkpoint()."
    )
    require(
      	checkpointDuration == null || checkpointDuration >= slideDuration,
      	s"The checkpoint interval for ${this.getClass.getSimpleName} has been set to " +
        s"$checkpointDuration which is lower than its slide time ($slideDuration). " +
        s"Please set it to at least $slideDuration."
    )
    require(
      	checkpointDuration == null || checkpointDuration.isMultipleOf(slideDuration),
      	s"The checkpoint interval for ${this.getClass.getSimpleName} has been set to " +
        s" $checkpointDuration which not a multiple of its slide time 
        ($slideDuration). " +
        s"Please set it to a multiple of $slideDuration."
    )
    require(
      checkpointDuration == null || storageLevel != StorageLevel.NONE,
      s"${this.getClass.getSimpleName} has been marked for checkpointing 
      but the storage " +
      "level has not been set to enable persisting. 
      Please use DStream.persist() to set the " +
      "storage level to use memory for better checkpointing performance."
    )
    require(
      	checkpointDuration == null || rememberDuration > checkpointDuration,
      	s"The remember duration for ${this.getClass.getSimpleName} has been set to " +
        s" $rememberDuration which is not more than the checkpoint interval" +
        s" ($checkpointDuration). Please set it to a value higher than
        $checkpointDuration."
    )
    dependencies.foreach(_.validateAtStart())
    logInfo(s"Slide time = $slideDuration")
    logInfo(s"Storage level = ${storageLevel.description}")
    logInfo(s"Checkpoint interval = $checkpointDuration")
    logInfo(s"Remember interval = $rememberDuration")
    logInfo(s"Initialized and validated $this")
    
    def setContext(s: StreamingContext): Unit
    功能: 设置streaming上下文
    if (ssc != null && ssc != s) {
      throw new SparkException(s"Context must not be set again for $this")
    }
    ssc = s
    logInfo(s"Set context for $this")
    dependencies.foreach(_.setContext(ssc))
    
    def setGraph(g: DStreamGraph): Unit
    功能: 设置离散流式图
    if (graph != null && graph != g) {
      throw new SparkException(s"Graph must not be set again for $this")
    }
    graph = g
    dependencies.foreach(_.setGraph(graph))
    
    def remember(duration: Duration): Unit 
    功能: 记忆指定长度的数据(设置记忆窗口长度)
    1. 设置自身的记忆窗口长度
    if (duration != null && (rememberDuration == null || duration > rememberDuration)) {
      rememberDuration = duration
      logInfo(s"Duration for remembering RDDs set to $rememberDuration for $this")
    }
    2. 设置父级记忆窗口长度
    dependencies.foreach(_.remember(parentRememberDuration))
    
    def isTimeValid(time: Time): Boolean
    功能: 检查指定时间是否合法
    if (!isInitialized) {
      throw new SparkException (this + " has not been initialized")
    } else if (time <= zeroTime || ! (time - zeroTime).isMultipleOf(slideDuration)) {
      logInfo(s"Time $time is invalid as zeroTime is $zeroTime" +
        s" , slideDuration is $slideDuration and difference is ${time - zeroTime}")
      false
    } else {
      logDebug(s"Time $time is valid")
      true
    }
    
    def getOrCompute(time: Time): Option[RDD[T]] 
    功能: 获取指定时间的RDD,可以从缓存检索也可以计算
    1. 如果RDD已经生成了,则从hashMap中检索,没有的话则计算
    generatedRDDs.get(time).orElse {
      if (isTimeValid(time)) {
        val rddOption = createRDDWithLocalProperties(time, displayInnerRDDOps = false) {
          SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
            compute(time)
          }
        }
        rddOption.foreach { case newRDD =>
          if (storageLevel != StorageLevel.NONE) {
            newRDD.persist(storageLevel)
            logDebug(s"Persisting RDD ${newRDD.id} for time $time to $storageLevel")
          }
          if (checkpointDuration != null && 
              (time - zeroTime).isMultipleOf(checkpointDuration)) {
            newRDD.checkpoint()
            logInfo(s"Marking RDD ${newRDD.id} for time $time for checkpointing")
          }
          generatedRDDs.put(time, newRDD)
        }
        rddOption
      } else {
        None
      }
    }
    
    def createRDDWithLocalProperties[U](
      time: Time,
      displayInnerRDDOps: Boolean)(body: => U): U
    功能: 使用本地参数创建RDD
    输入参数:
    displayInnerRDDOps	是否将RDD的信息反映到webUI上
    1. 获取作用域参数
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val scopeNoOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    2. 通过传递spark上下文中的配置到离散流操作作用域中且创建位置信息,由于方法可能被其他应用调用,需要临时存储旧的作用域,然后再创建新的位置信息.设置完毕,再回复旧值.
    // 缓存旧值
    val prevCallSite = CallSite(
      ssc.sparkContext.getLocalProperty(CallSite.SHORT_FORM),
      ssc.sparkContext.getLocalProperty(CallSite.LONG_FORM)
    )
    val prevScope = ssc.sparkContext.getLocalProperty(scopeKey)
    val prevScopeNoOverride = ssc.sparkContext.getLocalProperty(scopeNoOverrideKey)
    try {
      if (displayInnerRDDOps) {
        ssc.sparkContext.setLocalProperty(CallSite.SHORT_FORM, null)
        ssc.sparkContext.setLocalProperty(CallSite.LONG_FORM, null)
      } else {
        ssc.sparkContext.setCallSite(creationSite)
      }
      makeScope(time).foreach { s =>
        ssc.sparkContext.setLocalProperty(scopeKey, s.toJson)
        if (displayInnerRDDOps) {
          // Allow inner RDDs to add inner scopes
          ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, null)
        } else {
          // Do not allow inner RDDs to override the scope set by DStream
          ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, "true")
        }
      }

      body
    } finally {
      ssc.sparkContext.setCallSite(prevCallSite)
      ssc.sparkContext.setLocalProperty(scopeKey, prevScope)
      ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, prevScopeNoOverride)
    }
    
    def generateJob(time: Time): Option[Job] 
    功能: 生成指定时间的sparkstreaming job.这个内部方法不能直接调用,子类需要重写去创建自己的job
    val= getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => {
          val emptyFunc = { (iterator: Iterator[T]) => {} }
          context.sparkContext.runJob(rdd, emptyFunc)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
    
    def clearMetadata(time: Time): Unit
    功能: 清理操作记忆周期前的元数据信息
    val unpersistData = ssc.conf.getBoolean("spark.streaming.unpersist", true)
    val oldRDDs = generatedRDDs.filter(_._1 <= (time - rememberDuration))
    logDebug("Clearing references to old RDDs: [" +
      oldRDDs.map(x => s"${x._1} -> ${x._2.id}").mkString(", ") + "]")
    generatedRDDs --= oldRDDs.keys
    if (unpersistData) {
      logDebug(s"Unpersisting old RDDs: ${oldRDDs.values.map(_.id).mkString(", ")}")
      oldRDDs.values.foreach { rdd =>
        rdd.unpersist()
        rdd match {
          case b: BlockRDD[_] =>
            logInfo(s"Removing blocks of RDD $b of time $time")
            b.removeBlocks()
          case _ =>
        }
      }
    }
    logDebug(s"Cleared ${oldRDDs.size} RDDs that were older than " +
      s"${time - rememberDuration}: ${oldRDDs.keys.mkString(", ")}")
    dependencies.foreach(_.clearMetadata(time))
    
    def updateCheckpointData(currentTime: Time): Unit 
    功能: 更新指定时间的检查点数据
    logDebug(s"Updating checkpoint data for time $currentTime")
    checkpointData.update(currentTime)
    dependencies.foreach(_.updateCheckpointData(currentTime))
    logDebug(s"Updated checkpoint data for time $currentTime: $checkpointData")
    
    def clearCheckpointData(time: Time): Unit 
    功能: 清除检查点数据
    logDebug("Clearing checkpoint data")
    checkpointData.cleanup(time)
    dependencies.foreach(_.clearCheckpointData(time))
    logDebug("Cleared checkpoint data")
    
    def restoreCheckpointData(): Unit
    功能: 恢复检查点数据
    if (!restoredFromCheckpointData) {
      logInfo("Restoring checkpoint data")
      checkpointData.restore()
      dependencies.foreach(_.restoreCheckpointData())
      restoredFromCheckpointData = true
      logInfo("Restored checkpoint data")
    }
    
    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit
    功能: 序列化
    logDebug(s"${this.getClass().getSimpleName}.writeObject used")
    if (graph != null) {
      graph.synchronized {
        if (graph.checkpointInProgress) {
          oos.defaultWriteObject()
        } else {
          val msg = s"Object of ${this.getClass.getName} is being serialized " +
            " possibly as a part of closure of an RDD operation. This is because " +
            " the DStream object is being referred to from within the closure. " +
            " Please rewrite the RDD operation inside this DStream to avoid this. " +
            " This has been enforced to avoid bloating of Spark tasks " +
            " with unnecessary objects."
          throw new java.io.NotSerializableException(msg)
        }
      }
    } else {
      throw new java.io.NotSerializableException(
        "Graph is unexpectedly null when DStream is being serialized.")
    }
    
    @throws(classOf[IOException])
    private def readObject(ois: ObjectInputStream): Unit
    功能: 反序列化
    Utils.tryOrIOException {
        logDebug(s"${this.getClass().getSimpleName}.readObject used")
        ois.defaultReadObject()
        generatedRDDs = new HashMap[Time, RDD[T]]()
    }
    
    ---
    DStream 操作
    
    def map[U: ClassTag](mapFunc: T => U): DStream[U]
    功能: 应用指定函数@mapFunc到离散流中的数据,返回处理之后的离散流
    val= ssc.withScope {
        new MappedDStream(this, context.sparkContext.clean(mapFunc))
    }
    
    def flatMap[U: ClassTag](flatMapFunc: T => TraversableOnce[U]): DStream[U]
    功能: 对离散流中的元素使用指定函数,返回执行后的离散流
    val= ssc.withScope {
        new FlatMappedDStream(this, context.sparkContext.clean(flatMapFunc))
    }
    
    def filter(filterFunc: T => Boolean): DStream[T]
    功能: 对离散流中的元素使用指定函数,实现过滤功能,返回处理完毕的离散流
    val= ssc.withScope {
        new FilteredDStream(this, context.sparkContext.clean(filterFunc))
    }
    
    def glom(): DStream[Array[T]]
    功能: 同上,使得RDD的归并值合并成一个数组形式,返回相应的离散流
    val= ssc.withScope {
        new GlommedDStream(this)
    }
    
    def repartition(numPartitions: Int): DStream[T]
    功能: 离散流重分区
    val= ssc.withScope {
        this.transform(_.repartition(numPartitions))
    }
    
    def mapPartitions[U: ClassTag](
      mapPartFunc: Iterator[T] => Iterator[U],
      preservePartitioning: Boolean = false
    ): DStream[U] 
    功能: 对离散流中的RDD执行分区映射,可以选择是否保留原始分区情况
    val= ssc.withScope {
        new MapPartitionedDStream(
            this, context.sparkContext.clean(mapPartFunc), preservePartitioning)
    }
    
    def reduce(reduceFunc: (T, T) => T): DStream[T] 
    功能: 离散流reduce操作
    val= ssc.withScope {
        this.map((null, _)).reduceByKey(reduceFunc, 1).map(_._2)
    }
    
    def count(): DStream[Long]
    功能: 计算离散流RDD数量
    val= ssc.withScope {
        this.map(_ => (null, 1L))
        .transform(_.union(context.sparkContext.makeRDD(Seq((null, 0L)), 1)))
        .reduceByKey(_ + _)
        .map(_._2)
    }
    
    def countByValue(numPartitions: Int = ssc.sc.defaultParallelism)(
        implicit ord: Ordering[T] = null)
      : DStream[(T, Long)] 
    功能: 返回一个新的RDD,这里的RDD信息包括RDD内部去重元素数量.可以使用hash分区分成指定分区数量,这里指定了默认并行度个分区.
    val=ssc.withScope {
        this.map((_, 1L)).reduceByKey((x: Long, y: Long) => x + y, numPartitions)
    }
    
    def foreachRDD(foreachFunc: RDD[T] => Unit): Unit
    功能: 对于每个RDD进行指定操作
    ssc.withScope {
        val cleanedF = context.sparkContext.clean(foreachFunc, false)
        foreachRDD((r: RDD[T], _: Time) => cleanedF(r), displayInnerRDDOps = true)
    }
    
    def foreachRDD(foreachFunc: (RDD[T], Time) => Unit): Unit 
    功能: 同上,需要将RDD信息显示到webUI上
    foreachRDD(foreachFunc, displayInnerRDDOps = true)
    
    def foreachRDD(
      foreachFunc: (RDD[T], Time) => Unit,
      displayInnerRDDOps: Boolean): Unit
    功能: 同上,可以选择是否需要将RDD信息反映到webUI上
    new ForEachDStream(this,
      context.sparkContext.clean(foreachFunc, false), displayInnerRDDOps).register()
    
    def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
    功能: 对离散流中的RDD进行类型转换,获取新的离散流
    val= ssc.withScope {
        val cleanedF = context.sparkContext.clean(transformFunc, false)
        transform((r: RDD[T], _: Time) => cleanedF(r))
    }
    
    def transform[U: ClassTag](transformFunc: (RDD[T], Time) => RDD[U]): DStream[U] 
    功能: 同上,但是需要使用时间参数构建实时转换函数
    val= ssc.withScope {
        val cleanedF = context.sparkContext.clean(transformFunc, false)
        val realTransformFunc = (rdds: Seq[RDD[_]], time: Time) => {
            assert(rdds.length == 1)
            cleanedF(rdds.head.asInstanceOf[RDD[T]], time)
        }
        new TransformedDStream[U](Seq(this), realTransformFunc)
    }
    
    def transformWith[U: ClassTag, V: ClassTag](
      other: DStream[U], transformFunc: (RDD[T], RDD[U]) => RDD[V]
    ): DStream[V]
    功能: 在本类的离散流和给定的离散流同时使用转换函数,同时可以处理两个离散流
    val= ssc.withScope {
        val cleanedF = ssc.sparkContext.clean(transformFunc, false)
        transformWith(other, (
            rdd1: RDD[T], rdd2: RDD[U], time: Time) => cleanedF(rdd1, rdd2))
    }
    
    def transformWith[U: ClassTag, V: ClassTag](
      other: DStream[U], transformFunc: (RDD[T], RDD[U], Time) => RDD[V]
    ): DStream[V]
    功能: 同上
    val= ssc.withScope {
        val cleanedF = ssc.sparkContext.clean(transformFunc, false)
        val realTransformFunc = (rdds: Seq[RDD[_]], time: Time) => {
            assert(rdds.length == 2)
            val rdd1 = rdds(0).asInstanceOf[RDD[T]]
            val rdd2 = rdds(1).asInstanceOf[RDD[U]]
            cleanedF(rdd1, rdd2, time)
        }
        new TransformedDStream[V](Seq(this, other), realTransformFunc)
    }
    
    def print(): Unit
    功能: 打印每个RDD的前10个元素
    val= ssc.withScope {
        print(10)
    }
    
    def print(num: Int): Unit
    功能: 打印first K
    def foreachFunc: (RDD[T], Time) => Unit = {
      (rdd: RDD[T], time: Time) => {
        val firstNum = rdd.take(num + 1)
        // scalastyle:off println
        println("-------------------------------------------")
        println(s"Time: $time")
        println("-------------------------------------------")
        firstNum.take(num).foreach(println)
        if (firstNum.length > num) println("...")
        println()
        // scalastyle:on println
      }
    }
    foreachRDD(context.sparkContext.clean(foreachFunc), displayInnerRDDOps = false)
    
    def window(windowDuration: Duration): DStream[T] 
    功能: 返回一个新的离散流,每个RDD包含所有滑动窗口中可见的元素,新离散流与这个离散流有同样的时间间隔
    val= window(windowDuration, this.slideDuration)
    
    def window(windowDuration: Duration, slideDuration: Duration): DStream[T]
    功能: 同上,这里也设定了滑动周期
    val= ssc.withScope {
        new WindowedDStream(this, windowDuration, slideDuration)
    }
    
    def reduceByWindow(
      reduceFunc: (T, T) => T,
      windowDuration: Duration,
      slideDuration: Duration
    ): DStream[T]
    功能: 带有窗口的聚合操作
    val= ssc.withScope {
        this.reduce(reduceFunc).window(windowDuration, slideDuration).reduce(reduceFunc)
    }
    
    def reduceByWindow(
      reduceFunc: (T, T) => T,
      invReduceFunc: (T, T) => T,
      windowDuration: Duration,
      slideDuration: Duration
    ): DStream[T]
    功能: 同上,这里指定了可逆运算@invReduceFunc
    val= ssc.withScope {
        this.map((1, _))
        .reduceByKeyAndWindow(
            reduceFunc, invReduceFunc, windowDuration, slideDuration, 1)
        .map(_._2)
    }
    
    def countByWindow(
      windowDuration: Duration,
      slideDuration: Duration): DStream[Long] 
    功能: 获取窗口内元素的数量
    val= ssc.withScope {
        this.map(_ => 1L).reduceByWindow(_ + _, _ - _, windowDuration, slideDuration)
    }
    
    def countByValueAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int = ssc.sc.defaultParallelism)
      (implicit ord: Ordering[T] = null)
      : DStream[(T, Long)]
    功能: 计算每个RDD内去除元素的信息
    val= ssc.withScope {
        this.map((_, 1L)).reduceByKeyAndWindow(
            (x: Long, y: Long) => x + y,
            (x: Long, y: Long) => x - y,
            windowDuration,
            slideDuration,
            numPartitions,
            (x: (T, Long)) => x._2 != 0L
        )
    }
    
    def union(that: DStream[T]): DStream[T] 
    功能: 与指定离散流union
    val= ssc.withScope {
        new UnionDStream[T](Array(this, that))
    }
    
    def slice(interval: Interval): Seq[RDD[T]] 
    功能: 滑动指定距离,获取RDD数据列表
    val= ssc.withScope {
        slice(interval.beginTime, interval.endTime)
    }
    
    def slice(fromTime: Time, toTime: Time): Seq[RDD[T]]
    功能: 获取从@fromTime到@toTime直接的RDD数据列表
    val= ssc.withScope {
        if (!isInitialized) {
            throw new SparkException(this + " has not been initialized")
        }
        val alignedToTime = if ((toTime - zeroTime).isMultipleOf(slideDuration)) {
            toTime
        } else {
            logWarning(s"toTime ($toTime) is not a multiple of 
            slideDuration ($slideDuration)")
            toTime.floor(slideDuration, zeroTime)
        }
        val alignedFromTime = if ((fromTime - zeroTime).isMultipleOf(slideDuration)) {
            fromTime
        } else {
            logWarning(s"fromTime ($fromTime) is not a multiple 
            of slideDuration ($slideDuration)")
            fromTime.floor(slideDuration, zeroTime)
        }
        logInfo(s"Slicing from $fromTime to $toTime" +
                s" (aligned to $alignedFromTime and $alignedToTime)")
        alignedFromTime.to(alignedToTime, slideDuration).flatMap { time =>
            if (time >= zeroTime) getOrCompute(time) else None
        }
    }
    
    def saveAsObjectFiles(prefix: String, suffix: String = ""): Unit 
    功能: 保存为对象文件
    ssc.withScope {
        val saveFunc = (rdd: RDD[T], time: Time) => {
            val file = rddToFileName(prefix, suffix, time)
            rdd.saveAsObjectFile(file)
        }
        this.foreachRDD(saveFunc, displayInnerRDDOps = false)
    }
    
    def saveAsTextFiles(prefix: String, suffix: String = ""): Unit
    功能: 保存为文本文件
    ssc.withScope {
        val saveFunc = (rdd: RDD[T], time: Time) => {
            val file = rddToFileName(prefix, suffix, time)
            rdd.saveAsTextFile(file)
        }
        this.foreachRDD(saveFunc, displayInnerRDDOps = false)
    }
    
    def register(): DStream[T] 
    功能: 将当前离散流注册到离散流式图中
    ssc.graph.addOutputStream(this)
    val= this
}
```

```scala
object DStream {
    属性:
    #name @SPARK_CLASS_REGEX = """^org\.apache\.spark""".r	spark类正则表达式
    #name @SPARK_STREAMING_TESTCLASS_REGEX = """^org\.apache\.spark\.streaming\.test""".r
    	streaming测试类正则表达式
    #name @SPARK_EXAMPLES_CLASS_REGEX = """^org\.apache\.spark\.examples""".r	
    	spark示例类正则
    #name @SCALA_CLASS_REGEX = """^scala""".r	scala类正则
    操作集:
    def toPairDStreamFunctions[K, V](stream: DStream[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null):
    PairDStreamFunctions[K, V] 
    功能: 转化为kv对离散流函数
    val= new PairDStreamFunctions[K, V](stream)
    
    def streamingExclustionFunction(className: String): Boolean
    功能: streaming类排除函数
    def doesMatch(r: Regex): Boolean = r.findFirstIn(className).isDefined
    val isSparkClass = doesMatch(SPARK_CLASS_REGEX)
    val isSparkExampleClass = doesMatch(SPARK_EXAMPLES_CLASS_REGEX)
    val isSparkStreamingTestClass = doesMatch(SPARK_STREAMING_TESTCLASS_REGEX)
    val isScalaClass = doesMatch(SCALA_CLASS_REGEX)
    val= 
    (isSparkClass || isScalaClass) && !isSparkExampleClass && !isSparkStreamingTestClass
    
    def getCreationSite(): CallSite
    功能: 获取离散流创建位置
    val= org.apache.spark.util.Utils.getCallSite(streamingExclustionFunction)
}
```

#### DStreamCheckpointData

```scala
private[streaming]
class DStreamCheckpointData[T: ClassTag](dstream: DStream[T])
extends Serializable with Logging {
    介绍: 离散流检查点数据
    构造器参数:
    	dstream	离散流
    属性:
    #name @data = new HashMap[Time, AnyRef]()	时间映射的数据表
    #name @timeToCheckpointFile = new HashMap[Time, String]	时间->检查点文件映射表
    #name @timeToOldestCheckpointFileTime = new HashMap[Time, Time]	
    	时间->最初检查点文件时间映射
    操作集:
    def currentCheckpointFiles = data.asInstanceOf[HashMap[Time, String]]
    功能: 获取当前检查点文件的时间--> 检查点文件映射
    
    def update(time: Time): Unit
    功能: 更新离散流的检查点数据,每次检查点初始化的时候,这个方法就会被调用.默认实现记录了离散流生成的RDD对应的检查点文件.
    1. 从生成的RDD中获取检查点RDD
    val checkpointFiles = dstream.generatedRDDs.filter(_._2.getCheckpointFile.isDefined)
                                       .map(x => (x._1, x._2.getCheckpointFile.get))
    logDebug("Current checkpoint files:\n" + checkpointFiles.toSeq.mkString("\n"))
    2. 添加检查点文件,这些文件的数据需要被序列化
    if (!checkpointFiles.isEmpty) {
      currentCheckpointFiles.clear()
      currentCheckpointFiles ++= checkpointFiles
        // 添加当前检查点文件到所有检查点文件映射表中
      timeToCheckpointFile ++= currentCheckpointFiles
        // 记录当前状态的最早检查点RDD时间
      timeToOldestCheckpointFileTime(time) =
        currentCheckpointFiles.keys.min(Time.ordering)
    }
    
    def cleanup(time: Time): Unit 
    功能: 清除旧有的检查点数据,在检查点的时间写入到检查点目录中
    timeToOldestCheckpointFileTime.remove(time) match {
      case Some(lastCheckpointFileTime) =>
        val filesToDelete = timeToCheckpointFile.filter(_._1 < lastCheckpointFileTime)
        logDebug("Files to delete:\n" + filesToDelete.mkString(","))
        var fileSystem: FileSystem = null
        filesToDelete.foreach {
          case (time, file) =>
            try {
              val path = new Path(file)
              if (fileSystem == null) {
                fileSystem =
                  path.getFileSystem(dstream.ssc.sparkContext.hadoopConfiguration)
              }
              if (fileSystem.delete(path, true)) {
                logInfo("Deleted checkpoint file '" + file + "' for time " + time)
              } else {
                logWarning(s"Error deleting old checkpoint file '$file' for time $time")
              }
              timeToCheckpointFile -= time
            } catch {
              case e: Exception =>
                logWarning("Error deleting old checkpoint file '
                " + file + "' for time " + time, e)
                fileSystem = null
            }
        }
      case None =>
        logDebug("Nothing to delete")
    }
    
    def restore(): Unit
    功能: 恢复检查点数据,每次当离散流图从图检查点文件中恢复的时候就会被调用,默认实现了检查点目录中的RDD恢复功能.
    // 从检查点数据中创建RDD,并添加到离散流中(=> 检查点数据的恢复)
    currentCheckpointFiles.foreach {
      case(time, file) =>
        logInfo("Restoring checkpointed RDD for time " + time + " 
        from file '" + file + "'")
        dstream.generatedRDDs += (
            (time, dstream.context.sparkContext.checkpointFile[T](file)))
    }
    
    def toString: String 
    功能: 信息显示
    val= "[\n" + currentCheckpointFiles.size + " checkpoint files \n" +
      currentCheckpointFiles.mkString("\n") + "\n]"
    
    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit
    功能: 序列化
    Utils.tryOrIOException {
        logDebug(this.getClass().getSimpleName + ".writeObject used")
        if (dstream.context.graph != null) {
          dstream.context.graph.synchronized {
            if (dstream.context.graph.checkpointInProgress) {
              oos.defaultWriteObject()
            } else {
              val msg = "Object of " + this.getClass.getName + " is being serialized " +
                " possibly as a part of closure of an RDD operation. This is because " +
                " the DStream object is being referred to from within the closure. " +
                " Please rewrite the RDD operation inside this DStream to avoid this. " +
                " This has been enforced to avoid bloating of Spark tasks " +
                " with unnecessary objects."
              throw new java.io.NotSerializableException(msg)
            }
          }
        } else {
          throw new java.io.NotSerializableException(
            "Graph is unexpectedly null when DStream is being serialized.")
        }
    }
    
    @throws(classOf[IOException])
    private def readObject(ois: ObjectInputStream): Unit
    功能: 反序列化
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    timeToOldestCheckpointFileTime = new HashMap[Time, Time]
    timeToCheckpointFile = new HashMap[Time, String]
}
```

#### FileInputDStream

```markdown
介绍:
 	这个类代表的输入离散流,可以监视hadoop文件系统.用于创建文件或者创建文件输入流.可以按照下述方式工作:
 	在每个批次间隔中,文件系统需要询问给定目录中的文件,且端口那个批次选中文件的链接.在这种状态下,新建就意味着对于读取器来说是可见的.需要注意到的是创建完毕周文件也是可见的.正是因为这个目的,这个类能够记住选中文件在过去一段时间内批次信息,所以叫做记忆窗口.可以按照如下的表述:
 	{{{ 		
 	 忽略的部分 --> << 记忆窗口 >> <-- 当前批次时间	
 	}}}
 	这个窗口之前叫做忽略的部分,所有文件小于这个时间上限的因此会被忽略.文件修改时间在记忆窗口中的会被检测.在高版本情况下,新文件在每个批次中进行标识.问阿金的修改时间大于忽略界限且没有被视作记忆窗口的部分,请参考@isFile方法的描述,这里描述了这个区间的求法.
     这里做出一些假设,对于底层文件系统监视的假设
     - 文件系统时间假设与机器时间同步
     - 文件在目录列表中可见,必须在文件修改时间的指定周期内是可见的.这个周期就是`记忆窗口`.设置为1分钟.这里可以参考@FileInputDStream.minRememberDuration,否则文件永远不会被选取,因为在当文件可见的时候,修改时间总是小于容量值.
     - 一旦文件可见,修改时间就不能改变,如果需要扩展,进程语义并没有被定义.
```

```scala
private[streaming]
class FileInputDStream[K, V, F <: NewInputFormat[K, V]](
    _ssc: StreamingContext,
    directory: String,
    filter: Path => Boolean = FileInputDStream.defaultFilter,
    newFilesOnly: Boolean = true,
    conf: Option[Configuration] = None)
    (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F])
extends InputDStream[(K, V)](_ssc) {
    #name @serializableConfOpt = conf.map(new SerializableConfiguration(_))
    	序列化hadoop配置
    #name @minRememberDurationS	最小记忆周期(记忆窗口最小长度)
    val= Seconds(ssc.conf.getTimeAsSeconds(
        "spark.streaming.fileStream.minRememberDuration",
      ssc.conf.get("spark.streaming.minRememberDuration", "60s")))
    #name @checkpointData = new FileInputDStreamCheckpointData	检查点数据
    #name @initialModTimeIgnoreThreshold	初始化修改时间下限值(记忆窗口时间下限值)
    #name @numBatchesToRemember	需要记忆的批次数量
    val= FileInputDStream
    	.calculateNumBatchesToRemember(slideDuration, minRememberDurationS)
    #name @durationToRemember = slideDuration * numBatchesToRemember	记忆周期
    #name @batchTimeToSelectedFiles	选择文件的批次时间表
    val= new mutable.HashMap[Time, Array[String]]
    #name @recentlySelectedFiles = new mutable.HashSet[String]()	最近选择的文件集合
    #name @lastNewFileFindingTime = 0L	上次查询文件的时间
    #name @_path: Path = null	文件路径
    #name @_fs: FileSystem = null	文件所属文件系统
    操作集:
    def start(): Unit = { }
    def stop(): Unit = { }
    功能: 文件输入离散流的启动/停止
    
    def compute(validTime: Time): Option[RDD[(K, V)]] 
    功能: 计算指定时间离散流中的RDD
    val newFiles = findNewFiles(validTime.milliseconds)
    logInfo("New files at time " + validTime + ":\n" + newFiles.mkString("\n"))
    batchTimeToSelectedFiles.synchronized {
      batchTimeToSelectedFiles += ((validTime, newFiles))
    }
    recentlySelectedFiles ++= newFiles
    val rdds = Some(filesToRDD(newFiles))
    val metadata = Map(
      "files" -> newFiles.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> newFiles.mkString("\n"))
    val inputInfo = StreamInputInfo(id, 0, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    val= rdds
    
    def clearMetadata(time: Time): Unit 
    功能: 清除旧RDD的时间-> 文件映射关系
    super.clearMetadata(time)
    batchTimeToSelectedFiles.synchronized {
      val oldFiles = batchTimeToSelectedFiles.filter(_._1 < (time - rememberDuration))
      batchTimeToSelectedFiles --= oldFiles.keys
      recentlySelectedFiles --= oldFiles.values.flatten
      logInfo("Cleared " + oldFiles.size + " old files that were older than " +
        (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
      logDebug("Cleared files are:\n" +
        oldFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
    }
    
    def findNewFiles(currentTime: Long): Array[String]
    功能: 查询当前时间@currentTime下的新文件
    try {
      lastNewFileFindingTime = clock.getTimeMillis()
      val modTimeIgnoreThreshold = math.max(
        initialModTimeIgnoreThreshold,
        currentTime - durationToRemember.milliseconds  
      )
      logDebug(s"Getting new files for time $currentTime, " +
        s"ignoring files older than $modTimeIgnoreThreshold")
      val directories = 
        Option(fs.globStatus(directoryPath)).getOrElse(Array.empty[FileStatus])
        .filter(_.isDirectory)
        .map(_.getPath)
      val newFiles = directories.flatMap(dir =>
        fs.listStatus(dir)
          .filter(isNewFile(_, currentTime, modTimeIgnoreThreshold))
          .map(_.getPath.toString))
      val timeTaken = clock.getTimeMillis() - lastNewFileFindingTime
      logDebug(s"Finding new files took $timeTaken ms")
      if (timeTaken > slideDuration.milliseconds) {
        logWarning(
          s"Time taken to find new files $timeTaken exceeds the batch size. " +
            "Consider increasing the batch size or reducing the number of " +
            "files in the monitored directories."
        )
      }
      newFiles
    } catch {
      case e: FileNotFoundException =>
        logWarning(s"No directory to scan: $directoryPath: $e")
        Array.empty
      case e: Exception =>
        logWarning(s"Error finding new files under $directoryPath", e)
        reset()
        Array.empty
    }
    
    def filesToRDD(files: Seq[String]): RDD[(K, V)]
    功能: 由文件列表生成RDD
    val fileRDDs = files.map { file =>
      val rdd = serializableConfOpt.map(_.value) match {
        case Some(config) => context.sparkContext.newAPIHadoopFile(
          file,
          fm.runtimeClass.asInstanceOf[Class[F]],
          km.runtimeClass.asInstanceOf[Class[K]],
          vm.runtimeClass.asInstanceOf[Class[V]],
          config)
        case None => context.sparkContext.newAPIHadoopFile[K, V, F](file)
      }
      if (rdd.partitions.isEmpty) {
        logError("File " + file + " has no data in it. 
        Spark Streaming can only ingest " +
          "files that have been \"moved\" to the directory assigned 
          to the file stream. " +
          "Refer to the streaming programming guide for more details.")
      }
      rdd
    }
    val= new UnionRDD(context.sparkContext, fileRDDs)
    
    def directoryPath: Path
    功能: 获取目录路径
    if (_path == null) _path = new Path(directory)
    val= _path
    
    def fs: FileSystem 
    功能: 获取文件系统
    if (_fs == null) _fs =
    	directoryPath.getFileSystem(ssc.sparkContext.hadoopConfiguration)
    val= _fs
    
    def reset(): Unit
    功能: 重置文件系统
    _fs = null
    
    @throws(classOf[IOException])
    private def readObject(ois: ObjectInputStream): Unit
    功能: 反序列化
    Utils.tryOrIOException {
        logDebug(this.getClass().getSimpleName + ".readObject used")
        ois.defaultReadObject()
        generatedRDDs = new mutable.HashMap[Time, RDD[(K, V)]]()
        batchTimeToSelectedFiles = new mutable.HashMap[Time, Array[String]]
        recentlySelectedFiles = new mutable.HashSet[String]()
    }
    
    def isNewFile(
     fileStatus: FileStatus,
     currentTime: Long,
     modTimeIgnoreThreshold: Long): Boolean
    功能: 确定指定文件是否为新文件
    假设给定文件对于指定路径来说时新文件,必须要传递下述条件
    1. 必须传递用户定义的文件过滤器
    2. 时间值必须要大于忽略时间上限,假定所有文件都在忽略时间上限之前,这些文件在启动之间已经经过处理.
    3. 必须不能是这个类最近选择的文件
    4. 不能比批次时间还要新
    val path = fileStatus.getPath
    val pathStr = path.toString
    if (!filter(path)) {
      logDebug(s"$pathStr rejected by filter")
      return false
    }
    val modTime = fileStatus.getModificationTime()
    if (modTime <= modTimeIgnoreThreshold) {
      logDebug(s"$pathStr ignored as mod time $modTime
      <= ignore time $modTimeIgnoreThreshold")
      return false
    }
    if (modTime > currentTime) {
      logDebug(s"$pathStr not selected as mod time $modTime > current time $currentTime")
      return false
    }
    if (recentlySelectedFiles.contains(pathStr)) {
      logDebug(s"$pathStr already considered")
      return false
    }
    logDebug(s"$pathStr accepted with mod time $modTime")
    return true
    
    内部类:
    private[streaming]
    class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {
        介绍: 文件输入离散流检查点数据
        操作集:
        def hadoopFiles
        功能: 获取hadoop文件表
        val= data.asInstanceOf[mutable.HashMap[Time, Array[String]]]
        
        def update(time: Time): Unit
        功能: 更新hadoop文件表
        hadoopFiles.clear()
        batchTimeToSelectedFiles.synchronized { hadoopFiles++= batchTimeToSelectedFiles }
        
        def cleanup(time: Time): Unit = { }
        功能: 清空检查点数据
        
        def restore(): Unit
        功能: 恢复检查点数据
        hadoopFiles.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) =>
          logInfo("Restoring files for time " + t + " - " +
            f.mkString("[", ", ", "]") )
          batchTimeToSelectedFiles.synchronized { batchTimeToSelectedFiles += ((t, f)) }
          recentlySelectedFiles ++= f
          generatedRDDs += ((t, filesToRDD(f)))
        }
        
        def toString: String
        功能: 检查点数据显示
        val= "[\n" + hadoopFiles.size + " file sets\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
}
```

```scala
private[streaming]
object FileInputDStream {
    操作集:
    def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")
    功能; 确定指定路径是否合法
    
    def calculateNumBatchesToRemember(batchDuration: Duration,
                                    minRememberDurationS: Duration): Int
    功能: 计算需要计算的批次数量
    val= math.ceil(
        minRememberDurationS.milliseconds.toDouble / batchDuration.milliseconds).toInt
}
```

#### FilteredDStream

```scala
private[streaming]
class FilteredDStream[T: ClassTag](
    parent: DStream[T],
    filterFunc: T => Boolean
) extends DStream[T](parent.ssc) {
    介绍: 带有过滤功能的离散流
    构造器:
        parent	离散流
        filterFunc	过滤函数
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期(等于父离散流滑动周期)
    
    def compute(validTime: Time): Option[RDD[T]] 
    功能: 计算指定时间离散流中的RDD
    val= parent.getOrCompute(validTime).map(_.filter(filterFunc))
}
```

#### FlatMappedDStream

```scala
private[streaming]
class FlatMappedDStream[T: ClassTag, U: ClassTag](
    parent: DStream[T],
    flatMapFunc: T => TraversableOnce[U]
) extends DStream[U](parent.ssc) {
    介绍: 带有flatmap功能的离散流
    构造器参数:
    parent	父级离散流
    flatMapFunc	flatMap处理函数(拆解函数)
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]] 
    功能: 计算指定时间离散流中的RDD
    val= parent.getOrCompute(validTime).map(_.flatMap(flatMapFunc))
}
```

#### FlatMapValuedDStream

```scala
private[streaming]
class FlatMapValuedDStream[K: ClassTag, V: ClassTag, U: ClassTag](
    parent: DStream[(K, V)],
    flatMapValueFunc: V => TraversableOnce[U]
) extends DStream[(K, U)](parent.ssc) {
    介绍: 带有flatMapValue功能的离散流
    构造器参数:
    	parent	父级离散流
    	flatMapValueFunc	flatMapValue处理函数
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[(K, U)]]
    功能: 计算指定时间离散流中的RDD
}
```

#### ForEachDStream

```scala
private[streaming]
class ForEachDStream[T: ClassTag] (
    parent: DStream[T],
    foreachFunc: (RDD[T], Time) => Unit,
    displayInnerRDDOps: Boolean
) extends DStream[Unit](parent.ssc) {
    构造器参数:
    parent	父级离散流
    foreachFunc	foreach函数
    displayInnerRDDOps	是否RDD的具体操作位置会被汇报给webUI
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[Unit]] = None
    功能: 计算指定时间离散流的RDD
    
    def generateJob(time: Time): Option[Job] 
    功能: 生成指定时间的job
    val= parent.getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => createRDDWithLocalProperties(time, displayInnerRDDOps) {
          foreachFunc(rdd, time)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
}
```

#### GlommedDStream

```scala
private[streaming]
class GlommedDStream[T: ClassTag](parent: DStream[T]){
    介绍: 无功能的离散流
    构造器参数:
    parent	父级离散流
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[Array[T]]]
    功能: 计算指定时间离散流的RDD
    val= parent.getOrCompute(validTime).map(_.glom())
}
```

#### InputDStream

```markdown
介绍:
	这个是所有输入离散流的抽象类,这个类提供启动和停止的方法,使用sparkstreaming可以启动和停止数据的接收.输入离散流可以从新数据中产生RDD.通过运行服务/线程在驱动器节点上.(意味着不需要启动worker节点上的接收器),可以直接基础输入离散流@InputDStream实现,例如,文件输入离散流@FileInputDStream,就是这个的子类,可以在驱动器上监视HDFS目录.用于新建文件或者使用新文件产生RDD.实现的输入流需要在worker上运行接收器.
```

```scala
abstract class InputDStream[T: ClassTag](_ssc: StreamingContext)
extends DStream[T](_ssc) {
	属性:
    #name @lastValidTime: Time = null	上一个有效时间
    #name @id = ssc.getNewInputStreamId()	输入离散流唯一标识符
    #name @rateController: Option[RateController] = None	比例控制器
    #name @baseScope: Option[String] 基础作用域(用于离散流的操作)
    val= {
        val scopeName = Option(ssc.sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY))
          .map { json => RDDOperationScope.fromJson(json).name + s" [$id]" }
          .getOrElse(name.toLowerCase(Locale.ROOT))
        Some(new RDDOperationScope(scopeName).toJson)
    }
    初始化操作:
    ssc.graph.addInputStream(this)
    功能: 添加当前输入离散流到离散流图中
    操作集:
    def name: String 
    功能: 获取人类可读的输入离散流
    val newName = Utils.getFormattedClassName(this)
      .replaceAll("InputDStream", "Stream")
      .split("(?=[A-Z])")
      .filter(_.nonEmpty)
      .mkString(" ")
      .toLowerCase(Locale.ROOT)
      .capitalize
    val= s"$newName [$id]"
    
    def isTimeValid(time: Time): Boolean
    功能: 确定指定时间是否可用
    val= if (!super.isTimeValid(time)) {
      false // Time not valid
    } else {
      if (lastValidTime != null && time < lastValidTime) {
        logWarning(s"isTimeValid called with $time whereas the last valid time " +
          s"is $lastValidTime")
      }
      lastValidTime = time
      true
    }
    
    def dependencies: List[DStream[_]] = List()
    功能: 获取依赖列表(空)
    
    def slideDuration: Duration
    功能: 获取滑动周期
    if (ssc == null) throw new Exception("ssc is null")
    if (ssc.graph.batchDuration == null) throw new Exception("batchDuration is null")
    val= ssc.graph.batchDuration
    
    def start(): Unit
    def stop(): Unit
    功能: 启动/停止输入离散流
}
```

#### MapPartitionedDStream

```scala
private[streaming]
class MapPartitionedDStream[T: ClassTag, U: ClassTag](
    parent: DStream[T],
    mapPartFunc: Iterator[T] => Iterator[U],
    preservePartitioning: Boolean
) extends DStream[U](parent.ssc) {
    介绍: 带有mapPartition功能的离散流
    构造器参数:
    parent	父级RDD
    mapPartFunc	mapPartition函数
    preservePartitioning	是否保留分区
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]] 
    功能: 计算指定时间离散流的RDD
    val=parent.getOrCompute(validTime).map(
        _.mapPartitions[U](mapPartFunc, preservePartitioning))
}
```

#### MappedDStream

```scala
private[streaming]
class MappedDStream[T: ClassTag, U: ClassTag] (
    parent: DStream[T],
    mapFunc: T => U
) extends DStream[U](parent.ssc) {
    介绍: 带有map功能的离散流
    构造器参数:
    parent	父级离散流
    mapFunc	map映射函数
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]]
    功能: 计算指定时间离散流的RDD
    val= parent.getOrCompute(validTime).map(_.map[U](mapFunc))
}
```

#### MapValuedDStream

```scala
private[streaming]
class MapValuedDStream[K: ClassTag, V: ClassTag, U: ClassTag](
    parent: DStream[(K, V)],
    mapValueFunc: V => U
) extends DStream[(K, U)](parent.ssc) {
    介绍: 带有mapValue功能的离散流
    构造器参数:
    parent	父级离散流
    mapValueFunc	mapValue
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]]
    功能: 计算指定时间离散流的RDD
    val= parent.getOrCompute(validTime).map(_.mapValues[U](mapValueFunc))
}
```

#### MapWithStateDStream

```scala
private[streaming] class MapWithStateDStreamImpl[
    KeyType: ClassTag, ValueType: ClassTag, StateType: ClassTag, MappedType: ClassTag](
    dataStream: DStream[(KeyType, ValueType)],
    spec: StateSpecImpl[KeyType, ValueType, StateType, MappedType])
extends MapWithStateDStream[KeyType, ValueType, StateType, MappedType](dataStream.context) {
    介绍: 带有状态映射的离散流
    构造器参数:
        KeyType	key类型
        ValueType	value类型
        StateType	状态类型
        MappedType	映射类型
        dataStream	数据流表
        spec	状态配置信息
    属性:
    #name @internalStream	内部离散流
    val= new InternalMapWithStateDStream[
        KeyType, ValueType, StateType, MappedType](dataStream, spec)
    
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[MappedType]]
    功能: 计算指定时间离散流的RDD
    val= internalStream.getOrCompute(validTime).map { 
        _.flatMap[MappedType] { _.mappedData } }
    
    def checkpoint(checkpointInterval: Duration): DStream[MappedType]
    功能: 发送检查点到内部离散流,确保离散流不会处理检查点,只有内部离散流才会处理
    internalStream.checkpoint(checkpointInterval)
    val= this
    
    def stateSnapshots(): DStream[(KeyType, StateType)]
    功能: 获取状态快照
    val= internalStream.flatMap {
      _.stateMap.getAll().map { case (k, s, _) => (k, s) }.toTraversable }
    
    def keyClass: Class[_] = implicitly[ClassTag[KeyType]].runtimeClass
    def valueClass: Class[_] = implicitly[ClassTag[ValueType]].runtimeClass
    def stateClass: Class[_] = implicitly[ClassTag[StateType]].runtimeClass
    def mappedClass: Class[_] = implicitly[ClassTag[MappedType]].runtimeClass
    功能: 获取key/value/state/mapped类型
}
```

```scala
private[streaming]
class InternalMapWithStateDStream[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
    parent: DStream[(K, V)], spec: StateSpecImpl[K, V, S, E])
extends DStream[MapWithStateRDDRecord[K, S, E]](parent.context) {
    介绍: 这个离散流维护每个key的状态,任意记录基于状态更新产生,这个是@mapWithState操作在离散流上的主要实现.
    构造器参数:
        parent	父级离散流
        spec	@mapWithState参数
        K	key类型
        V	value类型
        S	状态类型
        E 	mapped类型
    属性:
    #name @partitioner	分区器
    val=  spec.getPartitioner().getOrElse(
   	 	new HashPartitioner(ssc.sc.defaultParallelism))
    #name @mappingFunction = spec.getFunction()	映射函数
    #name @mustCheckpoint = true	是否需要自动设置检查点
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def initialize(time: Time): Unit
    功能: 初始化默认检查点周期
    if (checkpointDuration == null) {
      checkpointDuration = slideDuration * DEFAULT_CHECKPOINT_DURATION_MULTIPLIER
    }
    super.initialize(time)
    
    def compute(validTime: Time): Option[RDD[MapWithStateRDDRecord[K, S, E]]] 
    功能: 计算指定时间离散流的RDD
    1. 获取之前的状态RDD
    val prevStateRDD = getOrCompute(validTime - slideDuration) match {
      case Some(rdd) =>
        if (rdd.partitioner != Some(partitioner)) {
          MapWithStateRDD.createFromRDD[K, V, S, E](
            rdd.flatMap { _.stateMap.getAll() }, partitioner, validTime)
        } else {
          rdd
        }
      case None =>
        MapWithStateRDD.createFromPairRDD[K, V, S, E](
          spec.getInitialStateRDD().getOrElse(new EmptyRDD[(K, S)](ssc.sparkContext)),
          partitioner,
          validTime
        )
    }
    2. 使用之前的状态RDD和分区数据计算新的状态RDD,如果是空数据,使用空的RDD或者创建新的RDD
    val dataRDD = parent.getOrCompute(validTime).getOrElse {
      context.sparkContext.emptyRDD[(K, V)]
    }
    val partitionedDataRDD = dataRDD.partitionBy(partitioner)
    val timeoutThresholdTime = spec.getTimeoutInterval().map { interval =>
      (validTime - interval).milliseconds
    }
    val= Some(new MapWithStateRDD(prevStateRDD, partitionedDataRDD, 
                                  mappingFunction, validTime, timeoutThresholdTime))
    
}
```

```scala
private[streaming] object InternalMapWithStateDStream {
  private val DEFAULT_CHECKPOINT_DURATION_MULTIPLIER = 10
    默认检查点周期乘法器
}
```

#### PairDStreamFunctions

```scala
class PairDStreamFunctions[K, V](self: DStream[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K])
extends Serializable {
    介绍: kv形式离散流函数
    构造器参数:
        self	本体离散流
        ord	排序方式
    操作集:
    def ssc = self.ssc
    功能: 获取离散流的streaming上下文
    
    def sparkContext = self.context.sparkContext
    功能: 获取离散流的spark上下文
    
    def defaultPartitioner(numPartitions: Int = self.ssc.sc.defaultParallelism)
    功能: 获取默认分区器(hash分区器)
    val= new HashPartitioner(numPartitions)
    
    def groupByKey(numPartitions: Int): DStream[(K, Iterable[V])]
    功能: 返回一个新的离散流,对每个RDD使用@groupByKey,使用hash分区器生成指定数量的RDD
    val= ssc.withScope { groupByKey(defaultPartitioner(numPartitions)) }
    
    def groupByKey(partitioner: Partitioner): DStream[(K, Iterable[V])]
    功能: 同上,指示这里指定了分区器,使用指定的分区器进行分区操作
    val= ssc.withScope {
        val createCombiner = (v: V) => ArrayBuffer[V](v)
        val mergeValue = (c: ArrayBuffer[V], v: V) => (c += v)
        val mergeCombiner = (c1: ArrayBuffer[V], c2: ArrayBuffer[V]) => (c1 ++ c2)
        combineByKey(createCombiner, mergeValue, mergeCombiner, partitioner)
          .asInstanceOf[DStream[(K, Iterable[V])]]
    }
    
    def reduceByKey(reduceFunc: (V, V) => V): DStream[(K, V)] 
    功能: 使用@reduceByKey 应用在每个RDD上,获取新的离散流,每个key的value值都被聚合了,使用hash分区器生成spark默认情况下的分区.
    输入参数: reduceFunc	聚合函数
    val= ssc.withScope {
        reduceByKey(reduceFunc, defaultPartitioner())
    }
    
    def reduceByKey(
      reduceFunc: (V, V) => V,
      numPartitions: Int): DStream[(K, V)]
    功能: 同上,这里指定了分区数量@numPartitions
    val= ssc.withScope {
        reduceByKey(reduceFunc, defaultPartitioner(numPartitions))
    }
    
    def reduceByKey(
      reduceFunc: (V, V) => V,
      partitioner: Partitioner): DStream[(K, V)] 
    功能: 同上,这里指定了分区器@partitioner,需要按照分区器的要求进行分区
    val= ssc.withScope {
        combineByKey((v: V) => v, reduceFunc, reduceFunc, partitioner)
      }
    
    def combineByKey[C: ClassTag](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiner: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true): DStream[(K, C)] 
    功能: 使用用户定义的函数合并离散流中的RDD元素,这个与普通RDD的@combineByKey的相似,逻辑参考普通RDD
    1. 清理相关闭包
    val cleanedCreateCombiner = sparkContext.clean(createCombiner)
    val cleanedMergeValue = sparkContext.clean(mergeValue)
    val cleanedMergeCombiner = sparkContext.clean(mergeCombiner)
    2. 获取结果离散流
    val= new ShuffledDStream[K, V, C](
      self,
      cleanedCreateCombiner,
      cleanedMergeValue,
      cleanedMergeCombiner,
      partitioner,
      mapSideCombine)
    
    def groupByKeyAndWindow(windowDuration: Duration): DStream[(K, Iterable[V])] 
    功能: 通过在一个滑动窗口上应用@groupByKey 获取一个新的离散流,相似于@DStream.groupByKey()应用在滑动窗口上,新的离散流使用相同时间间隔产生RDD.使用spark默认分区进行hash分区.
    输入参数:
    windowDuration	窗口周期,必须是批次间隔的整数倍
    val= ssc.withScope {
        groupByKeyAndWindow(windowDuration, self.slideDuration, defaultPartitioner())
    }
    
    def groupByKeyAndWindow(windowDuration: Duration, slideDuration: Duration)
      : DStream[(K, Iterable[V])]
    功能: 同上,这里指定了滑动周期@slideDuration,这个是窗口的时间间隔,必须是离散流批次间隔的整数倍
    
    def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): DStream[(K, Iterable[V])] 
    功能: 同上,这里指定了分区数量
    val= ssc.withScope {
        groupByKeyAndWindow(windowDuration, slideDuration, defaultPartitioner(
            numPartitions))
    }
    
    def groupByKeyAndWindow(
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, Iterable[V])]
    功能: 同上,这里使用分区器进行分区
    val= ssc.withScope {
        val createCombiner = (v: Iterable[V]) => new ArrayBuffer[V] ++= v
        val mergeValue = (buf: ArrayBuffer[V], v: Iterable[V]) => buf ++= v
        val mergeCombiner = (buf1: ArrayBuffer[V], buf2: ArrayBuffer[V]) => buf1 ++= buf2
        self.groupByKey(partitioner)
            .window(windowDuration, slideDuration)
            .combineByKey[ArrayBuffer[V]](
                createCombiner, mergeValue, mergeCombiner, partitioner)
            .asInstanceOf[DStream[(K, Iterable[V])]]
      }
    
    def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration
    ): DStream[(K, V)]
    功能: 通过在滑动窗口上使用@reduceByKey 返回一个新的离散流,类似于@DStream.reduceByKey(),指示应用在滑动窗口上,新的离散流按照同样的时间间隔产生RDD.使用spark默认的hash分区数量.
    val= ssc.withScope {
    	reduceByKeyAndWindow(
        	reduceFunc, windowDuration, self.slideDuration, defaultPartitioner())
    }
    
    def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration
    ): DStream[(K, V)]
    功能: 同上,指示这里指定了滑动周期,滑动周期是窗口的滑动时间间隔,必须是离散流批次间隔的整数倍
    val=ssc.withScope {
    	reduceByKeyAndWindow(
            reduceFunc, windowDuration, slideDuration, defaultPartitioner())
  	}
    
    def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int
    ): DStream[(K, V)]
    功能: 同上,这里指定了hash分区的分区数量
    val= ssc.withScope {
    	reduceByKeyAndWindow(reduceFunc, windowDuration, slideDuration,
      	defaultPartitioner(numPartitions))
  	}
    
    def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner
    ): DStream[(K, V)]
    功能: 同上,这里使用指定的分区器
    val= ssc.withScope {
    	self.reduceByKey(reduceFunc, partitioner)
        	.window(windowDuration, slideDuration)
        	.reduceByKey(reduceFunc, partitioner)
  	}
    
    def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration = self.slideDuration,
      numPartitions: Int = ssc.sc.defaultParallelism,
      filterFunc: ((K, V)) => Boolean = null
    ): DStream[(K, V)] 
    功能: 通过在滑动窗口上使用@reduceByKey获取的新的离散流,使用了旧的参考值计算新窗口的聚合值
    1. 聚合进入窗口的数据
    2. 反转聚合留在窗口中的旧数据.
    这个要比@reduceByKeyAndWindow高效,因为不需要使用反转聚合函数,但是仅仅使用可逆聚合函数@invReduceFunc,使用spark默认的hash分区器和分区数量.
    输入参数:
    reduceFunc	聚合函数
    invReduceFunc	反转聚合函数
    	对于任意的x,y满足invReduceFunc(reduceFunc(x, y), x) = y
    filterFunc	过滤函数
    val= ssc.withScope {
        reduceByKeyAndWindow(
          reduceFunc, invReduceFunc, windowDuration,
          slideDuration, defaultPartitioner(numPartitions), filterFunc
        )
      }
    
    def reduceByKeyAndWindow(
      reduceFunc: (V, V) => V,
      invReduceFunc: (V, V) => V,
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner,
      filterFunc: ((K, V)) => Boolean
    ): DStream[(K, V)]
    功能: 同上,这里指定了指定分区器进行分区
    val= ssc.withScope {
        val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
        val cleanedInvReduceFunc = ssc.sc.clean(invReduceFunc)
        val cleanedFilterFunc = if (filterFunc != null) Some(
            ssc.sc.clean(filterFunc)) else None
        new ReducedWindowedDStream[K, V](
            self, cleanedReduceFunc, cleanedInvReduceFunc, cleanedFilterFunc,
            windowDuration, slideDuration, partitioner
        )
    }
    
    def mapWithState[StateType: ClassTag, MappedType: ClassTag](
      spec: StateSpec[K, V, StateType, MappedType]
    ): MapWithStateDStream[K, V, StateType, MappedType] 
    功能: 通过对每个kv元素使用函数获取@MapWithStateDStream,同时维护每个key的状态数据,这个转换操作的映射操作(分区器,超时时间,输出状态数据等).可以使用@StateSpec	指定,状态数据可以在@State中获取.
    下述是@mapWithState的使用
    {{{
        def mappingFunction(
            key: String, value: Option[Int], state: State[Int]): Option[String]
        这个映射函数维护完整状态,并返回一个字符串
        {
            // 使用state.exists(), state.get(), state.update() and state.remove()
            // 操作去管理状态,并返回相应的字符串
            val spec = StateSpec.function(mappingFunction).numPartitions(10)
            val mapWithStateDStream = keyValueDStream.
            	mapWithState[StateType, MappedType](spec)
        }
    }}}
    输入参数:
    	spec	转换规范
    	StateType	状态类型
    	MappedType	映射数据类型
    val= new MapWithStateDStreamImpl[K, V, StateType, MappedType](
      self,
      spec.asInstanceOf[StateSpecImpl[K, V, StateType, MappedType]]
    )
    
    def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S]
    ): DStream[(K, S)] 
    功能: 使用给定函数更新每个key的状态,并返回新的带有状态的离散流.每个批次中,@updateFunc会更新每个状态,就算它们没有value值,hash分区使用默认分区数量.
    输入参数:
    updateFunc	更新函数
    val= ssc.withScope {
        updateStateByKey(updateFunc, defaultPartitioner())
    }
    
    def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      numPartitions: Int
    ): DStream[(K, S)] 
    功能: 同上,这里指定了分区数量
    val= ssc.withScope {
        updateStateByKey(updateFunc, defaultPartitioner(numPartitions))
    }
    
    def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner
    ): DStream[(K, S)]
    功能: 同上,这里指定了分区器
    val= ssc.withScope {
        val cleanedUpdateF = sparkContext.clean(updateFunc)
        val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
          iterator.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
        }
        updateStateByKey(newUpdateFunc, partitioner, true)
      }
    
    def updateStateByKey[S: ClassTag](
      updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      partitioner: Partitioner,
      rememberPartitioner: Boolean): DStream[(K, S)]
    功能: 同上,这里设定了@rememberPartitioner,这个参数指示是否需要在生产RDD的时候记住分区器,在@StateDStream中即首需要保持这个离散流RDD的分区情况.
    val= ssc.withScope {
        val cleanedFunc = ssc.sc.clean(updateFunc)
        val newUpdateFunc = (_: Time, it: Iterator[(K, Seq[V], Option[S])]) => {
          cleanedFunc(it)
        }
        new StateDStream(self, newUpdateFunc, partitioner, rememberPartitioner, None)
      }
    
    def updateStateByKey[S: ClassTag](
      updateFunc: (Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner,
      initialRDD: RDD[(K, S)]
    ): DStream[(K, S)] 
    功能: 同上,这里设定了初始化RDD
    val= ssc.withScope {
        val cleanedUpdateF = sparkContext.clean(updateFunc)
        val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
          iterator.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
        }
        updateStateByKey(newUpdateFunc, partitioner, true, initialRDD)
      }
    
    def updateStateByKey[S: ClassTag](
      updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
      partitioner: Partitioner,
      rememberPartitioner: Boolean,
      initialRDD: RDD[(K, S)]): DStream[(K, S)]
    功能: 同上
    val= ssc.withScope {
        val cleanedFunc = ssc.sc.clean(updateFunc)
        val newUpdateFunc = (_: Time, it: Iterator[(K, Seq[V], Option[S])]) => {
          cleanedFunc(it)
        }
        new StateDStream(
            self, newUpdateFunc, partitioner, rememberPartitioner, Some(initialRDD))
  	}
    
    def updateStateByKey[S: ClassTag](
      updateFunc: (Time, K, Seq[V], Option[S]) => Option[S],
      partitioner: Partitioner,
      rememberPartitioner: Boolean,
      initialRDD: Option[RDD[(K, S)]] = None): DStream[(K, S)] 
    功能: 同上
    val= ssc.withScope {
        val cleanedFunc = ssc.sc.clean(updateFunc)
        val newUpdateFunc = (time: Time, iterator: Iterator[(K, Seq[V], Option[S])]) => {
          iterator.flatMap(t => cleanedFunc(time, t._1, t._2, t._3).map(s => (t._1, s)))
        }
        new StateDStream(
            self, newUpdateFunc, partitioner, rememberPartitioner, initialRDD)
      }
    
    def mapValues[U: ClassTag](mapValuesFunc: V => U): DStream[(K, U)] 
    功能: 使用map函数在kv对上,在不改变key的情况下返回新的离散流
    val= new MapValuedDStream[K, V, U](self, sparkContext.clean(mapValuesFunc))
    
    def flatMapValues[U: ClassTag](
      flatMapValuesFunc: V => TraversableOnce[U]
    ): DStream[(K, U)]
    功能: 对每个kv对使用flatMap函数,获取新的离散流
    val= ssc.withScope {
        new FlatMapValuedDStream[K, V, U](self, sparkContext.clean(flatMapValuesFunc))
      }
    
    def cogroup[W: ClassTag](
      other: DStream[(K, W)]): DStream[(K, (Iterable[V], Iterable[W]))] 
    功能: 与其他离散流进行聚合
    val= ssc.withScope {
        cogroup(other, defaultPartitioner())
      }
    
    def cogroup[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int): DStream[(K, (Iterable[V], Iterable[W]))] 
    功能: 同上,指定了分区数量
    val= ssc.withScope {
        cogroup(other, defaultPartitioner(numPartitions))
      }
    
    def cogroup[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (Iterable[V], Iterable[W]))] 
    功能: 同上,指定了分区器
    val= ssc.withScope {
        self.transformWith(
          other,
          (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.cogroup(rdd2, partitioner)
        )
      }
    
    def join[W: ClassTag](other: DStream[(K, W)]): DStream[(K, (V, W))]
    功能: 获取两个RDD join的结果
    val= ssc.withScope {
        join[W](other, defaultPartitioner())
      }
    
    def join[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int): DStream[(K, (V, W))]
    功能: 同上,指定了分区数量
    val= ssc.withScope {
        join[W](other, defaultPartitioner(numPartitions))
      }
    
    def join[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (V, W))] 
    功能: 同上,指定了分区器
    val= ssc.withScope {
        self.transformWith(
          other,
          (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.join(rdd2, partitioner)
        )
      }
    
    def leftOuterJoin[W: ClassTag](
      other: DStream[(K, W)]): DStream[(K, (V, Option[W]))] 
    功能: 与指定离散流进行left join
    val= ssc.withScope {
        leftOuterJoin[W](other, defaultPartitioner())
      }
    
    def leftOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int
    ): DStream[(K, (V, Option[W]))] 
    功能: 同上,指定了分区数量
    val= ssc.withScope {
        leftOuterJoin[W](other, defaultPartitioner(numPartitions))
      }
    
    def leftOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (V, Option[W]))] 
    功能: 同上,指定了分区数量
    val= ssc.withScope {
        self.transformWith(
          other,
          (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.leftOuterJoin(rdd2, partitioner)
        )
      }
    
    def rightOuterJoin[W: ClassTag](
      other: DStream[(K, W)]): DStream[(K, (Option[V], W))] 
    功能: 入指定的离散流进行right join
    val= ssc.withScope {
        rightOuterJoin[W](other, defaultPartitioner())
      }
    
    def rightOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int
    ): DStream[(K, (Option[V], W))]
    功能: 同上,指定了分区数量
    val= ssc.withScope {
        rightOuterJoin[W](other, defaultPartitioner(numPartitions))
      }
    
    def rightOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (Option[V], W))]
    功能: 同上,指定了分区器
    val= ssc.withScope {
        self.transformWith(
          other,
          (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => 
            rdd1.rightOuterJoin(rdd2, partitioner)
        )
      }
    
    def fullOuterJoin[W: ClassTag](
      other: DStream[(K, W)]): DStream[(K, (Option[V], Option[W]))]
    功能: 与指定离散流进行full join
    val= ssc.withScope {
        fullOuterJoin[W](other, defaultPartitioner())
      }
    
    def fullOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      numPartitions: Int
    ): DStream[(K, (Option[V], Option[W]))]
    功能: 同上,指定了分区数量
    val= ssc.withScope {
        fullOuterJoin[W](other, defaultPartitioner(numPartitions))
      }
    
    def fullOuterJoin[W: ClassTag](
      other: DStream[(K, W)],
      partitioner: Partitioner
    ): DStream[(K, (Option[V], Option[W]))]
    功能: 同上,指定了分区器
    val= ssc.withScope {
        self.transformWith(
          other,
          (rdd1: RDD[(K, V)], rdd2: RDD[(K, W)]) => rdd1.fullOuterJoin(rdd2, partitioner)
        )
      }
    
    def keyClass: Class[_] = kt.runtimeClass
    def valueClass: Class[_] = vt.runtimeClass
    功能: 获取key/value的类名称
    
    def saveAsHadoopFiles[F <: OutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassTag[F]): Unit 
    功能: 保存当前离散流中的RDD到hadoop文件中,文件名包含指定前缀@prefix和后缀@suffix,中间是时间(ms)
    ssc.withScope {
        saveAsHadoopFiles(prefix, suffix, keyClass, valueClass,
          fm.runtimeClass.asInstanceOf[Class[F]])
    }
    
    def saveAsHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf(ssc.sparkContext.hadoopConfiguration)
    ): Unit
    功能: 同上
    ssc.withScope {
        val serializableConf = new SerializableJobConf(conf)
        val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
          val file = rddToFileName(prefix, suffix, time)
          rdd.saveAsHadoopFile(file, keyClass, valueClass, outputFormatClass,
            new JobConf(serializableConf.value))
        }
        self.foreachRDD(saveFunc)
    }
    
    def saveAsNewAPIHadoopFiles[F <: NewOutputFormat[K, V]](
      prefix: String,
      suffix: String
    )(implicit fm: ClassTag[F]): Unit
    功能: 同上
    ssc.withScope {
        saveAsNewAPIHadoopFiles(prefix, suffix, keyClass, valueClass,
          fm.runtimeClass.asInstanceOf[Class[F]])
    }
    
    def saveAsNewAPIHadoopFiles(
      prefix: String,
      suffix: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = ssc.sparkContext.hadoopConfiguration
    ): Unit 
    功能: 同上
    ssc.withScope {
        val serializableConf = new SerializableConfiguration(conf)
        val saveFunc = (rdd: RDD[(K, V)], time: Time) => {
          val file = rddToFileName(prefix, suffix, time)
          rdd.saveAsNewAPIHadoopFile(
            file, keyClass, valueClass, outputFormatClass, serializableConf.value)
        }
        self.foreachRDD(saveFunc)
    }
}
```

#### PluggableInputDStream

```scala
private[streaming]
class PluggableInputDStream[T: ClassTag](
  _ssc: StreamingContext,
  receiver: Receiver[T]) extends ReceiverInputDStream[T](_ssc) {
    介绍: 可插入离散输入流
    操作集:
    def getReceiver(): Receiver[T] = receiver
    功能: 获取接收器
}
```

#### QueueInputDStream

```scala
private[streaming]
class QueueInputDStream[T: ClassTag](
    ssc: StreamingContext,
    val queue: Queue[RDD[T]],
    oneAtATime: Boolean,
    defaultRDD: RDD[T]
) extends InputDStream[T](ssc) {
    介绍: 队列输入离散流
    构造器参数:
    ssc	streaming上下文
    queue	RDD队列
    oneAtATime	是否读取缓冲区数据
    defaultRDD	默认RDD
    操作集:
    def start(): Unit = { }
    功能: 启动输入离散流
    
    def stop(): Unit = { }
    功能: 关闭离散流
    
    def readObject(in: ObjectInputStream): Unit
    功能: 反序列化(不支持)
    throw new NotSerializableException("queueStream doesn't support checkpointing. " +
      "Please don't use queueStream when checkpointing is enabled.")
    
    def writeObject(oos: ObjectOutputStream): Unit
    功能: 序列化(不支持)
    logWarning("queueStream doesn't support checkpointing")
    
    def compute(validTime: Time): Option[RDD[T]] 
    功能: 计算指定时刻离散流的RDD
    1. 将队列中的RDD置于缓冲区中
    val buffer = new ArrayBuffer[RDD[T]]()
    queue.synchronized {
      if (oneAtATime && queue.nonEmpty) {
        buffer += queue.dequeue()
      } else {
        buffer ++= queue
        queue.clear()
      }
    }
    2. 获取此时的RDD
    val= if (buffer.nonEmpty) {
      if (oneAtATime) {
        Some(buffer.head)
      } else {
        Some(new UnionRDD(context.sc, buffer.toSeq))
      }
    } else if (defaultRDD != null) {
      Some(defaultRDD)
    } else {
      Some(ssc.sparkContext.emptyRDD)
    }
}
```

#### RawInputDStream

```scala
private[streaming]
class RawInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    host: String,
    port: Int,
    storageLevel: StorageLevel
) extends ReceiverInputDStream[T](_ssc) with Logging {
    介绍: 原始输入离散流
    这个输入流可以从指定的网络地址读取序列化数据块,数据块会直接插入数据块存储中,这种方式是sparkStreaming获取数据块最快的方式,尽管需要发送器获取批量数据并使用配置的序列化方案进行序列化.
    构造器参数:
    _ssc	streaming上下文
    host	主机名称
    port	端口号
    storageLevel	存储等级
    操作集:
    def getReceiver(): Receiver[T]
    功能: 获取接收器
    val= new RawNetworkReceiver(host, port, storageLevel).asInstanceOf[Receiver[T]]
}
```

```scala
private[streaming]
class RawNetworkReceiver(host: String, port: Int, storageLevel: StorageLevel)
extends Receiver[Any](storageLevel) with Logging {
    介绍: 原始网络接收器
    属性:
    #name @blockPushingThread: Thread = null	数据块添加线程
    操作集:
    def onStart(): Unit
    功能: 启动接收器
    1. 启动目标地址的socket,并保持读取
    logInfo("Connecting to " + host + ":" + port)
    val channel = SocketChannel.open()
    channel.configureBlocking(true)
    channel.connect(new InetSocketAddress(host, port))
    logInfo("Connected to " + host + ":" + port)
    val queue = new ArrayBlockingQueue[ByteBuffer](2)
    2. 设置数据块添加线程的线程处理过程
    blockPushingThread = new Thread {
      setDaemon(true)
      override def run(): Unit = {
        var nextBlockNumber = 0
        while (true) {
          val buffer = queue.take()
          nextBlockNumber += 1
          store(buffer)
        }
      }
    }
    blockPushingThread.start()
    3. 保持读取数据到队列中
    val lengthBuffer = ByteBuffer.allocate(4)
    while (true) {
      lengthBuffer.clear()
      readFully(channel, lengthBuffer)
      lengthBuffer.flip()
      val length = lengthBuffer.getInt()
      val dataBuffer = ByteBuffer.allocate(length)
      readFully(channel, dataBuffer)
      dataBuffer.flip()
      logInfo("Read a block with " + length + " bytes")
      queue.put(dataBuffer)
    }
    
    def onStop(): Unit
    功能: 停止接收器
    if (blockPushingThread != null) blockPushingThread.interrupt()
    
    def readFully(channel: ReadableByteChannel, dest: ByteBuffer): Unit
    功能: 读取指定通道的缓冲数据
    while (dest.position() < dest.limit()) {
      if (channel.read(dest) == -1) {
        throw new EOFException("End of channel")
      }
    }
}
```

#### ReceiverInputDStream

```markdown
介绍:
 	这个抽象类顶一个输入离散流@InputDStream,这个离散流必须啊哟启动worker上的接收器,用于接收外部数据.
    @ReceiverInputDStream 指定的实现必须定义@getReceiver函数,用于获取接收器的对象,这个会被发送给worker,用于接收数据
    构造器参数:
    _ssc	streaming上下文
    T	这个stream的类型
```

```scala
abstract class ReceiverInputDStream[T: ClassTag](_ssc: StreamingContext)
extends InputDStream[T](_ssc) {
    介绍: 接收器输入离散流
    属性:
    #name @rateController: Option[RateController] 	比例控制器
    val= if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new ReceiverRateController(id, RateEstimator.create(
          ssc.conf, ssc.graph.batchDuration)))
    } else {
      None
    }
    操作集:
    def getReceiver(): Receiver[T]
    功能: 获取接收器对象,这个对象会使用在worker上,用于接收数据
    
    def start(): Unit = {}
    def stop(): Unit = {}
    功能:离散流的启动/停止,这里不需要设置,因为接收器定位器@ReceiverTracker已经处理
    
    def compute(validTime: Time): Option[RDD[T]] 
    功能: 通过接收器接受的数据块生成RDD
    val blockRDD = {
      if (validTime < graph.startTime) {
          // 在上下文启动之前调用,则返回空RDD,这个可能发送在没有wal的情况下,试图恢复驱动器的失败,
          // 用于恢复之前失败的数据
        new BlockRDD[T](ssc.sc, Array.empty)
      } else {
          // 否则,询问定位器所有数据块(已经分配到这个批次的),并返回相应的数据块RDD
        val receiverTracker = ssc.scheduler.receiverTracker
        val blockInfos = receiverTracker.getBlocksOfBatch(validTime).getOrElse(
            id, Seq.empty)
		// 注册数据块信息到输入信息定位器中@InputInfoTracker
        val inputInfo = StreamInputInfo(id, blockInfos.flatMap(_.numRecords).sum)
        ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
		// 创建数据块RDD
        createBlockRDD(validTime, blockInfos)
      }
    }
    
    def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] 
    功能: 创建数据块RDD
    if (blockInfos.nonEmpty) {
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray
        // 确定是否wal处理了当前所有的数据块
      val areWALRecordHandlesPresent = blockInfos.forall { _.walRecordHandleOption.nonEmpty }
      if (areWALRecordHandlesPresent) {
          //如果所有的数据块由wal处理，则创建一个@WALBackedBlockRDD
        val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
        val walRecordHandles = blockInfos.map { _.walRecordHandleOption.get }.toArray
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, blockIds, walRecordHandles, isBlockIdValid)
      } else {
          // 否则就创建一个普通的数据块RDD@BlockRDD(
          // 如果WAL信息数据块数据块有一些但是不是所有的都是这样的也需要这样处理)
        if (blockInfos.exists(_.walRecordHandleOption.nonEmpty)) {
          if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
            logError("Some blocks do not have Write Ahead Log information; " +
              "this is unexpected and data may not be recoverable after driver failures")
          } else {
            logWarning("Some blocks have Write Ahead Log information; 
            this is unexpected")
          }
        }
        val validBlockIds = blockIds.filter { id =>
          ssc.sparkContext.env.blockManager.master.contains(id)
        }
        if (validBlockIds.length != blockIds.length) {
          logWarning("Some blocks could not be recovered as they were not 
          found in memory. " +
            "To prevent such data loss, enable Write Ahead Log (see programming guide " +
            "for more details.")
        }
        new BlockRDD[T](ssc.sc, validBlockIds)
      }
    } else {
        // 如果当前没有数据块,根据配置信息创建@WriteAheadLogBackedBlockRDD或者@BlockRDD
      if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, Array.empty, Array.empty, Array.empty)
      } else {
        new BlockRDD[T](ssc.sc, Array.empty)
      }
    }
    
    内部类:
    private[streaming] class ReceiverRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
        介绍: 接收器比例控制器
        构造器参数:
        	id	接收器编号
        	estimator	比例估算器
        def publish(rate: Long): Unit
        功能: 发布比例信息
        ssc.scheduler.receiverTracker.sendRateUpdate(id, rate)
    }
}
```

#### ReducedWindowedDStream

```scala
private[streaming]
class ReducedWindowedDStream[K: ClassTag, V: ClassTag](
    parent: DStream[(K, V)],
    reduceFunc: (V, V) => V,
    invReduceFunc: (V, V) => V,
    filterFunc: Option[((K, V)) => Boolean],
    _windowDuration: Duration,
    _slideDuration: Duration,
    partitioner: Partitioner
) extends DStream[(K, V)](parent.ssc) {
    介绍: 经过聚合的窗口离散流
    构造器参数:
        parent	父级离散流
        reduceFunc	聚合函数
        invReduceFunc	聚合函数
        filterFunc	过滤函数
        _windowDuration	窗口周期
        _slideDuration	滑动周期
        partitioner	分区器
    属性:
    #name @reducedStream = parent.reduceByKey(reduceFunc, partitioner)	聚合完成的离散流
    #name @mustCheckpoint = true	是否需要设置检查点
    初始化操作:
    require(_windowDuration.isMultipleOf(parent.slideDuration),
    	"The window duration of ReducedWindowedDStream (" + _windowDuration + ") " +
    	"must be multiple of the slide duration of parent DStream (
    	" + parent.slideDuration + ")"
	  )
    require(_slideDuration.isMultipleOf(parent.slideDuration),
    	"The slide duration of ReducedWindowedDStream (" + _slideDuration + ") " +
      	"must be multiple of the slide duration of parent DStream (" +
            parent.slideDuration + ")"
  	)
    功能: 校验窗口周期/滑动周期与父级离散流的关系(必须是父级的整数倍)
    
    super.persist(StorageLevel.MEMORY_ONLY_SER)
    reducedStream.persist(StorageLevel.MEMORY_ONLY_SER)
    功能: 默认持久化RDD到内存中,因为这些RDD需要被重新使用
    
    操作集:
    def windowDuration: Duration = _windowDuration
    功能: 获取窗口周期
    
    def dependencies: List[DStream[_]] = List(reducedStream)
    功能: 获取依赖列表
    
    def slideDuration: Duration = _slideDuration
    功能: 获取滑动周期
    
    def parentRememberDuration: Duration = rememberDuration + windowDuration
    功能: 获取父级离散流记忆周期(=窗口周期+父级记忆周期)
    
    def persist(storageLevel: StorageLevel): DStream[(K, V)]
    功能: 持久化离散流中的RDD,使用指定的存储等级@storageLevel
    super.persist(storageLevel)
    reducedStream.persist(storageLevel)
    val= this
    
    def checkpoint(interval: Duration): DStream[(K, V)] 
    功能: 按照指定时间间隔设置检查点
    super.checkpoint(interval)
    val= this
    
    def compute(validTime: Time): Option[RDD[(K, V)]] 
    功能: 计算指定时刻离散流的RDD
    1. 获取基本参数
    val reduceF = reduceFunc
    val invReduceF = invReduceFunc
    val currentTime = validTime
    val currentWindow = new Interval(currentTime - windowDuration + parent.slideDuration,
      currentTime) // 当前窗口
    val previousWindow = currentWindow - slideDuration // 上一个窗口=当前窗口-滑动周期
    logDebug("Window time = " + windowDuration)
    logDebug("Slide time = " + slideDuration)
    logDebug("Zero time = " + zeroTime)
    logDebug("Current window = " + currentWindow)
    logDebug("Previous window = " + previousWindow)
    2. 获取聚合后的RDD(旧的时间区域)
    (previousWindow.beginTime-> currentWindow.beginTime - parent.slideDuration)
    val oldRDDs =
      reducedStream.slice(
          previousWindow.beginTime, currentWindow.beginTime - parent.slideDuration)
    logDebug("# old RDDs = " + oldRDDs.size)
    3. 获取聚合后的RDD(新的时间区域)
    val newRDDs =
      reducedStream.slice(
          previousWindow.endTime + parent.slideDuration, currentWindow.endTime)
    logDebug("# new RDDs = " + newRDDs.size)
    4. 获取之前窗口的聚合RDD值
    val previousWindowRDD =
      getOrCompute(previousWindow.endTime).getOrElse(ssc.sc.makeRDD(Seq[(K, V)]()))
    5. 使RDD列表中的元素互相聚合,用于获取聚合值
    val allRDDs = 
    	new ArrayBuffer[RDD[(K, V)]]() += previousWindowRDD ++= oldRDDs ++= newRDDs
    6. 获取聚合后的RDD
    val cogroupedRDD = new CoGroupedRDD[K](
        allRDDs.toSeq.asInstanceOf[Seq[RDD[(K, _)]]],partitioner)
    7. 合并旧RDD和新的RDD
    val numOldValues = oldRDDs.size
    val numNewValues = newRDDs.size
    val mergeValues = (arrayOfValues: Array[Iterable[V]]) => {
      if (arrayOfValues.length != 1 + numOldValues + numNewValues) {
        throw new Exception("Unexpected number of sequences of reduced values")
      }
      val oldValues = (1 to numOldValues).map(
          i => arrayOfValues(i)).filter(!_.isEmpty).map(_.head)
      val newValues =
        (1 to numNewValues).map(i => arrayOfValues(
            numOldValues + i)).filter(!_.isEmpty).map(_.head)
      if (arrayOfValues(0).isEmpty) {
        if (newValues.isEmpty) {
          throw new Exception("Neither previous window has value for key, 
          nor new values found. " +
            "Are you sure your key class hashes consistently?")
        }
        newValues.reduce(reduceF) // return
      } else {
        var tempValue = arrayOfValues(0).head
        if (!oldValues.isEmpty) {
          tempValue = invReduceF(tempValue, oldValues.reduce(reduceF))
        }
        if (!newValues.isEmpty) {
          tempValue = reduceF(tempValue, newValues.reduce(reduceF))
        }
        tempValue // return
      }
    }
    8. 获取合并的RDD并返回
    val mergedValuesRDD = cogroupedRDD.asInstanceOf[RDD[(K, Array[Iterable[V]])]]
      .mapValues(mergeValues)
    val= if (filterFunc.isDefined) {
      Some(mergedValuesRDD.filter(filterFunc.get))
    } else {
      Some(mergedValuesRDD)
    }
}
```

#### ShuffledDStream

```scala
private[streaming]
class ShuffledDStream[K: ClassTag, V: ClassTag, C: ClassTag](
    parent: DStream[(K, V)],
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true
) extends DStream[(K, C)] (parent.ssc) {
    介绍: shuffle离散流
    构造器参数:
    parent	父级离散流
    createCombiner	创建合并器函数
    mergeValue	合并函数
    mergeCombiner	归并合并器值
    partitioner	分区器
    mapSideCombine	是否开启map侧归并
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[(K, C)]]
    功能: 计算指定时间离散流的RDD
    val= parent.getOrCompute(validTime) match {
      case Some(rdd) => Some(rdd.combineByKey[C](
          createCombiner, mergeValue, mergeCombiner, partitioner, mapSideCombine))
      case None => None
    }
}
```

#### SocketInputDStream

```scala
private[streaming]
class SocketInputDStream[T: ClassTag](
    _ssc: StreamingContext,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
) extends ReceiverInputDStream[T](_ssc) {
    介绍: socket输入离散流
    构造器参数:
    host	主机名
    port	端口号
    bytesToObjects	输入流转换函数
    storageLevel	存储等级
    操作集:
    def getReceiver(): Receiver[T] 
    功能: 获取接收器
    val= new SocketReceiver(host, port, bytesToObjects, storageLevel)
}
```

```scala
private[streaming]
class SocketReceiver[T: ClassTag](
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends Receiver[T](storageLevel) with Logging {
    介绍: socker接收器
    #name @socket: Socket = _	socket
    操作集:
    def onStart(): Unit
    功能: 启动接收器
    logInfo(s"Connecting to $host:$port")
    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }
    logInfo(s"Connected to $host:$port")
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run(): Unit = { receive() }
    }.start()
    
    def onStop(): Unit
    功能: 停止接收器
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
        logInfo(s"Closed socket to $host:$port")
      }
    }
    
    def receive(): Unit
    功能: 创建socket连接,接收数据直到接收器停止
    try {
      val iterator = bytesToObjects(socket.getInputStream())
      while(!isStopped && iterator.hasNext) {
        store(iterator.next())
      }
      if (!isStopped()) {
        restart("Socket data stream had no more data")
      } else {
        logInfo("Stopped receiving")
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Error receiving data", e)
        restart("Error receiving data", e)
    } finally {
      onStop()
    }
}
```

```scala
private[streaming]
object SocketReceiver  {
    def bytesToLines(inputStream: InputStream): Iterator[String]
    功能: 将输入流内容转换为字符串
    val dataInputStream = new BufferedReader(
      new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    new NextIterator[String] {
      protected override def getNext() = {
        val nextValue = dataInputStream.readLine()
        if (nextValue == null) {
          finished = true
        }
        nextValue
      }
      protected override def close(): Unit = {
        dataInputStream.close()
      }
    }
}
```

#### StateDStream

```scala
private[streaming]
class StateDStream[K: ClassTag, V: ClassTag, S: ClassTag](
    parent: DStream[(K, V)],
    updateFunc: (Time, Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
    partitioner: Partitioner,
    preservePartitioning: Boolean,
    initialRDD: Option[RDD[(K, S)]]
) extends DStream[(K, S)](parent.ssc) {
    介绍: 状态离散流
    构造器参数:
    parent	父级离散流
    updateFunc	更新函数
    partitioner	分区器
    preservePartitioning	是否保留分区
    initialRDD	初始化RDD
    属性:
    #name @mustCheckpoint = true	是否需要设置检查点
    初始化操作:
    super.persist(StorageLevel.MEMORY_ONLY_SER)
    功能: 将离散流中的数据存储到内存中
    
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def computeUsingPreviousRDD(
      batchTime: Time,
      parentRDD: RDD[(K, V)],
      prevStateRDD: RDD[(K, S)])
    功能: 计算正在使用的前一个RDD
    1. 定义函数用于mapPartition操作,先映射聚合的元组再更新
    val updateFuncLocal = updateFunc
    val finalFunc = (iterator: Iterator[(K, (Iterable[V], Iterable[S]))]) => {
      val i = iterator.map { t =>
        val itr = t._2._2.iterator
        val headOption = if (itr.hasNext) Some(itr.next()) else None
        (t._1, t._2._1.toSeq, headOption)
      }
      updateFuncLocal(batchTime, i)
    }
    val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
    val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
    val= Some(stateRDD)
    
    def compute(validTime: Time): Option[RDD[(K, S)]] 
    功能: 计算指定时间离散中的RDD
    getOrCompute(validTime - slideDuration) match {
      case Some(prevStateRDD) =>
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) =>    // If parent RDD exists, then compute as usual
            computeUsingPreviousRDD (validTime, parentRDD, prevStateRDD)
          case None =>     // If parent RDD does not exist
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, S)]) => {
              val i = iterator.map(t => (t._1, Seq.empty[V], Option(t._2)))
              updateFuncLocal(validTime, i)
            }
            val stateRDD = prevStateRDD.mapPartitions(finalFunc, preservePartitioning)
            Some(stateRDD)
        }
      case None =>
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) =>   // If parent RDD exists, then compute as usual
            initialRDD match {
              case None =>
                val updateFuncLocal = updateFunc
                val finalFunc = (iterator: Iterator[(K, Iterable[V])]) => {
                  updateFuncLocal (validTime,
                    iterator.map (tuple => (tuple._1, tuple._2.toSeq, None)))
                }
                val groupedRDD = parentRDD.groupByKey(partitioner)
                val sessionRDD = groupedRDD.mapPartitions(
                    finalFunc, preservePartitioning)
                Some (sessionRDD)
              case Some (initialStateRDD) =>
                computeUsingPreviousRDD(validTime, parentRDD, initialStateRDD)
            }
          case None => 
            // logDebug("Not generating state RDD (no previous state, no parent)")
            None
        }
    }
}
```

#### TransformedDStream

```scala
private[streaming]
class TransformedDStream[U: ClassTag] (
    parents: Seq[DStream[_]],
    transformFunc: (Seq[RDD[_]], Time) => RDD[U]
) extends DStream[U](parents.head.ssc) {
    介绍: 带有转换函数的离散流
    构造器参数:
    parents	父级离散流
    transformFunc	转换函数
    初始化操作:
    require(parents.nonEmpty, "List of DStreams to transform is empty")
    require(parents.map(_.ssc).distinct.size == 1, "Some 
    	of the DStreams have different contexts")
    require(parents.map(_.slideDuration).distinct.size == 1,
    	"Some of the DStreams have different slide durations")
    功能: 父级RDD校验
    
    def dependencies: List[DStream[_]] = parents.toList
    功能: 获取依赖列表
    
    def slideDuration: Duration = parents.head.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]]
    功能: 计算指定时间离散流RDD
    1. 获取父级离散流
    val parentRDDs = parents.map { parent => parent.getOrCompute(validTime).getOrElse(
      throw new SparkException(s"Couldn't generate RDD from parent at time $validTime"))
    }
    2. 获取转换完毕的RDD
    val transformedRDD = transformFunc(parentRDDs, validTime)
    if (transformedRDD == null) {
      throw new SparkException("Transform function must not return null. " +
        "Return SparkContext.emptyRDD() instead to represent no element " +
        "as the result of transformation.")
    }
    val= Some(transformedRDD)
    
    def createRDDWithLocalProperties[U](
      time: Time,
      displayInnerRDDOps: Boolean)(body: => U): U 
    功能: 使用本地参数创建RDD
    包装代码块,用于传递RDD(在body中创建),当@displayInnerRDDOps为true的时候,内部产生的RDD会显示在webUI上.
    val= super.createRDDWithLocalProperties(time, displayInnerRDDOps = true)(body)
}
```

#### UnionDStream

```scala
private[streaming]
class UnionDStream[T: ClassTag](parents: Array[DStream[T]])
extends DStream[T](parents.head.ssc) {
    介绍: 可以进行union操作的离散流
    初始化操作:
    require(parents.length > 0, "List of DStreams to union is empty")
    require(parents.map(_.ssc).distinct.length == 1, "Some of 
    	the DStreams have different contexts")
    require(parents.map(_.slideDuration).distinct.length == 1,
 	   "Some of the DStreams have different slide durations")
    功能: 父级离散流校验
    
    def dependencies: List[DStream[_]] = parents.toList
    功能: 获取依赖列表
    
    def slideDuration: Duration = parents.head.slideDuration
    功能: 获取滑动周期
    
 	def compute(validTime: Time): Option[RDD[T]]
    功能: 计算指定时间离散流的RDD
    val rdds = new ArrayBuffer[RDD[T]]()
    parents.map(_.getOrCompute(validTime)).foreach {
      case Some(rdd) => rdds += rdd
      case None => throw new SparkException("Could not generate RDD from a parent 
      for unifying at" +s" time $validTime")
    }
    if (rdds.nonEmpty) {
      Some(ssc.sc.union(rdds))
    } else {
      None
    }
}
```

#### WindowedDStream

```scala
private[streaming]
class WindowedDStream[T: ClassTag](
    parent: DStream[T],
    _windowDuration: Duration,
    _slideDuration: Duration)
extends DStream[T](parent.ssc) {
    介绍: 窗口离散流
    构造器参数:
    parent	父级离散流
    _windowDuration	窗口周期
    _slideDuration	滑动周期
    初始化操作:
    if (!_windowDuration.isMultipleOf(parent.slideDuration)) {
    	throw new Exception("The window duration of windowed DStream (
    	" + _windowDuration + ") " +
    	"must be a multiple of the slide duration of parent DStream 
    	(" + parent.slideDuration + ")")
  	}
    if (!_slideDuration.isMultipleOf(parent.slideDuration)) {
    	throw new Exception("The slide duration of windowed DStream (
    	" + _slideDuration + ") " +
    	"must be a multiple of the slide duration of parent DStream (
    	" + parent.slideDuration + ")")
  	}
    功能: 窗口时间校验
    
    parent.persist(StorageLevel.MEMORY_ONLY_SER)
    功能: 持久化父级离散流到内存中
    
    操作集:
    def windowDuration: Duration = _windowDuration
    功能: 获取窗口周期
    
    def dependencies: List[DStream[T]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = _slideDuration
    功能: 获取滑动周期
    
    def parentRememberDuration: Duration = rememberDuration + windowDuration
    功能: 获取父级离散流记忆时间周期
    
    def persist(level: StorageLevel): DStream[T]
    功能: 按照指定存储等级持久化数据
    parent.persist(level)
    val= this
    
    def compute(validTime: Time): Option[RDD[T]] 
    功能: 计算指定时间离散流的RDD
    1. 获取当前时间所处的窗口
    val currentWindow = new Interval(
        validTime - windowDuration + parent.slideDuration, validTime)
    2. 获取窗口中的RDD
    val rddsInWindow = parent.slice(currentWindow)
    val= Some(ssc.sc.union(rddsInWindow))
}
```

