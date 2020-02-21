##### **Spark-Etc**

---

1.  [Aggregator.scala](# Aggregator)
2. [BarrierCoordinator.scala](# BarrierCoordinator)
3. [BarrierTaskContext.scala](# BarrierTaskContext)
4. [BarrierTaskInfo.scala](# BarrierTaskInfo)
5. [ContextCleaner.scala](# ContextCleaner)
6. [Dependency.scala](# Dependency)
7. [ExecutorAllocationClient.scala](# ExecutorAllocationClient)
8. [ExecutorAllocationManager.scala](# ExecutorAllocationManager)
9. [FutureAction.scala](# FutureAction)
10. [Heartbeater.scala](# Heartbeater)
11. [HeartbeatReceiver.scala](# HeartbeatReceiver)
12. [InternalAccumulator.scala](# InternalAccumulator)
13. [InterruptibleIterator.scala](# InterruptibleIterator)
14.  [MapOutputStatistics.scala](# MapOutputStatistics)
15. [ShuffleStatus.scala](# ShuffleStatus)
16. [Partition.scala](# Partition)
17. [Partitioner.scala](# Partitioner)
18. [SecurityManager.scala](# SecurityManager)
19. [SerializableWritable.scala](# SerializableWritable)
20. [SparkConf.scala](# SparkConf)
21. [SparkContext.scala](# SparkContext)
22. [SparkEnv.scala](# SparkEnv)
23. [SparkException.scala](# SparkException)
24.  [SparkFiles.scala](# SparkFiles)
25.  [SparkStatusTracker.scala](# SparkStatusTracker)
26. [SSLOptions.scala](# SSLOptions)
27. [StatusAPIImpl.scala](# StatusAPIImpl)
28. [TaskContext.scala](# TaskContext)
29. [TaskContextImpl.scala](# TaskContextImpl)
30. [TaskEndReason.scala](# TaskEndReason)
31. [TaskKilledException.scala](# TaskKilledException)
32. [TaskNotSerializableException.scala](# TaskNotSerializableException)
33. [TaskOutputFileAlreadyExistException.scala](# TaskOutputFileAlreadyExistException)
34. [TaskState.scala](# TaskState)
35. [TestUtils.scala](# TaskState)

---

#### Aggregator

```scala
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {
    介绍: 用于合并数据的函数集合
    构造器:
        createCombiner	创建聚合初始值的函数
        mergeValue	将聚合值合并成新的值的函数
        mergeCombiners	对于多个@mergeValue 值,将其合并为一个值
    操作集:
    def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)]
    功能: 按照key进行合并
    val= {
        val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue
                                                           , mergeCombiners)
        combiners.insertAll(iter)
        updateMetrics(context, combiners)
        combiners.iterator
      }
    
    def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)]
    功能: 按照key聚合合并器
    val= {
        val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners,
                                                           mergeCombiners)
        combiners.insertAll(iter)
        updateMetrics(context, combiners)
        combiners.iterator
      }
    
    def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit
    功能: 更新度量值
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    }
}
```

#### BarrierCoordinator

```scala
private[spark] class BarrierCoordinator
    timeoutInSecs: Long,
    listenerBus: LiveListenerBus,
override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {
    介绍: 屏蔽的协调者,用于处理全局的同步请求(来自@BarrierTaskContext),每个全局同步请求产生于@BarrierTaskContext.barrier(),使用stageId + stageAttemptId + barrierEpoch 唯一标识.回应所有的块全局同步请求,对于一个收到的@barrier() 调用.如果协调者 不能够再指定时间内收集到指定数量的请求,使得所有请求失败,并抛出异常.    
    构造器参数:
    	timeoutInSecs	超时时间(秒)
    	listenerBus	监听总线
    	rpcEnv	RPC环境
    属性:
    #name @timer = new Timer("BarrierCoordinator barrier epoch increment timer")
    	计时器
    #name @listener #type @SparkListener	spark监听器(用于监听stage的完成)
    val= new SparkListener {
        override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit
            = {
          val stageInfo = stageCompleted.stageInfo
          val barrierId = ContextBarrierId(stageInfo.stageId, stageInfo.attemptNumber)
          cleanupBarrierStage(barrierId)
        }
      }
    #name @states = new ConcurrentHashMap[ContextBarrierId, ContextBarrierState]
    状态列表(对应于激活状态请求)
    #name @clearStateConsumer	清理状态消费者
    val= new Consumer[ContextBarrierState] {
        override def accept(state: ContextBarrierState) = state.clear()
      }
    
    操作集:
    def cleanupBarrierStage(barrierId: ContextBarrierId): Unit 
    功能: 清除指定stage请求编号的@ContextBarrierState状态位
    1. 从状态表中移除
    val barrierState = states.remove(barrierId)
    if (barrierState != null) {
      barrierState.clear()
    }
    
    def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
    功能: 接受并回应指定的RPC回调@context
    case request @ RequestToSync(numTasks, stageId, stageAttemptId, _, _) =>
      val barrierId = ContextBarrierId(stageId, stageAttemptId)
      states.computeIfAbsent(barrierId,
        (key: ContextBarrierId) => new ContextBarrierState(key, numTasks))
      val barrierState = states.get(barrierId)
      val= barrierState.handleRequest(context, request)
}
```

```scala
  private class ContextBarrierState(
      val barrierId: ContextBarrierId,
      val numTasks: Int) {
      介绍: 隔离上下文状态
      #name @barrierEpoch: Int = 0	barrier标识符
      可能一个barrier stage请求含有多个@barrier() 使用@barrierEpoch 用于唯一标识.
      #name @requesters: ArrayBuffer[RpcCallContext] = new ArrayBuffer[RpcCallContext](numTasks)
      RPC请求列表
      #name @timerTask: TimerTask = null	定时器任务
      #name @clearStateConsumer #type @new Consumer[ContextBarrierState]	清理状态消费者
      val= new Consumer[ContextBarrierState] {
        override def accept(state: ContextBarrierState) = state.clear()
      }
      操作集:
      def initTimerTask(state: ContextBarrierState): Unit 
      功能: 初始化定时器任务,使用@barrier()
      timerTask = new TimerTask {
        override def run(): Unit = state.synchronized {
          // 超时则将所有同步操作置为执行失败
          requesters.foreach(_.sendFailure(new SparkException("The coordinator didn't get all " +
            s"barrier sync requests for barrier epoch $barrierEpoch from $barrierId within " +
            s"$timeoutInSecs second(s).")))
          cleanupBarrierStage(barrierId)
        }
      }
      
      def cancelTimerTask(): Unit
      功能: 放弃当前定时器任务,并释放内存
      if (timerTask != null) {
        timerTask.cancel()
        timer.purge()
        timerTask = null
      }
      
      def maybeFinishAllRequesters(
        requesters: ArrayBuffer[RpcCallContext],
        numTasks: Int): Boolean
      功能: 完成所有的屏蔽的同步请求(如果已经获取了所有同步请求)
      val= if (requesters.size == numTasks) {
        requesters.foreach(_.reply(()))
        true
      } else {
        false
      }
      
      def cleanupBarrierStage(barrierId: ContextBarrierId): Unit
      功能: 清除指定的屏蔽stage
      val barrierState = states.remove(barrierId) // 移除状态表中的索引
      if (barrierState != null) {
          barrierState.clear() // 清除内部状态
      }
      
      def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
      功能: 接受并回应指定消息
      case request @ RequestToSync(numTasks, stageId, stageAttemptId, _, _) =>
      val barrierId = ContextBarrierId(stageId, stageAttemptId)
      states.computeIfAbsent(barrierId,
        (key: ContextBarrierId) => new ContextBarrierState(key, numTasks))
      val barrierState = states.get(barrierId)
      barrierState.handleRequest(context, request)
      
      def handleRequest(requester: RpcCallContext, request: RequestToSync): Unit
      功能: 处理请求@requester
      处理全局同步请求,如果收集到足够的请求则执行成功,反之将之前的所有请求会失败
      1. 获取请求编号
      val taskId = request.taskAttemptId // 请求编号
      val epoch = request.barrierEpoch // 屏蔽标识符
      2. 请求任务数量断言(需要等于任务数量)
      require(request.numTasks == numTasks, s"Number of tasks of $barrierId is " +
        s"${request.numTasks} from Task $taskId, previously it was $numTasks.")
      3. 处理请求
      if (epoch != barrierEpoch) { // 非当前屏蔽任务,抛出异常
        requester.sendFailure(new SparkException(s"The request to sync of $barrierId with " +
          s"barrier epoch $barrierEpoch has already finished. Maybe task $taskId is not " +
          "properly killed."))
      } else {
        if (requesters.isEmpty) { // 进行可能的定时器初始化(为了再指定时间内返回可靠结果)
          initTimerTask(this)
          timer.schedule(timerTask, timeoutInSecs * 1000)
        }
        requesters += requester // 更新请求列表
        logInfo(s"Barrier sync epoch $barrierEpoch from $barrierId received update from Task " +
          s"$taskId, current progress: ${requesters.size}/$numTasks.")
        if (maybeFinishAllRequesters(requesters, numTasks)) { // 获取处理结果,并在处理完成的情况下进行任务重置
          logInfo(s"Barrier sync epoch $barrierEpoch from $barrierId received all updates from " +
            s"tasks, finished successfully.")
          barrierEpoch += 1
          requesters.clear()
          cancelTimerTask()
        }
      }
  }
```

```scala
private[spark] sealed trait BarrierCoordinatorMessage extends Serializable
介绍: 屏蔽协调者消息
```

```scala
private[spark] case class RequestToSync(
    numTasks: Int,
    stageId: Int,
    stageAttemptId: Int,
    taskAttemptId: Long,
    barrierEpoch: Int) extends BarrierCoordinatorMessage
介绍: 同步请求,来自@BarrierTaskContext 的同步请求消息,使用identified by stageId + stageAttemptId + barrierEpoch 唯一标识
参数:
	numTasks	全局同步请求需要接受的数量
	stageId	stageID
	stageAttemptId	stage请求编号
	taskAttemptId	当前任务请求编号
	barrierEpoch	屏蔽标识符
```

#### BarrierTaskContext

```scala
@Experimental
@Since("2.4.0")
class BarrierTaskContext private[spark] (
    taskContext: TaskContext) extends TaskContext with Logging {
    介绍: @TaskContext 额外的上下文信息,屏蔽stage任务的工具,使用@BarrierTaskContext#get 获取屏蔽任务的屏蔽上下文.
    属性:
    #name @barrierCoordinator #type @RpcEndpointRef	屏蔽协调者
    val= {
        val env = SparkEnv.get
        RpcUtils.makeDriverRef("barrierSync", env.conf, env.rpcEnv)
      }
    #name @barrierEpoch = 0	屏蔽标识符
    #name @numTasks = getTaskInfos().size	当前屏蔽stage的任务数量
    操作集:
    @Experimental
    @Since("2.4.0")
    def getTaskInfos(): Array[BarrierTaskInfo]
    功能: 获取屏蔽任务信息列表,按照分区编号排序
    val addressesStr = Option(taskContext.getLocalProperty("addresses")).getOrElse("")
    addressesStr.split(",").map(_.trim()).map(new BarrierTaskInfo(_))
    
    def isCompleted(): Boolean = taskContext.isCompleted()
    功能: 确定任务是否完成
    
    def isInterrupted(): Boolean = taskContext.isInterrupted()
    功能: 确认任务是否中断
    
    def addTaskCompletionListener(listener: TaskCompletionListener): this.type 
    功能: 添加任务完成监听器
    val= {
        taskContext.addTaskCompletionListener(listener)
        this
      }
    
    def addTaskFailureListener(listener: TaskFailureListener): this.type
    功能: 添加任务失败监听器
    val= {
        taskContext.addTaskFailureListener(listener)
        this
      }
    
    def stageId(): Int = taskContext.stageId()
    功能: 获取stageID
    
    def stageAttemptNumber(): Int = taskContext.stageAttemptNumber()
    功能: 获取stage请求数量
    
    def partitionId(): Int = taskContext.partitionId()
    功能: 获取分区ID
    
    def attemptNumber(): Int = taskContext.attemptNumber()
    功能: 获取任务请求数量
    
    def taskAttemptId(): Long = taskContext.taskAttemptId()
    功能: 获取任务请求编号
    
    def getLocalProperty(key: String): String = taskContext.getLocalProperty(key)
    功能: 获取本地属性
    
    def taskMetrics(): TaskMetrics = taskContext.taskMetrics()
    功能: 获取任务度量器
    
    def getMetricsSources(sourceName: String): Seq[Source]=taskContext.getMetricsSources(sourceName)
    功能: 获取独立资源
    
    def resources(): Map[String, ResourceInformation] = taskContext.resources()
    功能: 获取任务资源列表
    
    def resourcesJMap(): java.util.Map[String, ResourceInformation] =resources().asJava
    功能: 获取任务资源列表
    
    def killTaskIfInterrupted(): Unit = taskContext.killTaskIfInterrupted()
    功能: 中断kill任务
    
    def getKillReason(): Option[String] = taskContext.getKillReason()
    功能: 获取被kill的原因
    
    def taskMemoryManager(): TaskMemoryManager=taskContext.taskMemoryManager()
    功能: 获取任务内存管理器
    
    def registerAccumulator(a: AccumulatorV2[_, _]): Unit=taskContext.setFetchFailed(fetchFailed)
    功能: 注册累加器
    
    def markInterrupted(reason: String): Unit=taskContext.markInterrupted(reason)
    功能: 标记中断
    
    def markTaskFailed(error: Throwable): Unit =taskContext.markTaskFailed(error)
    功能: 标记任务失败
    
    def markTaskCompleted(error: Option[Throwable]): Unit=taskContext.markTaskCompleted(error)
    功能: 标记任务完成
    
    def fetchFailed: Option[FetchFailedException]= taskContext.fetchFailed
    功能: 获取失败原因
    
    def getLocalProperties: Properties = taskContext.getLocalProperties
    功能: 获取本地属性
    
    @Experimental
    @Since("2.4.0")
    def barrier(): Unit
    功能: 设置全局屏蔽,等待直到这个stage命中屏蔽部分.类似于MPI中的MPI_Barrier.这个函数会阻塞到所有任务到达同样stage的位置出(用于同步,确定执行顺序).
    注意:
        1. 需要使用try-catch 放置出现等待超时
        2. 再同一个屏蔽stage中调用这个函数,会引发函数调用超时
    1. 获取时间参数
    val startTime = System.currentTimeMillis()
    val timerTask = new TimerTask {
      override def run(): Unit = {
        logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) waiting " +
          s"under the global sync since $startTime, has been waiting for " +
          s"${MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)} seconds, " +
          s"current barrier epoch is $barrierEpoch.")
      }
    }
    timer.schedule(timerTask, 60000, 60000)
    2. 处理启用线程
    try {
      val abortableRpcFuture = barrierCoordinator.askAbortable[Unit](
        message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId,
          barrierEpoch),
        timeout = new RpcTimeout(365.days, "barrierTimeout"))
      try {
        while (!abortableRpcFuture.toFuture.isCompleted) { // 同步等待
          try {
            ThreadUtils.awaitResult(abortableRpcFuture.toFuture, 1.second) // 获取等待结果
          } catch {
            case _: TimeoutException | _: InterruptedException => // 获取超时,kill任务
              taskContext.killTaskIfInterrupted()
          }
        }
      } finally {
        // 处理完毕,弃用线程
        abortableRpcFuture.abort(taskContext.getKillReason().getOrElse("Unknown reason."))
      }
      barrierEpoch += 1
      logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) finished " +
        "global sync successfully, waited for " +
        s"${MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)} seconds, " +
        s"current barrier epoch is $barrierEpoch.")
    } catch {
      case e: SparkException =>
        logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) failed " +
          "to perform global sync, waited for " +
          s"${MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)} seconds, " +
          s"current barrier epoch is $barrierEpoch.")
        throw e
    } finally {
      timerTask.cancel()
      timer.purge()
    }
}
```

```scala
@Experimental
@Since("2.4.0")
object BarrierTaskContext {
    属性:
    #name @timer = new Timer("Barrier task timer for barrier() calls.")	定时器
    操作集:
    @Experimental
    @Since("2.4.0")
    def get(): BarrierTaskContext = TaskContext.get().asInstanceOf[BarrierTaskContext]
    功能: 获取屏蔽任务上下文
}
```

#### ContextCleaner

```scala
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask
private case class CleanAccum(accId: Long) extends CleanupTask
private case class CleanCheckpoint(rddId: Int) extends CleanupTask
```

```scala
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
extends WeakReference(referent, referenceQueue)
介绍: 清除任务的弱引用
参数:
	task 清除任务
	referent	参考值
	referenceQueue	参考队列
```

```scala
private[spark] trait CleanerListener {
  介绍: 清理器监听器
  def rddCleaned(rddId: Int): Unit // 清理RDD
  def shuffleCleaned(shuffleId: Int): Unit // 清理Shuffle
  def broadcastCleaned(broadcastId: Long): Unit // 清理广播变量
  def accumCleaned(accId: Long): Unit // 清理累加器
  def checkpointCleaned(rddId: Long): Unit // 清理检查点
}

private object ContextCleaner {
  介绍: 上下文清理器
  private val REF_QUEUE_POLL_TIMEOUT = 100 // 参考队列轮询超时时间
}
```

```scala
private[spark] class ContextCleaner(
    sc: SparkContext,
    shuffleDriverComponents: ShuffleDriverComponents) extends Logging {
    介绍:
    异步RDD/Shuffle/广播变量清除器
    保持每个RDD/shuffle依赖/广播变量的弱引用,
    属性:
    #name @referenceBuffer	参考缓冲区（保证弱引用不会被离开垃圾回收）
    val= Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)
    #name @referenceQueue = new ReferenceQueue[AnyRef]	参考队列
    #name @listeners = new ConcurrentLinkedQueue[CleanerListener]()	监听器
    #name @cleaningThread = new Thread() { override def run(): Unit = keepCleaning() }	清理器
    #name @periodicGCService #type @ScheduledExecutorService	周期性GC服务
    val= ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")
    #name @periodicGCInterval = sc.conf.get(CLEANER_PERIODIC_GC_INTERVAL)	GC周期
    #name @blockOnCleanupTasks = sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING)	
    	清理线程是否会阻塞清理任务
    #name @blockOnShuffleCleanupTasks =sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE)
    	清理线程是否会阻塞shuffle清理任务
    #name @stopped = false volatile	停止状态
    操作集：
    def attachListener(listener: CleanerListener): Unit = listeners.add(listener)
    功能: 添加监听器 
    
    def start(): Unit
    功能： 启动监听器
    1. 启动线程
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
    2. 启动周期性GC
    periodicGCService.scheduleAtFixedRate(() => System.gc(),
      periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
    
    def stop(): Unit
    功能: 停止清理线程,等待线程完成当前任务
    stopped = true
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
    periodicGCService.shutdown()
    
    def registerRDDForCleanup(rdd: RDD[_]): Unit=registerForCleanup(rdd, CleanRDD(rdd.id))
    功能: GC时注册RDD
    
    def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit
    功能: GC时注册累加器
    val= registerForCleanup(a, CleanAccum(a.id))
    
    def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit
    功能: GC时注册shuffle
    val= registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
    
    def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit 
    功能: GC时注册广播变量
    val= registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
    
    def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit
    功能: GC时注册RDD检查点
    val= registerForCleanup(rdd, CleanCheckpoint(parentId))
    
    def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit
    功能: 注册信息@CleanupTaskWeakReference 到引用缓冲中@referenceBuffer
	referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
    
    def keepCleaning(): Unit
    功能: 保持RDD/shuffle/广播变量状态
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            ref.task match {
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
    
    def doCleanupRDD(rddId: Int, blocking: Boolean): Unit
    功能: 清理RDD
    try {
      logDebug("Cleaning RDD " + rddId)
      sc.unpersistRDD(rddId, blocking)
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logDebug("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
    
    def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit
    功能: 清理shuffle
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      shuffleDriverComponents.removeShuffle(shuffleId, blocking)
      listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
      logDebug("Cleaned shuffle " + shuffleId)
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
    
    def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit 
    功能: 清理广播变量
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
    
    def doCleanupAccum(accId: Long, blocking: Boolean): Unit
    功能: 清理累加器
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logDebug("Cleaned accumulator " + accId)
    } catch {
      case e: Exception => logError("Error cleaning accumulator " + accId, e)
    }
    
    def doCleanCheckpoint(rddId: Int): Unit 
    功能: 清理检查点
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logDebug("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception => logError("Error cleaning rdd checkpoint data " + rddId, e)
    }
    
    def broadcastManager = sc.env.broadcastManager
    功能: 获取广播变量管理器
    
    def mapOutputTrackerMaster
    功能: 获取Map输出定位器@MapOutputTrackerMaster
    val= sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}
```

#### BarrierTaskInfo

```scala
@Experimental
@Since("2.4.0")
class BarrierTaskInfo private[spark] (val address: String)
介绍: 将所有任务信息放入到一个隔离任务中
参数:
	address	隔离任务运行的执行器IPV 4 地址
```

#### Dependency

```scala
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}
介绍: 依赖基本单元
```

```scala
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
    介绍: 窄依赖基本单元(子RDD的每个分区依赖于父RDD的(少量)分区)
    def rdd: RDD[T] = _rdd
    功能: 获取RDD
    
    def getParents(partitionId: Int): Seq[Int]
    功能: 获取子RDD分区的父分区,返回依赖的父分区
}

@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  介绍: 一对一依赖,属于窄依赖,父子RDD分区具有一对一关系
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}

@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
extends NarrowDependency[T](rdd) {
    介绍: 范围依赖(窄依赖)
    介绍: 表示父子RDD指定范围内的分区一对一关系
    参数:
    	RDD 父RDD
    	inStart	父RDD开始分区
    	outStart	子RDD开始分区
    	outStart	范围长度
    def getParents(partitionId: Int): List[Int] 
    功能: 获取父RDD分区编号列表
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
}

@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false,
    val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor)
extends Dependency[Product2[K, V]] {
    介绍: 代表shuffle范围的输出依赖,注意到由于是存在shuffle,RDD是@transient的,因此执行侧段无法获取这个RDD.
    参数:
        _rdd	父RDD
        partitioner	分区器,用于对shuffle输出进行分区
        serializer	序列化器
        keyOrdering	RDD shuffle的key的排序
        aggregator	RDDshuffle的聚合器
        mapSideCombine	是否开启map侧,局部聚合
        shuffleWriterProcessor	shuffle写出进程(控制@ShuffleMapTask 的写操作)
    属性:
    #name @keyClassName: String = reflect.classTag[K].runtimeClass.getName	key的类型
    #name @valueClassName: String = reflect.classTag[V].runtimeClass.getName	value类型
    #name @combinerClassName: Option[String]=Option(reflect.classTag[C]).map(_.runtimeClass.getName)
    	合并器类型
    #name @shuffleId: Int = _rdd.context.newShuffleId()	shuffleID
    #name @shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    	shuffleId, this)	
    	shuffle处理器
    初始化操作：
    if (mapSideCombine) {
        require(aggregator.isDefined, "Map-side combine without Aggregator specified!")
      }
    功能: map 侧combine条件断言
    
    _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
    功能: 清除当前依赖
    
    _rdd.sparkContext.shuffleDriverComponents.registerShuffle(shuffleId)
    功能: 注册shuffle驱动器
    
    操作集:
    def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]
    功能: 获取RDD
}

```

#### ExecutorAllocationClient

```scala
private[spark] trait ExecutorAllocationClient {
    介绍: 执行器分配客户端,与集群管理器进行交互,对执行器进行请求或者kill,当前只支持Yarn模式
    操作集:
    def getExecutorIds(): Seq[String]
    功能: 获取执行器列表
    
    def isExecutorActive(id: String): Boolean
    功能: 确定指定执行器是否存活
    
    def requestExecutors(numAdditionalExecutors: Int): Boolean
    功能: 请求集群管理器中指定数量的执行器,返回这些执行器是否被集群管理器(Yarn)识别
    
    def killExecutorsOnHost(host: String): Boolean
    功能: kill指定host的执行器
    
    def killExecutors(
        executorIds: Seq[String], // 执行器列表
        adjustTargetNumExecutors: Boolean, // kill之后是否重新调整目标执行器列表
        countFailures: Boolean, //执行器上有任务运行,则计数值+1
        force: Boolean = false): Seq[String] // 是否强制删除,默认false
    功能: 请求集群管理器删除指定数量的执行器,返回kill的执行器
    
    def killExecutor(executorId: String): Boolean
    功能: kill指定执行器
    val= {
        val killedExecutors = killExecutors(Seq(executorId), adjustTargetNumExecutors = true,
          countFailures = false)
        killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
      }
}
```

#### ExecutorAllocationManager

```markdown
介绍:
	执行器分配管理器,这个代理依据负载动态的分配和移除执行器.
	执行器分配管理器@ExecutorAllocationManager 维护一个指定移动数量的执行器,这个会周期性的同步到集群管理器中.目标开始于指定起始值,会因待定或者运行的任务进行改变.
	减少目标执行器的数量发生在当前目标数量大于当前负载所需要的执行器.将会减少到可以立即运行待定或者运行任务的数量.
	增加目标执行器的数量发生在积压任务,并等待调度的过程中.如果调度队列在N秒内没有排空,那么会添加新的执行器.如果队列持久化了M秒,那么更多的执行器会被添加.每轮添加的执行器数量以上一轮呈指数形式上升,数量上限是配置的属性值或者是可以运行的待定以及运行任务.
	指数增加的原理有如下两层
	1. 执行器数量开始需要缓慢增加,因为其他执行器可能需要变小.否则,添加的执行器数量会超过之后需要移除的执行器数量.
	2. 执行器需要快速添加,因此执行器最大数量很大.否则,在重负载情况下将会花费较长时间
	移除策略很简单,如果执行器宕机K秒,意味着这段时间内没有受到执行器的调度.注意到当宕机时间超过L秒,执行器缓存的数据会被移除.
	在这些情况下都不存在重试的容错策略,因为假定了集群管理器最终会异步填充所有请求.
	相关spark属性包含如下:
	spark.dynamicAllocation.enabled	是否允许动态分配
	spark.dynamicAllocation.minExecutors	执行器数量下限
	spark.dynamicAllocation.maxExecutors	执行器数量上限
    spark.dynamicAllocation.initialExecutors	初始执行器数量
    spark.dynamicAllocation.executorAllocationRatio	执行器分配比例
    	用于降低动态分配的并发度(当任务小的时候,会浪费系统资源)
    spark.dynamicAllocation.schedulerBacklogTimeout(M)调度积压延时(超过这个数值,就会新建执行器)
    spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N)	维持积压延时
    	超过这个数值将会新建更多的执行器
   	spark.dynamicAllocation.executorIdleTimeout (K)	执行器(空载)idle时间
   		超过这个时间空载(没有缓存数据),执行器将会被移除
   	spark.dynamicAllocation.cachedExecutorIdleTimeout (L)	缓存执行器(非空载)延时
    	超过这个值,执行器也会被移除
  	构造器参数:
  		client	执行分配客户端
  		listenerBus	监听总线
  		conf	spark配置
  		cleaner	上下文清理器
  		clock	系统时钟
```

```scala
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf,
    cleaner: Option[ContextCleaner] = None,
    clock: Clock = new SystemClock())
extends Logging {
    属性:
    #name @minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)	最小执行器数量
    #name @maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)	最大执行器数量
    #name @initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)	
    	初始化执行器数量
    #name @schedulerBacklogTimeoutS = conf.get(DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT)
    	调度积压延时
    #name @sustainedSchedulerBacklogTimeoutS	维持积压延时
    val= conf.get(DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT)
    #name @testing = conf.get(DYN_ALLOCATION_TESTING)	测试
    #name @tasksPerExecutorForFullParallelism	每个执行器开启全部并行的task数量(只支持YARN调度)
    val= conf.get(EXECUTOR_CORES) / conf.get(CPUS_PER_TASK)
    #name @executorAllocationRatio=conf.get(DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO)
    	执行器分配比例
    #name @numExecutorsToAdd = 1	需要添加的执行器数量
    #name @numExecutorsTarget = initialNumExecutors	目标执行器数量
    #name @addTime: Long = NOT_SET	待定任务转化为未调度任务的时间
    #name @intervalMillis	轮询周期
    val= if (Utils.isTesting) {
      conf.get(TEST_SCHEDULE_INTERVAL)
    } else {
      100
    }
    #name @listener = new ExecutorAllocationListener	监听器
    #name @executorMonitor = new ExecutorMonitor(conf, client, listenerBus, clock)
    	执行器监视器
    #name @executor	调度任务执行器
    val= ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark
    -dynamic-executor-allocation")
	#name @executorAllocationManagerSource = new ExecutorAllocationManagerSource
    	执行器分配管理资源
    #name @initializing: Boolean = true	初始化状态
    	在执行器空载超时或者stage提交时为false
    #name @localityAwareTasks = 0	位置感知任务数量(用于执行器替代)
    #name @hostToLocalTaskCount: Map[String, Int] = Map.empty	本地任务表
    操作集：
    def validateSettings(): Unit
    功能： 参数校验
    
    def start(): Unit
    功能: 启动管理器
    1. 添加监听事件到监听总线
    listenerBus.addToManagementQueue(listener)
    listenerBus.addToManagementQueue(executorMonitor)
    cleaner.foreach(_.attachListener(executorMonitor))
    2. 获取调度任务
    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        try {
          schedule()
        } catch {
          case ct: ControlThrowable =>
            throw ct
          case t: Throwable =>
            logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        }
      }
    }
    3. 延时调度
    executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis,
                                    TimeUnit.MILLISECONDS)
    4. 向集群管理器提交任务调度
    val= client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
   
    def stop(): Unit
    功能：停止管理器
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
    
    def reset(): Unit
    功能： 重置管理器（当集群管理器无法追踪driver的时候）
    synchronized {
        addTime = 0L
        numExecutorsTarget = initialNumExecutors
        executorMonitor.reset()
      }
    
    def maxNumExecutorsNeeded(): Int
    功能： 最大执行器需要数量
    val= {
        // 获取最大任务数量
        val numRunningOrPendingTasks = listener.totalPendingTasks +
            listener.totalRunningTasks
        math.ceil(numRunningOrPendingTasks * executorAllocationRatio /
                  tasksPerExecutorForFullParallelism).toInt
      }
    
    def totalRunningTasks(): Int = synchronized {listener.totalRunningTasks}
    功能: 获取任务运行总量
    
    def schedule(): Unit
    功能: 定期调用,调节待定任务数量和运行任务数量.首先,基于添加时间和当前需求调节申请的执行器.其次,如果移除时间已经过期,kill执行器.
    val executorIdsToBeRemoved = executorMonitor.timedOutExecutors()
    if (executorIdsToBeRemoved.nonEmpty) {
      initializing = false
    }
    // 更新执行的数量
    updateAndSyncNumExecutorsTarget(clock.nanoTime())
    if (executorIdsToBeRemoved.nonEmpty) { // 移除过期的执行器
      removeExecutors(executorIdsToBeRemoved)
    }
    
    def addExecutors(maxNumExecutorsNeeded: Int): Int 
    功能: 添加多个执行器,如果到达上界,则重置并开启下一轮添加
    0. 进行重置
     if (numExecutorsTarget >= maxNumExecutors) {
      logDebug(s"Not adding executors because our current target total " +
        s"is already $numExecutorsTarget (limit $maxNumExecutors)")
      numExecutorsToAdd = 1
      return 0
    }
    1. 确定目标执行器数量
    val oldNumExecutorsTarget = numExecutorsTarget
    numExecutorsTarget = math.max(numExecutorsTarget, executorMonitor.executorCount)
    numExecutorsTarget += numExecutorsToAdd
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors),
                                  minNumExecutors)
    2. 分配需要添加的执行器
    val delta = numExecutorsTarget - oldNumExecutorsTarget
    if (delta == 0) {
        // 不需要添加,则设置相应的监听器事件
      if (listener.pendingTasks == 0 && listener.pendingSpeculativeTasks > 0) {
        numExecutorsTarget =
          math.max(math.min(maxNumExecutorsNeeded + 1, maxNumExecutors), minNumExecutors)
      } else {
        numExecutorsToAdd = 1
        return 0
      }
    }
    3. 确认集群管理器是否可以识别执行器
    val addRequestAcknowledged = try {
      testing ||
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    } catch {
      case NonFatal(e) =>
        logInfo("Error reaching cluster manager.", e)
        false
    }
    4. 确定最终需要的执行器
    if (addRequestAcknowledged) {
      val executorsString = "executor" + { if (delta > 1) "s" else "" }
      logInfo(s"Requesting $delta new $executorsString because tasks are backlogged" +
        s" (new desired total will be $numExecutorsTarget)")
      numExecutorsToAdd = if (delta == numExecutorsToAdd) {
        numExecutorsToAdd * 2
      } else {
        1
      }
      delta
    } else {
      logWarning(
        s"Unable to reach the cluster manager to request $numExecutorsTarget
        total executors!")
      numExecutorsTarget = oldNumExecutorsTarget
      0
    }
    
    def removeExecutors(executors: Seq[String]): Seq[String]
    功能: 移除给定执行器列表
    synchronized {
        1. 确定需要移除的执行器列表,确定剩余执行器数量
        val executorIdsToBeRemoved = new ArrayBuffer[String]
        val numExistingExecutors = executorMonitor.executorCount -
        	executorMonitor.pendingRemovalCount
        var newExecutorTotal = numExistingExecutors
        2. 移除执行器数量当指定值
        executors.foreach { executorIdToBeRemoved =>
          if (newExecutorTotal - 1 < minNumExecutors) {
            logDebug(s"Not removing idle executor 
            $executorIdToBeRemoved because there are only " +
              s"$newExecutorTotal executor(s) left (minimum number 
              of executor limit $minNumExecutors)")
          } else if (newExecutorTotal - 1 < numExecutorsTarget) {
            logDebug(s"Not removing idle executor $executorIdToBeRemoved 
            because there are only " +
              s"$newExecutorTotal executor(s) left (number of executor 
              target $numExecutorsTarget)")
          } else {
            executorIdsToBeRemoved += executorIdToBeRemoved
            newExecutorTotal -= 1
          }
        }
  		3. kill部分执行器
        if (executorIdsToBeRemoved.isEmpty) {
          return Seq.empty[String]
        }
        val executorsRemoved = if (testing) {
          executorIdsToBeRemoved
        } else {
          client.killExecutors(executorIdsToBeRemoved, adjustTargetNumExecutors = false,
            countFailures = false, force = false)
        }
        4. 更新执行器数量到指定
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks,
                                     hostToLocalTaskCount)
        5. 重置执行器总数
        newExecutorTotal = numExistingExecutors
        if (testing || executorsRemoved.nonEmpty) {
          newExecutorTotal -= executorsRemoved.size
          executorMonitor.executorsKilled(executorsRemoved)
          logInfo(s"Executors ${executorsRemoved.mkString(",")} removed due to 
          idle timeout." +
            s"(new desired total will be $newExecutorTotal)")
          executorsRemoved
        } else {
          logWarning(s"Unable to reach the cluster manager to kill executor/s " +
            s"${executorIdsToBeRemoved.mkString(",")} or no executor eligible to kill!")
          Seq.empty[String]
        }
    }
    
    def onSchedulerBacklogged(): Unit =
    功能： 当调度器接收到待定任务时，如果不需要添加的情况下，添加时间到任务中
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.nanoTime() + TimeUnit.SECONDS.toNanos(schedulerBacklogTimeoutS)
    }
    
    def onSchedulerQueueEmpty(): Unit
    功能： 调度队列空处理
    synchronized {
        logDebug("Clearing timer to add executors because there are no more 
        pending tasks")
        addTime = NOT_SET
        numExecutorsToAdd = 1
      }
    
    def updateAndSyncNumExecutorsTarget(now: Long): Int
    功能: 更新执行器目标值,同步结果到集群管理器中.
    synchronized {
        1. 获取最大需要的执行器数量
        val maxNeeded = maxNumExecutorsNeeded
        if (initializing) {
            // 初始状态下,不会改变目标值
          0
        } else if (maxNeeded < numExecutorsTarget) {
          // 最大需求值小于执行器目标数量,则申请新的执行器
          val oldNumExecutorsTarget = numExecutorsTarget
          numExecutorsTarget = math.max(maxNeeded, minNumExecutors)
          numExecutorsToAdd = 1
          if (numExecutorsTarget < oldNumExecutorsTarget) {
            // 申请新的执行器
            client.requestTotalExecutors(numExecutorsTarget, 
                                         localityAwareTasks, hostToLocalTaskCount)
            logDebug(s"Lowering target number of executors to 
            $numExecutorsTarget (previously " +
              s"$oldNumExecutorsTarget) because not all 
              requested executors are actually needed")
          }
          // 返回获取的执行器数目
          numExecutorsTarget - oldNumExecutorsTarget
        } else if (addTime != NOT_SET && now >= addTime) {
          val delta = addExecutors(maxNeeded)
          logDebug(s"Starting timer to add more executors (to " +
            s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
          addTime = now + TimeUnit.SECONDS.toNanos(sustainedSchedulerBacklogTimeoutS)
          delta
        } else {
          0
        }
    }
}
```

```scala
private case class StageAttempt(stageId: Int, stageAttemptId: Int) {
    override def toString: String = s"Stage $stageId (Attempt $stageAttemptId)"
}
介绍: stage请求信息

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue // 未设置标记
}
介绍： 执行器分配管理器
```

```scala
  private[spark] class ExecutorAllocationManagerSource extends Source {
      介绍: 执行器分配资源管理
      属性:
      #name @sourceName = "ExecutorAllocationManager"	资源名称
      #name @metricRegistry = new MetricRegistry()	度量注册器
      操作集：
      def registerGauge[T](name: String, value: => T, defaultValue: T): Unit
      功能: 注册计量值
      metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
        override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
      })
      初始化操作:
      registerGauge("numberExecutorsToAdd", numExecutorsToAdd, 0)
      registerGauge("numberExecutorsPendingToRemove", executorMonitor.pendingRemovalCount, 0)
      registerGauge("numberAllExecutors", executorMonitor.executorCount, 0)
      registerGauge("numberTargetExecutors", numExecutorsTarget, 0)
      registerGauge("numberMaxNeededExecutors", maxNumExecutorsNeeded(), 0)
  }
```

```scala
private[spark] class ExecutorAllocationListener extends SparkListener {
    介绍： 执行器分配监听器
    这是一个提示指定分配管理器在添加或删除执行器时的监听器，这个类故意时保守的（关于相关排序和监听事件的一致性）
    属性:
    #name @stageAttemptToNumTasks = new mutable.HashMap[StageAttempt, Int]
    	stage请求任务数表
    #name @stageAttemptToNumRunningTask = new mutable.HashMap[StageAttempt, Int]
    	stage请求运行任务数表
    #name @stageAttemptToTaskIndices = new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]
    	stage 请求任务索引表
    #name @stageAttemptToNumSpeculativeTasks = new mutable.HashMap[StageAttempt, Int]
    	stage推测任务请求表
    #name @stageAttemptToSpeculativeTaskIndices =
      new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]
    	stage推测任务索引表
    #name @stageAttemptToExecutorPlacementHints =
      new mutable.HashMap[StageAttempt, (Int, Map[String, Int])]
    	执行器替换表
	操作集:
    def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit
    功能: stage提交处理
    
    def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit 
    功能: stage完成处理
    
    def onTaskStart(taskStart: SparkListenerTaskStart): Unit
    功能: 任务完成处理
    
    def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit
    功能: 任务结束处理
    
    def onSpeculativeTaskSubmitted(speculativeTask:
                                   SparkListenerSpeculativeTaskSubmitted): Unit
    功能: 推测任务提交的处理
    
    def pendingTasks(): Int
    功能： 获取待定任务数量
    val= stageAttemptToNumTasks.map { case (stageAttempt, numTasks) =>
        numTasks - stageAttemptToTaskIndices.get(stageAttempt).map(_.size).getOrElse(0)
      }.sum
    
    def pendingSpeculativeTasks(): Int 
    功能: 获取待定推测任务
    val= stageAttemptToNumSpeculativeTasks.map { case (stageAttempt, numTasks) =>
        numTasks - 
        stageAttemptToSpeculativeTaskIndices.get(stageAttempt).map(_.size).getOrElse(0)
      }.sum
    
    def totalPendingTasks(): Int = pendingTasks + pendingSpeculativeTasks
    功能:获取待定任务总量
    
    def totalRunningTasks(): Int
    功能:获取运行任务数量
    
    def updateExecutorPlacementHints(): Unit 
    功能:更新执行器位置提示信息
    var localityAwareTasks = 0
      val localityToCount = new mutable.HashMap[String, Int]()
      stageAttemptToExecutorPlacementHints.values.foreach { 
        case (numTasksPending, localities) =>
        localityAwareTasks += numTasksPending
        localities.foreach { case (hostname, count) =>
          val updatedCount = localityToCount.getOrElse(hostname, 0) + count
          localityToCount(hostname) = updatedCount
        }
      }
      allocationManager.localityAwareTasks = localityAwareTasks
      allocationManager.hostToLocalTaskCount = localityToCount.toMap
}
```

#### FutureAction

```scala
trait FutureAction[T] extends Future[T] {
    介绍: 支持取消结果的动作线程任务类
    def cancel(): Unit
    功能: 取消动作
    
    def ready(atMost: Duration)(implicit permit: CanAwait): FutureAction.this.type
    功能: 阻塞直到动作完成(等待)
    输入参数: atMost	最大等待时间,可以为负数(不等待),也可以是Inf,表示无限等待.
    
    @throws(classOf[Exception])
    override def result(atMost: Duration)(implicit permit: CanAwait): T
    功能: 在最大等待时间内@atMost,等待和返回动作结果
    
    def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit
    功能: 动作完成调用此方法(无论是完成还是抛出异常)
    
    def isCompleted: Boolean
    功能: 确定是否完成
    
    def isCancelled: Boolean
    功能: 确定是否放弃
    
    def value: Option[Try[T]]
    功能: 返回当前future 的value,如果任务没完成,则返回None,如果任务完成则返回Some(Success(t)),或者Some(Failure(error))(包含异常)
    
    @throws(classOf[SparkException])
    def get(): T = ThreadUtils.awaitResult(this, Duration.Inf)
    功能: 返回当前job的结果
    
    def jobIds: Seq[Int]
    功能: 通过底层异步操作获取job ID列表
}
```

```scala
@DeveloperApi
class SimpleFutureAction[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
extends FutureAction[T] {
    介绍: 容纳触发job的动作结果,包含@count,@collect,@reduce操作
    参数:
    jobWaiter	DAG调度job
    属性:
    #name @_cancelled: Boolean = false	volatile	是否放弃当前任务
    操作集:
    def cancel(): Unit
    功能: 放弃执行
    _cancelled = true
    jobWaiter.cancel() // 调度放弃当前任务(挂起)
    
    def ready(atMost: Duration)(implicit permit: CanAwait): SimpleFutureAction.this.type
    功能: 等待任务直到动作完成
    val= = {
        jobWaiter.completionFuture.ready(atMost)
        this
      }
    
    @throws(classOf[Exception])
    override def result(atMost: Duration)(implicit permit: CanAwait): T 
    功能: 在规定时间内获取执行结果
    val= = {
        jobWaiter.completionFuture.ready(atMost)
        assert(value.isDefined, "Future has not completed properly")
        value.get.get
      }
    
    def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit 
    功能: 完成后动作,动作函数@func
    jobWaiter.completionFuture 
    onComplete {_ => func(value.get)}
    
    def isCompleted: Boolean = jobWaiter.jobFinished
    功能: 确认是否完成
    
    def isCancelled: Boolean = _cancelled
    功能: 确认任务是否放弃
    
    def jobIds: Seq[Int] = Seq(jobWaiter.jobId)
    功能: 异步获取job ID列表
    
    def value: Option[Try[T]]
    功能: 返回当前future 的value,如果任务没完成,则返回None,如果任务完成则返回Some(Success(t)),或者Some(Failure(error))(包含异常)
    val= jobWaiter.completionFuture.value.map {res => res.map(_ => resultFunc)}
    
    def transform[S](f: (Try[T]) => Try[S])(implicit e: ExecutionContext): Future[S]
    功能: 转换函数,将T->S
    val= jobWaiter.completionFuture.transform((u: Try[Unit]) => f(u.map(_ => resultFunc)))
    
    def transformWith[S](f: (Try[T]) => Future[S])(implicit e: ExecutionContext): Future[S]
    功能: 转换函数
    val= jobWaiter.completionFuture.transformWith((u: Try[Unit]) => f(u.map(_ => resultFunc)))
}
```

```scala
@DeveloperApi
trait JobSubmitter {
    def submitJob[T, U, R](
        rdd: RDD[T],
        processPartition: Iterator[T] => U,
        partitions: Seq[Int],
        resultHandler: (Int, U) => Unit,
        resultFunc: => R): FutureAction[R]
    功能: 提交job用于执行,并返回一个@FutureAction 这之中包含其执行结果
}
```

```scala
@DeveloperApi
class ComplexFutureAction[T](run : JobSubmitter => Future[T])
extends FutureAction[T] { self =>
	介绍: @FutureAction 用于可以触发多个spark job的动作.例如,@take @takeSample
    属性:
    #name @_cancelled = false	volatile	放弃执行标志
    #name @subActions: List[FutureAction[_]] = Nil	子动作列表
    #name @p = Promise[T]().tryCompleteWith(run(jobSubmitter))	起信信号量
    操作集:
    def cancel(): Unit
    功能: 放弃执行
    synchronized {
        _cancelled = true
        p.tryFailure(new SparkException("Action has been cancelled"))
        subActions.foreach(_.cancel())
      }
    
    def isCancelled: Boolean = _cancelled
    功能: 确认是否放弃执行
    
    @throws(classOf[InterruptedException])
    @throws(classOf[scala.concurrent.TimeoutException])
    override def ready(atMost: Duration)(implicit permit: CanAwait): this.type
    功能: 阻塞等待结果
    val= {
        p.future.ready(atMost)(permit)
        this
      }
    
    @throws(classOf[Exception])
    override def result(atMost: Duration)(implicit permit: CanAwait): T
    功能: 获取执行结果
    val= p.future.result(atMost)(permit)
    
    def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit
    功能: 动作完成执行内容
    p.future.onComplete(func)(executor)
    
    def isCompleted: Boolean = p.isCompleted
    功能: 确认是否完成
    
    def value: Option[Try[T]] = p.future.value
    功能: 返回当前future 的value,如果任务没完成,则返回None,如果任务完成则返回Some(Success(t)),或者Some(Failure(error))(包含异常)
    
    def jobIds: Seq[Int] = subActions.flatMap(_.jobIds)
    功能: 获取job ID列表
    
    def transform[S](f: (Try[T]) => Try[S])(implicit e:ExecutionContext):Future[S]
    功能: 获取转换之后的结果
    val= p.future.transform(f)
    
    def transformWith[S](f: (Try[T]) => Future[S])(implicit e: ExecutionContext): Future[S]
    功能: 同上
    val=  p.future.transformWith(f)
}
```

```scala
private[spark]
class JavaFutureActionWrapper[S, T](futureAction: FutureAction[S], converter: S => T)
extends JavaFutureAction[T] {
    介绍: java类型线程动作的包装
    def isCancelled: Boolean = futureAction.isCancelled
    
    def isDone: Boolean=futureAction.isCancelled || futureAction.isCompleted
    
    def jobIds(): java.util.List[java.lang.Integer]
    val=  Collections.unmodifiableList(futureAction.jobIds.map(Integer.valueOf).asJava)
    
    def getImpl(timeout: Duration): T
    val=  {
        ThreadUtils.awaitReady(futureAction, timeout)
        futureAction.value.get match {
          case scala.util.Success(value) => converter(value)
          case scala.util.Failure(exception) =>
            if (isCancelled) {
              throw new CancellationException("Job cancelled").initCause(exception)
            } else {
              throw new ExecutionException("Exception thrown by job", exception)
            }
        }
    }
    
    def get(): T = getImpl(Duration.Inf)
    
    def get(timeout: Long, unit: TimeUnit): T=getImpl(Duration.fromNanos(unit.toNanos(timeout)))
    
    def cancel(mayInterruptIfRunning: Boolean): Boolean
        val=  synchronized {
        if (isDone) {
          false
        } else {
          futureAction.cancel()
          true
        }
      }
}
```

#### Heartbeater

```scala
private[spark] class Heartbeater(
    reportHeartbeat: () => Unit,
    name: String,
    intervalMs: Long) extends Logging {
    介绍: 创建心跳线程,周期性调用汇报心跳@reportHeartbeat
    参数:
    	reportHeartbeat	汇报心跳函数
    	name	心跳名称
    	intervalMs	汇报周期
    #name @heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor(name) 执行器心跳任务
    操作集:
    def start(): Unit 
    功能: 调度任务开始汇报心跳
    1. 计算初始时延
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]
    2. 获取心跳任务
    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartbeat())
    }
    3. 定时汇报
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
    
    def stop(): Unit
    功能: 停止心跳线程
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
}
```

#### HeartbeatReceiver

```scala
private[spark] case class Heartbeat(
    executorId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId,
    executorUpdates: Map[(Int, Int), ExecutorMetrics])
介绍: 心跳,从执行器发送到驱动器上,定期共享信息,通过内部组件将执行器的存活信息汇报给驱动器.超过指定超时实现时,会失活,注意@spark.executor.heartbeatInterval < @spark.executor.heartbeatInterval

private[spark] case object TaskSchedulerIsSet
介绍: sparkContext提示心跳接收器@HeartbeatReceiver @SparkContext.taskScheduler已经创建

private[spark] case object ExpireDeadHosts
介绍: 崩溃的主机

private case class ExecutorRegistered(executorId: String)
介绍: 已经注册的执行器

private case class ExecutorRemoved(executorId: String)
介绍: 移除的执行器

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)
介绍: 心跳回应

private[spark] object HeartbeatReceiver {
   介绍: 心跳接收器
  val ENDPOINT_NAME = "HeartbeatReceiver"	端点名称
}
```

```scala
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
extends SparkListener with ThreadSafeRpcEndpoint with Logging {
    介绍: 存活在驱动器中,用于接收执行器的心跳
    参数:
    	sc	spark上下文
    	clock	时钟
    构造器:
    def this(sc: SparkContext) {
        this(sc, new SystemClock)
      }
    功能: 使用系统时钟
    
    sc.listenerBus.addToManagementQueue(this)
    功能: 添加事件监听
    属性:
    #name @rpcEnv: RpcEnv = sc.env.rpcEnv	RPC环境
    #name @scheduler: TaskScheduler = null	任务调度器
    #name @executorLastSeen = new HashMap[String, Long]	执行器最新可见时间映射表
    #name @executorTimeoutMs = sc.conf.get(config.STORAGE_BLOCKMANAGER_SLAVE_TIMEOUT)	执行器超时时间
    #name @checkTimeoutIntervalMs = sc.conf.get(Network.NETWORK_TIMEOUT_INTERVAL)	检查周期
    #name @executorHeartbeatIntervalMs = sc.conf.get(config.EXECUTOR_HEARTBEAT_INTERVAL) 执行器心跳周期
    #name @timeoutCheckingTask: ScheduledFuture[_] = null	超时检查任务
    #name @killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")
    	kill任务线程
    #name @eventLoopThread=ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-
    	receiver-event-loop-thread")
    	时间环线程
    操作集：
    def onStart(): Unit
    功能: 启动接收器
    1. 启动超时检查任务
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError { Option(self).foreach(_.ask[Boolean](ExpireDeadHosts)) },
      0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
    
    def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] 
    功能: 接受和回复RPC信息
    1. 本地收发处理
    case ExecutorRegistered(executorId) =>
      executorLastSeen(executorId) = clock.getTimeMillis()
      context.reply(true)
    case ExecutorRemoved(executorId) =>
      executorLastSeen.remove(executorId)
      context.reply(true)
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
      context.reply(true)
    case ExpireDeadHosts =>
      expireDeadHosts()
      context.reply(true)
    2. 回应远程执行器
    case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId, executorUpdates) =>
      if (scheduler != null) {
        if (executorLastSeen.contains(executorId)) {
          executorLastSeen(executorId) = clock.getTimeMillis()
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, accumUpdates, blockManagerId, executorUpdates)
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              context.reply(response)
            }
          })
        } else {
          logDebug(s"Received heartbeat from unknown executor $executorId")
          context.reply(HeartbeatResponse(reregisterBlockManager = true))
        }
      } else {
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
    
    def addExecutor(executorId: String): Option[Future[Boolean]]
    功能: 添加指定执行器@executorId
    val= Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
    
    def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit
    功能: 添加执行器后动作,需要提示执行器注册完成
    addExecutor(executorAdded.executorId)
    
    def removeExecutor(executorId: String): Option[Future[Boolean]]
    功能: 移除执行器
    val= Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
    
    def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit
    功能: 执行器移除动作,如果接收器没有停止,需要告知执行器移除消息,注意到必须要当执行器移除完毕之后采用汇报信息.主要是保护竞争条件.
    removeExecutor(executorRemoved.executorId)\
    
    def onStop(): Unit
    功能: 停止接收器
    1. 关闭检查任务
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    2. 关闭kill线程,和事件环线程
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
    
    def expireDeadHosts(): Unit
    功能: 移除失活host
    val now = clock.getTimeMillis()
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      if (now - lastSeenMs > executorTimeoutMs) {
        logWarning(s"Removing executor $executorId with no recent heartbeats: " +
          s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
        scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
          s"timed out after ${now - lastSeenMs} ms"))
        killExecutorThread.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            sc.killAndReplaceExecutor(executorId)
          }
        })
        executorLastSeen.remove(executorId)
      }
    }
}
```

#### InternalAccumulator

```scala
private[spark] object InternalAccumulator {
    介绍: 内部累加器,代表任务级别的度量
    #name @METRICS_PREFIX = "internal.metrics."	度量前缀
    #name @SHUFFLE_READ_METRICS_PREFIX = METRICS_PREFIX + "shuffle.read."	
    	shuffle读取度量前缀
    #name @SHUFFLE_WRITE_METRICS_PREFIX = METRICS_PREFIX + "shuffle.write."
    	shuffle写出度量前缀
    #name @OUTPUT_METRICS_PREFIX = METRICS_PREFIX + "output."
    	输出度量前缀
    #name @INPUT_METRICS_PREFIX = METRICS_PREFIX + "input."
    	输入度量前缀
    #name @EXECUTOR_DESERIALIZE_TIME = METRICS_PREFIX + "executorDeserializeTime"
    	执行器反序列化时间
    #name @EXECUTOR_DESERIALIZE_CPU_TIME = METRICS_PREFIX + "executorDeserializeCpuTime"
    	执行器反序列CPU时间
    #name @EXECUTOR_RUN_TIME=METRICS_PREFIX + "executorRunTime"	执行器运行时间
    #name @EXECUTOR_CPU_TIME = METRICS_PREFIX + "executorCpuTime"	执行器CPU时间
    #name @RESULT_SIZE = METRICS_PREFIX + "resultSize"	结果规模大小
    #name @JVM_GC_TIME = METRICS_PREFIX + "jvmGCTime"	GC时间
    #name @RESULT_SERIALIZATION_TIME = METRICS_PREFIX + "resultSerializationTime"
    	结果序列化时间
    #name @MEMORY_BYTES_SPILLED = METRICS_PREFIX + "memoryBytesSpilled"
    	内存溢写量
    #name @DISK_BYTES_SPILLED = METRICS_PREFIX + "diskBytesSpilled"
    	磁盘溢写字节量
    #name @PEAK_EXECUTION_MEMORY = METRICS_PREFIX + "peakExecutionMemory"
    	峰值内存使用量
    #name @UPDATED_BLOCK_STATUSES = METRICS_PREFIX + "updatedBlockStatuses"
    	更新数据块状态
    #name @TEST_ACCUM = METRICS_PREFIX + "testAccumulator" 测试累加器
}
```

```scala
object shuffleRead {
    #name @REMOTE_BLOCKS_FETCHED = SHUFFLE_READ_METRICS_PREFIX + "remoteBlocksFetched"
    远端数据块获取数量
    #name @LOCAL_BLOCKS_FETCHED = SHUFFLE_READ_METRICS_PREFIX + "localBlocksFetched"
    本地数据块获取数量
    #name @REMOTE_BYTES_READ = SHUFFLE_READ_METRICS_PREFIX + "remoteBytesRead"
    远端字节读取量
    #name @REMOTE_BYTES_READ_TO_DISK = 
    SHUFFLE_READ_METRICS_PREFIX + "remoteBytesReadToDisk"
    远端字节读取到磁盘字节数
    #name @LOCAL_BYTES_READ = SHUFFLE_READ_METRICS_PREFIX + "localBytesRead"
    本地字节读取量
    #name @FETCH_WAIT_TIME = SHUFFLE_READ_METRICS_PREFIX + "fetchWaitTime"
    获取等待时间
    #name @RECORDS_READ = SHUFFLE_READ_METRICS_PREFIX + "recordsRead"
    记录读取数量    
}
```

```scala
object shuffleWrite {
    #name @BYTES_WRITTEN = SHUFFLE_WRITE_METRICS_PREFIX + "bytesWritten"
    字节写出量
    #name @RECORDS_WRITTEN = SHUFFLE_WRITE_METRICS_PREFIX + "recordsWritten"
    记录写出量
    #name @WRITE_TIME = SHUFFLE_WRITE_METRICS_PREFIX + "writeTime"
    写出时间
}
```

```scala
object output {
    #name @BYTES_WRITTEN = OUTPUT_METRICS_PREFIX + "bytesWritten"
    字节写出量
    #name @RECORDS_WRITTEN = OUTPUT_METRICS_PREFIX + "recordsWritten"
    记录写出量
}
```

```scala
object input {
    #name @BYTES_READ = INPUT_METRICS_PREFIX + "bytesRead"
    字节读取量
    #name @RECORDS_READ = INPUT_METRICS_PREFIX + "recordsRead"
    记录读取量
}
```

#### InterruptibleIterator

```scala
@DeveloperApi
class InterruptibleIterator[+T](val context: TaskContext, val delegate: Iterator[T])
extends Iterator[T] {
    介绍: 对于中断的迭代器进行了一次包装,提供任务的基本kill功能,通过检查任务上下文@TaskContext 的中断标记进行工作.
    操作集:
    def hasNext: Boolean
    功能: 检查是否含有下一个参数
    context.killTaskIfInterrupted()
    val= delegate.hasNext
    
    def next(): T = delegate.next()
    功能: 获取下一个迭代器
}
```

#### MapOutputStatistics

```scala
private[spark] class MapOutputStatistics(val shuffleId: Int, val bytesByPartitionId: Array[Long])
介绍: 对map端的输出进行统计,后期可能使用@DeveloperApi 使得用户可以开发
参数:
	shuffleId	shuffle编号
	bytesByPartitionId	每个map输出的输出字节数量
```

#### MapoutTracker

#### Partition

```scala
trait Partition extends Serializable {
    def index: Int
    功能: 获取分区编号
    
    def hashCode(): Int = index
    功能: 获取分区hash值
    
    def equals(other: Any): Boolean = super.equals(other)
    功能: 判断两个分区是否相等
}
```

#### Partitioner

```scala
abstract class Partitioner extends Serializable {
    介绍: 这个对象定义了一个kv RDD是如何按照key进行分区的.映射每个分区到一个分区RDD中.注意到,分区器必须是确定的,即同一个key必须返回同一个分区号
    def numPartitions: Int
    功能: 获取分区数量
    def getPartition(key: Any): Int
    功能: 获取指定key所属的分区编号
}
```

```scala
object Partitioner {
    操作集:
    def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner
    功能: 获取默认分区器
    选择一个分区对多个RDD进行类似cogroup的操作.如果设置了@spark.default.parallelism,会使用SparkContext的默认参数作为默认并行度,否则使用上游分区最大值作为默认并行度.可以的话,选择最大数量分区的RDD的分区器.如果这个分区器,如果这个分区器是合法的.或含有分区数量大于等于默认分区数量,就使用这个分区器.否则需要新建一个hash分区器,分区数为默认分区数量.
    如果没有设置并行度参数@spark.default.parallelism,分区数量会和上游最大分区数量一致,这样不容易引发OOM.使用两个参数(rdd,others)强迫至少传入一个RDD.
    参数:
    	rdd	当前RDD
    	others	其他RDD列表
    1. 确定是否含有分区器(分区数是否为正)
    val rdds = (Seq(rdd) ++ others)
    val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))
    2. 获取最大分区数量
    val hasMaxPartitioner: Option[RDD[_]] = if (hasPartitioner.nonEmpty) {
      Some(hasPartitioner.maxBy(_.partitions.length))
    } else {
      None
    }
    3. 获取默认最大分区数量(默认并行度)
    val defaultNumPartitions = if
    	(rdd.context.conf.contains("spark.default.parallelism")) {
      rdd.context.defaultParallelism
    } else {
      rdds.map(_.partitions.length).max
    }
    4. 获取分区器
    if (hasMaxPartitioner.nonEmpty && 
        (isEligiblePartitioner(hasMaxPartitioner.get, rdds) ||
        defaultNumPartitions <= hasMaxPartitioner.get.getNumPartitions)) {
        // 可用的分区数超过默认参数,使用最大分区数量
      hasMaxPartitioner.get.partitioner.get
    } else {
      // 其余情况使用默认参数的hash分区器
      new HashPartitioner(defaultNumPartitions)
    }
    
    def isEligiblePartitioner(
     hasMaxPartitioner: RDD[_],
     rdds: Seq[RDD[_]]): Boolean
    功能： 判断是否是可以分区器
    val= {
        val maxPartitions = rdds.map(_.partitions.length).max
        log10(maxPartitions) - log10(hasMaxPartitioner.getNumPartitions) < 1
    }
}
```

```scala
class HashPartitioner(partitions: Int) extends Partitioner {
    介绍: hash分区器
    操作集:
    def numPartitions: Int = partitions
    功能: 获取分区数量
    
    def getPartition(key: Any): Int
    功能: 获取分区编号
    val= key match {
        case null => 0
        case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
    }
    
    def equals(other: Any): Boolean
    功能: 判断分区相等逻辑
    val= other match {
        case h: HashPartitioner =>
          h.numPartitions == numPartitions
        case _ =>
          false
      }
    
    def hashCode: Int = numPartitions
    功能: 获取hash值
}
```

```scala
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20)
extends Partitioner {
    介绍: 区域分区器,通过对记录粗略的排序实现分区的形成,范围由传入的数值决定
        partitions	分区数量
        rdd	RDD
        ascending	是否升序排序
        samplePointsPerPartitionHint	每个分区采样点数
    属性:
    #name @ordering = implicitly[Ordering[K]]	排序方式
    #name @binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]
    	二分查找函数
    #name @rangeBounds: Array[K]	区域界线
    val= if (partitions <= 1) {
      Array.empty
    } else {
        // 分区中采样指定数量的元素
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      // 计算每个分区的采样数量
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
    
    def getPartition(key: Any): Int
    功能: 获取分区编号
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      partition = binarySearch(rangeBounds, k)
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
    
    @throws(classOf[IOException])
    private def writeObject(out: ObjectOutputStream): Unit 
    功能: 写出属性值
    Utils.tryOrIOException {
        val sfactory = SparkEnv.get.serializer
        sfactory match {
          case js: JavaSerializer => out.defaultWriteObject()
          case _ =>
            out.writeBoolean(ascending)
            out.writeObject(ordering)
            out.writeObject(binarySearch)

            val ser = sfactory.newInstance()
            Utils.serializeViaNestedStream(out, ser) { stream =>
              stream.writeObject(scala.reflect.classTag[Array[K]])
              stream.writeObject(rangeBounds)
            }
        }
      }
    
    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit 
    功能: 读取属性值
    Utils.tryOrIOException {
        val sfactory = SparkEnv.get.serializer
        sfactory match {
          case js: JavaSerializer => in.defaultReadObject()
          case _ =>
            ascending = in.readBoolean()
            ordering = in.readObject().asInstanceOf[Ordering[K]]
            binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

            val ser = sfactory.newInstance()
            Utils.deserializeViaNestedStream(in, ser) { ds =>
              implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
              rangeBounds = ds.readObject[Array[K]]()
            }
        }
      }
    
    def hashCode(): Int
    功能: 计算分区器hash值
}
```

```scala
private[spark] object RangePartitioner {
    操作集：
    def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])])
    功能: 通过每个分区的采样值描摹输入RDD
    1. 获取RDD编号
    val shift = rdd.id
    2. 描摹每个分区（使用采样）
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    3. 获取采样总数并返回
    val numItems = sketched.map(_._2).sum
    val= (numItems, sketched)
    
    def determineBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int): Array[K]
    功能： 计算范围分区的上界，使用带权的候选元素
    输入参数：
    	candidates	未排序的带权候选值
    	partitions	分区数量
    返回: 选择的上界
    1. 对候选值进行排序
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    2. 求总权值，并求出每个分区的权值步长
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    3. 设置初始化界限
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    4. 设置其他界限
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    val= bounds.toArray
}
```

#### SecurityManager

```scala
private[spark] class SecurityManager(
    sparkConf: SparkConf,
    val ioEncryptionKey: Option[Array[Byte]] = None,
    authSecretFileConf: ConfigEntry[Option[String]] = AUTH_SECRET_FILE)
extends Logging with SecretKeyHolder {
    介绍: spark提供安全机制的类
    这个类需要使用sparkEnv实例化,大多数属性需要从其中获取.也有一些情况,sparkEnv无法初始化,这种情况下需要直接初始化.这个类实现了所有安全相关的配置.请参考相关特征的文档.
    属性:
    #name @WILDCARD_ACL = "*"	通配访问控制符
    #name @authOn = sparkConf.get(NETWORK_AUTH_ENABLED)	网络授权状态
    #name @aclsOn = sparkConf.get(ACLS_ENABLE)	允许访问控制状态
    #name @adminAcls: Set[String] = sparkConf.get(ADMIN_ACLS).toSet
    	管理访问控制列表
    #name @adminAclsGroups: Set[String] = sparkConf.get(ADMIN_ACLS_GROUPS).toSet
    	管理控制列表组
    #name @viewAcls: Set[String] = _
    	视图访问控制列表
    #name @viewAclsGroups: Set[String] = _
    	视图访问控制列表组
    #name @modifyAcls: Set[String] = _	修改控制列表
    #name @modifyAclsGroups: Set[String] = _	修改控制列表组
    #name @defaultAclUsers	默认访问用户
    val= Set[String](System.getProperty("user.name", ""),
    	Utils.getCurrentUserName())
    #name @secretKey= _	密钥
    #name @hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)	hadoop配置
    #name @defaultSSLOptions	默认ssl配置
    	val= SSLOptions.parse(sparkConf, hadoopConf, "spark.ssl", defaults = None)
    操作集:
    def getSSLOptions(module: String): SSLOptions
    功能: 获取SSL配置
    val= {
    val opts =
          SSLOptions.parse(sparkConf, hadoopConf, s"spark.ssl.$module",
                           Some(defaultSSLOptions))
        logDebug(s"Created SSL options for $module: $opts")
        opts
      }
    
    def setViewAcls(defaultUsers: Set[String], allowedUsers: Seq[String]): Unit 
    功能: 设置视图控制列表,管理控制列表设置是前提条件
    viewAcls = adminAcls ++ defaultUsers ++ allowedUsers
    logInfo("Changing view acls to: " + viewAcls.mkString(","))
    
    def setViewAcls(defaultUser: String, allowedUsers: Seq[String]): Unit
    功能: 设置视图控制列表
    setViewAcls(Set[String](defaultUser), allowedUsers)
    
    def setViewAclsGroups(allowedUserGroups: Seq[String]): Unit
    功能: 设置视图控制列表组
    viewAclsGroups = adminAclsGroups ++ allowedUserGroups
    
    def getViewAcls: String
    功能: 获取视图控制列表
    val= if (viewAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAcls.mkString(",")
    }
    
    def getViewAclsGroups: String 
    功能: 获取视图控制列表组
    val= if (viewAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      viewAclsGroups.mkString(",")
    }
    
    def setModifyAcls(defaultUsers: Set[String], allowedUsers: Seq[String]): Unit
    功能: 设置修改控制列表
    modifyAcls = adminAcls ++ defaultUsers ++ allowedUsers
    logInfo("Changing modify acls to: " + modifyAcls.mkString(","))
    
    def setModifyAclsGroups(allowedUserGroups: Seq[String]): Unit
    功能: 设置修改控制列表组
    modifyAclsGroups = adminAclsGroups ++ allowedUserGroups
    logInfo("Changing modify acls groups to: " + modifyAclsGroups.mkString(","))
    
    def getModifyAcls: String
    功能: 获取修改控制列表
    val= if (modifyAcls.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAcls.mkString(",")
    }
    
    def getModifyAclsGroups: String 
    功能: 获取修改控制列表组
    val= if (modifyAclsGroups.contains(WILDCARD_ACL)) {
      WILDCARD_ACL
    } else {
      modifyAclsGroups.mkString(",")
    }
    
    def setAdminAcls(adminUsers: Seq[String]): Unit
    功能: 设置管理控制列表组
    adminAcls = adminUsers.toSet
    logInfo("Changing admin acls to: " + adminAcls.mkString(","))
    
    def setAdminAclsGroups(adminUserGroups: Seq[String]): Unit = 
    功能: 设置管理控制列表组      
    adminAclsGroups = adminUserGroups.toSet
    logInfo("Changing admin acls groups to: " + adminAclsGroups.mkString(","))
    
    def setAcls(aclSetting: Boolean): Unit
    功能: 设置网络授权状态
    aclsOn = aclSetting
    
    def getIOEncryptionKey(): Option[Array[Byte]] = ioEncryptionKey
    功能: 获取加密密钥
    
    def aclsEnabled(): Boolean = aclsOn
    功能: 确认是否可以访问控制
    
    def checkAdminPermissions(user: String): Boolean
    功能: 检查管理权限
    val= isUserInACL(user, adminAcls, adminAclsGroups)
    
    def checkUIViewPermissions(user: String): Boolean
    功能: 检查是否有UI视图权限
    val= isUserInACL(user, viewAcls, viewAclsGroups)
    
    def checkModifyPermissions(user: String): Boolean
    功能: 检查是否含有修改权限
    val= isUserInACL(user, modifyAcls, modifyAclsGroups)
    
    def isAuthenticationEnabled(): Boolean = authOn
    功能: 是否允许授权
    
    def isEncryptionEnabled(): Boolean
    功能: 检查是否加密
    val= sparkConf.get(Network.NETWORK_CRYPTO_ENABLED) ||
    	sparkConf.get(SASL_ENCRYPTION_ENABLED)
    
    def getSaslUser(): String = "sparkSaslUser"
    功能: 获取Sasl用户(使用Sasl连接)
    
    def getSecretKey(): String
    功能: 获取密钥
    val= if (isAuthenticationEnabled) {
      val creds = UserGroupInformation.getCurrentUser().getCredentials()
      Option(creds.getSecretKey(SECRET_LOOKUP_KEY))
        .map { bytes => new String(bytes, UTF_8) }
        .orElse(Option(secretKey))
        .orElse(Option(sparkConf.getenv(ENV_AUTH_SECRET)))
        .orElse(sparkConf.getOption(SPARK_AUTH_SECRET_CONF))
        .orElse(secretKeyFromFile())
        .getOrElse {
          throw new IllegalArgumentException(
            s"A secret key must be specified via the $SPARK_AUTH_SECRET_CONF config")
        }
    } else {
      null
    }
    
    def secretKeyFromFile(): Option[String]
    功能: 获取文件中的密钥
    val= sparkConf.get(authSecretFileConf).flatMap { secretFilePath =>
      sparkConf.getOption(SparkLauncher.SPARK_MASTER).map {
        case k8sRegex() =>
          val secretFile = new File(secretFilePath)
          require(secretFile.isFile, s"No file found containing the secret key
          at $secretFilePath.")
          val base64Key = Base64.getEncoder.encodeToString(Files.readAllBytes(
              secretFile.toPath))
          require(!base64Key.isEmpty, s"Secret key from file located at 
          $secretFilePath is empty.")
          base64Key
        case _ =>
          throw new IllegalArgumentException(
            "Secret keys provided via files is only allowed in Kubernetes mode.")
      }
    }
    
    def getSaslUser(appId: String): String = getSaslUser()
    功能: 获取sasl用户
    
    def getSecretKey(appId: String): String = getSecretKey()
    功能: 获取密钥
    
    def isUserInACL(
      user: String,
      aclUsers: Set[String],
      aclGroups: Set[String]): Boolean
    功能: 确定指定用户@user 是否在指定访问控制表中@aclUsers
    val= if (user == null ||
        !aclsEnabled ||
        aclUsers.contains(WILDCARD_ACL) ||
        aclUsers.contains(user) ||
        aclGroups.contains(WILDCARD_ACL)) {
      true
    } else {
      val userGroups = Utils.getCurrentUserGroups(sparkConf, user)
      logDebug(s"user $user is in groups ${userGroups.mkString(",")}")
      aclGroups.exists(userGroups.contains(_))
    }
    
    def initializeAuth(): Unit
    功能: 初始化授权密钥,在yarn或者本地模式下,产生新的密钥,并将其存储在用户的数字证书中,其他模式下,断言配置中设定了密钥.
}
```

```scala
private[spark] object SecurityManager {
    属性:
    #name @k8sRegex = "k8s.*".r	k8s正则表达式
    #name @SPARK_AUTH_CONF = NETWORK_AUTH_ENABLED.key	spark授权配置
    #name @SPARK_AUTH_SECRET_CONF = AUTH_SECRET.key	spark授权配置
    #name @ENV_AUTH_SECRET = "_SPARK_AUTH_SECRET"	环境授权密钥
    #name @SECRET_LOOKUP_KEY = new Text("sparkCookie")	环境查找的key
}
```

#### SerializableWritable

```scala
@DeveloperApi
class SerializableWritable[T <: Writable](@transient var t: T) extends Serializable {
    介绍: 序列化可写形式
    操作集:
    def value: T = t
    功能: 获取value值
    
    def toString: String = t.toString
    功能: 信息显示
    
    def writeObject(out: ObjectOutputStream): Unit
    功能: 写出默认属性(不会写出@t)
    Utils.tryOrIOException {
        out.defaultWriteObject()
        new ObjectWritable(t).write(out)
      }
    
    def readObject(in: ObjectInputStream): Unit
    功能: 读取默认属性
    Utils.tryOrIOException {
        in.defaultReadObject()
        val ow = new ObjectWritable()
        // 写出配置,和读取的内容
        ow.setConf(new Configuration(false))
        ow.readFields(in)
        // 赋值给t(t是不可以直接进行读写的)
        t = ow.get().asInstanceOf[T]
      }   
}
```

#### SparkConf

```markdown
介绍:
	spark配置,按照kv的形式设置spark参数.大多数情况使用`new SparkConf()`创建SparkConf对象,会加载`spark.*`java系统下的系统属性.sparkConf配置优先于系统配置.
	对于单元测试来说,,你可以调用`new Spark(false)`,从而不用加载外部配置,且获取相同的配置,无论系统配置如何.允许链式调用setter方法比如`new SparkConf().setMaster("local").setAppName("My app")`
	注意: 一旦配置传递給spark,就不可以对其进行修改.spark不支持运行期修改配置.
	参数:
	loadDefaults	是否从java系统中获取配置
```

```scala
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {
    属性:
    #name @settings = new ConcurrentHashMap[String, String]()	配置列表
    #name @reader: ConfigReader	配置读取器
    val= {
        val _reader = new ConfigReader(new SparkConfigProvider(settings))
        _reader.bindEnv((key: String) => Option(getenv(key)))
        _reader
      }
    #name @avroNamespace = "avro.schema."	AVRO命名空间
    操作集:
    def loadFromSystemProperties(silent: Boolean): SparkConf
    功能: 加载spark.*下面的属性配置
    
    def set(key: String, value: String): SparkConf
    功能: 设置配置信息
    
    def set(key: String, value: String, silent: Boolean): SparkConf 
    功能: 设置配置信息,可以指定是否后台执行,否则以日志的形式表现出来
    
    def set[T](entry: ConfigEntry[T], value: T): SparkConf 
    功能: 设置配置信息
    set(entry.key, entry.stringConverter(value))
    val= this
    
    def set[T](entry: OptionalConfigEntry[T], value: T): SparkConf 
    功能: 设置配置信息
    
    def setMaster(master: String): SparkConf = set("spark.master", master)
    功能: 设置master
    
    def setAppName(name: String): SparkConf= set("spark.app.name", name)
    功能: 设置应用名称
    
    def setJars(jars: Seq[String]): SparkConf
    功能: 设置jar列表,分布于集群中
    for (jar <- jars if (jar == null)) logWarning("null jar passed to 
    SparkContext constructor")
    set(JARS, jars.filter(_ != null))
    
    def setJars(jars: Array[String]): SparkConf= setJars(jars.toSeq)
    功能: 设置jar列表,分布于集群中,java使用
    
    def setExecutorEnv(variable: String, value: String): SparkConf
    功能: 设置执行器环境
    
    def setExecutorEnv(variables: Seq[(String, String)]): SparkConf 
    功能: 设置执行器环境列表
    
    def setExecutorEnv(variables: Array[(String, String)]): SparkConf 
    功能: 设置执行器环境
    
    def setSparkHome(home: String): SparkConf 
    功能: 设置spark工作节点安装位置
    
    def setAll(settings: Iterable[(String, String)]): SparkConf 
    功能: 多参数设置
    
    def setIfMissing(key: String, value: String): SparkConf 
    功能: 缺失此项配置上设置
    
    def setIfMissing[T](entry: ConfigEntry[T], value: T): SparkConf
    功能: 缺失此项配置上设置
    
    def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): SparkConf 
    功能: 缺失此项配置上设置
    
    def registerKryoClasses(classes: Array[Class[_]]): SparkConf
    功能: 注册指定类@classess到kryo序列化器中调用多次
    
    def registerAvroSchemas(schemas: Schema*): SparkConf 
    功能: 注册AVRO的schema,以便于序列化时减少网络IO
    for (schema <- schemas) {
      set(avroNamespace + SchemaNormalization.parsingFingerprint64(schema),
          schema.toString)
    }
    val= this
    
    def getAvroSchema: Map[Long, String]
    功能: 获取AVRO schema列表
    val= getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
      .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
      .toMap
    
    def remove(key: String): SparkConf
    功能: 移除指定配置
    
    def remove(entry: ConfigEntry[_]): SparkConf
    功能: 移除指定配置
    
    def get(key: String): String
    功能: 获取指定配置,没有的时候抛出异常
    
    def get(key: String, defaultValue: String): String
    功能: 获取指定配置,没有的时候为默认值
    
    def get[T](entry: ConfigEntry[T]): T
    功能: 从@reader中检索指定配置参数@entry
    val= entry.readFrom(reader)
    
    def getTimeAsSeconds(key: String): Long
    功能: 获取时间,没有设置则抛出异常
    
    def getTimeAsSeconds(key: String, defaultValue: String): Long
    功能: 获取时间,没有设置则为默认值
    
    def getTimeAsMs(key: String): Long
    功能: 获取时间,单位ms,没有设置则抛出异常
    
    def getTimeAsMs(key: String, defaultValue: String): Long
    功能: 获取时间,没有设置则为默认值
    
    def getSizeAsBytes(key: String): Long
    功能: 获取大小,没有设置抛出异常(单位字节)
    
    def getSizeAsBytes(key: String, defaultValue: String): Long
    功能: 获取大小,没有设置返回默认值(单位字节)
    
    def getSizeAsKb(key: String): Long
    功能: 获取大小,没有设置抛出异常(单位KB)
    
    def getSizeAsKb(key: String, defaultValue: String): Long 
    功能: 获取大小,没有设置返回默认值(单位KB)
    
    def getSizeAsMb(key: String): Long
    功能: 获取大小,没有设置抛出异常(单位MB)
    
    def getSizeAsMb(key: String, defaultValue: String): Long
    功能: 获取大小,没有设置返回默认值(单位MB)
    
    def getSizeAsGb(key: String): Long
    功能: 获取大小,没有设置抛出异常(单位GB)
    
    def getSizeAsGb(key: String, defaultValue: String): Long
    功能: 获取大小,没有设置返回默认值(单位GB)
    
    def getOption(key: String): Option[String]
    功能: 获取指定key的可选配置
    
    def getWithSubstitution(key: String): Option[String] 
    功能: 使用变量替换,获取key的value值
    
    def getAll: Array[(String, String)]
    功能: 获取所有配置
    
    def getAllWithPrefix(prefix: String): Array[(String, String)] 
    功能: 获取所有配置,并带有前缀
    
    def getInt(key: String, defaultValue: Int): Int
    功能: 获取int值,失败则获取默认值
    
    def getLong(key: String, defaultValue: Long): Long 
    功能: 获取long值,失败则获取默认值
    
    def getDouble(key: String, defaultValue: Double): Double
    功能: 获取double值,失败则获取默认值
    
    def getBoolean(key: String, defaultValue: Boolean): Boolean
    功能: 获取boolean值,失败则获取默认值
    
    def getExecutorEnv: Seq[(String, String)]
    功能: 获取执行器环境变量
    
    def getAppId: String = get("spark.app.id")
    功能: 获取应用ID
    
    def contains(key: String): Boolean
    功能: 确认是否包含指定key
    
    def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)
    功能: 确认是否包含指定@entry
    
    def clone: SparkConf
    功能: 对象复制
    
    def getenv(name: String): String = System.getenv(name)
    功能: 获取系统环境
    
    def catchIllegalValue[T](key: String)(getValue: => T): T
    功能: 步骤非法值
    输入参数:
    	key	key值
    	getValue	获取value函数
    val= try {
      getValue
    } catch {
      case e: NumberFormatException =>
        throw new NumberFormatException(s"Illegal value for config key 
        $key: ${e.getMessage}")
            .initCause(e)
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Illegal value for config key $key:
        ${e.getMessage}", e)
    }
    
    def toDebugString: String
    功能: 显示debug信息
    
    def validateSettings(): Unit 
    功能: 验证配置信息是否合法，非法则抛出异常
}
```

```scala
private[spark] object SparkConf extends Logging {
    属性:
    #name @deprecatedConfigs: Map[String, DeprecatedConfig]	弃用配置表
    val= {
        val configs = Seq(
          DeprecatedConfig("spark.cache.class", "0.8",
            "The spark.cache.class property is no longer being used! Specify storage levels using " +
            "the RDD.persist() method instead."),
          DeprecatedConfig("spark.yarn.user.classpath.first", "1.3",
            "Please use spark.{driver,executor}.userClassPathFirst instead."),
          DeprecatedConfig("spark.kryoserializer.buffer.mb", "1.4",
            "Please use spark.kryoserializer.buffer instead. The default value for " +
              "spark.kryoserializer.buffer.mb was previously specified as '0.064'. Fractional values " +
              "are no longer accepted. To specify the equivalent now, one may use '64k'."),
          DeprecatedConfig("spark.rpc", "2.0", "Not used anymore."),
          DeprecatedConfig("spark.scheduler.executorTaskBlacklistTime", "2.1.0",
            "Please use the new blacklisting options, spark.blacklist.*"),
          DeprecatedConfig("spark.yarn.am.port", "2.0.0", "Not used anymore"),
          DeprecatedConfig("spark.executor.port", "2.0.0", "Not used anymore"),
          DeprecatedConfig("spark.shuffle.service.index.cache.entries", "2.3.0",
            "Not used anymore. Please use spark.shuffle.service.index.cache.size"),
          DeprecatedConfig("spark.yarn.credentials.file.retention.count", "2.4.0", "Not used anymore."),
          DeprecatedConfig("spark.yarn.credentials.file.retention.days", "2.4.0", "Not used anymore."),
          DeprecatedConfig("spark.yarn.services", "3.0.0", "Feature no longer available."),
          DeprecatedConfig("spark.executor.plugins", "3.0.0",
            "Feature replaced with new plugin API. See Monitoring documentation.")
        )
        Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
    }
    #name @configsWithAlternatives #type @Map[String, Seq[AlternateConfig]]	替代式配置
    val= Map[String, Seq[AlternateConfig]](
        EXECUTOR_USER_CLASS_PATH_FIRST.key -> Seq(
          AlternateConfig("spark.files.userClassPathFirst", "1.3")),
        UPDATE_INTERVAL_S.key -> Seq(
          AlternateConfig("spark.history.fs.update.interval.seconds", "1.4"),
          AlternateConfig("spark.history.fs.updateInterval", "1.3"),
          AlternateConfig("spark.history.updateInterval", "1.3")),
        CLEANER_INTERVAL_S.key -> Seq(
          AlternateConfig("spark.history.fs.cleaner.interval.seconds", "1.4")),
        MAX_LOG_AGE_S.key -> Seq(
          AlternateConfig("spark.history.fs.cleaner.maxAge.seconds", "1.4")),
        "spark.yarn.am.waitTime" -> Seq(
          AlternateConfig("spark.yarn.applicationMaster.waitTries", "1.3",
            // Translate old value to a duration, with 10s wait time per try.
            translation = s => s"${s.toLong * 10}s")),
        REDUCER_MAX_SIZE_IN_FLIGHT.key -> Seq(
          AlternateConfig("spark.reducer.maxMbInFlight", "1.4")),
        KRYO_SERIALIZER_BUFFER_SIZE.key -> Seq(
          AlternateConfig("spark.kryoserializer.buffer.mb", "1.4",
            translation = s => s"${(s.toDouble * 1000).toInt}k")),
        KRYO_SERIALIZER_MAX_BUFFER_SIZE.key -> Seq(
          AlternateConfig("spark.kryoserializer.buffer.max.mb", "1.4")),
        SHUFFLE_FILE_BUFFER_SIZE.key -> Seq(
          AlternateConfig("spark.shuffle.file.buffer.kb", "1.4")),
        EXECUTOR_LOGS_ROLLING_MAX_SIZE.key -> Seq(
          AlternateConfig("spark.executor.logs.rolling.size.maxBytes", "1.4")),
        IO_COMPRESSION_SNAPPY_BLOCKSIZE.key -> Seq(
          AlternateConfig("spark.io.compression.snappy.block.size", "1.4")),
        IO_COMPRESSION_LZ4_BLOCKSIZE.key -> Seq(
          AlternateConfig("spark.io.compression.lz4.block.size", "1.4")),
        RPC_NUM_RETRIES.key -> Seq(
          AlternateConfig("spark.akka.num.retries", "1.4")),
        RPC_RETRY_WAIT.key -> Seq(
          AlternateConfig("spark.akka.retry.wait", "1.4")),
        RPC_ASK_TIMEOUT.key -> Seq(
          AlternateConfig("spark.akka.askTimeout", "1.4")),
        RPC_LOOKUP_TIMEOUT.key -> Seq(
          AlternateConfig("spark.akka.lookupTimeout", "1.4")),
        "spark.streaming.fileStream.minRememberDuration" -> Seq(
          AlternateConfig("spark.streaming.minRememberDuration", "1.5")),
        "spark.yarn.max.executor.failures" -> Seq(
          AlternateConfig("spark.yarn.max.worker.failures", "1.5")),
        MEMORY_OFFHEAP_ENABLED.key -> Seq(
          AlternateConfig("spark.unsafe.offHeap", "1.6")),
        RPC_MESSAGE_MAX_SIZE.key -> Seq(
          AlternateConfig("spark.akka.frameSize", "1.6")),
        "spark.yarn.jars" -> Seq(
          AlternateConfig("spark.yarn.jar", "2.0")),
        MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM.key -> Seq(
          AlternateConfig("spark.reducer.maxReqSizeShuffleToMem", "2.3")),
        LISTENER_BUS_EVENT_QUEUE_CAPACITY.key -> Seq(
          AlternateConfig("spark.scheduler.listenerbus.eventqueue.size", "2.3")),
        DRIVER_MEMORY_OVERHEAD.key -> Seq(
          AlternateConfig("spark.yarn.driver.memoryOverhead", "2.3")),
        EXECUTOR_MEMORY_OVERHEAD.key -> Seq(
          AlternateConfig("spark.yarn.executor.memoryOverhead", "2.3")),
        KEYTAB.key -> Seq(
          AlternateConfig("spark.yarn.keytab", "3.0")),
        PRINCIPAL.key -> Seq(
          AlternateConfig("spark.yarn.principal", "3.0")),
        KERBEROS_RELOGIN_PERIOD.key -> Seq(
          AlternateConfig("spark.yarn.kerberos.relogin.period", "3.0")),
        KERBEROS_FILESYSTEMS_TO_ACCESS.key -> Seq(
          AlternateConfig("spark.yarn.access.namenodes", "2.2"),
          AlternateConfig("spark.yarn.access.hadoopFileSystems", "3.0")),
        "spark.kafka.consumer.cache.capacity" -> Seq(
          AlternateConfig("spark.sql.kafkaConsumerCache.capacity", "3.0"))
      )
    
    #name @allAlternatives: Map[String, (String, AlternateConfig)] 	所有替代配置
    val= {
        configsWithAlternatives.keys.flatMap { key =>
          configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
        }.toMap
      }
    
    def isSparkPortConf(name: String): Boolean
    功能： 是否为spark端口配置
    
    def isExecutorStartupConf(name: String): Boolean
    功能： 确定配置是否需要传递到执行器上开始运行
    val= {
    (name.startsWith("spark.auth") && name != SecurityManager.SPARK_AUTH_SECRET_CONF)||
    name.startsWith("spark.rpc") ||
    name.startsWith("spark.network") ||
    isSparkPortConf(name)
 	 }
    
    def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String]
    功能： 获取失效配置
    
    def logDeprecationWarning(key: String): Unit
    功能: 展示失效警告信息
    
    内部类:
    private case class DeprecatedConfig(
      key: String,
      version: String, // 版本号
      deprecationMessage: String) // 失效信息
    介绍: 容纳失效信息
    
    private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)
    功能: 失效信息的替代配置
}
```

#### SparkContext

#### SparkEnv

```markdown
介绍:
	允许所有运行时环境变量,用于允许spark实例(master/worker).包括序列化器,RPC环境,块管理器,map输出定位器,等等.spark代码通过全局变量发现sparkEnv,所以所有的线程获取的是一个sparkEnv,使用sparkEnv.get获取.
```

```scala
@DeveloperApi
class SparkEnv (
    val executorId: String, // 执行器编号
    private[spark] val rpcEnv: RpcEnv, // rpc环境
    val serializer: Serializer, // 序列化器
    val closureSerializer: Serializer, // 闭包序列化器
    val serializerManager: SerializerManager, // 序列化管理器
    val mapOutputTracker: MapOutputTracker, // 输出定位器
    val shuffleManager: ShuffleManager, // shuffle管理器
    val broadcastManager: BroadcastManager, // 广播变量管理器
    val blockManager: BlockManager, // 块管理器
    val securityManager: SecurityManager, // 安全管理器
    val metricsSystem: MetricsSystem, // 度量系统
    val memoryManager: MemoryManager, // 内存管理器
    val outputCommitCoordinator: OutputCommitCoordinator, // 输出提交协调者
    val conf: SparkConf)  // spark配置
extends Logging {
    属性:
    #name @isStopped = false	停止状态
    #name @pythonWorkers 	pythons 的worker列表
    val= mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()
    #name @hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()
    	hadoop job元数据
    #name @driverTmpDir: Option[String] = None	driver临时目录
    操作集:
    def stop(): Unit
    功能: 停止当前环境变量
    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
      driverTmpDir match {
        // 如果只删除sc,但是driver进程仍然运行,则需要删除临时目录,否则会创建很多临时目录
        case Some(path) =>
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        case None => 
      }
    }
    
    def createPythonWorker(pythonExec: String, envVars: Map[String, String])
    : java.net.Socket
    功能: 创建python worker
    
    def destroyPythonWorker(pythonExec: String,
      envVars: Map[String, String], worker: Socket): Unit
    功能: 销毁python worker
    
    def releasePythonWorker(pythonExec: String,
      envVars: Map[String, String], worker: Socket): Unit
    功能: 释放python worker
}
```

```scala
object SparkEnv extends Logging {
    属性
    #name @env: SparkEnv = _ volatile	spark环境变量
    #name @driverSystemName = "sparkDriver"	driver系统名称
    #name @executorSystemName = "sparkExecutor"	执行器系统名称
    操作集:
    def set(e: SparkEnv): Unit = env=e
    功能: 设置环境变量
    
    def get: SparkEnv = env
    功能: 获取环境变量
    
    def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv
    功能: 创建驱动器的环境变量
    输入参数:
    	conf	spark配置
    	isLocal	是否为本地模式
    	listenerBus	监听总线
    	numCores	核心数量
    	mockOutputCommitCoordinator	输出协作者
    1. 驱动器参数断言
    assert(conf.contains(DRIVER_HOST_ADDRESS),
      s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains(DRIVER_PORT), s"${DRIVER_PORT.key} is not set on the driver!")
    2. 获取端口地址信息
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    val port = conf.get(DRIVER_PORT)
    3. 获取加密key值
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }
    val= create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      Option(port),
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
    
    def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      hostname: String,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv
    功能: 创建执行器环境
    val env = create(
      conf,
      executorId,
      bindAddress,
      hostname,
      None,
      isLocal,
      numCores,
      ioEncryptionKey
    )
    SparkEnv.set(env)
    val= env
    
    def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv
    功能: 创建执行器环境
    val= createExecutorEnv(conf, executorId, hostname,
      hostname, numCores, ioEncryptionKey, isLocal)
    
    def instantiateClass[T](className: String): T = {
      功能: 获取实例化类
      val cls = Utils.classForName(className)
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }
    
    def instantiateClassFromConf[T](propertyName: ConfigEntry[String]): T = {
      功能: 从配置中获取实例化类
      instantiateClass[T](conf.get(propertyName))
    }
    
    def create(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Option[Int],
      isLocal: Boolean,
      numUsableCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv
    功能: 创建一个环境变量给(驱动器/执行器)
    1. 驱动器特殊处理
    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER
    if (isDriver) {// 驱动器添加监听事件
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }
    2. 配置安全管理器
    val authSecretFileConf = if (isDriver) AUTH_SECRET_FILE_DRIVER else AUTH_SECRET_FILE_EXECUTOR
    val securityManager = new SecurityManager(conf, ioEncryptionKey, authSecretFileConf)
    if (isDriver) { // 驱动器需要初始化授权信息
      securityManager.initializeAuth()
    }
    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }
    3. 设置RPC信息
    val systemName = if (isDriver) driverSystemName else executorSystemName
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf,
      securityManager, numUsableCores, !isDriver)
    if (isDriver) {
      conf.set(DRIVER_PORT, rpcEnv.address.port)
    }
    4. 设置序列化器/序列化管理器信息
    val serializer = instantiateClassFromConf[Serializer](SERIALIZER)
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)
    val closureSerializer = new JavaSerializer(conf)
    5. 设置广播变量管理器
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
    6. 获取输出定位器
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }
    //设置定位器的定位点信息
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(
        MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))
	7. 设置shuffle管理器
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.
                                 SortShuffleManager].getName)
    val shuffleMgrName = conf.get(config.SHUFFLE_MANAGER)
    val shuffleMgrClass =
      shortShuffleMgrNames.getOrElse(shuffleMgrName.
                                     toLowerCase(Locale.ROOT), shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
    val externalShuffleClient = if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
      val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
      Some(new ExternalBlockStoreClient(transConf, securityManager,
        securityManager.isAuthenticationEnabled(),
                                        conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT)))
    } else {
      None
    }
    8. 设置内存管理器
    val memoryManager: MemoryManager = UnifiedMemoryManager(conf, numUsableCores)
    9. 设置块管理器
    val blockManagerPort = if (isDriver) {
      conf.get(DRIVER_BLOCK_MANAGER_PORT)
    } else {
      conf.get(BLOCK_MANAGER_PORT)
    }
    val blockManagerInfo = new concurrent.TrieMap[BlockManagerId, BlockManagerInfo]()
    val blockManagerMaster = new BlockManagerMaster(
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_ENDPOINT_NAME,
        new BlockManagerMasterEndpoint(
          rpcEnv,
          isLocal,
          conf,
          listenerBus,
          if (conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)) {
            externalShuffleClient
          } else {
            None
          }, blockManagerInfo)),
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_HEARTBEAT_ENDPOINT_NAME,
        new BlockManagerMasterHeartbeatEndpoint(rpcEnv, isLocal, blockManagerInfo)),
      conf,
      isDriver)
    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
        blockManagerPort, numUsableCores, blockManagerMaster.driverEndpoint)
    val blockManager = new BlockManager(
      executorId,
      rpcEnv,
      blockManagerMaster,
      serializerManager,
      conf,
      memoryManager,
      mapOutputTracker,
      shuffleManager,
      blockTransferService,
      securityManager,
      externalShuffleClient)
    10. 设置度量系统
    val metricsSystem = if (isDriver) {
      MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, 
                                        conf, securityManager)
    } else {
      conf.set(EXECUTOR_ID, executorId)
      val ms = MetricsSystem.createMetricsSystem(MetricsSystemInstances.EXECUTOR, conf,
        securityManager)
      ms.start(conf.get(METRICS_STATIC_SOURCES_ENABLED))
      ms
    }
    11. 设置输出提交协作者
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)
    12. 获取实例
    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)
    12. 设置驱动器临时目录
    if (isDriver) {
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf)
                                              , "userFiles").getAbsolutePath
      envInstance.driverTmpDir = Some(sparkFilesDir)
    }
    val= envInstance
    
    def environmentDetails(
      conf: SparkConf,
      hadoopConf: Configuration,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]]
    功能: 返回一个map,表示JVM信息,spark属性,系统属性和类路径的信息.key表示类别,value按kv对形式展示.主要用于spark监听器环境的更新.
    1. 获取spark属性
    val schedulerMode =
      if (!conf.contains(SCHEDULER_MODE)) {
        Seq((SCHEDULER_MODE.key, schedulingMode))
      } else {
        Seq.empty[(String, String)]
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted
    2. 获取系统属性
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted
    3. 获取类路径属性
     val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted
    4. 添加hadoop属性
    val hadoopProperties = hadoopConf.asScala
      .map(entry => (entry.getKey, entry.getValue)).toSeq.sorted
    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "Hadoop Properties" -> hadoopProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
}
```

#### SparkException

```scala
class SparkException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}
介绍: spark异常

private[spark] class SparkDriverExecutionException(cause: Throwable)
  extends SparkException("Execution error", cause)
介绍: spark执行器异常

private[spark] case class SparkUserAppException(exitCode: Int)
  extends SparkException(s"User application exited with $exitCode")
介绍: spark 用户程序异常

private[spark] case class ExecutorDeadException(message: String)
  extends SparkException(message)
介绍: 执行器失活异常
```

#### SparkFiles

```scala
object SparkFiles {
    介绍: 解决通过@SparkContext.addFile() 添加的路径
    操作集:
    def get(filename: String): String
    功能: 获取指定文件名的绝对文件路径
    val= new File(getRootDirectory(), filename).getAbsolutePath()
    
    def getRootDirectory(): String = SparkEnv.get.driverTmpDir.getOrElse(".")
    功能: 获取根目录
}
```

#### SparkStatusTracker

```scala
class SparkStatusTracker private[spark] (sc: SparkContext, store: AppStatusStore) {
    操作集:
    def getJobIdsForGroup(jobGroup: String): Array[Int]
    功能: 获取job组的job ID列表
    val= {
        val expected = Option(jobGroup)
        store.jobsList(null).filter(_.jobGroup == expected).map(_.jobId).toArray
      }
    
    def getActiveStageIds(): Array[Int]
    功能: 获取激活stage ID列表
    val= store.stageList(Arrays.asList(StageStatus.ACTIVE)).map(_.stageId).toArray
    
    def getActiveJobIds(): Array[Int] 
    功能: 获取激活job的列表
    val= store.jobsList(Arrays.asList(JobExecutionStatus.RUNNING)).map(_.jobId).toArray
    
    def getJobInfo(jobId: Int): Option[SparkJobInfo]
    功能: 获取指定job@jobId 的job信息
    val= store.asOption(store.job(jobId)).map { job =>
      new SparkJobInfoImpl(jobId, job.stageIds.toArray, job.status)
    }
    
    def getStageInfo(stageId: Int): Option[SparkStageInfo] 
    功能: 获取指定stage@stageId的stage信息
    val= store.asOption(store.lastStageAttempt(stageId)).map { stage =>
      new SparkStageInfoImpl(
        stageId,
        stage.attemptId,
        stage.submissionTime.map(_.getTime()).getOrElse(0L),
        stage.name,
        stage.numTasks,
        stage.numActiveTasks,
        stage.numCompleteTasks,
        stage.numFailedTasks)
    }
    
    def getExecutorInfos: Array[SparkExecutorInfo]
    功能: 获取指定执行器信息列表
    val= {
        store.executorList(true).map { exec =>
          val (host, port) = exec.hostPort.split(":", 2) match {
            case Array(h, p) => (h, p.toInt)
            case Array(h) => (h, -1)
          }
          val cachedMem = exec.memoryMetrics.map { mem =>
            mem.usedOnHeapStorageMemory + mem.usedOffHeapStorageMemory
          }.getOrElse(0L)

          new SparkExecutorInfoImpl(
            host,
            port,
            cachedMem,
            exec.activeTasks,
            exec.memoryMetrics.map(_.usedOffHeapStorageMemory).getOrElse(0L),
            exec.memoryMetrics.map(_.usedOnHeapStorageMemory).getOrElse(0L),
            exec.memoryMetrics.map(_.totalOffHeapStorageMemory).getOrElse(0L),
            exec.memoryMetrics.map(_.totalOnHeapStorageMemory).getOrElse(0L))
        }.toArray
      }
}
```

#### SSLOptions

```scala
private[spark] case class SSLOptions(
    enabled: Boolean = false, // 是否开启SSL,设置为false则无视其他项
    port: Option[Int] = None, // SSL服务器绑定的端口,不设置则基于无SSL端口服务
    keyStore: Option[File] = None, // keyStore 文件的路径
    keyStorePassword: Option[String] = None, // 获取keystore的密码
    keyPassword: Option[String] = None, // 获取Key-store中私有key的密码
    keyStoreType: Option[String] = None, // key-store类型
    needClientAuth: Boolean = false, // 是否需要客户端验证
    trustStore: Option[File] = None, // trust-store 路径
    trustStorePassword: Option[String] = None, // trust-store 密码
    trustStoreType: Option[String] = None, // trust-store 类型
    protocol: Option[String] = None, // 协议
    enabledAlgorithms: Set[String] = Set.empty)// 允许的算法集
extends Logging {
    介绍: 这个类使用SSL配置的通用选项配置,提供了配置SSL的方法,用于不同的通信协议.用于提供最大范围的SSL属性集配置,支持协议.
    属性:
    #name @supportedAlgorithms	支持算法集合
    val= if (enabledAlgorithms.isEmpty) {
        Set.empty
      } else {
        var context: SSLContext = null
        if (protocol.isEmpty) {
          logDebug("No SSL protocol specified")
          context = SSLContext.getDefault
        } else {
          try {
            context = SSLContext.getInstance(protocol.get)
            context.init(null, null, null)
          } catch {
            case nsa: NoSuchAlgorithmException =>
              logDebug(s"No support for requested SSL protocol ${protocol.get}")
              context = SSLContext.getDefault
          }
        }
        val providerAlgorithms =context.getServerSocketFactory
        .getSupportedCipherSuites.toSet
        (enabledAlgorithms &~ providerAlgorithms).foreach { cipher =>
          logDebug(s"Discarding unsupported cipher $cipher")
        }
        val supported = enabledAlgorithms & providerAlgorithms
        require(supported.nonEmpty || sys.env.contains("SPARK_TESTING"),
          "SSLContext does not support any of the enabled algorithms: " +
            enabledAlgorithms.mkString(","))
        supported
      }
    
    操作集:
    def toString: String
    功能: 显示信息
    val= s"SSLOptions{enabled=$enabled, port=$port, " +
      s"keyStore=$keyStore, keyStorePassword=${keyStorePassword.map(_ => "xxx")}, " +
      s"trustStore=$trustStore, trustStorePassword=${trustStorePassword.
      map(_ => "xxx")}, " +
      s"protocol=$protocol, enabledAlgorithms=$enabledAlgorithms}"
    
    def createJettySslContextFactory(): Option[SslContextFactory]
    功能: 获取jetty SSL上下文工厂类
    val= if (enabled) {
      val sslContextFactory = new SslContextFactory.Server()

      keyStore.foreach(file => sslContextFactory.setKeyStorePath(file.getAbsolutePath))
      keyStorePassword.foreach(sslContextFactory.setKeyStorePassword)
      keyPassword.foreach(sslContextFactory.setKeyManagerPassword)
      keyStoreType.foreach(sslContextFactory.setKeyStoreType)
      if (needClientAuth) {
        trustStore.foreach(file =>
                           sslContextFactory.setTrustStorePath(file.getAbsolutePath))
        trustStorePassword.foreach(sslContextFactory.setTrustStorePassword)
        trustStoreType.foreach(sslContextFactory.setTrustStoreType)
      }
      protocol.foreach(sslContextFactory.setProtocol)
      if (supportedAlgorithms.nonEmpty) {
        sslContextFactory.setIncludeCipherSuites(supportedAlgorithms.toSeq: _*)
      }
      Some(sslContextFactory)
    } else {
      None
    } 
}
```

```scala
private[spark] object SSLOptions extends Logging {
    操作集:
    def parse(conf: SparkConf,hadoopConf: Configuration,ns: String,
      defaults: Option[SSLOptions] = None): SSLOptions
    功能: 转换配置@SSLOptions,在指定命名空间@ns中解决spark配置的SSL配置
    运行配置下述属性:
    [ns].enabled	控制SSL的开启/关闭
    [ns].port	SSL绑定端口
    [ns].keyStore	key-store路径,与当前目录相关
    [ns].keyStorePassword	keyStore 密码
    [ns].keyPassword		私有key的密码
    [ns].keyStoreType		key-store类型
    [ns].needClientAuth		是否SSL需要客户端授权
    [ns].trustStore			trust-store文件路径,与当前目录相关
    [ns].trustStorePassword	trust-store文件路径密码
    [ns].trustStoreType		trust-store类型
    [ns].protocol	协议名称
    [ns].enabledAlgorithms	开启/关闭算法功能
	输入参数:
    	conf	spark配置信息
    	hadoopConf	hadoop配置信息
    	ns	命名空间名称
    	defaults	默认配置
    1. 获取参数值
    val enabled = conf.getBoolean(s"$ns.enabled", defaultValue = defaults.exists(_.enabled))

    val port = conf.getWithSubstitution(s"$ns.port").map(_.toInt)
    port.foreach { p =>
      require(p >= 0, "Port number must be a non-negative value.")
    }

    val keyStore = conf.getWithSubstitution(s"$ns.keyStore").map(new File(_))
        .orElse(defaults.flatMap(_.keyStore))

    val keyStorePassword = conf.getWithSubstitution(s"$ns.keyStorePassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.keyStorePassword")).map(new String(_)))
        .orElse(defaults.flatMap(_.keyStorePassword))

    val keyPassword = conf.getWithSubstitution(s"$ns.keyPassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.keyPassword")).map(new String(_)))
        .orElse(defaults.flatMap(_.keyPassword))

    val keyStoreType = conf.getWithSubstitution(s"$ns.keyStoreType")
        .orElse(defaults.flatMap(_.keyStoreType))

    val needClientAuth =
      conf.getBoolean(s"$ns.needClientAuth", defaultValue = defaults.exists(_.needClientAuth))

    val trustStore = conf.getWithSubstitution(s"$ns.trustStore").map(new File(_))
        .orElse(defaults.flatMap(_.trustStore))

    val trustStorePassword = conf.getWithSubstitution(s"$ns.trustStorePassword")
        .orElse(Option(hadoopConf.getPassword(s"$ns.trustStorePassword")).map(new String(_)))
        .orElse(defaults.flatMap(_.trustStorePassword))

    val trustStoreType = conf.getWithSubstitution(s"$ns.trustStoreType")
        .orElse(defaults.flatMap(_.trustStoreType))

    val protocol = conf.getWithSubstitution(s"$ns.protocol")
        .orElse(defaults.flatMap(_.protocol))

    val enabledAlgorithms = conf.getWithSubstitution(s"$ns.enabledAlgorithms")
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSet)
        .orElse(defaults.map(_.enabledAlgorithms))
        .getOrElse(Set.empty)
    2. 返回SSL配置
    val= new SSLOptions(
      enabled,
      port,
      keyStore,
      keyStorePassword,
      keyPassword,
      keyStoreType,
      needClientAuth,
      trustStore,
      trustStorePassword,
      trustStoreType,
      protocol,
      enabledAlgorithms)
}
```

#### StatusAPIImpl

```scala
private class SparkJobInfoImpl (
    val jobId: Int,
    val stageIds: Array[Int],
    val status: JobExecutionStatus)
extends SparkJobInfo
介绍: job信息
参数:
	jobId	jobID
	stageIds	stage编号列表
	status	job执行器状态

private class SparkStageInfoImpl(
    val stageId: Int,
    val currentAttemptId: Int,
    val submissionTime: Long,
    val name: String,
    val numTasks: Int,
    val numActiveTasks: Int,
    val numCompletedTasks: Int,
    val numFailedTasks: Int)
extends SparkStageInfo
介绍: stage信息
参数:
	stageId	stage编号
	currentAttemptId	当前请求编号
	submissionTime	提交时间
	name	stage名称
	numTasks	任务数量
	numActiveTasks	激活任务数量
	numCompletedTasks	完成任务数量
	numFailedTasks	失败任务数量

private class SparkExecutorInfoImpl(
    val host: String,
    val port: Int,
    val cacheSize: Long,
    val numRunningTasks: Int,
    val usedOnHeapStorageMemory: Long,
    val usedOffHeapStorageMemory: Long,
    val totalOnHeapStorageMemory: Long,
    val totalOffHeapStorageMemory: Long)
extends SparkExecutorInfo
介绍: 执行器信息
参数:
	host 主机名称
	port 端口号
	cacheSize	缓存大小
	numRunningTasks	运行任务数量
	usedOnHeapStorageMemory	堆上内存使用量
	usedOffHeapStorageMemory	非堆内存使用量
	totalOnHeapStorageMemory	总堆上内存
	totalOffHeapStorageMemory	总非堆内存

```

#### TaskContext

```scala
object TaskContext {
    介绍: 任务上下文
    属性:
    #name @taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]	任务上下文
    操作集:
    def get(): TaskContext = taskContext.get
    功能: 返回当前激活的上下文,可以在用户函数内部调用,获取运行任务的上下文信息.
    
    def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)
    功能: 设置任务上下文
    
    def unset(): Unit = taskContext.remove()
    功能: 解除线程本地的任务上下文,对于spark内部而言
    
    def empty(): TaskContextImpl 
    功能: 返回一个空的任务上下文
    val= new TaskContextImpl(0, 0, 0, 0, 0, null, new Properties, null)
    
    def getPartitionId(): Int
    功能: 获取分区编号
    val tc = taskContext.get()
    val= if (tc eq null) {
      0
    } else {
      tc.partitionId()
    }
}
```

```scala
abstract class TaskContext extends Serializable{
    介绍: 一个任务的上下文信息,可以在执行期间读取或者是修改.使用@org.apache.spark.TaskContext.get() 获取一个运行任务的任务上下文.
    操作集:
    def isCompleted(): Boolean
    功能: 确认任务是否完成
    
    def isInterrupted(): Boolean
    功能: 确认任务是否中断
    
    def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext
    功能: 添加任务完成监听器
    
    def addTaskCompletionListener[U](f: (TaskContext) => U): TaskContext
    功能: 添加任务完成的监听器,添加监听器到已经完成的任务中,会使得监听器立即被调用.
    	f为完成时的回调函数
    val= addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    })
    
    def addTaskFailureListener(listener: TaskFailureListener): TaskContext
    功能: 添加任务失败监听器
    
    def addTaskFailureListener(f: (TaskContext, Throwable) => Unit): TaskContext
    功能: 添加任务失败监听器
    val= addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit
        = f(context, error)
    })
    
    def stageId(): Int
    功能: 获取任务所属stage编号
    
    def stageAttemptNumber(): Int
    功能： stage 请求数量
    
    def partitionId(): Int
    功能: 获取分区ID
    
    def attemptNumber(): Int
    功能: 获取任务请求数量
    
    def taskAttemptId(): Long
    功能: 获取任务请求编号(同一个sparkContext 确保唯一性)
    
    def getLocalProperty(key: String): String
    功能: 获取本地属性
    
    @Evolving
    def resources(): Map[String, ResourceInformation]
    功能: 获取任务资源分配表
    
    @Evolving
    def resourcesJMap(): java.util.Map[String, ResourceInformation]
    功能: 获取任务资源分配表
    
    @DeveloperApi
    def taskMetrics(): TaskMetrics
    功能: 获取度量参数值
    
    @DeveloperApi
    def getMetricsSources(sourceName: String): Seq[Source]
    功能: 获取度量信息列表
    
    def killTaskIfInterrupted(): Unit
    功能: 中断任务
    
    def getKillReason(): Option[String]
    功能: 获取任务被kill的原因
    
    def taskMemoryManager(): TaskMemoryManager
    功能: 获取任务的任务内存管理器
    
    def registerAccumulator(a: AccumulatorV2[_, _]): Unit
    功能: 注册指定累加器@a 到当前任务中
    
    def setFetchFailed(fetchFailed: FetchFailedException): Unit
    功能: 设置获取失败异常
    
    def markInterrupted(reason: String): Unit
    功能: 标记中断及其原因@reason
    
    def markTaskFailed(error: Throwable): Unit
    功能: 标记任务失败
    
    def markTaskCompleted(error: Option[Throwable]): Unit
    功能: 标记任务完成
    
    def fetchFailed: Option[FetchFailedException]
    功能: 获取失败
    
    def getLocalProperties: Properties
    功能: 获取本地属性(设置在上游driver中)
}
```

#### TaskContextImpl

```scala
private[spark] class TaskContextImpl(
    override val stageId: Int, // stage编号
    override val stageAttemptNumber: Int, // stage请求编号,与stage编号构成唯一的标识符
    override val partitionId: Int, //分区编号
    override val taskAttemptId: Long, // 任务请求编号
    override val attemptNumber: Int, // 请求编号
    override val taskMemoryManager: TaskMemoryManager,// 任务内存管理器
    localProperties: Properties, // 本地属性
    @transient private val metricsSystem: MetricsSystem, // 度量系统
    override val taskMetrics: TaskMetrics = TaskMetrics.empty, // 任务度量器
    override val resources: Map[String, ResourceInformation] = Map.empty) // 资源映射表
  extends TaskContext
with Logging {
    介绍: 任务上下文的实现
    属性:
    #name @onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]
    任务完成时的回调函数列表
    #name @onFailureCallbacks = new ArrayBuffer[TaskFailureListener]
    任务失败时的回调函数列表
    #name @reasonIfKilled: Option[String] = None volatile	任务被kill的原因
    #name @completed: Boolean = false	任务完成状态
    #name @failed: Boolean = false	任务失败原因
    #name @failure: Throwable = _	引起任务失败的异常
    #name @_fetchFailedException: Option[FetchFailedException] = None
    任务失败的异常(确保不会隐藏用户代码异常),详情请看SPARK-19276
    操作集:
    def getLocalProperties(): Properties = localProperties
    功能: 获取本地属性
    
    def fetchFailed: Option[FetchFailedException] = _fetchFailedException
    功能: 获取任务失败原因
    
    def fetchFailed: Option[FetchFailedException] = _fetchFailedException
    功能: 设置任务失败原因
    this._fetchFailedException = Option(fetchFailed)
    
    def registerAccumulator(a: AccumulatorV2[_, _]): Unit
    功能: 注册累加器到指定的任务度量器中
    taskMetrics.registerAccumulator(a)
    
    def getMetricsSources(sourceName: String): Seq[Source]
    功能: 获取指定的度量资源@sourceName列表
    val= metricsSystem.getSourcesByName(sourceName)
    
    def getLocalProperty(key: String): String = localProperties.getProperty(key)
    功能: 获取本地指定@key 的资源信息
    
    def isInterrupted(): Boolean = reasonIfKilled.isDefined
    功能: 确定任务是否被中断
    
    @GuardedBy("this")
    def isCompleted(): Boolean = synchronized(completed)
    功能: 确定任务是否被完成
    
    def getKillReason(): Option[String] = reasonIfKilled
    功能: 获取任务中断原因
    
    def killTaskIfInterrupted(): Unit
    功能: 中断任务
    val reason = reasonIfKilled
    if (reason.isDefined) {
      throw new TaskKilledException(reason.get)
    }
    
    def markInterrupted(reason: String): Unit= reasonIfKilled = Some(reason)
    功能: 标记中断
    
    def resourcesJMap(): java.util.Map[String, ResourceInformation] 
    功能: 获取资源列表
    
    def invokeListeners[T](
      listeners: Seq[T],
      name: String,
      error: Option[Throwable])(
      callback: T => Unit): Unit
    功能: 调用监听器@listeners,执行回调函数@callback
    val errorMsgs = new ArrayBuffer[String](2)
    listeners.reverse.foreach { listener =>
      try {
        callback(listener)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError(s"Error in $name", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs, error)
    }
    
    @GuardedBy("this")
    def markTaskCompleted(error: Option[Throwable]): Unit
    功能: 标记任务完成
    synchronized {
        if (completed) return
        completed = true
        invokeListeners(onCompleteCallbacks, "TaskCompletionListener", error) {
          _.onTaskCompletion(this)
        }
      }
    
    @GuardedBy("this")
    def markTaskFailed(error: Throwable): Unit 
    功能: 标记任务失败
    synchronized {
        if (failed) return
        failed = true
        failure = error
        invokeListeners(onFailureCallbacks, "TaskFailureListener", Option(error)) {
          _.onTaskFailure(this, error)
        }
      }
    
    @GuardedBy("this")
    def addTaskFailureListener(listener: TaskFailureListener)
    功能: 添加任务失败监听器内容
    if (failed) {
      listener.onTaskFailure(this, failure)
    } else {
      onFailureCallbacks += listener
    }
    val= this
    
    @GuardedBy("this")
    def addTaskCompletionListener(listener: TaskCompletionListener): this.type
    功能: 添加任务完成监听器
    val= synchronized {
        if (completed) {
          listener.onTaskCompletion(this)
        } else {
          onCompleteCallbacks += listener
        }
        this
      }
}
```

#### TaskEndReason

```scala
@DeveloperApi
sealed trait TaskEndReason
介绍: 任务结束原因

@DeveloperApi
case object Success extends TaskEndReason
介绍: 任务结束原因为任务执行成功

@DeveloperApi
sealed trait TaskFailedReason extends TaskEndReason{
    介绍: 任务结束原因为任务失败
    操作集:
    def toErrorString: String
    功能: 用于WEB UI显示失败信息
    
    def countTowardsTaskFailures: Boolean = true
    功能: 是否对任务失败次数进行计数
}

@DeveloperApi
case object Resubmitted extends TaskFailedReason {
    介绍: @org.apache.spark.scheduler.ShuffleMapTask 任务很早的就成功执行完毕,但是在stage完成之前,executor先注销了,这就意味着spark需要对任务重新进行调度,在其他执行器上重新执行.
  override def toErrorString: String = "Resubmitted (resubmitted due to lost executor)"
}

@DeveloperApi
case class FetchFailed(
    bmAddress: BlockManagerId,  
    shuffleId: Int,
    mapId: Long,
    mapIndex: Int,
    reduceId: Int,
    message: String)
extends TaskFailedReason{
    介绍: 任务失败的一种 -> 获取数据块失败
    参数:
        bmAddress	块管理器ID
        shuffleId	shuffleId
        mapId	mapId
        mapIndex	map索引
        reduceId	reduce ID
    	message	消息	
	override def toErrorString: String = {
        val bmAddressString = if (bmAddress == null) "null" else bmAddress.toString
        s"FetchFailed($bmAddressString, shuffleId=$shuffleId, mapIndex=$mapIndex, " +
          s"mapId=$mapId, reduceId=$reduceId, message=\n$message\n)"
      }
    
    def countTowardsTaskFailures: Boolean = false
}

@DeveloperApi
case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement],
    fullStackTrace: String,
    private val exceptionWrapper: Option[ThrowableSerializationWrapper],
    accumUpdates: Seq[AccumulableInfo] = Seq.empty,
    private[spark] var accums: Seq[AccumulatorV2[_, _]] = Nil,
    private[spark] var metricPeaks: Seq[Long] = Seq.empty)
extends TaskFailedReason{
    介绍： 由于引发异常引起的任务失败
    fullStackTrace	栈追踪信息的最好表示方法，因为包含了所有的追踪信息
    操作集：
    def withAccums(accums: Seq[AccumulatorV2[_, _]]): ExceptionFailure = {
        this.accums = accums
        this
    }
    功能： 设置累加器
    
    def withMetricPeaks(metricPeaks: Seq[Long]): ExceptionFailure = {
        this.metricPeaks = metricPeaks
        this
    }
    功能: 设置计量列表
    
    def exception: Option[Throwable] = exceptionWrapper.flatMap(w => Option(w.exception))
    功能: 获取异常原因
    
    def toErrorString: String 
    功能: 显示错误信息,优选全栈追踪
    if (fullStackTrace == null) {
      exceptionString(className, description, stackTrace)
    } else {
      fullStackTrace
    }
    
    def exceptionString(
        className: String,
        description: String,
        stackTrace: Array[StackTraceElement]): String = {
        val desc = if (description == null) "" else description
        val st = if (stackTrace == null) "" else stackTrace.map("        " + _).mkString("\n")
        s"$className: $desc\n$st"
    }
    介绍: 异常原因
    
    private[spark] class ThrowableSerializationWrapper(var exception: Throwable) extends
    Serializable with Logging {
        介绍: 这个类是用于当反序列化遇到异常时,恢复异常的处理方法.如果这个异常不可以被反序列化,这个就是null,但是栈追踪信息和消息会被保存到@SparkException
          def readObject(in: ObjectInputStream): Unit = {
            try {
              exception = in.readObject().asInstanceOf[Throwable]
            } catch {
              case e : Exception => log.warn("Task exception could 
              not be deserialized", e)
            }
          }
          功能: 读取输入,可以是异常信息数据,如果异常信息函数不能会被序列化,在保存到@SparkExeption
    }
    
    @DeveloperApi
    case object TaskResultLost extends TaskFailedReason {
        介绍: 失败原因为任务结果丢失
      override def toErrorString: String = "TaskResultLost (result 
      lost from block manager)"
    }
    
    @DeveloperApi
    case class TaskKilled(
        reason: String,
        accumUpdates: Seq[AccumulableInfo] = Seq.empty,
        private[spark] val accums: Seq[AccumulatorV2[_, _]] = Nil,
        metricPeaks: Seq[Long] = Seq.empty)
    extends TaskFailedReason {
        介绍: 失败原因是任务被内部kill,需要重新调度
        override def toErrorString: String = s"TaskKilled ($reason)"
        override def countTowardsTaskFailures: Boolean = false
    }
    
    @DeveloperApi
    case class TaskCommitDenied(
        jobID: Int,
        partitionID: Int,
        attemptNumber: Int) extends TaskFailedReason {
        介绍: 任务请求驱动器提交,但是身份或者权限不符合
        override def toErrorString: String = s"TaskCommitDenied (Driver denied 
        task commit)" +
        s" for job: $jobID, partition: $partitionID, attemptNumber: $attemptNumber"
        
       	override def countTowardsTaskFailures: Boolean = false
    }
    
    @DeveloperApi
    case class ExecutorLostFailure(
        execId: String,
        exitCausedByApp: Boolean = true,
        reason: Option[String]) extends TaskFailedReason {
        介绍: 执行器丢失引发的异常,可能是由于任务使得JVM崩溃
        override def toErrorString: String = {
            val exitBehavior = if (exitCausedByApp) {
              "caused by one of the running tasks"
            } else {
              "unrelated to the running tasks"
            }
            s"ExecutorLostFailure (executor ${execId} exited ${exitBehavior})" +
              reason.map { r => s" Reason: $r" }.getOrElse("")
          }
        
        override def countTowardsTaskFailures: Boolean = exitCausedByApp
    }
    
    @DeveloperApi
    case object UnknownReason extends TaskFailedReason {
        介绍: 未知原因引发的失败
      override def toErrorString: String = "UnknownReason"
    }
}
```

#### TaskKilledException

```scala
@DeveloperApi
class TaskKilledException(val reason: String) extends RuntimeException {
  def this() = this("unknown reason")
}
介绍: 当任务意外的被kill,抛出的异常
```

#### TaskNotSerializableException

```scala
private[spark] class TaskNotSerializableException(error: Throwable) extends Exception(error)
介绍: 任务不能序列化抛出的异常
```

#### TaskOutputFileAlreadyExistException

```scala
private[spark] class TaskOutputFileAlreadyExistException(error: Throwable) extends Exception(error)
介绍: 由于任务写出输出文件已经存在的异常
```

#### TaskState

```scala
private[spark] object TaskState extends Enumeration {
    属性:
    #name @LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value
    任务运行状态(启动，运行，完成，失败，kill，lost)
    #name @FINISHED_STATES = Set(FINISHED, FAILED, KILLED, LOST)
    完成状态
    type TaskState = Value	任务状态
    操作集:
    def isFailed(state: TaskState): Boolean = (LOST == state) || (FAILED == state)
    确认任务是否失败
    def isFinished(state: TaskState): Boolean = FINISHED_STATES.contains(state)
    确认任务是否完成
}
```

#### TestUtils

```scala
private[spark] object TestUtils {
    介绍:测试通用类
    属性:
    #name @SOURCE = JavaFileObject.Kind.SOURCE	
    操作集:
    def createJarWithClasses(
      classNames: Seq[String],
      toStringValue: String = "",
      classNamesWithBase: Seq[(String, String)] = Seq.empty,
      classpathUrls: Seq[URL] = Seq.empty): URL
    功能: 创建指定名称的jar包
    
    def createJarWithFiles(files: Map[String, String], dir: File = null): URL 
    功能: 创建包含多个文件的jar包
    
    def createJar(
      files: Seq[File],
      jarFile: File,
      directoryPrefix: Option[String] = None,
      mainClass: Option[String] = None): URL 
    功能: 创建包含指定文件集@files 的jar包,位于指定目录@directoryPrefix下或者在jar包根目录下
    
    def createURI(name: String) 
    功能: 创建同一地址标识符
    val=  URI.create(s"string:///${name.replace(".", "/")}${SOURCE.extension}")
    
    def createCompiledClass(
      className: String,
      destDir: File,
      sourceFile: JavaSourceFromString,
      classpathUrls: Seq[URL]): File 
    功能: 创建使用source文件编译的类,类文件置于@destDir 下
    
    def createCompiledClass(
      className: String,
      destDir: File,
      toStringValue: String = "",
      baseClass: String = null,
      classpathUrls: Seq[URL] = Seq.empty): File 
    功能: 创建指定名称的类文件,类文件置于@destDir下
    
    def assertSpilled(sc: SparkContext, identifier: String)(body: => Unit): Unit 
    功能: 溢写断言
    
    def assertNotSpilled(sc: SparkContext, identifier: String)(body: => Unit): Unit
    功能: 断言job没有溢写
    
    def assertExceptionMsg(exception: Throwable, msg: String, ignoreCase: Boolean = false): Unit
    功能: 断言异常信息
    
    def testCommandAvailable(command: String): Boolean 
    功能: 测试指令是否正常执行
    val= {
        val attempt = Try(Process(command).run(ProcessLogger(_ => ())).exitValue())
        attempt.isSuccess && attempt.get == 0
      }
    
    def httpResponseCode(
      url: URL,
      method: String = "GET",
      headers: Seq[(String, String)] = Nil): Int
    功能： 获取HTTP/HTTPS响应码
    
    def withHttpConnection[T](
      url: URL,
      method: String = "GET",
      headers: Seq[(String, String)] = Nil)
      (fn: HttpURLConnection => T): T
    功能： 获取HTTP连接结果
    
    def withListener[L <: SparkListener](sc: SparkContext, listener: L) (body: L => Unit): Unit
    功能： 监听代码块@body，代码运行完毕之后，方法会等待所有事件到监听总线上。然后才会移除事件。
    
    def configTestLog4j(level: String): Unit
    功能：配置log4j属性
    
    def recursiveList(f: File): Array[File]
    功能：递归展示文件
    
    def createTempJsonFile(dir: File, prefix: String, jsonValue: JValue): String
    功能： 创建临时json文件
    
    def createTempScriptWithExpectedOutput(dir: File, prefix: String, output: String): String
    功能： 创建临时shell脚本
    
    def waitUntilExecutorsUp(
      sc: SparkContext,
      numExecutors: Int,
      timeout: Long): Unit 
    功能: 等待直到达到指定的执行器数量@numExecutors
}
```

```scala
private class SpillListener extends SparkListener {
    介绍: 溢写监听器
    属性:
    #name @stageIdToTaskMetrics = new mutable.HashMap[Int, ArrayBuffer[TaskMetrics]]
    	stage中任务度量器映射表(一对多)
    #name @spilledStageIds = new mutable.HashSet[Int]	溢写stage列表
    操作集:
    def numSpilledStages: Int= synchronized {spilledStageIds.size}
    功能: 获取溢写stage数量
    
    def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit
    功能: 任务结束处理
    synchronized {
        stageIdToTaskMetrics.getOrElseUpdate(
          taskEnd.stageId, new ArrayBuffer[TaskMetrics]) += taskEnd.taskMetrics
      }
    
    def onStageCompleted(stageComplete: SparkListenerStageCompleted): Unit
    功能: stage完成处理
    synchronized {
        val stageId = stageComplete.stageInfo.stageId
        val metrics = stageIdToTaskMetrics.remove(stageId).toSeq.flatten
        val spilled = metrics.map(_.memoryBytesSpilled).sum > 0
        if (spilled) {
          spilledStageIds += stageId
        }
      }
}
```

#### 基础拓展

1.  SSL
2.  [加密协议](https://blogs.oracle.com/java-platform-group/entry/diagnosing_tls_ssl_and_https)
3.   [内存屏障]([https://zh.wikipedia.org/zh-hans/%E5%86%85%E5%AD%98%E5%B1%8F%E9%9A%9C](https://zh.wikipedia.org/zh-hans/内存屏障))