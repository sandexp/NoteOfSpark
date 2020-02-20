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
	执行器分配管理器,
```

```scala

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

#### SecurityManager

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

#### SparkContext

#### SparkEnv

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

#### 基础拓展

1.  SSL
2.  [加密协议](https://blogs.oracle.com/java-platform-group/entry/diagnosing_tls_ssl_and_https)
3.   [内存屏障]([https://zh.wikipedia.org/zh-hans/%E5%86%85%E5%AD%98%E5%B1%8F%E9%9A%9C](https://zh.wikipedia.org/zh-hans/内存屏障))