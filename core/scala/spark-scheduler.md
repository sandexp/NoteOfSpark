#### spark-scheduler

---

1.  [cluster](# cluster)
2.  [dynalloc](# dynalloc)
3.  [local](# local)
4.  [AccumulableInfo.scala](# AccumulableInfo)
5.  [ActiveJob.scala](# ActiveJob)
6.  [AsyncEventQueue.scala](# AsyncEventQueue)
7. [BarrierJobAllocationFailed.scala](# BarrierJobAllocationFailed)
8.  [BlacklistTracker.scala](# BlacklistTracker)
9. [DAGScheduler.scala](# DAGScheduler)
10. [DAGSchedulerEvent.scala](# DAGSchedulerEvent)
11. [DAGSchedulerSource.scala](# DAGSchedulerSource)
12. [EventLoggingListener.scala](# EventLoggingListener)
13. [ExecutorFailuresInTaskSet.scala](# ExecutorFailuresInTaskSet)
14. [ExecutorLossReason.scala](# ExecutorLossReason)
15. [ExecutorResourceInfo.scala](# ExecutorResourceInfo)
16. [ExternalClusterManager.scala](# ExternalClusterManager)
17. [InputFormatInfo.scala](# InputFormatInfo)
18. [JobListener.scala](# JobListener)
19. [JobResult.scala](# JobResult)
20. [JobWaiter.scala](# JobWaiter)
21. [LiveListenerBus.scala](# LiveListenerBus)
22. [MapStatus.scala](# MapStatus)
23. [OutputCommitCoordination.scala](# OutputCommitCoordination)
24. [Pool.scala](# Pool)
25. [ReplayListenerBus.scala](# ReplayListenerBus)
26. [ResultStage.scala](# ResultStage)
27. [ResultTask.scala](# ResultTask)
28. [Schedulable.scala](# Schedulable)
29. [SchedulableBuilder.scala](# SchedulableBuilder)
30. [SchedulerBackend.scala](# SchedulerBackend)
31. [SchedulingAlgorithm.scala](# SchedulingAlgorithm)
32. [SchedulingMode.scala](# SchedulingMode)
33. [ShuffleMapStage.scala](# ShuffleMapStage)
34. [ShuffleMapTask.scala](# ShuffleMapTask)
35. [SparkListenerEvent.scala](# SparkListenerEvent)
36. [SparkListenerBus.scala](# SparkListenerBus)
37. [SplitInfo.scala](# SplitInfo)
38. [Stage.scala](# Stage)
39. [StageInfo.scala](# StageInfo)
40. [StatsReportListener.scala](# StatsReportListener)
41. [Task.scala](# Task)
42. [TaskDescription.scala](# TaskDescription)
43. [TaskInfo.scala](# TaskInfo)
44. [TaskLocality.scala](# TaskLocality)
45. [TaskLocation.scala](# TaskLocation)
46. [TaskResult.scala](# TaskResult)
47. [TaskResultGetter.scala](# TaskResultGetter)
48. [TaskScheduler.scala](# TaskScheduler)
49. [TaskSchedulerImpl.scala](# TaskSchedulerImpl)
50. [TaskSet.scala](# TaskSet)
51. [TaskSetBlacklist.scala](# TaskSetBlacklist)
52. [TaskSetManager.scala](# TaskSetManager)
53. [WorkerOffer.scala](# WorkerOffer)

---

#### cluster

1.  [CoarseGrainedClusterMessage.scala](# CoarseGrainedClusterMessage)

2.  [CoarseGrainedSchedulerBackend.scala](# CoarseGrainedSchedulerBackend)

3.  [ExecutorData.scala](# ExecutorData)

4.  [ExecutorInfo.scala](# ExecutorInfo)

5.  [SchedulerBackendUtils.scala](# SchedulerBackendUtils)

6.  [StandaloneSchedulerBackend.scala](# StandaloneSchedulerBackend)

   ---

   #### CoarseGrainedClusterMessage

   ```scala
   private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable
   介绍: 粒状集群消息
   ```

   ```scala
   private[spark] object CoarseGrainedClusterMessages {
       样例类:
       case object RetrieveSparkAppConfig extends CoarseGrainedClusterMessage
       介绍: spark检索配置
       
       case class SparkAppConfig(
         sparkProperties: Seq[(String, String)],
         ioEncryptionKey: Option[Array[Byte]],
         hadoopDelegationCreds: Option[Array[Byte]])
       extends CoarseGrainedClusterMessage
       介绍: spark应用配置
       参数:
       	sparkProperties	spark属性列表
       	ioEncryptionKey	密钥列表
       	hadoopDelegationCreds	hadoop授权密钥
       
       case object RetrieveLastAllocatedExecutorId extends CoarseGrainedClusterMessage
       介绍: 检索上次分配的执行器ID
       
       case class LaunchTask(data: SerializableBuffer) extends
       CoarseGrainedClusterMessage
       介绍: 运行任务(driver发送给执行器的)
       
       case class KillTask(taskId: Long, executor: String, interruptThread: Boolean,
                           reason: String) extends CoarseGrainedClusterMessage
       介绍: kill指定任务消息
       
       case class KillExecutorsOnHost(host: String)
       extends CoarseGrainedClusterMessage
       介绍: kill指定主机上的执行器
       
       sealed trait RegisterExecutorResponse
       介绍: 注册执行器回应
       
       case object RegisteredExecutor extends CoarseGrainedClusterMessage 
       with RegisterExecutorResponse
       介绍: 注册执行器
       
       case class RegisterExecutorFailed(message: String) extends
       CoarseGrainedClusterMessage with RegisterExecutorResponse
       介绍: 支持执行器失败消息
       
       case class UpdateDelegationTokens(tokens: Array[Byte])
       extends CoarseGrainedClusterMessage
       介绍: 更新授权密钥
       
       case class RegisterExecutor(
           executorId: String,
           executorRef: RpcEndpointRef,
           hostname: String,
           cores: Int,
           logUrls: Map[String, String],
           attributes: Map[String, String],
           resources: Map[String, ResourceInformation])
       extends CoarseGrainedClusterMessage
       介绍: 注册执行器信息
       参数:
       	executorId	执行器名称
       	executorRef	执行器RPC通信端点
       	hostname	主机名
       	cores	核心数量
       	logUrls	url列表
       	attributes	属性表
       	resources	资源列表
       
       case class LaunchedExecutor(executorId: String) extends
       CoarseGrainedClusterMessage
       功能: 运行执行器
       
       case class StatusUpdate(
         executorId: String,
         taskId: Long,
         state: TaskState,
         data: SerializableBuffer,
         resources: Map[String, ResourceInformation] = Map.empty)
       extends CoarseGrainedClusterMessage
       功能: 任务状态更新
       参数:
       	executorId	执行器编号
       	taskId	任务编号
       	state	任务状态
       	data	序列化缓冲区
       	resources	资源列表
       
       case object ReviveOffers extends CoarseGrainedClusterMessage
       功能: 恢复驱动器状态(驱动器内部调用)
       
       case object StopDriver extends CoarseGrainedClusterMessage
       功能: 停止驱动器
       
       case object StopExecutor extends CoarseGrainedClusterMessage
       功能: 停止执行器
       
       case object StopExecutors extends CoarseGrainedClusterMessage
       功能: 停止多个驱动器
       
       case class RemoveExecutor(executorId: String, reason: ExecutorLossReason)
       extends CoarseGrainedClusterMessage
       功能: 移除指定驱动器
       
       case class RemoveWorker(workerId: String, host: String, message: String)
       extends CoarseGrainedClusterMessage
       功能: 移除worker
       
       case class SetupDriver(driver: RpcEndpointRef) extends CoarseGrainedClusterMessage
       功能: 创建驱动器
       
       case class AddWebUIFilter(
         filterName: String, filterParams: Map[String, String], proxyBase: String)
       extends CoarseGrainedClusterMessage
       功能: 创建webUI过滤器
       
       case class RegisterClusterManager(am: RpcEndpointRef) 
       extends CoarseGrainedClusterMessage
       功能: 交换驱动器和集群管理器在yarn模式下的执行器分配消息.主要在驱动器和AM之间进行交换.
       
       object RetrieveDelegationTokens extends CoarseGrainedClusterMessage
       功能: yarn客户端模式下,AM检索当前授权密钥
       
       case class RequestExecutors(
         requestedTotal: Int,
         localityAwareTasks: Int,
         hostToLocalTaskCount: Map[String, Int],
         nodeBlacklist: Set[String])
       extends CoarseGrainedClusterMessage
       功能: 请求执行消息
       参数:
       	requestedTotal	请求执行器总数
       	localityAwareTasks	位置感知任务数量
       	hostToLocalTaskCount	本地任务计数
       	nodeBlacklist	黑名单节点列表
       
       case class GetExecutorLossReason(executorId: String) extends
       CoarseGrainedClusterMessage
       功能: 获取执行器失效原因
       
       case class KillExecutors(executorIds: Seq[String]) extends
       CoarseGrainedClusterMessage
       功能: kill指定列表中的执行器
       
       case object Shutdown extends CoarseGrainedClusterMessage
       功能: 执行器内部执行,用于关闭自己
   }
   ```

   #### CoarseGrainedSchedulerBackend

   ```scala
   private[spark] object CoarseGrainedSchedulerBackend {
     介绍: 粒度调度器后端
     属性:
     val ENDPOINT_NAME = "CoarseGrainedScheduler"	端点名称
   }
   ```

   ```markdown
   介绍:
   	一个调度器后端等待粒度执行器连接，后端在spark job执行器期间容纳每个执行器。而不是任务执行完毕抛弃执行器，且当任务来的时候请求调度器重新调度执行器执行。执行器可以运行在多种模式下，比如Mesos模式或者独立模式。
   ```

   ```scala
   class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv)
   extends ExecutorAllocationClient with SchedulerBackend with Logging {
       参数:
       	scheduler	任务调度器
       	rpcEnv	RPC环境
       #name @totalCoreCount = new AtomicInteger(0)	核心总数
       #name @totalRegisteredExecutors = new AtomicInteger(0)	注册执行器总量
       #name @conf = scheduler.sc.conf	调度器配置
       #name @maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)	最大RPC消息大小
       #name @defaultAskTimeout = RpcUtils.askRpcTimeout(conf)	默认超时访问时间
       #name @	_minRegisteredRatio	最小登记比例
       val= math.min(1, conf.get(SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO)
                     .getOrElse(0.0))
       #name @maxRegisteredWaitingTimeNs	最大登记等待时间
       val= TimeUnit.MILLISECONDS.toNanos(
       conf.get(SCHEDULER_MAX_REGISTERED_RESOURCE_WAITING_TIME))
       #name @createTimeNs = System.nanoTime()	当前创建时间
       #name @taskResourceNumParts: Map[String, Int]	任务资源数量表
           val= if (scheduler.resourcesReqsPerTask != null) {
             scheduler.resourcesReqsPerTask.map(req => req.resourceName ->
                                                req.numParts).toMap
           } else {
             Map.empty
           }
       #name @executorDataMap = new HashMap[String, ExecutorData]	执行器数据表
       #name @requestedTotalExecutors=0 	请求总执行器数量
       #name @numPendingExecutors = 0	待定执行器数量
       #name @listenerBus = scheduler.sc.listenerBus	监听总线
       #name @executorsPendingToRemove = new HashMap[String, Boolean] 待定执行器需要移除列表
       #name @hostToLocalTaskCount: Map[String, Int] = Map.empty 主机运行任务数量信息表
       #name @localityAwareTasks = 0	位置感应任务数量
       #name @currentExecutorIdCounter = 0	当前执行器计数值
       #name @delegationTokens = new AtomicReference[Array[Byte]]()	委托授权
       #name @delegationTokenManager: Option[HadoopDelegationTokenManager] = None
       	委托授权管理器
       #name @reviveThread	恢复线程
       	val= ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver
       	-revive-thread")
       #name @driverEndpoint	驱动器端点
       val= rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint())
       操作集:
       def minRegisteredRatio: Double = _minRegisteredRatio
       功能：获取最小注册比例
       
       def start(): Unit
       功能：启动调度器后端
       if (UserGroupInformation.isSecurityEnabled()) {
         // 进行可能的安全检查
         delegationTokenManager = createTokenManager()
         delegationTokenManager.foreach { dtm =>
           val ugi = UserGroupInformation.getCurrentUser()
           val tokens = if (dtm.renewalEnabled) {
             dtm.start()
           } else {
             val creds = ugi.getCredentials()
             dtm.obtainDelegationTokens(creds)
             if (creds.numberOfTokens() > 0 || creds.numberOfSecretKeys() > 0) {
               SparkHadoopUtil.get.serialize(creds)
             } else {
               null
             }
           }
           if (tokens != null) {
             updateDelegationTokens(tokens)
           }
         }
       }
       
       def createDriverEndpoint(): DriverEndpoint = new DriverEndpoint()
       功能: 创建驱动器端点
       
       def stopExecutors(): Unit
       功能: 订制驱动器
       try {
         if (driverEndpoint != null) {
           logInfo("Shutting down all executors")
           driverEndpoint.askSync[Boolean](StopExecutors) // 发送停止驱动器消息
         }
       } catch {
         case e: Exception =>
           throw new SparkException("Error asking standalone scheduler to shut
           down executors", e)
       }
       
       def stop(): Unit
       功能: 停止调度器后端
       reviveThread.shutdownNow() // 关闭恢复线程
       stopExecutors() // 关闭执行器
       delegationTokenManager.foreach(_.stop()) //停止授权管理器
       try {
         if (driverEndpoint != null) {
           driverEndpoint.askSync[Boolean](StopDriver) // driver端发出通知driver的消息
         }
       } catch {
         case e: Exception =>
           throw new SparkException("Error stopping standalone scheduler's
           driver endpoint", e)
       }
       
       def reset(): Unit
       功能: 重置调度器后端
       1. 获取执行器列表
       val executors: Set[String] = synchronized {
         requestedTotalExecutors = 0
         numPendingExecutors = 0
         executorsPendingToRemove.clear()
         executorDataMap.keys.toSet
       }
       2. 移除执行器
       executors.foreach { eid =>
         removeExecutor(eid, SlaveLost("Stale executor after cluster manager
         re-registered."))
       }
       
       def reviveOffers(): Unit= driverEndpoint.send(ReviveOffers)
       功能: 恢复空间
   
       def killTask(taskId: Long, executorId: String, 
                    interruptThread: Boolean, reason: String): Unit
       功能: kill指定编号的task
       driverEndpoint.send(KillTask(taskId, executorId, interruptThread, reason))
       
       def defaultParallelism(): Int
       功能: 设置默认并行度参数
       conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
       
       def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit
       功能: 移除指定执行器
       driverEndpoint.send(RemoveExecutor(executorId, reason)) // RPC发出移除消息给执行器
       
       def removeWorker(workerId: String, host: String, message: String): Unit 
       功能: 移除指定worker
       driverEndpoint.ask[Boolean](RemoveWorker(workerId, host,message)).failed.
       foreach(t => logError(t.getMessage, t))(ThreadUtils.sameThread)
       
       def sufficientResourcesRegistered(): Boolean = true
       功能: 确认是否注册高效使用资源
       
       def isReady(): Boolean
       功能: 确定当前是否准备完毕
       if (sufficientResourcesRegistered) {
         logInfo("SchedulerBackend is ready for scheduling beginning after " +
           s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
         return true
       }
       if ((System.nanoTime() - createTimeNs) >= maxRegisteredWaitingTimeNs) {
         logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
           s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeNs(ns)")
         return true
       }
       false
       
       def numExistingExecutors: Int = synchronized { executorDataMap.size }
       功能: 获取存活的执行器数量
       
       def getExecutorIds():Seq[String]= synchron执行器ized { executorDataMap.keySet.toSeq}
       功能: 获取执行器列表
       
       def isExecutorActive(id: String): Boolean
       功能: 确定指定执行器是否存活
       val= synchronized {
           executorDataMap.contains(id) && !executorsPendingToRemove.contains(id)
         }
       
       def maxNumConcurrentTasks(): Int
       功能: 获取最大并发任务数量
       val= synchronized {
           executorDataMap.values.map { executor =>
             executor.totalCores / scheduler.CPUS_PER_TASK
           }.sum
         }
       
       def getExecutorAvailableResources(
         executorId: String): Map[String, ExecutorResourceInfo]
       功能: 获取执行器可用资源列表
       val= synchronized {
           executorDataMap.get(executorId).map(_.resourcesInfo).getOrElse(Map.empty)
         }
       
       def requestExecutors(numAdditionalExecutors: Int): Boolean
       功能: 从集群管理器上请求指定数量的执行器
       0. 参数断言
       if (numAdditionalExecutors < 0) {
         throw new IllegalArgumentException(
           "Attempted to request a negative number of additional executor(s) " +
           s"$numAdditionalExecutors from the cluster manager. Please specify a 
           positive number!")
       }
       1. 获取请求的响应
       val response = synchronized {
         requestedTotalExecutors += numAdditionalExecutors
         numPendingExecutors += numAdditionalExecutors
         logDebug(s"Number of pending executors is now $numPendingExecutors")
         if (requestedTotalExecutors !=
             (numExistingExecutors + numPendingExecutors -
              executorsPendingToRemove.size)) {
           logDebug(
             s"""requestExecutors($numAdditionalExecutors): 
             Executor request doesn't match:
                |requestedTotalExecutors  = $requestedTotalExecutors
                |numExistingExecutors     = $numExistingExecutors
                |numPendingExecutors      = $numPendingExecutors
                |executorsPendingToRemove = 
                ${executorsPendingToRemove.size}""".stripMargin)
         }
         doRequestTotalExecutors(requestedTotalExecutors)
       }
       2. 等待请求结果
       val= defaultAskTimeout.awaitResult(response)
       
       def requestTotalExecutors(
         numExecutors: Int,
         localityAwareTasks: Int,
         hostToLocalTaskCount: Map[String, Int]):Boolean
       介绍: 按照调度需求更新集群管理器,
       参数:
       	numExecutors	执行器数量
       	localityAwareTasks	位置感知任务数量(包含运行,待定,完成任务)
       	hostToLocalTaskCount	主机任务数量映射表
       返回参数:	这个请求是否被集群管理器识别
       1. 参数断言
       if (numExecutors < 0) {
         throw new IllegalArgumentException(
           "Attempted to request a negative number of executor(s) " +
             s"$numExecutors from the cluster manager. Please specify a 
             positive number!")
       }
       2. 获取响应
       val response = synchronized {
         this.requestedTotalExecutors = numExecutors
         this.localityAwareTasks = localityAwareTasks
         this.hostToLocalTaskCount = hostToLocalTaskCount
         numPendingExecutors =
           math.max(numExecutors - numExistingExecutors + 
                    executorsPendingToRemove.size, 0)
         doRequestTotalExecutors(numExecutors)
       }
       defaultAskTimeout.awaitResult(response)
       
       def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean]
       功能: 从集群管理器中请求执行器,通过指定需要的请求数量,包含待定和运行的任务,返回执行结果,异步执行.
       
       def killExecutors(
         executorIds: Seq[String],
         adjustTargetNumExecutors: Boolean,
         countFailures: Boolean,
         force: Boolean): Seq[String]
       功能: kill执行器列表
       1. 获取请求信息
       val response = withLock {
         val (knownExecutors, unknownExecutors) = 
           executorIds.partition(executorDataMap.contains)
         unknownExecutors.foreach { id =>
           logWarning(s"Executor to kill $id does not exist!")
         }
         val executorsToKill = knownExecutors
           .filter { id => !executorsPendingToRemove.contains(id) }
           .filter { id => force || !scheduler.isExecutorBusy(id) }
         executorsToKill.foreach { id => executorsPendingToRemove(id) = !countFailures }
         val adjustTotalExecutors =
           if (adjustTargetNumExecutors) {
             requestedTotalExecutors = math.max(requestedTotalExecutors - executorsToKill.size, 0)
             if (requestedTotalExecutors !=
                 (numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)) {
               logDebug(
                 s"""killExecutors($executorIds, $adjustTargetNumExecutors, $countFailures, $force):
                    |Executor counts do not match:
                    |requestedTotalExecutors  = $requestedTotalExecutors
                    |numExistingExecutors     = $numExistingExecutors
                    |numPendingExecutors      = $numPendingExecutors
                    |executorsPendingToRemove = ${executorsPendingToRemove.size}""".stripMargin)
             }
             doRequestTotalExecutors(requestedTotalExecutors)
           } else {
             numPendingExecutors += executorsToKill.size
             Future.successful(true)
           }
       }
         val killExecutors: Boolean => Future[Boolean] =
           if (executorsToKill.nonEmpty) {
             _ => doKillExecutors(executorsToKill)
           } else {
             _ => Future.successful(false)
           }
         val killResponse = adjustTotalExecutors.flatMap(killExecutors)
       (ThreadUtils.sameThread
         killResponse.flatMap(killSuccessful =>
           Future.successful (if (killSuccessful) executorsToKill else Seq.empty[String])
         )(ThreadUtils.sameThread)
       2. 等待响应结果
        val= defaultAskTimeout.awaitResult(response)
        
       def doKillExecutors(executorIds: Seq[String]): Future[Boolean]
       功能: kill指定执行器列表@executorIds
       val= Future.successful(false)
       
       def killExecutorsOnHost(host: String): Boolean
       功能: 向集群管理器申请删除指定主机上的执行器
       driverEndpoint.send(KillExecutorsOnHost(host))
       val= true
        
        
       def createTokenManager(): Option[HadoopDelegationTokenManager] = None
        功能： 创建委托管理器
        
       def currentDelegationTokens: Array[Byte] = delegationTokens.get()
       功能： 获取当前授权密钥
       
       def withLock[T](fn: => T): T 
       功能： 为了修正@TaskSchedulerImpl 和 @CoarseGrainedSchedulerBackend 之间的死锁问题，需要借此来处理
       val= scheduler.synchronized {
       	CoarseGrainedSchedulerBackend.this.synchronized { fn }
     	}
      
       def updateDelegationTokens(tokens: Array[Byte]): Unit
       功能： 更新授权密钥
       SparkHadoopUtil.get.addDelegationTokens(tokens, conf)
       delegationTokens.set(tokens)
       executorDataMap.values.foreach { ed =>
         ed.executorEndpoint.send(UpdateDelegationTokens(tokens))
       }
   }
   ```

   ```scala
   class DriverEndpoint extends IsolatedRpcEndpoint with Logging {
       介绍: driver端点
       属性:
       #name @rpcEnv: RpcEnv = CoarseGrainedSchedulerBackend.this.rpcEnv	RPC环境
       #name @executorsPendingLossReason = new HashSet[String]	执行器待定丢失原因
       #name @addressToExecutorId = new HashMap[RpcAddress, String] RPC地址与执行器ID映射表
       #name @sparkProperties	spark属性
       val= scheduler.sc.conf.getAll.filter { case (k, _) => k.startsWith("spark.") }
         .toSeq
       #name @logUrlHandler: ExecutorLogUrlHandler	URL处理器
       val= new ExecutorLogUrlHandler(conf.get(UI.CUSTOM_EXECUTOR_LOG_URL))
       操作集:
       def onStart(): Unit
       功能: 开启驱动器侧端点
       val reviveIntervalMs = conf.get(SCHEDULER_REVIVE_INTERVAL).getOrElse(1000L)
       reviveThread.scheduleAtFixedRate(() => Utils.tryLogNonFatalError {
           Option(self).foreach(_.send(ReviveOffers))
       }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
       
       def receive: PartialFunction[Any, Unit]
       功能: 接受消息
       case StatusUpdate(executorId, taskId, state, data, resources) => 
       	// 接收到状态更新消息
           scheduler.statusUpdate(taskId, state, data.value)
           if (TaskState.isFinished(state)) {
             executorDataMap.get(executorId) match {
               case Some(executorInfo) =>
                 executorInfo.freeCores += scheduler.CPUS_PER_TASK
                 resources.foreach { case (k, v) =>
                   executorInfo.resourcesInfo.get(k).foreach { r =>
                     r.release(v.addresses)
                   }
                 }
                 makeOffers(executorId)
               case None =>
                 logWarning(s"Ignored task status update ($taskId state $state) " +
                   s"from unknown executor with ID $executorId")
             }
           }
         case ReviveOffers => // 收到空间移除的消息
           makeOffers()
         case KillTask(taskId, executorId, interruptThread, reason) =>
       	// 介绍kill人物的消息
           executorDataMap.get(executorId) match {
             case Some(executorInfo) =>
               executorInfo.executorEndpoint.send(
                 KillTask(taskId, executorId, interruptThread, reason))
             case None =>
               logWarning(s"Attempted to kill task $taskId for unknown 
               executor $executorId.")
           }
         case KillExecutorsOnHost(host) =>
       	// 接受kill主机的执行器消息
           scheduler.getExecutorsAliveOnHost(host).foreach { exec =>
             killExecutors(exec.toSeq, adjustTargetNumExecutors = false, 
                           countFailures = false,force = true)
           }
         case UpdateDelegationTokens(newDelegationTokens) => // 更新授权密钥
           updateDelegationTokens(newDelegationTokens)
         case RemoveExecutor(executorId, reason) => // 移除执行器消息
           executorDataMap.get(executorId).foreach(_.executorEndpoint.send(StopExecutor))
           removeExecutor(executorId, reason)
         case LaunchedExecutor(executorId) => // 运行执行器消息
           executorDataMap.get(executorId).foreach { data =>
             data.freeCores = data.totalCores
           }
           makeOffers(executorId)
       
       def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
       功能: 回应处理方式
       case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls,
             attributes, resources) => // 注册执行器消息
           if (executorDataMap.contains(executorId)) {
             executorRef.send(RegisterExecutorFailed("Duplicate executor ID: " 
                                                     + executorId))
             context.reply(true)
           } else if (scheduler.nodeBlacklist.contains(hostname)) {
             logInfo(s"Rejecting $executorId as it has been blacklisted.")
             executorRef.send(RegisterExecutorFailed(s"Executor is blacklisted:
             $executorId"))
             context.reply(true)
           } else {
             val executorAddress = if (executorRef.address != null) {
                 executorRef.address
               } else {
                 context.senderAddress
               }
             addressToExecutorId(executorAddress) = executorId
             totalCoreCount.addAndGet(cores)
             totalRegisteredExecutors.addAndGet(1)
             val resourcesInfo = resources.map{ case (k, v) =>
               (v.name,
                new ExecutorResourceInfo(v.name, v.addresses,
                  taskResourceNumParts.getOrElse(v.name, 1)))
             }
             val data = new ExecutorData(executorRef, executorAddress, hostname,
               0, cores, logUrlHandler.applyPattern(logUrls, attributes), attributes,
               resourcesInfo)
             CoarseGrainedSchedulerBackend.this.synchronized {
               executorDataMap.put(executorId, data)
               if (currentExecutorIdCounter < executorId.toInt) {
                 currentExecutorIdCounter = executorId.toInt
               }
               if (numPendingExecutors > 0) {
                 numPendingExecutors -= 1
                 logDebug(s"Decremented number of pending executors 
                 ($numPendingExecutors left)")
               }
             }
             executorRef.send(RegisteredExecutor)
             context.reply(true)
             listenerBus.post(
               SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
           }
         case StopDriver => // 回应停止驱动器消息
           context.reply(true)
           stop()
         case StopExecutors => // 回应停止执行器消息
           logInfo("Asking each executor to shut down")
           for ((_, executorData) <- executorDataMap) {
             executorData.executorEndpoint.send(StopExecutor)
           }
           context.reply(true)
         case RemoveWorker(workerId, host, message) => // 回应移除worker消息
           removeWorker(workerId, host, message)
           context.reply(true)
         case RetrieveSparkAppConfig => // 回应检索spark 应用配置消息
           val reply = SparkAppConfig(
             sparkProperties,
             SparkEnv.get.securityManager.getIOEncryptionKey(),
             Option(delegationTokens.get()))
           context.reply(reply)
       
       def onDisconnected(remoteAddress: RpcAddress): Unit
       功能: 端口与指定执行器RPC连接
       addressToExecutorId
           .get(remoteAddress)
           .foreach(removeExecutor(_, SlaveLost("Remote RPC client disassociated.
           Likely due to " +"containers exceeding thresholds, or network issues. 
           Check driver logs for WARN " +
             "messages.")))
       
       def makeOffers(): Unit 
       功能: 在所有执行器上制造伪资源信息
       val taskDescs = withLock {
           val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
           val workOffers = activeExecutors.map {
             case (id, executorData) =>
               new WorkerOffer(id, executorData.executorHost, executorData.freeCores,
                 Some(executorData.executorAddress.hostPort),
                 executorData.resourcesInfo.map { case (rName, rInfo) =>
                   (rName, rInfo.availableAddrs.toBuffer)
                 })
           }.toIndexedSeq
           scheduler.resourceOffers(workOffers)
         }
         if (taskDescs.nonEmpty) {
           launchTasks(taskDescs)
         }
       
       def makeOffers(executorId: String): Unit
       功能: 在指定执行器上制造伪资源
   	val taskDescs = withLock {
           if (executorIsAlive(executorId)) {
             val executorData = executorDataMap(executorId)
             val workOffers = IndexedSeq(
               new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores,
                 Some(executorData.executorAddress.hostPort),
                 executorData.resourcesInfo.map { case (rName, rInfo) =>
                   (rName, rInfo.availableAddrs.toBuffer)
                 }))
             scheduler.resourceOffers(workOffers)
           } else {
             Seq.empty
           }
         }
         if (taskDescs.nonEmpty) {
           launchTasks(taskDescs)
         }
       
       def executorIsAlive(executorId: String): Boolean 
       功能: 确定执行器是否存活
       val= synchronized {
         !executorsPendingToRemove.contains(executorId) &&
           !executorsPendingLossReason.contains(executorId)
       }
       
       def removeWorker(workerId: String, host: String, message: String): Unit
       功能: 移除worker
       scheduler.workerRemoved(workerId, host, message)
       
       def disableExecutor(executorId: String): Boolean 
       功能: 取消指定执行器
       1. 确认是否取消
       val shouldDisable = CoarseGrainedSchedulerBackend.this.synchronized {
           if (executorIsAlive(executorId)) {
               executorsPendingLossReason += executorId
               true
           } else {
               executorsPendingToRemove.contains(executorId)
           }
       }
       2. 取消指定执行器
       if (shouldDisable) {
           logInfo(s"Disabling executor $executorId.")
           scheduler.executorLost(executorId, LossReasonPending)
       }
       val= shouldDisable
       
       def launchTasks(tasks: Seq[Seq[TaskDescription]]): Unit 
       功能: 运行资源列表中的任务
       for (task <- tasks.flatten) {
           val serializedTask = TaskDescription.encode(task)
           if (serializedTask.limit() >= maxRpcMessageSize) {
             Option(scheduler.taskIdToTaskSetManager.get(task.taskId)).foreach { taskSetMgr =>
               try {
                 var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                   s"${RPC_MESSAGE_MAX_SIZE.key} (%d bytes). Consider increasing " +
                   s"${RPC_MESSAGE_MAX_SIZE.key} or using broadcast variables for large values."
                 msg = msg.format(task.taskId, task.index, serializedTask.limit(), maxRpcMessageSize)
                 taskSetMgr.abort(msg)
               } catch {
                 case e: Exception => logError("Exception in error callback", e)
               }
             }
           }
           else {
             val executorData = executorDataMap(task.executorId)
             executorData.freeCores -= scheduler.CPUS_PER_TASK
             task.resources.foreach { case (rName, rInfo) =>
               assert(executorData.resourcesInfo.contains(rName))
               executorData.resourcesInfo(rName).acquire(rInfo.addresses)
             }
             executorData.executorEndpoint.send(LaunchTask(
                 new SerializableBuffer(serializedTask)))
           }
         }
   }
   ```

   #### ExecutorData

   ```scala
   private[cluster] class ExecutorData(
       val executorEndpoint: RpcEndpointRef,
       val executorAddress: RpcAddress,
       override val executorHost: String,
       var freeCores: Int,
       override val totalCores: Int,
       override val logUrlMap: Map[String, String],
       override val attributes: Map[String, String],
       override val resourcesInfo: Map[String, ExecutorResourceInfo]
   ) extends ExecutorInfo(executorHost, totalCores, logUrlMap, attributes, resourcesInfo)
   介绍: 执行器数据
   参数:
   	executorEndpoint	执行器RPC端点参考
   	executorAddress	执行器地址
   	executorHost	执行器主机名称
   	freeCores	释放核心数量
   	totalCores	总计核心数量
   	logUrlMap	url映射表
   	attributes	属性映射表
   	resourcesInfo	资源信息表
   ```

   #### ExecutorInfo

   ```scala
   @DeveloperApi
   class ExecutorInfo(
      val executorHost: String,
      val totalCores: Int,
      val logUrlMap: Map[String, String],
      val attributes: Map[String, String],
      val resourcesInfo: Map[String, ResourceInformation]) {
       介绍: 执行器信息
       参数:
       	executorHost	执行器主机名称
       	totalCores	核心总数
       	logUrlMap	url映射表
       	attributes	属性表
       	resourcesInfo	资源信息列表
       操作集:
       def hashCode(): Int 
       功能: hash函数
       val state = Seq(executorHost, totalCores, logUrlMap, attributes, resourcesInfo)
       val= state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
       
       def equals(other: Any): Boolean
       功能: 判断两个实例是否相等
       
       def canEqual(other: Any): Boolean = other.isInstanceOf[ExecutorInfo]
       功能: 确认是否可以比较
       
       构造器:
       def this(executorHost: String, totalCores: Int, logUrlMap: Map[String, String])
       功能: 创建空属性,空资源的实例
       val= this(executorHost, totalCores, logUrlMap, Map.empty, Map.empty)
       
       def this(
         executorHost: String,
         totalCores: Int,
         logUrlMap: Map[String, String],
         attributes: Map[String, String])
       功能: 创建空资源的实例
       val= this(executorHost, totalCores, logUrlMap, attributes, Map.empty)
   }
   ```

   #### SchedulerBackendUtils

   ```scala
   private[spark] object SchedulerBackendUtils {
       介绍: 后端调度工具类
       属性:
       #name @DEFAULT_NUMBER_EXECUTORS = 2	默认执行器数量
       操作集:
       def getInitialTargetExecutorNumber(
         conf: SparkConf,
         numExecutors: Int = DEFAULT_NUMBER_EXECUTORS): Int 
       功能: 根据动态分配的原则,获取指定数量的执行器,如果不使用动态分配,那么使用用户配置的信息
       val= if (Utils.isDynamicAllocationEnabled(conf)) {
         val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
         val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)
         val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
         require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= 
                 maxNumExecutors,
           s"initial executor number $initialNumExecutors must between min 
           executor number " +
             s"$minNumExecutors and max executor number $maxNumExecutors")
         initialNumExecutors
       } else {
         conf.get(EXECUTOR_INSTANCES).getOrElse(numExecutors)
       }
   }
   ```

   #### StandaloneSchedulerBackend

   ```scala
   private[spark] class StandaloneSchedulerBackend(
       scheduler: TaskSchedulerImpl,
       sc: SparkContext,
       masters: Array[String])
   extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
   with StandaloneAppClientListener with Logging {
       介绍: spark独立集群调度器的后端
       属性:
       #name @client: StandaloneAppClient = null	独立应用客户端
       #name @stopping = new AtomicBoolean(false)	停止标记位
       #name @launcherBackend	运行后端
       val= new LauncherBackend() {
           override protected def conf: SparkConf = sc.conf
           override protected def onStopRequest(): Unit = 
           	stop(SparkAppHandle.State.KILLED)
         }
       #name @shutdownCallback: StandaloneSchedulerBackend => Unit = _	关闭回调
       #name @appId: String = _	应用编号
       #name @registrationBarrier = new Semaphore(0)	注册屏蔽信号量
       #name @maxCores = conf.get(config.CORES_MAX)	最大核心数量
       #name @totalExpectedCores = maxCores.getOrElse(0)	总计核心数量
       操作集:
       def start(): Unit
       功能: 启动调度器后端
       if (sc.deployMode == "client") {
         launcherBackend.connect()
   
       }
       1. 获取启动参数
       val driverUrl = RpcEndpointAddress( // 获取驱动器RPC地址
         sc.conf.get(config.DRIVER_HOST_ADDRESS),
         sc.conf.get(config.DRIVER_PORT),
         CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
       val args = Seq( // 获取启动参数列表
         "--driver-url", driverUrl,
         "--executor-id", "{{EXECUTOR_ID}}",
         "--hostname", "{{HOSTNAME}}",
         "--cores", "{{CORES}}",
         "--app-id", "{{APP_ID}}",
         "--worker-url", "{{WORKER_URL}}")
       // 获取其他java配置属性
       val extraJavaOpts = sc.conf.get(config.EXECUTOR_JAVA_OPTIONS)
         .map(Utils.splitCommandString).getOrElse(Seq.empty)
       // 获取类路径信息
       val classPathEntries = sc.conf.get(config.EXECUTOR_CLASS_PATH)
         .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
       val libraryPathEntries = sc.conf.get(config.EXECUTOR_LIBRARY_PATH)
         .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
       val testingClassPath = // 获取测试类路径
         if (sys.props.contains(IS_TESTING.key)) {
           sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
         } else {
           Nil
         }
       val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
       val javaOpts = sparkJavaOpts ++ extraJavaOpts
       val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
         args, sc.executorEnvs, classPathEntries ++ testingClassPath, 
                             libraryPathEntries, javaOpts)
       val webUrl = sc.ui.map(_.webUrl).getOrElse("")
       val coresPerExecutor = conf.getOption(config.EXECUTOR_CORES.key).map(_.toInt)
       val initialExecutorLimit =
         if (Utils.isDynamicAllocationEnabled(conf)) {
           Some(0)
         } else {
           None
         }
       val executorResourceReqs = ResourceUtils.parseResourceRequirements(conf,
         config.SPARK_EXECUTOR_PREFIX)
       val appDesc = ApplicationDescription(sc.appName, maxCores, 
          	sc.executorMemory, command,webUrl, sc.eventLogDir, sc.eventLogCodec,
           coresPerExecutor, initialExecutorLimit,
         	resourceReqsPerExecutor = executorResourceReqs)
       2. 启动客户端
       client = new StandaloneAppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
       client.start()
       3. 设置后台状态
       launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
       waitForRegistration()
       launcherBackend.setState(SparkAppHandle.State.RUNNING)
       
       def stop(): Unit 
       功能: 停止调度器后台
       stop(SparkAppHandle.State.FINISHED)
       
       def connected(appId: String): Unit 
       功能: 连接指定应用
       this.appId = appId
       notifyContext()
       launcherBackend.setAppId(appId)
       
       def disconnected(): Unit
       功能: 断开连接
       notifyContext()
       if (!stopping.get) {
         logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
       }
       
       def dead(reason: String): Unit
       功能: 中断应用处理
       notifyContext()
       if (!stopping.get) {
         launcherBackend.setState(SparkAppHandle.State.KILLED)
         logError("Application has been killed. Reason: " + reason)
         try {
           scheduler.error(reason)
         } finally {
           sc.stopInNewThread()
         }
       }
       
       def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
       memory: Int): Unit
       功能: 给执行分配资源
       logInfo("Granted executor ID %s on hostPort %s with %d core(s), %s RAM".format(
         fullId, hostPort, cores, Utils.megabytesToString(memory)))
       
       def executorRemoved(fullId: String, message: String, exitStatus: Option[Int],
                           workerLost: Boolean): Unit
       功能: 移除执行器
       val reason: ExecutorLossReason = exitStatus match {
         case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
         case None => SlaveLost(message, workerLost = workerLost)
       }
       logInfo("Executor %s removed: %s".format(fullId, message))
       removeExecutor(fullId.split("/")(1), reason)
       
       def workerRemoved(workerId: String, host: String, message: String): Unit
       功能: 移除worker
       removeWorker(workerId, host, message)
       
       def applicationId(): String
       功能: 获取应用ID
       val= Option(appId).getOrElse {
         logWarning("Application ID is not initialized yet.")
         super.applicationId
       }
       
       def sufficientResourcesRegistered(): Boolean
       功能: 确认资源是否足够
       val= totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
       
       def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean]
       功能: 请求master上的执行器,通过指定需要的执行器数量,包括待定执行器,和运行执行器
       val= Option(client) match {
         case Some(c) => c.requestTotalExecutors(requestedTotal)
         case None =>
           logWarning("Attempted to request executors before driver fully initialized.")
           Future.successful(false)
       }
       
       def doKillExecutors(executorIds: Seq[String]): Future[Boolean]
       功能: kill执行器列表
       val= Option(client) match {
         case Some(c) => c.killExecutors(executorIds)
         case None =>
           logWarning("Attempted to kill executors before driver fully initialized.")
           Future.successful(false)
       }
       
       def waitForRegistration() 
       功能: 等待注册
       registrationBarrier.acquire() // 获取信号量
       
       def notifyContext() 
       功能: 提示上下文
       registrationBarrier.release() // 释放信号量
       
       def stop(finalState: SparkAppHandle.State): Unit 
       功能: 停止后台
       if (stopping.compareAndSet(false, true)) {
         try {
           super.stop()
           if (client != null) {
             client.stop()
           }
           val callback = shutdownCallback
           if (callback != null) {
             callback(this)
           }
         } finally {
           launcherBackend.setState(finalState)
           launcherBackend.close()
         }
       }
   }
   ```

#### dynalloc

```scala
private[spark] class ExecutorMonitor(
    conf: SparkConf,
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    clock: Clock) extends SparkListener with CleanerListener with Logging {
    介绍: 监视执行器动作,使用@ExecutorAllocationManager 解除空载执行器
    属性:
    #name @idleTimeoutNs 	空载超时时间
    val= TimeUnit.SECONDS.toNanos(conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT))
    #name @storageTimeoutNs	存储超时时间
    val= TimeUnit.SECONDS.toNanos(conf.get(DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT))
    #name @shuffleTimeoutNs	shuffle超时时间
    val= TimeUnit.MILLISECONDS.toNanos(conf.get(DYN_ALLOCATION_SHUFFLE_TIMEOUT))
    #name @fetchFromShuffleSvcEnabled	shuffle是否开启SVC
    val= conf.get(SHUFFLE_SERVICE_ENABLED) && conf.get(SHUFFLE_SERVICE_FETCH_RDD_ENABLED)
    #name @shuffleTrackingEnabled	是否允许shuffle定位
    val= !conf.get(SHUFFLE_SERVICE_ENABLED) && conf.get(DYN_ALLOCATION_SHUFFLE_TRACKING)
    #name @executors = new ConcurrentHashMap[String, Tracker]()	执行器定位信息表
    #name @nextTimeout = new AtomicLong(Long.MaxValue)	下一个超时时间
    #name @timedOutExecs = Seq.empty[String]	超时执行器列表
    #name @shuffleToActiveJobs = new mutable.HashMap[Int, mutable.ArrayBuffer[Int]]()
    	shuffle与激活job列表映射关系,线程不安全,只在时间线程中使用
    #name @stageToShuffleID = new mutable.HashMap[Int, Int]()	
    	stage与shuffle的映射关系
    #name @jobToStageIDs = new mutable.HashMap[Int, Seq[Int]]()
    	job映射stage列表关系
    操作集:
    def reset(): Unit
    功能: 重置监视器
    executors.clear()
    nextTimeout.set(Long.MaxValue)
    timedOutExecs = Nil
    
    def timedOutExecutors(): Seq[String] 
    功能: 获取当前超时执行器列表
    if (now >= nextTimeout.get()) {
      nextTimeout.set(Long.MaxValue)
      var newNextTimeout = Long.MaxValue
      // 过滤超时执行器
      timedOutExecs = executors.asScala
        .filter { case (_, exec) => !exec.pendingRemoval && !exec.hasActiveShuffle }
        .filter { case (_, exec) =>
          val deadline = exec.timeoutAt
          if (deadline > now) {
            newNextTimeout = math.min(newNextTimeout, deadline)
            exec.timedOut = false
            false
          } else {
            exec.timedOut = true
            true
          }
        }
        .keys
        .toSeq
      updateNextTimeout(newNextTimeout)
    }
    
    def executorsKilled(ids: Seq[String]): Unit
    功能: 标记指定执行器@ids为待定,以便于移除,只能在EAM线程中调用
    1.标记位待定清除
    ids.foreach { id =>
      val tracker = executors.get(id)
      if (tracker != null) {
        tracker.pendingRemoval = true
      }
    }
    nextTimeout.set(Long.MinValue)
    
    def executorCount: Int = executors.size()
    功能: 获取执行器数量
    
    def pendingRemovalCount: Int
    功能: 计算需要待定移除的执行器数量
    val= executors.asScala.count { case (_, exec) => exec.pendingRemoval }
    
    def onJobStart(event: SparkListenerJobStart): Unit
    功能: job启动事件处理
    0. shuffle参数检测
    if (!shuffleTrackingEnabled) {
      return
    }
    1. 获取shuffle的stage列表
    val shuffleStages = event.stageInfos.flatMap { s =>
      s.shuffleDepId.toSeq.map { shuffleId =>
        s.stageId -> shuffleId
      }
    }
    2. 更新执行器
    var updateExecutors = false
    shuffleStages.foreach { case (stageId, shuffle) =>
      val jobIDs = shuffleToActiveJobs.get(shuffle) match {
        case Some(jobs) =>
          logDebug(s"Reusing shuffle $shuffle in job ${event.jobId}.")
          updateExecutors = true
          jobs
        case _ =>
          logDebug(s"Registered new shuffle $shuffle (from stage $stageId).")
          val jobs = new mutable.ArrayBuffer[Int]()
          shuffleToActiveJobs(shuffle) = jobs
          jobs
      }
      jobIDs += event.jobId
    }
    if (updateExecutors) {
      val activeShuffleIds = shuffleStages.map(_._2).toSeq
      var needTimeoutUpdate = false
      val activatedExecs = new ExecutorIdCollector()
      executors.asScala.foreach { case (id, exec) =>
        if (!exec.hasActiveShuffle) {
          exec.updateActiveShuffles(activeShuffleIds)
          if (exec.hasActiveShuffle) {
            needTimeoutUpdate = true
            activatedExecs.add(id)
          }
        }
      }
      if (activatedExecs.nonEmpty) {
        logDebug(s"Activated executors $activatedExecs due to shuffle
        data needed by new job" +
          s"${event.jobId}.")
      }
      if (needTimeoutUpdate) {
        nextTimeout.set(Long.MinValue)
      }
    }
    3. 更新映射表信息
    stageToShuffleID ++= shuffleStages
    jobToStageIDs(event.jobId) = shuffleStages.map(_._1).toSeq
    
    def onJobEnd(event: SparkListenerJobEnd): Unit
    功能: job结束处理
    if (!shuffleTrackingEnabled) {
      return
    }
    var updateExecutors = false
    val activeShuffles = new mutable.ArrayBuffer[Int]()
    shuffleToActiveJobs.foreach { case (shuffleId, jobs) =>
      jobs -= event.jobId
      if (jobs.nonEmpty) {
        activeShuffles += shuffleId
      } else {
        updateExecutors = true
      }
    }
    if (updateExecutors) {
      if (log.isDebugEnabled()) {
        if (activeShuffles.nonEmpty) {
          logDebug(
            s"Job ${event.jobId} ended, shuffles 
            ${activeShuffles.mkString(",")} still active.")
        } else {
          logDebug(s"Job ${event.jobId} ended, no active shuffles remain.")
        }
      }
      val deactivatedExecs = new ExecutorIdCollector()
      executors.asScala.foreach { case (id, exec) =>
        if (exec.hasActiveShuffle) {
          exec.updateActiveShuffles(activeShuffles)
          if (!exec.hasActiveShuffle) {
            deactivatedExecs.add(id)
          }
        }
      }
      if (deactivatedExecs.nonEmpty) {
        logDebug(s"Executors $deactivatedExecs do not have active 
        shuffle data after job " +
          s"${event.jobId} finished.")
      }
    }
    jobToStageIDs.remove(event.jobId).foreach { stages =>
      stages.foreach { id => stageToShuffleID -= id }
    }
    
    def onTaskStart(event: SparkListenerTaskStart): Unit
    功能: 任务开始处理
    val executorId = event.taskInfo.executorId
    if (client.isExecutorActive(executorId)) {
      val exec = ensureExecutorIsTracked(executorId)
      exec.updateRunningTasks(1)
    }
    
    def onTaskEnd(event: SparkListenerTaskEnd): Unit
    功能: 任务结束处理
    val executorId = event.taskInfo.executorId
    val exec = executors.get(executorId)
    if (exec != null) {
      if (shuffleTrackingEnabled && event.reason == Success) {
        stageToShuffleID.get(event.stageId).foreach { shuffleId =>
          exec.addShuffle(shuffleId)
        }
      }
      exec.updateRunningTasks(-1)
    }
    
    def onExecutorAdded(event: SparkListenerExecutorAdded): Unit
    功能: 处理添加执行器事件
    val exec = ensureExecutorIsTracked(event.executorId)
    exec.updateRunningTasks(0)
    logInfo(s"New executor ${event.executorId} has registered (new total
    is ${executors.size()})")
    
    def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit 
    功能: 执行器移除事件
    val removed = executors.remove(event.executorId)
    if (removed != null) {
      logInfo(s"Executor ${event.executorId} removed (new total is ${executors.size()})")
      if (!removed.pendingRemoval) {
        nextTimeout.set(Long.MinValue)
      }
    }
    
    def onBlockUpdated(event: SparkListenerBlockUpdated): Unit 
    功能: 处理数据块更新事件
    0. 参数检测
    if (!event.blockUpdatedInfo.blockId.isInstanceOf[RDDBlockId]) {
      return
    }
    1. 获取数据块信息
    val exec = ensureExecutorIsTracked(event.blockUpdatedInfo.blockManagerId.executorId)
    val storageLevel = event.blockUpdatedInfo.storageLevel
    val blockId = event.blockUpdatedInfo.blockId.asInstanceOf[RDDBlockId]
    2. 处理数据块
    if (storageLevel.isValid && (!fetchFromShuffleSvcEnabled || !storageLevel.useDisk)) {
      val hadCachedBlocks = exec.cachedBlocks.nonEmpty
      val blocks = exec.cachedBlocks.getOrElseUpdate(blockId.rddId,
        new mutable.BitSet(blockId.splitIndex))
      blocks += blockId.splitIndex
      if (!hadCachedBlocks) {
        exec.updateTimeout()
      }
    } else {
      exec.cachedBlocks.get(blockId.rddId).foreach { blocks =>
        blocks -= blockId.splitIndex
        if (blocks.isEmpty) {
          exec.cachedBlocks -= blockId.rddId
          if (exec.cachedBlocks.isEmpty) {
            exec.updateTimeout()
          }
        }
      }
    }
    
    def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit
    功能: 解除RDD持久化RDD
    executors.values().asScala.foreach { exec =>
      exec.cachedBlocks -= event.rddId // 解除持久化操作
      if (exec.cachedBlocks.isEmpty) {
        exec.updateTimeout()
      }
    }
    
    def onOtherEvent(event: SparkListenerEvent): Unit
    功能: 处理其他类型事件
    event match {
        case ShuffleCleanedEvent(id) => cleanupShuffle(id) // 处理清理shuffle事件
        case _ => // 其他类型不予处理
      }
    
    def rddCleaned(rddId: Int): Unit = { }
    功能: 清理RDD
    
    def shuffleCleaned(shuffleId: Int): Unit
    功能: 清理shuffle
    if (shuffleTrackingEnabled) {
        listenerBus.post(ShuffleCleanedEvent(shuffleId))
    }
    
    def broadcastCleaned(broadcastId: Long): Unit = { }
    功能: 清除广播变量
    
    def accumCleaned(accId: Long): Unit = { }
    功能: 清除累加器
    
    def checkpointCleaned(rddId: Long): Unit = { }
    功能: 清除检查点
    
    def isExecutorIdle(id: String): Boolean 
    功能: 确认指定执行器是否空载,测试使用
    
    def timedOutExecutors(when: Long): Seq[String]
    功能: 确认超过@when时间超时的执行器列表
    val= executors.asScala.flatMap { case (id, tracker) =>
      if (tracker.isIdle && tracker.timeoutAt <= when) Some(id) else None
    }.toSeq
    
    def executorsPendingToRemove(): Set[String]
    功能: 获取待定删除的执行器列表
    val= executors.asScala.filter { case (_, exec) => exec.pendingRemoval }.keys.toSet
    
    def ensureExecutorIsTracked(id: String): Tracker
    功能: 确定执行器是否定位成功
    val= executors.computeIfAbsent(id, _ => new Tracker())
    
    def updateNextTimeout(newValue: Long): Unit
    功能: 更新下一次的超时时间
    while (true) {
      val current = nextTimeout.get()
      if (newValue >= current || nextTimeout.compareAndSet(current, newValue)) {
        return
      }
    }
    
    def cleanupShuffle(id: Int): Unit
    功能: 清除shuffle
    shuffleToActiveJobs -= id
    executors.asScala.foreach { case (_, exec) =>
      exec.removeShuffle(id)
    }
}
```

```scala
private class Tracker {
    介绍: 定位器
    属性:
    #name @timeoutAt: Long = Long.MaxValue	超时时间
    #name @timedOut: Boolean = false	超时标志
    #name @pendingRemoval: Boolean = false	是否待定移除
    #name @hasActiveShuffle: Boolean = false	是否含有激活的shuffle
    #name @idleStart: Long = -1	空载起始值
    #name @runningTasks: Int = 0	运行任务数量
    #name @cachedBlocks = new mutable.HashMap[Int, mutable.BitSet]()	缓存数据块
    	映射RDD编号和分区编号
    #name @shuffleIds = if (shuffleTrackingEnabled) new mutable.HashSet[Int]() else null
    	shuffleID列表
    操作集:
    def isIdle: Boolean = idleStart >= 0 && !hasActiveShuffle
    功能: 确定是否空载
    
    def updateRunningTasks(delta: Int): Unit
    功能: 更新运行中的任务
    runningTasks = math.max(0, runningTasks + delta)
    idleStart = if (runningTasks == 0) clock.nanoTime() else -1L
    updateTimeout()
    
    def updateTimeout(): Unit
    功能: 更新超时信息
    
    def addShuffle(id: Int): Unit
    功能: 添加shuffle 其中shuffleid=@id
    if (shuffleIds.add(id)) {
        hasActiveShuffle = true
    }
    
    def updateActiveShuffles(ids: Iterable[Int]): Unit
    功能: 更新激活的shuffle @id
    val hadActiveShuffle = hasActiveShuffle
      hasActiveShuffle = ids.exists(shuffleIds.contains)
      if (hadActiveShuffle && isIdle) {
        updateTimeout()
      }
    
    def removeShuffle(id: Int): Unit
    功能: 移除指定shuffle
    if (shuffleIds.remove(id) && shuffleIds.isEmpty) {
        hasActiveShuffle = false
        if (isIdle) {
          updateTimeout()
        }
      }
}
```

```scala
private case class ShuffleCleanedEvent(id: Int) extends SparkListenerEvent {
    override protected[spark] def logEvent: Boolean = false
  }
介绍: shuffle清除事件

private class ExecutorIdCollector {
    介绍: 执行器ID收集器,用于消息的debug
    属性:
        ids	执行器列表
        excess	执行器数量
    def nonEmpty: Boolean = ids != null && ids.nonEmpty
    功能: 确认shuffle列表是否为空
    
    def toString(): String
    功能: 信息显示
    
    def add(id: String): Unit
    功能: 添加shuffle @id
}
```

#### Local

```scala
private case class ReviveOffers()
介绍: 恢复供应

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)
介绍: 状态更新

private case class KillTask(taskId: Long, interruptThread: Boolean, reason: String)
介绍: kill任务

private case class StopExecutor()
介绍: 停止执行器
```

```scala
private[spark] class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    userClassPath: Seq[URL],
    scheduler: TaskSchedulerImpl,
    executorBackend: LocalSchedulerBackend,
    private val totalCores: Int)
extends ThreadSafeRpcEndpoint with Logging {
    介绍: 本地端点,@LocalSchedulerBackend通过本地端点来序列化,使用RPC端点使得对@LocalSchedulerBackend的调用异步进行,这样可以有效地防止死锁.
    参数:
    	rpcEnv	RPC环境
    	userClassPath	用户类路径列表
    	scheduler	调度器
    	executorBackend	本地调度器后台
    	totalCores	总核心数量
    属性:
    #name @freeCores = totalCores	空闲CPU数量
    #name @localExecutorId = SparkContext.DRIVER_IDENTIFIER	本地执行器ID
    #name @localExecutorHostname = Utils.localCanonicalHostName()	本地执行器主机名称
    #name @executor	执行器
    	val= new Executor(localExecutorId, localExecutorHostname,
                          SparkEnv.get, userClassPath, isLocal = true)
    操作集:
    def receive: PartialFunction[Any, Unit]
    功能: 接受消息
    case ReviveOffers =>  // 恢复供应消息
      reviveOffers()
    case StatusUpdate(taskId, state, serializedData) => // 状态更新消息
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }
    case KillTask(taskId, interruptThread, reason) => // 中断任务消息
      executor.killTask(taskId, interruptThread, reason)
    
    def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
    功能: 回应消息
    case StopExecutor => // 停止执行器
      executor.stop()
      context.reply(true)
    
    def reviveOffers(): Unit
    功能: 恢复供应
    val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname,
                                            freeCores,Some(rpcEnv.address.hostPort)))
    for (task <- scheduler.resourceOffers(offers).flatten) {
      freeCores -= scheduler.CPUS_PER_TASK
      executor.launchTask(executorBackend, task)
    }
}
```

```scala
private[spark] class LocalSchedulerBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int)
extends SchedulerBackend with ExecutorBackend with Logging {
    介绍: 本地调度后端,执行器,后台,master全部运行在同一个JVM中,且运行本地spark时,这个类运行在@TaskSchedulerImpl后,在一个执行器上处理任务.由@LocalSchedulerBackend 创建,本地运行.
    属性:
    #name @appId = "local-" + System.currentTimeMillis	应用ID
    #name @localEndpoint: RpcEndpointRef = null	本地RPC端点
    #name @userClassPath = getUserClasspath(conf)	用户类路径
    #name @listenerBus = scheduler.sc.listenerBus	监听总线
    #name @launcherBackend = new LauncherBackend()	运行后端
      val= new LauncherBackend() {
        override def conf: SparkConf = LocalSchedulerBackend.this.conf
        override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
      }
    操作集:
    def getUserClasspath(conf: SparkConf): Seq[URL] = {
        val userClassPathStr = conf.get(config.EXECUTOR_CLASS_PATH)
        userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
      }
    功能： 获取类路径列表
    
    def start(): Unit
    功能： 启动调度器后端
    1. 配置启动参数
    val rpcEnv = SparkEnv.get.rpcEnv
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, 
                                             scheduler, this, totalCores)
    localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint"
                                         , executorEndpoint)
    2. 发送监听信息
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty,
        Map.empty)))
    3. 设置任务信息
    launcherBackend.setAppId(appId)
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
    
    def stop(): Unit
    功能：停止调度器后端
    stop(SparkAppHandle.State.FINISHED)
    
    stop(SparkAppHandle.State.FINISHED)= localEndpoint.send(ReviveOffers)
    功能： 恢复供应
    
    def defaultParallelism(): Int
    功能: 获取默认并行度
    val= scheduler.conf.getInt("spark.default.parallelism", totalCores)
    
    def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String): Unit
    功能: kill指定任务
    localEndpoint.send(KillTask(taskId, interruptThread, reason))
    
    def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer): Unit
    功能： 更新指定任务的状态
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
    
    def applicationId(): String = appId
    功能: 获取应用编号
    
    def maxNumConcurrentTasks(): Int = totalCores / scheduler.CPUS_PER_TASK
	功能: 获取最大并发任务数量
   	
    def stop(finalState: SparkAppHandle.State): Unit
    功能: 修改停止标记位
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
}
```

#### AccumulableInfo

```scala
@DeveloperApi
case class AccumulableInfo private[spark] (
    id: Long,
    name: Option[String],
    update: Option[Any], // represents a partial update within a task
    value: Option[Any],
    private[spark] val internal: Boolean,
    private[spark] val countFailedValues: Boolean,
    private[spark] val metadata: Option[String] = None)
介绍: 累加信息
参数:
	id	唯一标识符
	name	名称
	update	更新值
	value	值
	internal	是否内部使用
	countFailedValues	是否考虑失败value
	metadata	元数据信息
```

#### ActiveJob

```scala
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val callSite: CallSite,
    val listener: JobListener,
    val properties: Properties) {
    介绍: 在DAGSchedule中运行的任务,可以分为两个类型:
    1. 结果类型job,计算一个@ResultStage 去执行一个动作或者一个map-stage job(计算ShuffleMapStage的map输出)
    2. 可适应性查询计划,在提交之后的stage之前,静态查找map输出.
    job只可以使用客户端直接提交的`leaf`stages来追踪,通过@DAGScheduler 的@submitJob 或者@submitMapStage方法.但是无论哪种类型的job都会导致其他更早的job执行(前驱任务执行),且多个job会同时共有一个stage.依赖信息管理在@DAGScheduler中.
    参数:
    	jobId	job编号
    	finalStage	这个job计算的stage
    	callSite	用于程序中job实例化的地方
    	listener	监听器,提示任务执行的成功或者失败
    	properties	属性
    属性：
    #name @numPartitions	分区数量
    val= finalStage match {
        case r: ResultStage => r.partitions.length // 结果类型
        case m: ShuffleMapStage => m.rdd.partitions.length // shuffle类型
      }
    #name @finished = Array.fill[Boolean](numPartitions)(false)	分区结束标记
    #name @numFinished = 0	完成数量
    操作集:
    def resetAllPartitions(): Unit
    功能: 重置所有分区
    (0 until numPartitions).foreach(finished.update(_, false))
    numFinished = 0
}
```

#### AsyncEventQueue

```scala
private class AsyncEventQueue(
    val name: String,
    conf: SparkConf,
    metrics: LiveListenerBusMetrics,
    bus: LiveListenerBus)
extends SparkListenerBus with Logging {
    介绍: 异步事件队列,放置到队列的事件会被发送到子监听器上.
    发送仅仅会当@start()调用时才会开始,没有事件传送时调用@stop关闭
    属性:
    #name @capacity:Int	队列容量(当置入速度快于排空速度时,会抛出异常)
    val=  {
        val queueSize = conf.getInt(s"$LISTENER_BUS_EVENT_QUEUE_PREFIX.$name.capacity",
                                    conf.get(LISTENER_BUS_EVENT_QUEUE_CAPACITY))
		// 容量值断言
        assert(queueSize > 0, s"capacity for event queue $name must be greater than 0, " 		+s"but $queueSize is configured.")
        queueSize
    }
    #name @eventQueue = new LinkedBlockingQueue[SparkListenerEvent](capacity)	事件队列
    #name @eventCount = new AtomicLong()	事件计数器
    #name @droppedEventsCounter = new AtomicLong(0L)	抛弃事件计数器
    #name @lastReportTimestamp = 0L	上次更新时间
    #name @logDroppedEvent = new AtomicBoolean(false)	是否记录抛弃事件
    #name @sc: SparkContext = null	spark上下文
    #name @started = new AtomicBoolean(false)	启动标志
    #name @stopped = new AtomicBoolean(false)	停止标志
    #name @droppedEvents= metrics.metricRegistry.counter(s"queue.$name.numDroppedEvents")
    	抛弃事件数量
    #name @processingTime 	进行时间
    val= metrics.metricRegistry.timer(s"queue.$name.listenerProcessingTime")
    #name @dispatchThread	分配线程
    val= new Thread(s"spark-listener-group-$name") {
        setDaemon(true)
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          dispatch()
        }
      }
    
    def getTimer(listener: SparkListenerInterface): Option[Timer]
    功能: 获取计时器
    val= metrics.getTimerForListenerClass(
        listener.getClass.asSubclass(classOf[SparkListenerInterface]))
    
    def dispatch(): Unit
    功能: 分配函数
    LiveListenerBus.withinListenerThread.withValue(true) {
        var next: SparkListenerEvent = eventQueue.take() //获取队列下一个事件
        while (next != POISON_PILL) { //没有收到截止事件,则继续发送到子监听器
          val ctx = processingTime.time()
          try {
            super.postToAll(next)
          } finally {
            ctx.stop()
          }
          eventCount.decrementAndGet() //减计数
          next = eventQueue.take() //获取下一个事件
        }
        eventCount.decrementAndGet()
    }
    
    def start(sc: SparkContext): Unit
    功能: 启动异步线程,用于分发事件到底层监听器
    if (started.compareAndSet(false, true)) {
      this.sc = sc
      dispatchThread.start() // 开启分发线程
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
    
    def stop(): Unit
    功能: 停止监听总线,会等到队列事件处理完毕,但是新事件会被抛弃
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that 
      has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
      eventCount.incrementAndGet()
      eventQueue.put(POISON_PILL)
    }
    if (Thread.currentThread() != dispatchThread) {
      dispatchThread.join()
    }
    
    def waitUntilEmpty(deadline: Long): Boolean
    功能: 有限等待直到队列中没有事件,测试使用
    while (eventCount.get() != 0) {
      if (System.currentTimeMillis > deadline) {
        return false
      }
      Thread.sleep(10)
    }
    val= true
    
    def removeListenerOnError(listener: SparkListenerInterface): Unit
    功能: 从监听总线上移除监听器
    bus.removeListener(listener)
    
    def post(event: SparkListenerEvent): Unit
    功能: 发送事件
    0. 参数校验
    if (stopped.get()) {
      return
    }
    eventCount.incrementAndGet()
    if (eventQueue.offer(event)) {
      return
    }
    1. 更新事件计量参数
    eventCount.decrementAndGet()
    droppedEvents.inc()
    droppedEventsCounter.incrementAndGet()
    if (logDroppedEvent.compareAndSet(false, true)) {
      logError(s"Dropping event from queue $name. " +
        "This likely means one of the listeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
    logTrace(s"Dropping event $event")
    2. 处理发送的事件
    val droppedCount = droppedEventsCounter.get
    if (droppedCount > 0) {
      if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
        if (droppedEventsCounter.compareAndSet(droppedCount, 0)) {
          val prevLastReportTimestamp = lastReportTimestamp
          lastReportTimestamp = System.currentTimeMillis()
          val previous = new java.util.Date(prevLastReportTimestamp)
          logWarning(s"Dropped $droppedCount events from $name since " +
            s"${if (prevLastReportTimestamp == 0) 
            "the application started" else s"$previous"}.")
        }
      }
    }
}
```

```scala
private object AsyncEventQueue {
  属性:
  val POISON_PILL = new SparkListenerEvent() { } 	截止事件
}
```

#### BarrierJobAllocationFailed

```scala
private[spark] class BarrierJobAllocationFailed(message: String) extends SparkException(message)
介绍: 屏蔽任务执行失败异常

private[spark] class BarrierJobUnsupportedRDDChainException
  extends BarrierJobAllocationFailed(    BarrierJobAllocationFailed.ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)
介绍: 屏蔽任务不支持RDD链异常

private[spark] class BarrierJobRunWithDynamicAllocationException
  extends BarrierJobAllocationFailed(
    BarrierJobAllocationFailed.ERROR_MESSAGE_RUN_BARRIER_WITH_DYN_ALLOCATION)
介绍: 动态分配屏蔽任务异常

private[spark] class BarrierJobSlotsNumberCheckFailed(
    val requiredConcurrentTasks: Int,
    val maxConcurrentTasks: Int)
extends BarrierJobAllocationFailed(    BarrierJobAllocationFailed.ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER)
介绍: 屏蔽任务检查槽数量失败异常

private[spark] object BarrierJobAllocationFailed {
    属性:
    #name @ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN
    	不支持RDD链式形式异常信息
    val= "[SPARK-24820][SPARK-24821]: Barrier execution mode does not allow the 
    following pattern of " +"RDD chain within a barrier stage:\n1. Ancestor RDDs
    that have different number of " +"partitions from the resulting RDD (eg.
    union()/coalesce()/first()/take()/" +"PartitionPruningRDD). A workaround for
    first()/take() can be barrierRdd.collect().head " +
    "(scala) or barrierRdd.collect()[0] (python).\n" +
    "2. An RDD that depends on multiple barrier RDDs (eg. barrierRdd1.zip(barrierRdd2))."
    
    #name @ERROR_MESSAGE_RUN_BARRIER_WITH_DYN_ALLOCATION	动态分配异常消息
    val= "[SPARK-24942]: Barrier execution mode does not support dynamic resource 
    	allocation for " +
    	"now. You can disable dynamic resource allocation by setting Spark conf " +
      	s""""${DYN_ALLOCATION_ENABLED.key}" to "false"."""
    
    #name @ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER 槽数量检查异常
    val= "[SPARK-24819]: Barrier execution mode does not allow run a barrier stage that 	requires " +
    "more slots than the total number of slots in the cluster currently. Please init a 
    new " +
    "cluster with more CPU cores or repartition the input RDD(s) to reduce the number of
    " +
    "slots required to run this barrier stage."
}

```

