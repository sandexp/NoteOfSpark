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

#### BlacklistTracker

```markdown
介绍:
	黑名单定位器用于有问题的执行器或者节点.支持驱动器的黑名单执行器和节点,通过完整应用.任务集管理器@TaskSetManagers添加额外的执行器或者节点信息给指定任务或者stage中,与此处的黑名单呼应.
	定位器需要处理各式各样的工作负载,例如:
	1. 错误的用户代码,会引起许多任务失败,可能不会计算在单个执行器内
	2. 许多小的stage: 这个可能会会避免生成一个包含许多错误的执行器.但是很有可能整个执行器内全是失败.
	3. 薄片执行器: 不会每个任务都失败,但是仍然适合黑名单追踪
	参考SPARK-8425进行深刻的讨论.
```

```scala
private[scheduler] class BlacklistTracker (
    private val listenerBus: LiveListenerBus,
    conf: SparkConf,
    allocationClient: Option[ExecutorAllocationClient],
    clock: Clock = new SystemClock()) extends Logging {
    构造器参数
    	listenerBus	监听总线
    	conf	spark配置
    	allocationClient	执行器分配客户端
    	clock	系统时钟
    属性:
    #name @MAX_FAILURES_PER_EXEC = conf.get(config.MAX_FAILURES_PER_EXEC)单个执行最大失败次数
    #name @MAX_FAILED_EXEC_PER_NODE = conf.get(config.MAX_FAILED_EXEC_PER_NODE)
    	单节点最大失败次数
    #name @BLACKLIST_TIMEOUT_MILLIS = BlacklistTracker.getBlacklistTimeout(conf)	
    	黑名单超时时间
    #name @BLACKLIST_FETCH_FAILURE_ENABLED	是否允许黑名单获取失败
    val= conf.get(config.BLACKLIST_FETCH_FAILURE_ENABLED)
    #name @executorIdToFailureList = new HashMap[String, ExecutorFailureList]()
    	执行器id与失败列表映射关系
    #name @executorIdToBlacklistStatus = new HashMap[String, BlacklistedExecutor]()
    	黑名单状态映射表
    #name @nodeIdToBlacklistExpiryTime = new HashMap[String, Long]()
    	节点黑名单逾期映射表
    #name @_nodeBlacklist = new AtomicReference[Set[String]](Set())
    	节点黑名单列表
    #name @nextExpiryTime: Long = Long.MaxValue	下个到期时间
    #name @nodeToBlacklistedExecs = new HashMap[String, HashSet[String]]()
    	节点黑名单映射表（节点-->黑名单执行器列表）
    操作集：
    def applyBlacklistTimeout(): Unit
    功能: 解除黑名单执行器/节点(范围设置在@BLACKLIST_TIMEOUT_MILLIS内的)
    if (now > nextExpiryTime) {
        // 获取需要解除的执行器类别
      val execsToUnblacklist = executorIdToBlacklistStatus.filter(_._2.expiryTime < now).keys
      if (execsToUnblacklist.nonEmpty) {
        execsToUnblacklist.foreach { exec => // 解除执行器的黑名单状态
          val status = executorIdToBlacklistStatus.remove(exec).get
          val failedExecsOnNode = nodeToBlacklistedExecs(status.node)
          listenerBus.post(SparkListenerExecutorUnblacklisted(now, exec))
          failedExecsOnNode.remove(exec)
          if (failedExecsOnNode.isEmpty) {
            nodeToBlacklistedExecs.remove(status.node)
          }
        }
      }
        // 获取需要解除的节点列表
      val nodesToUnblacklist = nodeIdToBlacklistExpiryTime.filter(_._2 < now).keys
      if (nodesToUnblacklist.nonEmpty) { // 解除节点的黑名单状态
        nodesToUnblacklist.foreach { node =>
          nodeIdToBlacklistExpiryTime.remove(node)
          listenerBus.post(SparkListenerNodeUnblacklisted(now, node))
        }
        _nodeBlacklist.set(nodeIdToBlacklistExpiryTime.keySet.toSet)
      }
      updateNextExpiryTime()
    }
    
    def updateNextExpiryTime(): Unit
    功能: 更新下一个超期时间
    1. 获取执行器最小超期时间
    val execMinExpiry = if (executorIdToBlacklistStatus.nonEmpty) {
      executorIdToBlacklistStatus.map{_._2.expiryTime}.min
    } else {
      Long.MaxValue
    }
    2. 获取节点最小超期时间
    val nodeMinExpiry = if (nodeIdToBlacklistExpiryTime.nonEmpty) {
      nodeIdToBlacklistExpiryTime.values.min
    } else {
      Long.MaxValue
    }
    3. 获取下一个超期时间
    nextExpiryTime = math.min(execMinExpiry, nodeMinExpiry)
    
    def killExecutor(exec: String, msg: String): Unit
    功能: 中断指定执行器
    allocationClient match {
      case Some(a) =>
        logInfo(msg)
        a.killExecutors(Seq(exec), adjustTargetNumExecutors = false,
                        countFailures = false,force = true)
      case None =>
        logInfo(s"Not attempting to kill blacklisted executor id $exec " +
          s"since allocation client is not defined.")
    }
    
    def killBlacklistedExecutor(exec: String): Unit 
    功能: kill黑名单执行器
    if (conf.get(config.BLACKLIST_KILL_ENABLED)) {
      killExecutor(exec,
        s"Killing blacklisted executor id $exec since
        ${config.BLACKLIST_KILL_ENABLED.key} is set.")
    }
    
    def killBlacklistedIdleExecutor(exec: String): Unit
    功能: 中断黑名单空载执行器
    killExecutor(exec,
      s"Killing blacklisted idle executor id $exec because of task unschedulability 
      and trying " + "to acquire a new executor.")
    
    def killExecutorsOnBlacklistedNode(node: String): Unit
    功能: 中断指定节点上的执行器
    if (conf.get(config.BLACKLIST_KILL_ENABLED)) {
      allocationClient match {
        case Some(a) =>
          logInfo(s"Killing all executors on blacklisted host $node " +
            s"since ${config.BLACKLIST_KILL_ENABLED.key} is set.")
          if (a.killExecutorsOnHost(node) == false) {
            logError(s"Killing executors on node $node failed.")
          }
        case None =>
          logWarning(s"Not attempting to kill executors on blacklisted host $node " +
            s"since allocation client is not defined.")
      }
    }
    
    def updateBlacklistForFetchFailure(host: String, exec: String): Unit
    功能: 获取失败,更新黑名单列表
    if (BLACKLIST_FETCH_FAILURE_ENABLED) {
      val now = clock.getTimeMillis()
      val expiryTimeForNewBlacklists = now + BLACKLIST_TIMEOUT_MILLIS
      if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
        if (!nodeIdToBlacklistExpiryTime.contains(host)) {
          logInfo(s"blacklisting node $host due to fetch failure of external 
          shuffle service")
          nodeIdToBlacklistExpiryTime.put(host, expiryTimeForNewBlacklists)
          listenerBus.post(SparkListenerNodeBlacklisted(now, host, 1))
          _nodeBlacklist.set(nodeIdToBlacklistExpiryTime.keySet.toSet)
          killExecutorsOnBlacklistedNode(host)
          updateNextExpiryTime()
        }
      } else if (!executorIdToBlacklistStatus.contains(exec)) {
        logInfo(s"Blacklisting executor $exec due to fetch failure")
        executorIdToBlacklistStatus.put(exec, BlacklistedExecutor(host, expiryTimeForNewBlacklists))
        listenerBus.post(SparkListenerExecutorBlacklisted(now, exec, 1))
        updateNextExpiryTime()
        killBlacklistedExecutor(exec)
        val blacklistedExecsOnNode = nodeToBlacklistedExecs.getOrElseUpdate(host, HashSet[String]())
        blacklistedExecsOnNode += exec
      }
    }
    
    def updateBlacklistForSuccessfulTaskSet(
      stageId: Int,
      stageAttemptId: Int,
      failuresByExec: HashMap[String, ExecutorFailuresInTaskSet]): Unit
    功能: 更新执行成功任务集的黑名单列表
    
    def isExecutorBlacklisted(executorId: String): Boolean
    功能: 确定指定执行器是否处于黑名单中
    val= executorIdToBlacklistStatus.contains(executorId)
    
    def nodeBlacklist(): Set[String]= _nodeBlacklist.get()
    功能: 获取节点黑名单列表(线程安全)
    
    def isNodeBlacklisted(node: String): Boolean
    功能: 确认指定节点是否处于黑名单中
    val= nodeIdToBlacklistExpiryTime.contains(node)
    
    def handleRemovedExecutor(executorId: String): Unit
    功能: 移除指定执行器
    executorIdToFailureList -= executorId
}
```

```scala
private[scheduler] final class ExecutorFailureList extends Logging {
    介绍： 执行器失败列表，定位一个执行器的所有失败（没有超过时间限制）
    #name @failuresAndExpiryTimes = ArrayBuffer[(TaskId, Long)]()
    成功执行任务中的失败信息列表(任务编号&超时时间二元组)
    #name @minExpiryTime = Long.MaxValue	最小超时时间
    
    操作集:
    def numUniqueTaskFailures: Int = failuresAndExpiryTimes.size
    功能: 获取任务失败次数
    
    def isEmpty: Boolean = failuresAndExpiryTimes.isEmpty
    功能: 确定失败列表是否为空
    
    def toString(): String= s"failures = $failuresAndExpiryTimes"
    功能: 信息显示
    
    def addFailures(
        stage: Int,
        stageAttempt: Int,
        failuresInTaskSet: ExecutorFailuresInTaskSet): Unit 
    功能: 添加失败任务
    failuresInTaskSet.taskToFailureCountAndFailureTime.foreach {
        case (taskIdx, (_, failureTime)) =>
          val expiryTime = failureTime + BLACKLIST_TIMEOUT_MILLIS
          failuresAndExpiryTimes += ((TaskId(stage, stageAttempt, taskIdx), expiryTime))
          if (expiryTime < minExpiryTime) {
            minExpiryTime = expiryTime
          }
      }
    
    def dropFailuresWithTimeoutBefore(dropBefore: Long): Unit
    功能: 指定时限内丢弃失败任务
    if (minExpiryTime < dropBefore) { // 丢弃指定时限内的任务
        var newMinExpiry = Long.MaxValue
        // 获取失败列表
        val newFailures = new ArrayBuffer[(TaskId, Long)]
        // 收集期望时间内的任务
        failuresAndExpiryTimes.foreach { case (task, expiryTime) =>
          if (expiryTime >= dropBefore) {
            newFailures += ((task, expiryTime))
            if (expiryTime < newMinExpiry) {
              newMinExpiry = expiryTime
            }
          }
        }
        failuresAndExpiryTimes = newFailures
        minExpiryTime = newMinExpiry // 最终知晓期望时间为最短失败任务时间
      }
}
```

```scala
private final case class BlacklistedExecutor(node: String, expiryTime: Long)
介绍: 黑名单执行器
参数:
	node 节点名称
	expiryTime	期望执行时间
```

```scala
private[spark] object BlacklistTracker extends Logging {
    介绍: 黑名单定位器
    属性：
    #name @DEFAULT_TIMEOUT = "1h"	默认超时时间
    操作集：
    def mustBePos(k: String, v: String): Unit 
    功能: 正数断言
    throw new IllegalArgumentException(s"$k was $v, but must be > 0.")
    
    def validateBlacklistConfs(conf: SparkConf): Unit
    功能: 验证黑名单配置
    1. 正数断言
    Seq(
      config.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
      config.MAX_TASK_ATTEMPTS_PER_NODE,
      config.MAX_FAILURES_PER_EXEC_STAGE,
      config.MAX_FAILED_EXEC_PER_NODE_STAGE,
      config.MAX_FAILURES_PER_EXEC,
      config.MAX_FAILED_EXEC_PER_NODE
    ).foreach { config =>
      val v = conf.get(config)
      if (v <= 0) {
        mustBePos(config.key, v.toString)
      }
    }
    2. 超时时间断言
    val timeout = getBlacklistTimeout(conf)
    if (timeout <= 0) {
      conf.get(config.BLACKLIST_TIMEOUT_CONF) match {
        case Some(t) =>
          mustBePos(config.BLACKLIST_TIMEOUT_CONF.key, timeout.toString)
        case None =>
          mustBePos(config.BLACKLIST_LEGACY_TIMEOUT_CONF.key, timeout.toString)
      }
    }
    3. 请求数量与失败次数关系断言
    val maxTaskFailures = conf.get(config.TASK_MAX_FAILURES)
    val maxNodeAttempts = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)
    if (maxNodeAttempts >= maxTaskFailures) {
      throw new IllegalArgumentException(s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
        s"( = ${maxNodeAttempts}) was >= ${config.TASK_MAX_FAILURES.key} " +
        s"( = ${maxTaskFailures} ).  Though blacklisting is enabled, with this
        configuration, " +
        s"Spark will not be robust to one bad node.  Decrease " +
        s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key}, increase
        ${config.TASK_MAX_FAILURES.key}, " +
        s"or disable blacklisting with ${config.BLACKLIST_ENABLED.key}")
    }
    
    def getBlacklistTimeout(conf: SparkConf): Long 
    功能： 获取黑名单超时时间
    val= conf.get(config.BLACKLIST_TIMEOUT_CONF).getOrElse {
      conf.get(config.BLACKLIST_LEGACY_TIMEOUT_CONF).getOrElse {
        Utils.timeStringAsMs(DEFAULT_TIMEOUT)
      }
    }
    
    def isBlacklistEnabled(conf: SparkConf): Boolean
    功能: 确定是否允许黑名单
    val= conf.get(config.BLACKLIST_ENABLED) match {
      case Some(enabled) =>
        enabled
      case None =>
        val legacyKey = config.BLACKLIST_LEGACY_TIMEOUT_CONF.key
        conf.get(config.BLACKLIST_LEGACY_TIMEOUT_CONF).exists { legacyTimeout =>
          if (legacyTimeout == 0) {
            logWarning(s"Turning off blacklisting due to legacy configuration:
            $legacyKey == 0")
            false
          } else {
            logWarning(s"Turning on blacklisting due to legacy configuration:
            $legacyKey > 0")
            true
          }
        }
    }
}
```

#### DAGScheduler

```markdown
介绍:
     高级调度层，实现了基于stage的调度。计算每个job的stage的DAG图。保持追踪RDD和stage输出的实体化，寻找最小调度，用于运行job。然后提交stage到底层的任务调度器实现中，这个任务调度器的实现可以运行在集群上。任务集包含完全的任务依赖，所以可以基于数据立刻运行在集群上。尽管可能由于数据不可用而失败。
     通过使用shuffle将RDD图进行分界，从而创建spark stage。RDD操作具有窄依赖(例如,map(),filter)会合并到一个stage中,但是使用shuffle依赖的操作需要使用多个stage(一个需要写出map输出,拎一个需要在并发屏障之后读取这个文件的数据).最后,每个stage仅仅有在其他stage的依赖信息,且可能在其中进行多次计算.实际这些操作的流式线发生在RDD.compute()之中.
 	除了使用stage的DAG图之外,DAG调度器还可以借鉴每个任务的运行位置,主要是基于当前缓存状态,将其传递给底层的任务调度器@TaskScheduler.此外,会处理由于shuffle输出文件丢失导致的失败情况,在这种情况下旧的stage需要重新提交.发生失败的stage中由于shuffle文件丢失引起的失败由@TaskScheduler处理,在放弃整个stafe之前对每个任务都进行一定次数的尝试,但是由于对实时性能的要求,这种容错的次数不能够过大.
 	查看代码的时候,有如下几个重要的概念:
 	1. job 
 	其中激活的job是提交到调度器@scheduler 的顶层实体.例如,用于调用了一个动作,例如@count(),一个job通过@submitJob提交job.每个job需要执行对个stage去构建中间结果.
 	2. stage
 	stage是一个任务集,这些任务计算了job的中间结果,每个任务计算同一个RDD分区上的同一个函数.stage可以使用shuffle中断,这里就必须引入了并发处理中常用的操作-Barrier(屏障).用于保证并发状态下map和reduce侧的执行顺序(也就是说map out先与reduce read).
 	有两种类型的stage
 	+ @ResultStage 这个stage用于最后的stage执行动作
 	+ @ShuffleMapStage	shufflestage,用于写出shuffle的map输出.
 	stage通常由多个job共享,即这些job重用同样的RDD.
 	3. task
 	任务,执行单元,用于分发到机器上
 	4. Cache tracking	
 	缓存追踪,DAG调度器指出哪些RDD需要缓存,用于避免重新计算.同样地计算map侧的map out文件,用于避免重新计算map侧的任务,避免置信屏蔽任务(耗时).此外这些map输出文件记录在内存中不需要从外存调页.
 	5. Preferred locations
 	最佳位置,dag调度器还需要计算stage每个任务的最佳执行位置,或者是缓存/shuffle数据的位置.通常顺序为同一台主机>同一个机架> 其他
 	6. clean up
 	当job执行完毕的时候,所有的数据结构会被清理.防止长期运行造成的内存泄漏.
 	为了能够从失败中恢复,一些stage可能需要运行多次,叫做尝试,是一种容错措施.如果任务调度器@TaskScheduler汇报信息时由于上个stage的map输出文件丢失,dag调度器则需要重新提交丢失的stage(重新运行这段的任务).这个可以通过监听器@CompletionEvent 的@FetchFailed 或者@ExecutorLost发现出这个问题.然后dag调度器会等待一段时间,检查其他节点或者任务是否失败,然后重新提交任务集且计算丢失的任务.作为进程的一部分,可能必须要创建完成stage额对象.因为旧的stage请求仍然有可能还会运行,注意必须要转化为接受成功的stage为止才行.
 	这个是当在类中检查或者作出改变的时候的检查列表
 	+ 为了避免长期运行导致的内存泄漏情况,必须要在job执行完毕的时候将相关数据结构全部清除.
 	+ 当添加新的数据结构的时候,使用@DAGSchedulerSuite.assertDataStructuresEmpty断言,这里会保证捕捉到内存泄漏异常.
```

```scala
private[spark] class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
extends Logging {
    构造器参数:
        sc	spark上下文
        taskScheduler	任务调度器
        listenerBus	监听总线
        mapOutputTracker	map输出定位器
        blockManagerMaster	数据块管理器master
        env	spark环境
        clock	系统时钟
    构造器:
    def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
        this(
            sc,
            taskScheduler,
            sc.listenerBus,
            sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
            sc.env.blockManager.master,
            sc.env)
    }
    def this(sc: SparkContext) = this(sc, sc.taskScheduler)
    
    属性:
    #name @metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)	调度器度量资源
    #name @nextJobId = new AtomicInteger(0)	当前job名称
    #name @nextStageId = new AtomicInteger(0)	当前stage编号
    #name @jobIdToStageIds = new HashMap[Int, HashSet[Int]]	job编号-->stage列表映射
    #name @stageIdToStage = new HashMap[Int, Stage]	stage编号--> stage映射
    #name @jobIdToActiveJob = new HashMap[Int, ActiveJob] job编号-->激活job的映射关系
    #name @waitingStages = new HashSet[Stage]	处于等待中的stage集合
    #name @runningStages = new HashSet[Stage]	处于运行中的stage集合
    #name @failedStages = new HashSet[Stage]	失败stage集合
    #name @activeJobs = new HashSet[ActiveJob]	集合任务集合
    #name @shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]	shuffle依赖编号->shuffle映射stage
    #name @cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]	缓存位置映射表
    	包含每个RDD分区的缓存位置,key是RDD的编号,value是由分区编号索引的列表,每个值代表分区的执行位置
    #name @failedEpoch = new HashMap[String, Long]	失败的位置信息
    用于定位失败节点,使用@MapOutputTracker的代编号,每个task都会发送,当发现一个节点失败的时候.注意到当前的代际编号和新任务的增加量,使用这个去忽视丢失的@ShuffleMapTask 任务.
    #name @outputCommitCoordinator = env.outputCommitCoordinator	输出提交协调器
    #name @closureSerializer = SparkEnv.get.closureSerializer.newInstance()	闭包序列化器(可重用)
    	这个是安全的,因为@DAGScheduler单线程运行
    #name @disallowStageRetryForTest = sc.getConf.get(TEST_NO_STAGE_RETRY) 关闭在test状态的stage容错措施
    #name @unRegisterOutputOnHostOnFetchFailure	是否在获取失败的收解除主机上所有的输出文件(默认false)
    	意味着只能在相关执行器失败的情况下解除注册
    val= sc.getConf.get(config.UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE)
    #name @maxConsecutiveStageAttempts	最大连续stage请求(stage放弃之前)
    val= sc.getConf.getInt("spark.stage.maxConsecutiveAttempts",
      DAGScheduler.DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS)
    #name @barrierJobIdToNumTasksCheckFailures = new ConcurrentHashMap[Int, Int]
    	屏蔽job编号--> 失败检测次数映射表
    #name @timeIntervalNumTasksCheck	检测最大并发任务的时间间隔
    val= sc.getConf
    	.get(config.BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL)
    #name @maxFailureNumTasksCheck	最大并发任务的检查次数
    val= sc.getConf
    	.get(config.BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES)
    #name @messageScheduler	消息调度线程
    val= ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")
    #name @eventProcessLoop	时间进程环
    val= new DAGSchedulerEventProcessLoop(this)
    
    
    初始化操作:
    taskScheduler.setDAGScheduler(this)
    功能: 设置dag调度器
    
    操作集:
    def numTotalJobs: Int = nextJobId.get()
    功能: 获取job数量
    
    def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit
    功能: 由@TaskSetManager调用,汇报任务已经开始
    
    def taskGettingResult(taskInfo: TaskInfo): Unit
    功能: 使用@TaskSetManager调用,汇报制度任务@taskInfo已经完成,执行的结果可以远端获取
    eventProcessLoop.post(GettingResultEvent(taskInfo))
    
    def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      metricPeaks: Array[Long],
      taskInfo: TaskInfo): Unit
    功能: 使用@TaskSetManager汇报指定任务的完成或者失败
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, metricPeaks, taskInfo))
    
    def executorHeartbeatReceived(
      execId: String,
      // (taskId, stageId, stageAttemptId, accumUpdates)
      accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId,
      // (stageId, stageAttemptId) -> metrics
      executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean
    功能: 更新处于运行中任务的度量信息,告知master数据块@BlockManager处于存活状态如果驱动器直到指定数据块管理器@blockManagerId的存在,返回true.否则返回false,表名了数据块管理器需要重新注册
    1. 监听总线发送执行器度量器更新的消息(发送事件到接收队列所有元素中)
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates,
      executorUpdates))
    2. 驱动器发起数据块管理器的心跳确认消息
    val=blockManagerMaster.driverHeartbeatEndPoint.askSync[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(10.minutes, "BlockManagerHeartbeat"))
    
    def executorLost(execId: String, reason: ExecutorLossReason): Unit
    功能: 在执行器失败的时候任务调度器@TaskScheduler调用
    eventProcessLoop.post(ExecutorLost(execId, reason))
    
    def workerRemoved(workerId: String, host: String, message: String): Unit 
    功能: 当workr被移除的时候,任务调度器@TaskScheduler调用
    val= eventProcessLoop.post(WorkerRemoved(workerId, host, message))
    
    def executorAdded(execId: String, host: String): Unit 
    功能: 任务执行器在主机@host添加的时候调用
    eventProcessLoop.post(ExecutorAdded(execId, host))
    
    def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit 
    功能: 任务集管理器@TaskSetManager 调用,用于放弃整个任务集(可能是重复的失败或者job自身的放弃)
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
    
    def speculativeTaskSubmitted(task: Task[_]): Unit
    功能: 当需要执行推测任务的时候,任务集管理器调用@TaskSetManager
    eventProcessLoop.post(SpeculativeTaskSubmitted(task))
    
    def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]]
    功能: 获取指定RDD各个分区的缓存执行位置列表
    cacheLocs.synchronized {
        // 注意到这里不要使用@getOrElse方法,这个会执行O(任务数量)次调用
        if (!cacheLocs.contains(rdd.id)) {
            // 注意到如果存储等级为None,就不需要从数据块管理器上获取位置信息
          val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
              // 空列表	
            IndexedSeq.fill(rdd.partitions.length)(Nil)
          } else {
              // 获取当前分区RDD锁对应的数据块编号列表,并映射为任务执行位置@TaskLocation
            val blockIds =
              rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
            blockManagerMaster.getLocations(blockIds).map { bms =>
              bms.map(bm => TaskLocation(bm.host, bm.executorId))
            }
          }
          cacheLocs(rdd.id) = locs
        }
        cacheLocs(rdd.id)
      }
    
    def clearCacheLocs(): Unit
    功能: 清除缓存位置表
    cacheLocs.synchronized {
        cacheLocs.clear()
    }
    
    def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage
    功能: 获取shuffle map的stage(如果存在有shuffle映射mapstage中存在有这条job记录的情况下),如果不存在则会创建这个job对应的shuffle map.
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => // 存在,则返回shuffle map条目
        stage
      case None => // 不存在,创建一个shuffle map
		// 对于所有丢失shuffle依赖的祖先RDD创建stage
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId)
    }
    
    def checkBarrierStageWithRDDChainPattern(rdd: RDD[_], numTasksInStage: Int): Unit
    功能: 确保不会再不支持的RDD链上执行屏蔽stage.下述几类不支持
    + 有不同分区数量的祖先RDD(例如,union()/coalesce()/first()/take()/PartitionPruningRDD)
    + 依赖于多个屏蔽RDD的RDD(例如,barrierRdd1.zip(barrierRdd2))
    if (rdd.isBarrier() &&
        !traverseParentRDDsWithinStage(rdd, (r: RDD[_]) =>
          r.getNumPartitions == numTasksInStage &&
          r.dependencies.count(_.rdd.isBarrier()) <= 1)) {
      throw new BarrierJobUnsupportedRDDChainException
    }
    
    def createShuffleMapStage[K, V, C](
      shuffleDep: ShuffleDependency[K, V, C], jobId: Int): ShuffleMapStage 
    功能: 创建shuffle map stage,这个stage会产生指定的shuffle依赖分区,如果之前运行的stage生成了相同的shuffle数据.这个函数会拷贝输出位置(生一个shuffle map仍然可以使用,用于避免不必要的重新生成数据)
    1. 检查屏蔽stage相关属性
    val rdd = shuffleDep.rdd
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd)
    checkBarrierStageWithRDDChainPattern(rdd, rdd.getNumPartitions)
    2. 获取指定依赖对应rdd的stage
    val numTasks = rdd.partitions.length
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(
      id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep, mapOutputTracker)
    3. 注册到@stageIdToStage,@shuffleIdToMapStage中
    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)
    4. 注册shuffle到map输出定位器中
    if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      logInfo(s"Registering RDD ${rdd.id} (${rdd.getCreationSite}) as input to " +
        s"shuffle ${shuffleDep.shuffleId}")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    val= stage
    
    def checkBarrierStageWithDynamicAllocation(rdd: RDD[_]): Unit
    功能: 不支持在开启动态资源分配的情况下运行屏蔽任务,这样会导致溢写令人困惑的执行结果.比如说,当使用了动态资源分配的时候,可能会申请执行器(但是不足以运行屏蔽stage中的所有任务).且之后由于执行器空载超时释放这些资源,然后又申请了这些资源.所以需要在运行屏蔽任务直接检测这个参数:
    if (rdd.isBarrier() && Utils.isDynamicAllocationEnabled(sc.getConf)) {
      throw new BarrierJobRunWithDynamicAllocationException
    }
    
    def checkBarrierStageWithNumSlots(rdd: RDD[_]): Unit
    功能: 检测是否屏蔽stage需要更多的槽数(拥有这些槽数就可以运行stage中的所有任务).如果屏蔽stage需要更多的槽数的话,检查就不会通过.如果连续检查失败,则会使得当前job提交失败.
    val numPartitions = rdd.getNumPartitions
    val maxNumConcurrentTasks = sc.maxNumConcurrentTasks
    if (rdd.isBarrier() && numPartitions > maxNumConcurrentTasks) {
        // 槽数没有申请够,抛出异常
      throw new BarrierJobSlotsNumberCheckFailed(numPartitions, maxNumConcurrentTasks)
    }
    
    def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage 
    功能: 创建结果处理的stage
    1. 屏蔽任务参数检测
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd)
    checkBarrierStageWithRDDChainPattern(rdd, partitions.toSet.size)
    2. 获取当前RDD的stage
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    3. 更新注册表@stageIdToStage 信息
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    val= stage
   
    def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage]
    功能: 获取或者创建指定RDD的父stage,新的stage使用指定@firstJobId创建
    val= getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
    
    def getMissingAncestorShuffleDependencies(
      rdd: RDD[_]): ListBuffer[ShuffleDependency[_, _, _]] 
    功能: 获取没有注册到@shuffleToMapStage 中的祖先依赖
    1. 初始化相关数据结构
    val ancestors = new ListBuffer[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    2. 手动维护栈,防止栈内存溢出,迭代寻找祖先依赖,检查是否访问过
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        getShuffleDependencies(toVisit).foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.prepend(shuffleDep) // 从列表的前端添加
            waitingForVisit.prepend(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    val= ancestors
    
    def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] 
    功能: 获取指定rdd的shuffle依赖
    这个函数不会返回很长的依赖,只会找到上一层依赖,这个函数在单元测试中有调度可见性
    1. 初始化数据结构
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    2. 使用类型BFS的方式形成依赖链条
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)
        }
      }
    }
    val= parents
    
    def traverseParentRDDsWithinStage(rdd: RDD[_], predicate: RDD[_] => Boolean): Boolean 
    功能: 在stage中变量父级RDD,且检查是否所有RDD都满足指定要求@predicate(采用BFS)
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        if (!predicate(toVisit)) {
          return false
        }
        visited += toVisit
        toVisit.dependencies.foreach {
          case _: ShuffleDependency[_, _, _] =>
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)
        }
      }
    }
    val= true // 缺省
    
    def getMissingParentStages(stage: Stage): List[Stage] 
    功能: 获取丢失的父级stage
    1. 初始化数据结构
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += stage.rdd
    2. 定义依赖中单个RDD访问函数
    def visit(rdd: RDD[_]): Unit = {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.prepend(narrowDep.rdd)
            }
          }
        }
      }
    }
    3. 迭代访问(BFS)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    val= missing.toList
    
    def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit
    功能: 更新job边-> stage编号映射表
    1. 定义更新函数
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]): Unit = {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    2. 更新所有的stage
    updateJobIdStageIdMapsList(List(stage))
    
    def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit
    功能: 清除指定job的状态,这个stage对于其他来说不重要的情况下进行这个操作.不会处理任务的放弃或者提示spark监听器关于job/stage/task的完成信息.
    1. 移除stage信息
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
              // 定义stage移除函数
            def removeStage(stageId: Int): Unit = {
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }
              // 从主持表中移除当前job
            jobSet -= job.jobId
              // 迭代移除job中的stage
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    2. 解除相关主持表中的job
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    3. 根据job所处的位置不同,进行不同方式的job移除
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
    
    def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U]
    功能: 提交job到调度器
    输入参数:
    	rdd	任务运行的目标RDD
    	func	运行在每个分区上的函数
    	partitions	所运行的分区列表
    	callSite	用户程序调用的位置
    	resultHandler	每个结果的回调函数
    	properties	连接到这个job的调度属性
    返回: @JobWaiter对象，用于阻塞到任务的完成，或者可以用于放弃job
    1. 检查运行的分区是否都存在
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }
    2. 空分区列表处理
    val jobId = nextJobId.getAndIncrement()
    if (partitions.isEmpty) {
      val time = clock.getTimeMillis()
      val dummyStageInfo =
        new StageInfo(
          StageInfo.INVALID_STAGE_ID,
          StageInfo.INVALID_ATTEMPT_ID,
          callSite.shortForm,
          0,
          Seq.empty[RDDInfo],
          Seq.empty[Int],
          "")
      listenerBus.post(
        SparkListenerJobStart(
          jobId, time, Seq[StageInfo](dummyStageInfo), Utils.cloneProperties(properties)))
      listenerBus.post(
        SparkListenerJobEnd(jobId, time, JobSucceeded))
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }
    3. 获取@waiter,并通过事件环发送job提交的消息
    assert(partitions.nonEmpty)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter[U](this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      Utils.cloneProperties(properties)))
    val=waiter
    
    def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit
    功能: 运行指定RDD的job,返回所有的结果给回调函数@resultHandler
    输入参数:
        rdd	job运行的RDD
        func	每个分区的处理函数
        partitions	运行的分区列表
        callSite	用户程序调用位置
        resultHandler	回调函数
        properties	调度器属性
    1. 提交job
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    2. 等待job完成
    ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
    3. 处理任务直接的结果
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) => // 执行失败需要抛出异常
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
    
    def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R]
    功能: 运行近似任务
    输入参数:
        evaluator	估值器
        timeout	job运行的最大时间
    1. 确定当前job编号
    val jobId = nextJobId.getAndIncrement()
    2. 空分区处理
    if (rdd.partitions.isEmpty) {
      val time = clock.getTimeMillis()
      // 接连发送任务启动/停止的消息
      listenerBus.post(SparkListenerJobStart(jobId, time, Seq[StageInfo](), properties))
      listenerBus.post(SparkListenerJobEnd(jobId, time, JobSucceeded))
      return new PartialResult(evaluator.currentResult(), true)
    }
    3. 设置近似计算的监听器
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    4. 发送job提交的消息,并等待监听器执行完成
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, rdd.partitions.indices.toArray, callSite, listener,
      Utils.cloneProperties(properties)))
    listener.awaitResult() 
    
    def submitMapStage[K, V, C](
      dependency: ShuffleDependency[K, V, C],
      callback: MapOutputStatistics => Unit,
      callSite: CallSite,
      properties: Properties): JobWaiter[MapOutputStatistics]
    功能: 提交shuffle mapstage.必须要阻塞到job执行完成或者job的放弃执行.这个方法使用了合适的查询计划,
    输入参数: 
    callback	回调函数
    callSite	用户程序调用位置
    1. 空分区处理
    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }
    2. 设置map stage的@JobWaiter,并发map job提交消息
    val waiter = new JobWaiter[MapOutputStatistics](
      this, jobId, 1,
      (_: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, Utils.cloneProperties(properties)))
    val= waiter
    
    def cancelJob(jobId: Int, reason: Option[String]): Unit 
    功能: 放弃指定job,并指明原因@reason
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId, reason))
    
    def cancelJobGroup(groupId: String): Unit
    功能: 放弃job组
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
    
    def cancelAllJobs(): Unit
    功能: 放弃所有job
    eventProcessLoop.post(AllJobsCancelled)
    
    def doCancelAllJobs(): Unit
    功能: 放弃所有运行中的job
     runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      Option("as part of cancellation of all jobs")))
    activeJobs.clear()
    jobIdToActiveJob.clear() 
    
    def cancelStage(stageId: Int, reason: Option[String]): Unit
    功能: 放弃指定stage,指定原因为@reason
    eventProcessLoop.post(StageCancelled(stageId, reason))
    
    def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean
    功能: kill指定的任务请求,可以选择是否中断线程
    taskScheduler.killTaskAttempt(taskId, interruptThread, reason)
    
    def resubmitFailedStages(): Unit
    功能: 重新提交失败的stage
    if (failedStages.nonEmpty) {
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      // 备份,清除失败的stage
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      // 重新提交失败的stage
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
    
    def submitWaitingChildStages(parent: Stage): Unit
    功能: 提交等待的子stage
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
    
    def activeJobForStage(stage: Stage): Option[Int]
    功能: 找到需要指定stage的最小激活job,返回job的编号
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
    
    def handleJobGroupCancelled(groupId: String): Unit
    功能: 放弃指定job组@groupId
    1. 获取指定激活组
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    2. 处理组中job的放弃
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_,
        Option("part of cancelled job group %s".format(groupId))))
    
    def handleBeginEvent(task: Task[_], taskInfo: TaskInfo): Unit
    功能: 处理指定任务的开始
    val stageAttemptId =
      stageIdToStage.get(task.stageId).map(_.latestInfo.attemptNumber).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
    
    def handleSpeculativeTaskSubmitted(task: Task[_]): Unit
    功能: 处理推测任务的提交
    val= listenerBus.post(SparkListenerSpeculativeTaskSubmitted(task.stageId, task.stageAttemptId))
    
    def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit
    功能: 处理指定任务集的失败
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
    
    def cleanUpAfterSchedulerStop(): Unit
    功能: 调度停止之后的清空工作
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }

    def handleGetTaskResult(taskInfo: TaskInfo): Unit
    功能: 处理获取指定任务结果
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
    
    def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties): Unit
    功能: 处理job的提交
    1. 获取最终的stage
    var finalStage: ResultStage = null
    try {
        // 返回新建的最终stage,job会运行在底层HDFS上
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: BarrierJobSlotsNumberCheckFailed =>
        // If jobId doesn't exist in the map, Scala coverts its value null to 0: Int automatically.
        val numCheckFailures = barrierJobIdToNumTasksCheckFailures.compute(jobId,
          (_: Int, value: Int) => value + 1)
        logWarning(s"Barrier stage in job $jobId requires ${e.requiredConcurrentTasks} slots, " +
          s"but only ${e.maxConcurrentTasks} are available. " +
          s"Will retry up to ${maxFailureNumTasksCheck - numCheckFailures + 1} more times")
        if (numCheckFailures <= maxFailureNumTasksCheck) {
          messageScheduler.schedule(
            new Runnable {
              override def run(): Unit = eventProcessLoop.post(JobSubmitted(jobId, finalRDD, func,
                partitions, callSite, listener, properties))
            },
            timeIntervalNumTasksCheck,
            TimeUnit.SECONDS
          )
          return
        } else {
          // job失败,清除内部数据
          barrierJobIdToNumTasksCheckFailures.remove(jobId)
          listener.jobFailed(e)
          return
        }
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    2. 提交job,清除内部数据
    barrierJobIdToNumTasksCheckFailures.remove(jobId)
    3. 清除缓存执行位置列表
    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))
    4. 将激活的job编号注册到相应的注册表
    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    5. 监听器发送信息到各个节点上,并提交stage
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)
    
    def handleMapStageSubmitted(jobId: Int,
      dependency: ShuffleDependency[_, _, _],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties): Unit
    功能: 处理shuffle map stage的提交
    1. 确定最终stage
    var finalStage: ShuffleMapStage = null
    try {
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    2. 确定stage所属的job
    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))
    3. 注册job信息到相关注册表中
    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    4. 提交stage
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)
    5. 标记stage完成,告知监听器移除stage
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
    
    def submitStage(stage: Stage): Unit
    功能: 提交指定stage
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug(s"submitStage($stage (name=${stage.name};" +
        s"jobs=${stage.jobIds.toSeq.sorted.mkString(",")}))")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
    
    def submitMissingTasks(stage: Stage, jobId: Int): Unit 
    功能: 提交指定job指定stage下的遗失任务
    1. 在找到遗失的分区之前,获取中间状态且首先进行移除工作,这里的操作保证部分完成的中间状态,每次使用@findMissingPartitions返回所有分区
    logDebug("submitMissingTasks(" + stage + ")")
    stage match {
      case sms: ShuffleMapStage if stage.isIndeterminate && !sms.isAvailable =>
        mapOutputTracker.unregisterAllMapOutput(sms.shuffleDep.shuffleId)
      case _ =>
    }
    2. 找出需要计算的分区
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
    3. 使用调度池,job组信息联系这个stage
    val properties = jobIdToActiveJob(jobId).properties
    4. 更新运行的stage列表
    runningStages += stage
    5. 启动stage
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    6. 确定任务所对应的任务执行位置信息
     val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }
    7. 发送stage提交消息
    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    if (partitionsToCompute.nonEmpty) {
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    }
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
    8. 任务使用二进制类型的广播变量,用于分发任务到执行器上,注意到广播序列化副本,对于每个任务都可以反序列化.这个提供了强烈的任务隔离,且在相关的闭包中修改状态.在hadoop中是必要的.
    var taskBinary: Broadcast[Array[Byte]] = null
    var partitions: Array[Partition] = null
    try {
      var taskBinaryBytes: Array[Byte] = null
      RDDCheckpointData.synchronized {
        taskBinaryBytes = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }
        partitions = stage.rdd.partitions
      }
      if (taskBinaryBytes.length > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024) {
        logWarning(s"Broadcasting large task binary with size " +
          s"${Utils.bytesToString(taskBinaryBytes.length)}")
      }
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage
        return
      case e: Throwable =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }
    9. 获取任务列表
    val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
        case stage: ShuffleMapStage =>
          stage.pendingPartitions.clear()
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = partitions(id)
            stage.pendingPartitions += id
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
              Option(sc.applicationId), sc.applicationAttemptId, stage.rdd.isBarrier())
          }
        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, locs, id, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
              stage.rdd.isBarrier())
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }
    10. 提交任务列表
    if (tasks.nonEmpty) {
      logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) (first 15 " +
        s"tasks are for partitions ${tasks.take(15).map(_.partitionId)})")
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties))
    } else {
      markStageAsFinished(stage, None)
      stage match {
        case stage: ShuffleMapStage =>
          logDebug(s"Stage ${stage} is actually done; " +
              s"(available: ${stage.isAvailable}," +
              s"available outputs: ${stage.numAvailableOutputs}," +
              s"partitions: ${stage.numPartitions})")
          markMapStageJobsAsFinished(stage)
        case stage : ResultStage =>
          logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
      }
      submitWaitingChildStages(stage)
    }
    
    def postTaskEnd(event: CompletionEvent): Unit
    功能: 发送任务结束事件
    1. 获取任务度量器
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            val taskId = event.taskInfo.taskId
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }
    2. 发送度量信息
    listenerBus.post(SparkListenerTaskEnd(event.task.stageId, event.task.stageAttemptId,
      Utils.getFormattedClassName(event.task), event.reason, event.taskInfo,
      new ExecutorMetrics(event.metricPeaks), taskMetrics))
    
    def shouldInterruptTaskThread(job: ActiveJob): Boolean
    功能: 确定是否需要中断线程
    if (job.properties == null) {
      false
    } else {// 由调度属性中指定
      val shouldInterruptThread =
        job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
      try {
        shouldInterruptThread.toBoolean
      } catch {
        case e: IllegalArgumentException =>
          logWarning(s"${SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL} in Job ${job.jobId} " +
            s"is invalid: $shouldInterruptThread. Using 'false' instead", e)
          false
      }
    }
    
    def handleResubmittedFailure(task: Task[_], stage: Stage): Unit 
    功能: 处理重新提交失败的情况
    logInfo(s"Resubmitted $task, so marking it as still running.")
    stage match {
      case sms: ShuffleMapStage =>
        sms.pendingPartitions += task.partitionId
      case _ =>
        throw new SparkException("TaskSetManagers should only send Resubmitted task " +
          "statuses for tasks in ShuffleMapStages.")
    }
    
    def markMapStageJobsAsFinished(shuffleStage: ShuffleMapStage): Unit 
    功能: 标记shuffle map stage完成
    if (shuffleStage.isAvailable && shuffleStage.mapStageJobs.nonEmpty) {
      val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
      for (job <- shuffleStage.mapStageJobs) {
        markMapStageJobAsFinished(job, stats)
      }
    }
    
    def handleExecutorLost(
      execId: String,
      workerLost: Boolean): Unit
    功能: 处理执行器的丢失
    1. 获取丢失的文件
    val fileLost = workerLost || !env.blockManager.externalShuffleServiceEnabled
    2. 移除执行器,并解除输出的注册
    removeExecutorAndUnregisterOutputs(
      execId = execId,
      fileLost = fileLost,
      hostToUnregisterOutputs = None,
      maybeEpoch = None)
    
    def removeExecutorAndUnregisterOutputs(
      execId: String,
      fileLost: Boolean,
      hostToUnregisterOutputs: Option[String],
      maybeEpoch: Option[Long] = None): Unit
    功能: 移除执行器@execId并解除输出的注册@hostToUnregisterOutputs
    1. 获取当前代际点
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    2. 从输出定位器中移除输出,并清除缓存
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)
      if (fileLost) {
        hostToUnregisterOutputs match {
          case Some(host) =>
            logInfo("Shuffle files lost for host: %s (epoch %d)".format(host, currentEpoch))
            mapOutputTracker.removeOutputsOnHost(host)
          case None =>
            logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
            mapOutputTracker.removeOutputsOnExecutor(execId)
        }
        clearCacheLocs()
      } else {
        logDebug("Additional executor lost message for %s (epoch %d)".format(execId, currentEpoch))
      }
    }
    
    def handleWorkerRemoved(
      workerId: String,
      host: String,
      message: String): Unit 
    功能: 移除指定worker
    logInfo("Shuffle files lost for worker %s on host %s".format(workerId, host))
    mapOutputTracker.removeOutputsOnHost(host)
    clearCacheLocs()
    
    def handleExecutorAdded(execId: String, host: String): Unit 
    功能: 处理执行器添加
    if (failedEpoch.contains(execId)) { // 如果在失败记录表中,则从注册表中移除
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
    
    
}
```



#### DAGSchedulerEvent

```scala
private[scheduler] sealed trait DAGSchedulerEvent
介绍: DAGScheduler 处理的时间类型,@DAGScheduler 使用事件队列结构,任何线程都可以发送事件(任务完成或者任务提交时间),但是有一个逻辑线程,可以读取这个事件,并做出决定.会简化同步过程.

private[scheduler] case class JobSubmitted(
    jobId: Int,
    finalRDD: RDD[_],
    func: (TaskContext, Iterator[_]) => _,
    partitions: Array[Int],
    callSite: CallSite,
    listener: JobListener,
    properties: Properties = null)
extends DAGSchedulerEvent
介绍: 任务提交事件
参数:
	jobId	job编号
	finalRDD	最后一个RDD
	func	任务处理函数
	partitions	分区列表
	callSite	用户调用
	listener	监听器
	properties	属性

private[scheduler] case class MapStageSubmitted(
  jobId: Int,
  dependency: ShuffleDependency[_, _, _],
  callSite: CallSite,
  listener: JobListener,
  properties: Properties = null)
  extends DAGSchedulerEvent
介绍: map stage作为单独job提交
参数:
	jobId	job编号
	dependency	依赖
	callsite	用户调用
	listner		监听器	
	properties	属性

private[scheduler] case class StageCancelled(
    stageId: Int,
    reason: Option[String])
extends DAGSchedulerEvent
介绍: stage取消事件
参数
	stageId	stage编号
	reason	取消原因

private[scheduler] case class JobCancelled(
    jobId: Int,
    reason: Option[String])
extends DAGSchedulerEvent
介绍: job取消事件
参数:
	jobId	job参数
	reason	取消原因

private[scheduler] case class JobGroupCancelled(groupId: String) extends DAGSchedulerEvent
介绍: job组抛弃
参数:
	groupId	job组ID

private[scheduler] case object AllJobsCancelled extends DAGSchedulerEvent
功能: 抛弃所有job事件

private[scheduler]
case class BeginEvent(task: Task[_], taskInfo: TaskInfo) extends DAGSchedulerEvent
功能: 开启指定任务的事件

private[scheduler]
case class GettingResultEvent(taskInfo: TaskInfo) extends DAGSchedulerEvent
功能: 获取指定任务的事件结果

private[scheduler] case class ExecutorAdded(execId: String, host: String) extends DAGSchedulerEvent
功能: 添加执行器事件

private[scheduler] case class ExecutorLost(execId: String, reason: ExecutorLossReason)
  extends DAGSchedulerEvent
功能: 丢弃指定执行器事件

private[scheduler] case class WorkerRemoved(workerId: String, host: String, message: String) extends DAGSchedulerEvent
功能: 移除指定worker

private[scheduler] case class TaskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]) extends DAGSchedulerEvent
功能: 指定任务集失败事件

private[scheduler] case object ResubmitFailedStages extends DAGSchedulerEvent
功能: 重新提交失败的stage

private[scheduler]
case class SpeculativeTaskSubmitted(task: Task[_]) extends DAGSchedulerEvent
功能: 提交推测任务

private[scheduler] case class CompletionEvent(
    task: Task[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Seq[AccumulatorV2[_, _]],
    metricPeaks: Array[Long],
    taskInfo: TaskInfo)
  extends DAGSchedulerEvent
功能: 完成任务事件
参数:
	task	任务
	reason	完成事件原因
	result	执行结果
	accumUpdates	更新累加器
	metricPeaks	度量信息
	taskInfo	任务信息
```

#### DAGSchedulerSource

```scala
private[scheduler] class DAGSchedulerSource(val dagScheduler: DAGScheduler)
extends Source {
    介绍: DAG调度器资源
    构造器参数：
    dagScheduler	dag调度器
    属性:
    #name @metricRegistry = new MetricRegistry()	度量注册器
    #name @sourceName = "DAGScheduler"	资源名称
    #name @messageProcessingTimer	消息处理计时器
    val= metricRegistry.timer(MetricRegistry.name("messageProcessingTime"))
    初始化操作:
    metricRegistry.register(
        MetricRegistry.name("stage", "failedStages"), new Gauge[Int] {
    	override def getValue: Int = dagScheduler.failedStages.size})
    功能: 注册失败stage
    
    metricRegistry.register(
        MetricRegistry.name("stage", "runningStages"), new Gauge[Int] {
    	override def getValue: Int = dagScheduler.runningStages.size
  	})
    功能: 注册运行stage
    
    metricRegistry.register(
        MetricRegistry.name("stage", "waitingStages"), new Gauge[Int] {
    	override def getValue: Int = dagScheduler.waitingStages.size
  	})
    功能: 注册等待stage
    
    metricRegistry.register(
        MetricRegistry.name("job", "allJobs"), new Gauge[Int] {
        override def getValue: Int = dagScheduler.numTotalJobs
  	})
    功能: 注册所有job
    
    metricRegistry.register(
        MetricRegistry.name("job", "activeJobs"), new Gauge[Int] {
        override def getValue: Int = dagScheduler.activeJobs.size
    })
    功能： 注册激活job
}
```

#### EventLoggingListener

```scala
private[spark] class EventLoggingListener(
    appId: String, // 应用名称
    appAttemptId : Option[String], // 应用请求编号
    logBaseDir: URI, // 日志基本目录
    sparkConf: SparkConf, // spark应用配置
    hadoopConf: Configuration) //hadoop配置
extends SparkListener with Logging {
    介绍: spark监听器将事件记录并持久化到存储器上.
    事件记录由下述几个配置指定:
    1. spark.eventLog.enabled 开启/关闭事件日志记录
    2. spark.eventLog.dir	事件记录目录位置
    3. spark.eventLog.logBlockUpdates.enabled	是否记录数据块更新记录
    4. spark.eventLog.logStageExecutorMetrics.enabled	是否记录stage执行器度量器
    具体日志文件维护的参数,请参考@EventLogFileWriter,获取更多的细节
    属性:
    #name @logWriter: EventLogFileWriter	日志记录器
    val= EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    #name @loggedEvents = new mutable.ArrayBuffer[JValue]	记录事件列表
    #name @shouldLogBlockUpdates = sparkConf.get(EVENT_LOG_BLOCK_UPDATES)	
    	是否需要记录数据块更新信息
    #name @shouldLogStageExecutorMetrics	是否需要更新执行器度量信息
    val= sparkConf.get(EVENT_LOG_STAGE_EXECUTOR_METRICS)
    #name @testing = sparkConf.get(EVENT_LOG_TESTING)	是否为测试状态
    #name @liveStageExecutorMetrics	存活stage执行器度量信息表(stage标识符-> 度量信息)
    val= mutable.HashMap.empty[(Int, Int), mutable.HashMap[String, ExecutorMetrics]]
    
    操作集:
    def start(): Unit
    功能: 启动日志监听器
    logWriter.start() // 开启日志记录器
    initEventLog() // 初始化事件记录
    
    def initEventLog(): Unit
    功能: 初始化时间记录
    val metadata = SparkListenerLogStart(SPARK_VERSION) // 获取元数据信息
    val eventJson = JsonProtocol.logStartToJson(metadata) // 获取日志json
    val metadataJson = compact(eventJson)
    logWriter.writeEvent(metadataJson, flushLogger = true)
    if (testing && loggedEvents != null) { // 添加事件日志(json格式)
      loggedEvents += eventJson
    }
    
    def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false): Unit
    功能: 使用json格式注册事件
    val eventJson = JsonProtocol.sparkEventToJson(event)
    logWriter.writeEvent(compact(render(eventJson)), flushLogger)
    if (testing) {
      loggedEvents += eventJson
    }
    
    def onStageSubmitted(event: SparkListenerStageSubmitted): Unit 
    功能: 处理stage提交事件(事件不会触发刷写操作)
    1. 记录事件
    logEvent(event)
    2. 记录度量信息
    if (shouldLogStageExecutorMetrics) {
      liveStageExecutorMetrics.put((event.stageInfo.stageId,
                                    event.stageInfo.attemptNumber()),
        mutable.HashMap.empty[String, ExecutorMetrics])
    }
    
    def onTaskStart(event: SparkListenerTaskStart): Unit = logEvent(event)
    功能: 任务开始事件处理
    
    def onTaskGettingResult(event: SparkListenerTaskGettingResult):Unit = logEvent(event)
    功能: 获取事件处理结果
    
    def onTaskEnd(event: SparkListenerTaskEnd): Unit
    功能: 任务结束处理
    1. 计量事件
    logEvent(event)
    2. 记录度量信息
    if (shouldLogStageExecutorMetrics) {
      val stageKey = (event.stageId, event.stageAttemptId)
      liveStageExecutorMetrics.get(stageKey).map { metricsPerExecutor =>
        val metrics = metricsPerExecutor.getOrElseUpdate(
          event.taskInfo.executorId, new ExecutorMetrics())
        metrics.compareAndUpdatePeakValues(event.taskExecutorMetrics)
      }
    }
    
    def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit 
    功能: 处理更新环境参数事件
    logEvent(redactEvent(event))
    
    def onStageCompleted(event: SparkListenerStageCompleted): Unit
    功能: 处理stage执行完成
    1. 更新度量参数
    if (shouldLogStageExecutorMetrics) {
      val prevAttemptId = event.stageInfo.attemptNumber() - 1
      for (attemptId <- 0 to prevAttemptId) {
        liveStageExecutorMetrics.remove((event.stageInfo.stageId, attemptId))
      }
      val executorOpt = liveStageExecutorMetrics.remove(
        (event.stageInfo.stageId, event.stageInfo.attemptNumber()))
      executorOpt.foreach { execMap =>
        execMap.foreach { case (executorId, peakExecutorMetrics) =>
            logEvent(new SparkListenerStageExecutorMetrics(executorId, event.stageInfo.stageId,
              event.stageInfo.attemptNumber(), peakExecutorMetrics))
        }
      }
    }
    2. 注册事件(需要刷新日志到存储系统中)
    logEvent(event, flushLogger = true)
    
    def onJobStart(event: SparkListenerJobStart): Unit=logEvent(event,flushLogger = true)
    功能: 处理job开始事件
    
    def onJobEnd(event: SparkListenerJobEnd): Unit = logEvent(event, flushLogger = true)
    功能: 处理job结束事件
    
    def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit
    功能: 处理添加块管理器事件
    logEvent(event, flushLogger = true)
    
    def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit
    功能: 处理块管理器移除事件
    logEvent(event, flushLogger = true)
    
    def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit
    功能: 处理RDD的去持久化事件
    logEvent(event, flushLogger = true)
    
    def onApplicationStart(event: SparkListenerApplicationStart): Unit
    功能: 处理应用开始事件
    logEvent(event, flushLogger = true)
    
    def onApplicationEnd(event: SparkListenerApplicationEnd): Unit
    功能: 处理应用结束事件
    logEvent(event, flushLogger = true)
    
    def onExecutorAdded(event: SparkListenerExecutorAdded): Unit
    功能: 处理执行器
    logEvent(event, flushLogger = true)
    
    def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit
    功能: 处理执行器移除事件
    logEvent(event, flushLogger = true)
    
    def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit
    功能: 处理黑名单执行器事件
    logEvent(event, flushLogger = true)
    
    def onExecutorBlacklistedForStage(
      event: SparkListenerExecutorBlacklistedForStage): Unit
    功能: 处理stage处于黑名单事件
    logEvent(event, flushLogger = true)
    
    def onNodeBlacklistedForStage(event: SparkListenerNodeBlacklistedForStage): Unit
    功能: 处理节点stage处于黑名单事件
    logEvent(event, flushLogger = true)
    
    def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit
    功能: 处理执行器从黑名单中是否的事件
    logEvent(event, flushLogger = true)
    
    def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit
    功能: 处理节点在黑名单上的事件
    logEvent(event, flushLogger = true)
    
    def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit 
    功能: 处理节点从黑名单中移除事件
    logEvent(event, flushLogger = true)
    
    def onBlockUpdated(event: SparkListenerBlockUpdated): Unit
	功能: 处理数据块更新的事件
    if (shouldLogBlockUpdates) {
      logEvent(event, flushLogger = true)
    }
    
    def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit 
    功能: 处理执行器度量值更新
    if (shouldLogStageExecutorMetrics) {
      event.executorUpdates.foreach { case (stageKey1, newPeaks) =>
        liveStageExecutorMetrics.foreach { case (stageKey2, metricsPerExecutor) =>
          if (stageKey1 == DRIVER_STAGE_KEY || stageKey1 == stageKey2) {
            val metrics = metricsPerExecutor.getOrElseUpdate(
              event.execId, new ExecutorMetrics())
            metrics.compareAndUpdatePeakValues(newPeaks)
          }
        }
      }
    }

    def onOtherEvent(event: SparkListenerEvent): Unit 
    功能: 处理其他类型事件
    if (event.logEvent) {
      logEvent(event, flushLogger = true)
    }
    
    def stop(): Unit
    功能: 停止记录事件
    logWriter.stop()
    
    def redactEvent(
      event: SparkListenerEnvironmentUpdate): SparkListenerEnvironmentUpdate
    功能: 编辑事件
    1. 获取编辑属性
    val redactedProps = event.environmentDetails.map{ case (name, props) =>
      name -> Utils.redact(sparkConf, props)
    }
    2. 更新属性
    val= SparkListenerEnvironmentUpdate(redactedProps)
}
```

```scala
private[spark] object EventLoggingListener extends Logging {
    #name @DEFAULT_LOG_DIR = "/tmp/spark-events"	默认日志目录
    #name @DRIVER_STAGE_KEY = (-1, -1)	驱动器stage标识符
}
```

#### ExecutorFailuresInTaskSet

```scala
private[scheduler] class ExecutorFailuresInTaskSet(val node: String) {
    介绍: 定位黑名单中的失败任务的辅助类,所有的失败信息在一个执行上,一个任务集内.
    构造器参数:
    node	节点名称
    属性:
    #name taskToFailureCountAndFailureTime = HashMap[Int, (Int, Long)]()
    任务失败信息表(任务编号--> 失败信息(失败次数,失败时间))
    操作集:
    def toString(): String
    功能: 信息显示
    val= {
        s"numUniqueTasksWithFailures = $numUniqueTasksWithFailures; " +
          s"tasksToFailureCount = $taskToFailureCountAndFailureTime"
      }
    
    def numUniqueTasksWithFailures: Int = taskToFailureCountAndFailureTime.size
    功能: 获取失败信息数量(去重)
    
    def getNumTaskFailures(index: Int): Int
    功能: 获取失败任务次数
    val= taskToFailureCountAndFailureTime.getOrElse(index, (0, 0))._1
    
    def updateWithFailure(taskIndex: Int, failureTime: Long): Unit
    功能: 更新失败信息
    1. 获取旧值
    val (prevFailureCount, prevFailureTime) =
      taskToFailureCountAndFailureTime.getOrElse(taskIndex, (0, -1L))
    2. 设置新值
    val newFailureTime = math.max(prevFailureTime, failureTime)
    taskToFailureCountAndFailureTime(taskIndex) = (prevFailureCount + 1, newFailureTime)
}
```

#### ExecutorLossReason

```scala
private[spark]
class ExecutorLossReason(val message: String) extends Serializable {
    介绍: 执行器丢失原因
    参数:
    	message	丢失信息
    操作集:
    def toString: String = message
    功能: 信息显示
}

private[spark]
case class ExecutorExited(exitCode: Int, exitCausedByApp: Boolean, reason: String)
  extends ExecutorLossReason(reason)
介绍: 执行器丢失原因 --> 执行器退出

private[spark] object ExecutorExited {
  def apply(exitCode: Int, exitCausedByApp: Boolean): ExecutorExited = {
    ExecutorExited(
      exitCode,
      exitCausedByApp,
      ExecutorExitCode.explainExitCode(exitCode))
  }
  功能: 获取执行器退出原因实例(退出码)
}

private[spark] object ExecutorKilled extends ExecutorLossReason("Executor killed by driver.")
介绍: 执行器丢失原因 --> 驱动器中断执行器

private [spark] object LossReasonPending extends ExecutorLossReason("Pending loss reason.")
介绍: 待定丢失原因 --> 丢失原因不明

private[spark]
case class SlaveLost(_message: String = "Slave lost", workerLost: Boolean = false)
  extends ExecutorLossReason(_message)
参数: 从节点丢失(执行器丢失原因)
参数:
	message	丢失原因 --> 从节点丢失
	workerLost	worker丢失状态
```

#### ExecutorResourceInfo

```scala
private[spark] class ExecutorResourceInfo(
    name: String,
    addresses: Seq[String],
    numParts: Int)
extends ResourceInformation(name, addresses.toArray) with ResourceAllocator {
    介绍: 存放执行器资源信息,信息有后台管理@SchedulerBackend,任务调度器可以对其进行调度@TaskScheduler,作用于空载执行器上.
    构造器参数:
    	name 	资源名称
    	addresses	执行器资源地址
    	numParts	调度时资源调用的途径数量
    操作集:
    def resourceName = this.name
    def resourceAddresses = this.addresses
    def slotsPerAddress: Int = numParts
}
```

#### ExternalClusterManager

```scala
private[spark] trait ExternalClusterManager {
    介绍: 外部集群管理器
    操作集:
    def canCreate(masterURL: String): Boolean
    功能: 检查是否可以创建外部集群管理器
    输入参数:
    	masterURL	master地址
    
    def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler
    功能: 创建用于任务处理的任务调度器
    
    def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend
    功能: 创建任务调度器后端,创建之后才可以创建任务调度器
    
    def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit
    功能: 初始化任务调度器,和后端调度器,在调度组件创建完毕之后才可以使用
}
```

#### InputFormatInfo

```scala
@DeveloperApi
class InputFormatInfo(val configuration: Configuration, val inputFormatClazz: Class[_],
    val path: String) extends Logging {
    介绍: 输入类型格式
    构造器参数:
        configuration	hadoop配置
        inputFormatClazz	输入类标签
        path	路径名称
    属性:
    #name @mapreduceInputFormat: Boolean = false	是否为MapReduce输入
    #name @mapredInputFormat: Boolean = false	是否为mapred输入
    操作集:
    def toString: String
    功能: 信息展示
    
    def hashCode(): Int 
    功能: 计算hash值(与输入类型和路径名称相关)
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + path.hashCode
    val= hashCode
    
    def equals(other: Any): Boolean
    功能: 相等逻辑判断
    val= other match {
        case that: InputFormatInfo =>
          this.inputFormatClazz == that.inputFormatClazz &&
            this.path == that.path
        case _ => false
      }
    
    def validate(): Unit 
    功能: 参数检测
    
    def prefLocsFromMapreduceInputFormat(): Set[SplitInfo] 
    功能: 计算MapReduce输入的最佳位置列表(分片信息)
    val conf = new JobConf(configuration)
    SparkHadoopUtil.get.addCredentials(conf)
    FileInputFormat.setInputPaths(conf, path)
    val instance: org.apache.hadoop.mapreduce.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], conf).asInstanceOf[org.apache.hadoop.mapreduce.InputFormat[_, _]]
    val job = Job.getInstance(conf)
    val retval = new ArrayBuffer[SplitInfo]()
    val list = instance.getSplits(job)
    for (split <- list.asScala) {
      retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, split)
    }
    val= retval.toSet
    
    def prefLocsFromMapredInputFormat(): Set[SplitInfo] 
    功能: 计算MapRed输入的分片信息列表
    val jobConf = new JobConf(configuration)
    SparkHadoopUtil.get.addCredentials(jobConf)
    FileInputFormat.setInputPaths(jobConf, path)
    val instance: org.apache.hadoop.mapred.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], jobConf).asInstanceOf[
        org.apache.hadoop.mapred.InputFormat[_, _]]
    val retval = new ArrayBuffer[SplitInfo]()
    instance.getSplits(jobConf, jobConf.getNumMapTasks()).foreach(
        elem => retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, elem)
    )
    val= retval.toSet
    
    def findPreferredLocations(): Set[SplitInfo] 
    功能: 查找最佳分片信息列表
    val= if (mapreduceInputFormat) {
      prefLocsFromMapreduceInputFormat()
    }
    else {
      assert(mapredInputFormat)
      prefLocsFromMapredInputFormat()
    }
}
```

```scala
object InputFormatInfo {
    操作集:
    def computePreferredLocations(formats: Seq[InputFormatInfo]):
    Map[String, Set[SplitInfo]] 
    功能: 计算最佳位置信息
    基于输入计算最佳位置,返回数据块map,典型使用方法如下:
    1. 对于每个主机,计算主机的分片数量
    2. 减少当前主机上分配容器的数量
    3. 对于每个主机计算机架,更新机架信息(根据2)
    4. 基于机架分配节点(根据3)
    5. 根据分配结果,避免单节点上job负载过重,返回1,直到需要的节点分配完毕
    如果一个节点死了,按照同样的步骤选取.
    1. 获取需要分配的分片信息
    val nodeToSplit = new HashMap[String, HashSet[SplitInfo]]
    2. 分配分片信息
    for (inputSplit <- formats) {
        // 选取当前分片最佳位置
      val splits = inputSplit.findPreferredLocations()
      for (split <- splits) {
        val location = split.hostLocation // 获取分配主机位置
        // 更新主机分片列表
        val set = nodeToSplit.getOrElseUpdate(location, new HashSet[SplitInfo])
        set += split
      }
    }
    3. 获取分配结果
    val= nodeToSplit.mapValues(_.toSet).toMap
}
```

#### JobListener

```scala
private[spark] trait JobListener {
  def taskSucceeded(index: Int, result: Any): Unit
  def jobFailed(exception: Exception): Unit
}
介绍: 监听job完成或者失败的接口,每次任务执行成功,会收到提示,或者整个job失败也会收到提示.
```

#### JobResult

```scala
@DeveloperApi
sealed trait JobResult
介绍: DAG调度器中job的执行结果

@DeveloperApi
case object JobSucceeded extends JobResult
介绍: job执行成功

private[spark] case class JobFailed(exception: Exception) extends JobResult
功能: job执行失败
```

#### JobWaiter

```scala
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler,
    val jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit)
extends JobListener with Logging {
    介绍: 等待DAG调度器job完成的对象,任务结束时,传递结果给指定处理函数.
    参数:
    	dagScheduler	dag调度器
    	jobId	job标识符
    	totalTasks	总任务数量
    	resultHandler	结果处理函数
    属性:
    #name @finishedTasks = new AtomicInteger(0)	完成任务数量
    #name @jobPromise: Promise[Unit]	job返回结果(0分区RDD将会直接返回执行成功)
    val= if (totalTasks == 0) Promise.successful(()) else Promise()
    操作集:
    def jobFinished: Boolean = jobPromise.isCompleted
    功能: 确认任务是否完成
    
    def completionFuture: Future[Unit] = jobPromise.future
    功能: 获取job执行线程任务体
    
    def cancel(): Unit= dagScheduler.cancelJob(jobId, None)
    功能: 放弃执行job
    
    def taskSucceeded(index: Int, result: Any): Unit
    功能: 任务成功处理
    1. 进行结果处理
    synchronized {
      resultHandler(index, result.asInstanceOf[T])
    }
    2. 确定job是否执行完成
    if (finishedTasks.incrementAndGet() == totalTasks) {
      jobPromise.success(())
    }
    
    def jobFailed(exception: Exception): Unit
    功能: 处理job失败情况
    if (!jobPromise.tryFailure(exception)) {
      logWarning("Ignore failure", exception)
    }
}
```

#### LiveListenerBus

```scala
private[spark] class LiveListenerBus(conf: SparkConf) {
    介绍: 异步的注册spark监听事件@SparkListenerEvents 将其注册到spark监听器上
    属性:
    #name @sparkContext: SparkContext = _	spark上下文
    #name @metrics = new LiveListenerBusMetrics(conf)	监听器度量器
    #name @started = new AtomicBoolean(false)	启动标记
    #name @stopped = new AtomicBoolean(false)	停止标记
    #name @droppedEventsCounter = new AtomicLong(0L)	抛弃的事件数量
    #name @lastReportTimestamp = 0L	上次汇报时间
    #name @queues = new CopyOnWriteArrayList[AsyncEventQueue]()	异步事件队列
    #name @queuedEvents = new mutable.ListBuffer[SparkListenerEvent]()	已入队事件(测试使用)
    操作集:
    def addToSharedQueue(listener: SparkListenerInterface): Unit 
    功能: 添加到共享队列(非内部监听器使用)
    addToQueue(listener, SHARED_QUEUE)
    
    def addToManagementQueue(listener: SparkListenerInterface): Unit
    功能: 添加事件到执行器管理队列
    addToQueue(listener, EXECUTOR_MANAGEMENT_QUEUE)
    
    def addToEventLogQueue(listener: SparkListenerInterface): Unit 
    功能: 添加事件到事件日志队列
    addToQueue(listener, EVENT_LOG_QUEUE)
    
    def addToQueue(
      listener: SparkListenerInterface,
      queue: String): Unit
    功能: 添加事件到指定队列
    synchronized {
        if (stopped.get()) {
          throw new IllegalStateException("LiveListenerBus is stopped.")
        }
        queues.asScala.find(_.name == queue) match {
          case Some(queue) =>
            queue.addListener(listener)
          case None =>
            val newQueue = new AsyncEventQueue(queue, conf, metrics, this)
            newQueue.addListener(listener)
            if (started.get()) {
              newQueue.start(sparkContext)
            }
            queues.add(newQueue)
        }
      }
    
    def removeListener(listener: SparkListenerInterface): Unit
    功能: 移除监听器(所有添加过的队列都要移除,且停止空队列)
    queues.asScala
      .filter { queue =>
        queue.removeListener(listener)
        queue.listeners.isEmpty()
      }
      .foreach { toRemove =>
        if (started.get() && !stopped.get()) {
          toRemove.stop()
        }
        queues.remove(toRemove)
      }
    
    def post(event: SparkListenerEvent): Unit
    功能: 发送事件到所有队列
    if (stopped.get()) {
      return
    }
    metrics.numEventsPosted.inc()
    if (queuedEvents == null) {
      postToQueues(event)
      return
    }
    synchronized {
      if (!started.get()) {
        queuedEvents += event
        return
      }
    }
    postToQueues(event)
    
    def postToQueues(event: SparkListenerEvent): Unit
    功能: 发送事件给队列中的每个监听器
    val it = queues.iterator()
    while (it.hasNext()) {
      it.next().post(event)
    }
    
    def start(sc: SparkContext, metricsSystem: MetricsSystem): Unit 
    功能: 启动发送事件给监听器
    synchronized {
        if (!started.compareAndSet(false, true)) {
          throw new IllegalStateException("LiveListenerBus already started.")
        }
        this.sparkContext = sc
        queues.asScala.foreach { q =>
          q.start(sc)
          queuedEvents.foreach(q.post)
        }
        queuedEvents = null
        metricsSystem.registerSource(metrics)
      }
    
    @throws(classOf[TimeoutException])
    private[spark] def waitUntilEmpty(): Unit 
    功能: 测试使用,等待直到没有事件发送到队列
    waitUntilEmpty(TimeUnit.SECONDS.toMillis(10))
    
    @throws(classOf[TimeoutException])
    def waitUntilEmpty(timeoutMillis: Long): Unit
    功能: 同上,可以指定等待时间
    val deadline = System.currentTimeMillis + timeoutMillis
    queues.asScala.foreach { queue =>
      if (!queue.waitUntilEmpty(deadline)) {
        throw new TimeoutException(s"The event queue is not empty after 
        $timeoutMillis ms.")
      }
    }
    
    def stop(): Unit
    功能: 停止监听总线
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop bus that has not yet started!")
    }
    if (!stopped.compareAndSet(false, true)) {
      return
    }
    synchronized {
      queues.asScala.foreach(_.stop())
      queues.clear()
    }
    
    def findListenersByClass[T <: SparkListenerInterface : ClassTag](): Seq[T]
    功能: 按照类名查找监听器列表,测试使用
    val= queues.asScala.flatMap { queue => queue.findListenersByClass[T]() }
    
    def listeners: JList[SparkListenerInterface]
    功能: 获取监听器列表(测试使用,java使用)
    val=  queues.asScala.flatMap(_.listeners.asScala).asJava
    
    def activeQueues(): Set[String]
    功能: 获取激活队列,测试使用
    val= queues.asScala.map(_.name).toSet
    
    def getQueueCapacity(name: String): Option[Int]
    功能: 获取队列容量,测试使用
    val=  queues.asScala.find(_.name == name).map(_.capacity)
}
```

```scala
private[spark] object LiveListenerBus {
    属性:
    #name @withinListenerThread #type @DynamicVariable[Boolean]	监听线程状态
    val= new DynamicVariable[Boolean](false)
    #name @SHARED_QUEUE = "shared"	共享队列
    #name @APP_STATUS_QUEUE="appStatus"	应用状态队列
    #name @EXECUTOR_MANAGEMENT_QUEUE="executorManagement"	执行器管理队列
    #name @EVENT_LOG_QUEUE = "eventLog"	事件日志队列
}
```

```scala
private[spark] class LiveListenerBusMetrics(conf: SparkConf)
extends Source with Logging {
    介绍: 监听总线参数度量器
    属性:
    #name @sourceName: String = "LiveListenerBus"	资源名称
    #name @metricRegistry: MetricRegistry = new MetricRegistry	度量注册器
    #name @numEventsPosted: Counter 发送事件数量
    val= metricRegistry.counter(MetricRegistry.name("numEventsPosted"))
    #name @perListenerClassTimers = mutable.Map[String, Timer]()	每个监听器计时参数
    操作集:
    def getTimerForListenerClass(cls: Class[_ <: SparkListenerInterface]): Option[Timer]
    功能: 获取指定监听类的计时器@Timer
}
```

#### MapStatus

```scala
private[spark] sealed trait MapStatus {
    介绍: @ShuffleMapTask 返回给调度器的状态,包含块管理器地址(任务可以运行,且传输输出作为reduce任务的输入).
    操作集:
    def location: BlockManagerId
    功能: 获取块管理器标识符
    
    def getSizeForBlock(reduceId: Int): Long
    功能: 获取指定reduce数据块的大小
    
    def mapId: Long
    功能: 获取唯一的shuffle mapID
}
```

```scala
private[spark] object MapStatus {
    属性:
    #name @minPartitionsToUseHighlyCompressMapStatus	使用高度压缩的最小分区
    val= Option(SparkEnv.get)
    .map(_.conf.get(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS))
    .getOrElse(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS.defaultValue.get)
    #name @LOG_BASE = 1.1	记录基值
    操作集:
    def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long): MapStatus
    功能: 获取指定数据块@loc 的map状态@MapStatus
    if (uncompressedSizes.length > minPartitionsToUseHighlyCompressMapStatus) {
      // 未压缩的大小过大,采用高度压缩
      HighlyCompressedMapStatus(loc, uncompressedSizes, mapTaskId)
    } else { // 采用普通压缩
      new CompressedMapStatus(loc, uncompressedSizes, mapTaskId)
    }
    
    def compressSize(size: Long): Byte
    功能: 获取压缩大小(用于高效汇报map输出大小,对size的压缩)
    val= if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
    
    def decompressSize(compressedSize: Byte): Long
    功能: 解压缩(压缩的逆操作)
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
}
```

```scala
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte],
    private[this] var _mapTaskId: Long)
extends MapStatus with Externalizable {
    介绍: 压缩Map状态
    操作集:
    def location: BlockManagerId = loc
    功能: 获数据块管理器标识符
    
    def getSizeForBlock(reduceId: Int): Long
    功能: 获取指定reduce数据块的大小
    val= MapStatus.decompressSize(compressedSizes(reduceId))
    
    def mapId: Long = _mapTaskId
    功能: 获取mapID
    
    def writeExternal(out: ObjectOutput): Unit
    功能: 写出属性
    Utils.tryOrIOException {
        loc.writeExternal(out)
        out.writeInt(compressedSizes.length)
        out.write(compressedSizes)
        out.writeLong(_mapTaskId)
      }
    
    def readExternal(in: ObjectInput): Unit
    功能: 读取外部属性
    Utils.tryOrIOException {
        loc = BlockManagerId(in)
        val len = in.readInt()
        compressedSizes = new Array[Byte](len)
        in.readFully(compressedSizes)
        _mapTaskId = in.readLong()
      }
}
```

```scala
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long,
    private[this] var hugeBlockSizes: scala.collection.Map[Int, Byte],
    private[this] var _mapTaskId: Long)
extends MapStatus with Externalizable {
    介绍: 高度压缩的map状态,使用巨大数据块,对其进行命中,可以加大压缩率
    构造器参数:
        loc	块管理器标识符
        numNonEmptyBlocks	非空数据块数量
        emptyBlocks	空数据块
        avgSize	平均大小
        hugeBlockSizes	巨大数据块长度信息表
        _mapTaskId	map任务ID
    操作集:
    def location: BlockManagerId = loc
    功能: 获取数据块管理标识符
    
    def getSizeForBlock(reduceId: Int): Long 
    功能: 获取指定reduceId的数据块大小
    1. 断言当前存在大数据块
    assert(hugeBlockSizes != null)
    2. 获取数据块大小
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      hugeBlockSizes.get(reduceId) match {
        case Some(size) => MapStatus.decompressSize(size) // 命中数据块大小
        case None => avgSize // 没有命中,取平均值
      }
    }
    
    def mapId: Long = _mapTaskId
    功能: 获取mapID
    
    def writeExternal(out: ObjectOutput): Unit 
    功能: 写出属性值
    Utils.tryOrIOException {
        loc.writeExternal(out)
        emptyBlocks.writeExternal(out)
        out.writeLong(avgSize)
        out.writeInt(hugeBlockSizes.size)
        hugeBlockSizes.foreach { kv =>
          out.writeInt(kv._1)
          out.writeByte(kv._2)
        }
        out.writeLong(_mapTaskId)
      }
    
    def readExternal(in: ObjectInput): Unit
    功能: 读取属性值
    Utils.tryOrIOException {
        loc = BlockManagerId(in)
        emptyBlocks = new RoaringBitmap()
        emptyBlocks.readExternal(in)
        avgSize = in.readLong()
        val count = in.readInt()
        val hugeBlockSizesImpl = mutable.Map.empty[Int, Byte]
        (0 until count).foreach { _ =>
          val block = in.readInt()
          val size = in.readByte()
          hugeBlockSizesImpl(block) = size
        }
        hugeBlockSizes = hugeBlockSizesImpl
        _mapTaskId = in.readLong()
      }
}
```

```scala
private[spark] object HighlyCompressedMapStatus {
    操作集:
    def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long): HighlyCompressedMapStatus
    功能: 获取高度压缩map状态的实例
}
```

#### OutputCommitCoordinationMessage

```scala
private sealed trait OutputCommitCoordinationMessage extends Serializable
介绍: 输出协调者消息

private case object StopCoordinator extends OutputCommitCoordinationMessage
介绍: 停止协调者

private case class AskPermissionToCommitOutput(
    stage: Int,
    stageAttempt: Int,
    partition: Int,
    attemptNumber: Int)
介绍: 请求运行提交输出

private[spark] object OutputCommitCoordinator {
    介绍: 输出提交协调者
    内部类:
    class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {
        介绍: 输出协调者端点
        参数:
        rpcEnv						rpc环境
        outputCommitCoordinator		输出协调者
        操作集:
        def receive: PartialFunction[Any, Unit]
        功能: 接受远端RPC消息
        case StopCoordinator => // 接受远端停止协调者功能
        	logInfo("OutputCommitCoordinator stopped!")
        	stop()
        
        def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
        功能: 接受并回应远端RPC端点
        case AskPermissionToCommitOutput(
            stage, stageAttempt, partition, attemptNumber) =>
        context.reply(outputCommitCoordinator.handleAskPermissionToCommit(
            stage, stageAttempt, partition,attemptNumber))
    }
}
```

```scala
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {
    介绍: 输出协调者,通过授权,确定任务是否可以提交到HDFS上,使用首次提交获胜策略(first committer wins).输出协调器可在驱动器或者执行器上实例化,在执行器上时,需要配置驱动器的RPC端点,请求提交输出会通过RPC发送到驱动器上.
    构造器参数:
    	isDriver	是否为driver
    样例类:
    case class TaskIdentifier(stageAttempt: Int, taskAttempt: Int)
    介绍: 任务唯一标识
    case class StageState(numPartitions: Int){
        介绍: 任务状态
        val authorizedCommitters = Array.fill[TaskIdentifier](numPartitions)(null)
        	授权提交者列表	
        val failures = mutable.Map[Int, mutable.Set[TaskIdentifier]]()
        	失败任务信息表
    }
    
    属性:
    #name @coordinatorRef: Option[RpcEndpointRef] = None	协调者RPC端点
    #name @stageStates = mutable.Map[Int, StageState]()	stage状态表
    操作集:
    def isEmpty: Boolean= stageStates.isEmpty
    功能: 确定stage状态表是否为空
    
    def canCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean
    功能: 确定任务是否能够提交
    1. 确定需要发送的RPC消息
    val msg = AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber)
    2. 获取远端处理结果
    coordinatorRef match {
      case Some(endpointRef) =>
        ThreadUtils.awaitResult(endpointRef.ask[Boolean](msg),
          RpcUtils.askRpcTimeout(conf).duration)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv 
          shutdown in progress)?")
        false
    }
    
    def stageEnd(stage: Int): Unit= synchronized { stageStates.remove(stage) }
    功能: stage结束处理(移除stage)
    
    def stageStart(stage: Int, maxPartitionId: Int): Unit
    功能: 开启stage,并指定最大分区数量
    synchronized {
        stageStates.get(stage) match {
          case Some(state) =>
            require(state.authorizedCommitters.length == maxPartitionId + 1)
            logInfo(s"Reusing state from previous attempt of stage $stage.")

          case _ =>
            stageStates(stage) = new StageState(maxPartitionId + 1)
        }
      }
    
    def taskCompleted(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int,
      reason: TaskEndReason): Unit
    功能: 任务完成处理
    1. 获取stage状态
    val stageState = stageStates.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    2. 根据执行情况进行处理
    reason match {
      case Success => // 成功完成
      case _: TaskCommitDenied => // 提交权限不足
        logInfo(s"Task was denied committing, stage: $stage.$stageAttempt, " +
          s"partition: $partition, attempt: $attemptNumber")
      case _ =>
        // 其他情况,视为失败处理
        val taskId = TaskIdentifier(stageAttempt, attemptNumber)
        stageState.failures.getOrElseUpdate(partition, mutable.Set()) += taskId
        if (stageState.authorizedCommitters(partition) == taskId) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          stageState.authorizedCommitters(partition) = null
        }
    }
    
    def stop(): Unit 
    功能: 停止协调者
    synchronized {
        if (isDriver) {
          coordinatorRef.foreach(_ send StopCoordinator)
          coordinatorRef = None
          stageStates.clear()
        }
      }
    
    def attemptFailed(
      stageState: StageState,
      stageAttempt: Int,
      partition: Int,
      attempt: Int): Boolean
    功能: 请求失败处理方案
    val= synchronized {
        val failInfo = TaskIdentifier(stageAttempt, attempt)
        stageState.failures.get(partition).exists(_.contains(failInfo))
      }
    
    def handleAskPermissionToCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean =
    功能: 处理申请权限提交输出
    synchronized {
    stageStates.get(stage) match {
      case Some(state) if attemptFailed(state, stageAttempt, partition, attemptNumber) =>
        logInfo(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          s"task attempt $attemptNumber already marked as failed.")
        false
      case Some(state) =>
        val existing = state.authorizedCommitters(partition)
        if (existing == null) {
          logDebug(s"Commit allowed for stage=$stage.$stageAttempt, 
          partition=$partition, " +
            s"task attempt $attemptNumber")
          state.authorizedCommitters(partition) = TaskIdentifier(stageAttempt, attemptNumber)
          true
        } else {
          logDebug(s"Commit denied for stage=$stage.$stageAttempt, 
          partition=$partition: " +
            s"already committed by $existing")
          false
        }
      case None =>
        logDebug(s"Commit denied for stage=$stage.$stageAttempt,
        partition=$partition: " +
          "stage already marked as completed.")
        false
    }
}
```

#### Pool

```scala
private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
extends Schedulable with Logging {
    介绍: 代表池或者任务集管理器的调度化实例
    构造器参数:
    	poolName	池名称
    	schedulingMode	调度模式
    	initMinShare	初始最小共享量
    	initWeight	初始大小
    #name @schedulableQueue = new ConcurrentLinkedQueue[Schedulable]	调度队列
    #name @schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
    	调度名称映射表
    #name @weight = initWeight	权重
    #name @minShare = initMinShare	最小共享量
    #name @runningTasks = 0	运行任务数量
    #name @priority = 0	优先级
    #name @stageId = -1	stage编号
    #name @name = poolName	名称
    #name @parent: Pool = null	父调度池
    #name @taskSetSchedulingAlgorithm #type @SchedulingAlgorithm	任务集调度算法
    val= schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode.
        Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
    操作集:
    def addSchedulable(schedulable: Schedulable): Unit
    功能: 添加调度器@schedulable
    require(schedulable != null)
    schedulableQueue.add(schedulable) // 置入调度队列
    schedulableNameToSchedulable.put(schedulable.name, schedulable) // 存放映射信息
    schedulable.parent = this // 作为当前池的后续调度
    
    def removeSchedulable(schedulable: Schedulable): Unit
    功能: 移除调度器
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
    
    def getSchedulableByName(schedulableName: String): Schedulable
    功能: 按照名称获取调度器@Schedulable
    1. 直接获取
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)// 时间复杂度O(1)
    }
    2. 从调度队列中获取
    for (schedulable <- schedulableQueue.asScala) {// 时间复杂度(O(nlog n))
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
    
    def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
    功能: 处理指定执行器丢失问题
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
    
    def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean
    功能: 检查推测任务(队列中有一个是,则表示含有推测任务)
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    val= shouldRevive
    
    def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
    功能: 获取排序完成的任务集队列
    1. 按照任务集进行排序
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    2. 按照调度器进行排序
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    3. 将调度器中的任务集放入结果中
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
    
    def increaseRunningTasks(taskNum: Int): Unit
    功能: 增加运行任务数量
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
    
    def decreaseRunningTasks(taskNum: Int): Unit
    功能: 减少运行任务数量
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
}
```

#### ReplayListenerBus

```scala
private[spark] class ReplayListenerBus extends SparkListenerBus with Logging {
    介绍: spark监听总线,用于对序列化事件数据重新演绎
    操作集:
    def replay(
      logData: InputStream,
      sourceName: String,
      maybeTruncated: Boolean = false,
      eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): Boolean
    功能: 重演指定输入流顺序中的所有事件,流式数据需要包含每行包含一个json编码的监听器@SparkListenerEvent.方法可以多次调用,但是监听器一旦经过错误之后就会被移除.
    输入参数:
    	logData	包含事件日志数据的输入流
    	sourceName	文件名称(事件数据读取位置)
    	maybeTruncated	是否日志数据可以被删除
    	eventsFilter	事件过滤器(选取json事件的过滤函数)
    val= {
        val lines = Source.fromInputStream(logData)(Codec.UTF8).getLines()
        replay(lines, sourceName, maybeTruncated, eventsFilter)
      }
    
    def replay(
      lines: Iterator[String],
      sourceName: String,
      maybeTruncated: Boolean,
      eventsFilter: ReplayEventsFilter): Boolean
    功能: 重载@replay方法,使用迭代器而不使用输入流,可以被@ApplicationHistoryProvider实现
    
    def isIgnorableException(e: Throwable): Boolean =e.isInstanceOf[HaltReplayException]
    功能: 确认是否被忽略
    1. 设置初始参数
    var currentLine: String = null // 当前行内容
    var lineNumber: Int = 0 // 行编号
    val unrecognizedEvents = new scala.collection.mutable.HashSet[String]//未识别事件列表
    val unrecognizedProperties =new scala.collection.mutable.HashSet[String]//为识别属性列表
    2. 获取行内容集合
    val lineEntries = lines
        .zipWithIndex
        .filter { case (line, _) => eventsFilter(line) }
    3. 处理行文本内容
    while (lineEntries.hasNext) {
        try {
          val entry = lineEntries.next()
          currentLine = entry._1
          lineNumber = entry._2 + 1
		// 发送json数据到所有的监听器上
          postToAll(JsonProtocol.sparkEventFromJson(parse(currentLine)))
        } catch {
          case e: ClassNotFoundException =>
            if (!unrecognizedEvents.contains(e.getMessage)) {
              logWarning(s"Drop unrecognized event: ${e.getMessage}")
              unrecognizedEvents.add(e.getMessage)
            }
            logDebug(s"Drop incompatible event log: $currentLine")
          case e: UnrecognizedPropertyException =>
            if (!unrecognizedProperties.contains(e.getMessage)) {
              logWarning(s"Drop unrecognized property: ${e.getMessage}")
              unrecognizedProperties.add(e.getMessage)
            }
            logDebug(s"Drop incompatible event log: $currentLine")
          case jpe: JsonParseException =>
            if (!maybeTruncated || lineEntries.hasNext) {
              throw jpe
            } else {
              logWarning(s"Got JsonParseException from log file $sourceName" +
                s" at line $lineNumber, the file might not have finished
                writing cleanly.")
            }
        }
      }
}
```

```scala
private[spark] class HaltReplayException extends RuntimeException
介绍: 停止重新演绎异常

private[spark] object ReplayListenerBus {
    介绍: 重演监听器
    type ReplayEventsFilter = (String) => Boolean
    介绍: 重演过滤函数
    
    #name @SELECT_ALL_FILTER: ReplayEventsFilter = { (eventString: String) => true }
    	所有过滤器形成额的重演事件
}
```

#### ResultStage

```scala
private[spark] class ResultStage(
    id: Int,
    rdd: RDD[_],
    val func: (TaskContext, Iterator[_]) => _,
    val partitions: Array[Int],
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite)
extends Stage(id, rdd, partitions.length, parents, firstJobId, callSite) {
    介绍： 结果stage，在RDD的溢写分区上使用函数，用于计算动作的结果。@ResultStage 捕捉函数@func去执行，这个函数会应用到每个分区中。和每个分区ID列表中。一些stage可能不会跑完RDD的所有分区，比如@first(),@lookup()
    构造器参数:
        id	stage编号
        rdd	RDD
        func	分区计算函数
        partitions	分区列表
        parents	父stage列表
        firstJobId	第一个job编号
        callSite	用户调用
    属性:
    #name @_activeJob: Option[ActiveJob] = None	激活job
    操作集:
    def activeJob: Option[ActiveJob] = _activeJob
    功能: 获取激活job
    
    def setActiveJob(job: ActiveJob): Unit= _activeJob = Option(job)
    功能: 设置激活job
    
    def removeActiveJob(): Unit = _activeJob = None
    功能: 移除激活job
    
    def findMissingPartitions(): Seq[Int]
    功能: 获取遗失的分区
    
    def toString: String = "ResultStage " + id
    功能: 信息显示
}
```

#### ResultTask

```markdown
介绍:
	发送输出给driver应用的任务,参考@Task 获取更多
	构造器参数
        stageId	stage编号
        stageAttemptId	stage请求编号
        taskBinary	
        	序列化RDD的广播变量版本和分区应用函数类型为RDD[T], (TaskContext, Iterator[T]) => U
        partition	分区
        locs	本地调度任务最佳执行位置
        outputId	本job的task索引
        localProperties	驱动器端用户本地线程属性
        serializedTaskMetrics	序列化任务度量器
        jobId	jobID
        appId	应用ID
        appAttemptId	应用请求ID
        isBarrier	是否屏蔽执行
```

```scala
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    locs: Seq[TaskLocation],
    val outputId: Int,
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
extends Task[U](stageId, stageAttemptId, partition.index, localProperties, serializedTaskMetrics,
    jobId, appId, appAttemptId, isBarrier)
with Serializable {
    属性: 
    #name @preferredLocs: Seq[TaskLocation]	任务最佳执行位置列表
    操作集:
    def runTask(context: TaskContext): U
    功能： 运行任务@context 返回任务执行结果
    1. 使用广播变量反序列化RDD和分区处理函数
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L
    2. 对RDD的分区进行处理
    val= func(context, rdd.iterator(partition, context))
    
    def preferredLocations: Seq[TaskLocation] = preferredLocs
    功能： 获取任务最佳执行位置（只能执行在driver侧）
    
    def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
    功能: 显示信息
}
```

#### Schedulable

```scala
private[spark] trait Schedulable {
    介绍: 可调度实例接口,含有两个类型的调度实例(调度池@Pool和任务集管理器@TaskSetManagers)
    属性:
    #name @parent: Pool	父调度池
    操作集：
    def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
    功能: 获取调度队列
    
    def schedulingMode: SchedulingMode
    功能: 获取调度模式
    
    def weight: Int
    功能: 获取权重
    
    def minShare: Int
    功能: 获取最小分享量
    
    def runningTasks: Int
    功能: 获取运行任务数量
    
    def priority: Int
    功能: 获取优先级
    
    def stageId: Int
    功能: 获取stageId
    
    def name: String
    功能: 获取调度器名称
    
    def addSchedulable(schedulable: Schedulable): Unit
    功能: 添加可调度实例@schedulable
    
    def removeSchedulable(schedulable: Schedulable): Unit
    功能: 移除可调度实例
    
    def getSchedulableByName(name: String): Schedulable
    功能: 按照名称获取调度实例
    
    def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
    功能: 处理指定执行器丢失情景
    
    def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean
    功能: 检查推测任务
    
    def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
    功能: 获取排序完成的任务集队列
}
```

#### SchedulableBuilder

```scala
private[spark] trait SchedulableBuilder {
    介绍: 构建调度树的接口
    buildPools	构建树的结点
    addTaskSetManager	构建叶子结点
    操作集:
    def rootPool: Pool
    功能: 获取树根调度池
    
    def buildPools(): Unit
    功能: 构建树的结点
    
    def addTaskSetManager(manager: Schedulable, properties: Properties): Unit
    功能: 构建树的叶子节点
}

private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
extends SchedulableBuilder with Logging {
    介绍: FIFO调度的构建
    参数:
    	rootPool	调度树根结点
    操作集:
    def buildPools(): Unit={} 
    功能: 构建树的节点
    
    def addTaskSetManager(manager: Schedulable, properties: Properties): Unit 
    功能: 构建叶子结点
    rootPool.addSchedulable(manager)
}
```

```scala
private[spark] class FairSchedulableBuilder(val rootPool: Pool, conf: SparkConf)
extends SchedulableBuilder with Logging {
    介绍: 公平调度器构建器
    参数:
    	rootPool	根结点
    属性:
    #name @schedulerAllocFile = conf.get(SCHEDULER_ALLOCATION_FILE)	调度器分配文件
    #name @DEFAULT_SCHEDULER_FILE = "fairscheduler.xml" 默认调度文件
    #name @FAIR_SCHEDULER_PROPERTIES = SparkContext.SPARK_SCHEDULER_POOL
    	公平调度器属性
    #name @DEFAULT_POOL_NAME = "default"	默认调度池名称
    #name @MINIMUM_SHARES_PROPERTY = "minShare"	最小共享属性
    #name @SCHEDULING_MODE_PROPERTY = "schedulingMode"	调度模式属性
    #name @WEIGHT_PROPERTY = "weight"	权重属性
    #name @POOL_NAME_PROPERTY = "@name"	调度池名称属性
    #name @POOLS_PROPERTY = "pool"	调度池属性
    #name @DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO	调度模式属性
    #name @DEFAULT_MINIMUM_SHARE = 0	默认最小共享数量
    #name @DEFAULT_WEIGHT = 1	默认权重
    操作集:
    def buildPools(): Unit
    功能: 构建调度池
    1. 设置调度文件对应的内存数据结构
    var fileData: Option[(InputStream, String)] = None
    2. 填充数据结构
    fileData = schedulerAllocFile.map { f =>
        // 找到对应的调度文件
        val fis = new FileInputStream(f)
        logInfo(s"Creating Fair Scheduler pools from $f")
        Some((fis, f))
      }.getOrElse {
        // 没有找到,需要读取默认调度文件
        val is = Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        if (is != null) {
          logInfo(s"Creating Fair Scheduler pools from default file:
          $DEFAULT_SCHEDULER_FILE")
          Some((is, DEFAULT_SCHEDULER_FILE))
        } else {
          logWarning("Fair Scheduler configuration file not found so jobs will be
          scheduled in " +
            s"FIFO order. To use fair scheduling, configure pools in
            $DEFAULT_SCHEDULER_FILE or " +
            s"set ${SCHEDULER_ALLOCATION_FILE.key} to a file that contains the
            configuration.")
          None
        }
      }
    3. 对于每个文件,构建公平调度器
    val= fileData.foreach { case (is, fileName) => buildFairSchedulerPool(is, fileName) }
    4. 创建默认调度池
    buildDefaultPool()
    
    def buildDefaultPool(): Unit
    功能: 构建默认调度池(默认调度池名称的调度池)
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(pool)
      logInfo("Created default pool: %s, schedulingMode: %s, minShare: %d, 
      weight: %d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE,
          DEFAULT_WEIGHT))
    }
    
    def buildFairSchedulerPool(is: InputStream, fileName: String): Unit
    功能: 构建指定文件的公平调度池
    1. 使用xml加载输入流
    val xml = XML.load(is)
    2. 获取xml中的每个节点,添加到调度池中
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {
      val poolName = (poolNode \ POOL_NAME_PROPERTY).text
      val schedulingMode = getSchedulingModeValue(poolNode, poolName,
        DEFAULT_SCHEDULING_MODE, fileName)
      val minShare = getIntValue(poolNode, poolName, MINIMUM_SHARES_PROPERTY,
        DEFAULT_MINIMUM_SHARE, fileName)
      val weight = getIntValue(poolNode, poolName, WEIGHT_PROPERTY,
        DEFAULT_WEIGHT, fileName)
      rootPool.addSchedulable(new Pool(poolName, schedulingMode, minShare, weight))
      logInfo("Created pool: %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, schedulingMode, minShare, weight))
    }
    
    def getSchedulingModeValue(
      poolNode: Node,
      poolName: String,
      defaultValue: SchedulingMode,
      fileName: String): SchedulingMode
    功能: 获取调度模式的值
    1. 获取xml调度模式下的调度名称
    val xmlSchedulingMode =
      (poolNode \ SCHEDULING_MODE_PROPERTY).text.trim.toUpperCase(Locale.ROOT)
    val warningMessage = s"Unsupported schedulingMode: $xmlSchedulingMode found in " +
      s"Fair Scheduler configuration file: $fileName, using " +
      s"the default schedulingMode: $defaultValue for pool: $poolName"
    2. 确定是否为xml调度名称,不是则返回默认值
    val= if (SchedulingMode.withName(xmlSchedulingMode) != SchedulingMode.NONE) {
        SchedulingMode.withName(xmlSchedulingMode)
      } else {
        logWarning(warningMessage)
        defaultValue
      }
    
    def getIntValue(
      poolNode: Node,
      poolName: String,
      propertyName: String,
      defaultValue: Int,
      fileName: String): Int
    功能: 获取int值
    val data = (poolNode \ propertyName).text.trim
    try {
      data.toInt
    } catch {
      case e: NumberFormatException =>
        logWarning(s"Error while loading fair scheduler configuration from $fileName: " +
          s"$propertyName is blank or invalid: $data, using the default $propertyName: 
          " +
          s"$defaultValue for pool: $poolName")
        defaultValue
    }
    
    def addTaskSetManager(manager: Schedulable, properties: Properties): Unit
    功能: 添加任务集管理器
    1. 获取调度池名称
    val poolName = if (properties != null) {
        properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      } else {
        DEFAULT_POOL_NAME
      }
    2. 获取父级调度池
    var parentPool = rootPool.getSchedulableByName(poolName)
    3. 将本调度实体加入到父级调度之后
    if (parentPool == null) {
      parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(parentPool)
      logWarning(s"A job was submitted with scheduler pool $poolName, which has not been " +
        "configured. This can happen when the file that pools are read from isn't set, or " +
        s"when that file doesn't contain $poolName. Created $poolName with default " +
        s"configuration (schedulingMode: $DEFAULT_SCHEDULING_MODE, " +
        s"minShare: $DEFAULT_MINIMUM_SHARE, weight: $DEFAULT_WEIGHT)")
    }
    parentPool.addSchedulable(manager)
}
```

#### SchedulerBackend

```markdown
介绍:
	调度器后台,调度系统允许在一个@TaskSchedulerImpl 下面使用不同的插件,假定类似mesos模型,应用可以获取资源(因为机器变得可以获取且可以在上面允许任务)
```

```scala
private[spark] trait SchedulerBackend {
    属性:
    #name @appId = "spark-application-" + System.currentTimeMillis	应用ID
    操作集:
    def start(): Unit
    功能: 启动后端
    
    def stop(): Unit
    功能: 停止后端
    
    def defaultParallelism(): Int
    功能: 获取默认并行度
    
    def reviveOffers(): Unit
    功能: 恢复后端状态
    
    def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit =
    throw new UnsupportedOperationException
    功能: 中断指定任务(不支持)
    
    def isReady(): Boolean = true
    功能: 确定是否准备好
    
    def applicationId(): String = appId
    功能: 获取应用ID
    
    def applicationAttemptId(): Option[String] = None
    功能: 获取应用请求编号
    
    def getDriverLogUrls: Option[Map[String, String]] = None
    功能: 获取driver日志的地址映射表
    
    def getDriverAttributes: Option[Map[String, String]] = None
    功能: 获取driver的属性映射表
    
    def maxNumConcurrentTasks(): Int
    功能: 获取最大并发任务数量
}
```

#### SchedulingAlgorithm

```scala
private[spark] trait SchedulingAlgorithm {
    介绍: 调度算法
    FIFO	任务集管理器之间使用
    FS	调度池之间/调度池与任务集管理器之间使用公平调度
    def comparator(s1: Schedulable, s2: Schedulable): Boolean
    功能: 比较调度实例的大小
}
```

```scala
private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
    介绍: FIFO调度器
    def comparator(s1: Schedulable, s2: Schedulable): Boolean
    功能: 比较调度实例的大小
    1. 获取优先级并比较
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2) // 正为1,负数为-1,0为0
 	2. 第二关键字比较
    if (res == 0) {
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    val= res < 0
}
```

```scala
private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
    def comparator(s1: Schedulable, s2: Schedulable): Boolean
    功能: 比较两个调度实例的大小(多关键字排序)
    满足需求第一@runningTasks1 < minShare1;
    需求占比第二@runningTasks1.toDouble / math.max(minShare1, 1.0)
    权重比第三@runningTasks1.toDouble / s1.weight.toDouble
    名称最末@name
    1. 计算两个实例需要比较的参数
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble
    2. 计算比较值
    var compare = 0
    if (s1Needy && !s2Needy) {
      return true
    } else if (!s1Needy && s2Needy) {
      return false
    } else if (s1Needy && s2Needy) {
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    3. 获取结果
    val= if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      s1.name < s2.name
    }
}
```

#### SchedulingMode

```scala
object SchedulingMode extends Enumeration {
	介绍: 调度模式	
    type SchedulingMode = Value
    val FAIR, FIFO, NONE = Value
}
```

#### ShuffleMapStage

```scala
private[spark] class ShuffleMapStage(
    
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _],
    mapOutputTrackerMaster: MapOutputTrackerMaster)
extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {
    介绍: 
    属性:
    #name @_mapStageJobs: List[ActiveJob] = Nil	mapstage job列表
    #name @pendingPartitions = new HashSet[Int]	待定分区列表
    操作集:
    def toString: String = "ShuffleMapStage " + id
    功能: 信息显示
    
    def mapStageJobs: Seq[ActiveJob] = _mapStageJobs
    功能: 获取激活任务列表
    
    def removeActiveJob(job: ActiveJob): Unit
    功能: 从激活列表中移除指定job@job
    _mapStageJobs = _mapStageJobs.filter(_ != job)
    
    def numAvailableOutputs: Int
    功能: 获取可以获取的map 输出数量
    val= mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)
    
    def isAvailable: Boolean = numAvailableOutputs == numPartitions
    功能: 确定当前map stage是否处于准备状态,所有分区都有shuffle输出
    
    def findMissingPartitions(): Seq[Int]
    功能: 寻找丢失掉的分区列表
    val= mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
    
}
```

#### ShuffleMapTask

```scala
介绍:
	一个@ShuffleMapTask 将RDD元素分配到多个桶中(基于分区器),参考@org.apache.spark.scheduler.Task
获取更多信息.
	构造器参数:
        stageId	stage编号
        stageAttemptId	stage请求号
        taskBinary	任务信息(RDD,ShuffleDependency)
        partition	RDD分区数量
        locs	本地调度的最佳位置列表
        localProperties	驱动器侧本地属性
        serializedTaskMetrics	序列化的任务度量器
        jobId	jobID
        appId	应用ID
        appAttemptId	应用请求ID
        isBarrier	是否屏蔽
```

```scala
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId, isBarrier)
with Logging {
    属性:
    #name @preferredLocs: Seq[TaskLocation]	最佳任务位置列表
    val= if (locs == null) Nil else locs.toSet.toSeq
    
    操作集:
    def preferredLocations: Seq[TaskLocation] = preferredLocs
    功能: 获取最佳位置列表
    
    def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
    功能: 信息显示
    
    def runTask(context: TaskContext): MapStatus
    功能: 运行任务,返回MapStatus
    1. 使用广播变量反序列化RDD
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val rddAndDep = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L
    2. 获取mapId
    val rdd = rddAndDep._1
    val dep = rddAndDep._2.
    val mapId = if (SparkEnv.get.conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
      partitionId
    } else context.taskAttemptId()
    val= dep.shuffleWriterProcessor.write(rdd, dep, mapId, context, partition)
}
```

#### SparkListenerEvent

```scala
@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent {
    介绍: spark监听器事件
    def logEvent: Boolean = true
    功能: 是否输出日志到事件日志中   
}

@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null) extends SparkListenerEvent
介绍: spark监听器stage提交事件

@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent
介绍: stage完成事件

@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
  extends SparkListenerEvent
介绍: 任务开始事件

@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent
介绍: 获取任务结果事件

@DeveloperApi
case class SparkListenerSpeculativeTaskSubmitted(
    stageId: Int,
    stageAttemptId: Int = 0)
  extends SparkListenerEvent
介绍: 提交推测任务事件

@DeveloperApi
case class SparkListenerTaskEnd(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    taskExecutorMetrics: ExecutorMetrics,
    @Nullable taskMetrics: TaskMetrics)
  extends SparkListenerEvent
介绍: 任务结束事件

@DeveloperApi
case class SparkListenerJobStart(
    jobId: Int,
    time: Long,
    stageInfos: Seq[StageInfo],
    properties: Properties = null)
extends SparkListenerEvent {
    介绍: job开始事件
    属性:
    #name @stageIds: Seq[Int] = stageInfos.map(_.stageId)	stage列表
}

@DeveloperApi
case class SparkListenerJobEnd(
    jobId: Int,
    time: Long,
    jobResult: JobResult)
extends SparkListenerEvent
介绍: 任务结束事件

@DeveloperApi
case class SparkListenerEnvironmentUpdate(environmentDetails: Map[String, Seq[(String, String)]]) extends SparkListenerEvent
介绍: 更新环境变量事件

@DeveloperApi
case class SparkListenerBlockManagerAdded(
    time: Long,
    blockManagerId: BlockManagerId,
    maxMem: Long,
    maxOnHeapMem: Option[Long] = None,
    maxOffHeapMem: Option[Long] = None) extends SparkListenerEvent {
}
介绍: 添加块管理器事件

@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends SparkListenerEvent
介绍: 移除块管理器事件

@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent
介绍: RDD去持久化事件

@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
  extends SparkListenerEvent
介绍: 添加执行器事件

@DeveloperApi
case class SparkListenerExecutorBlacklisted(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent
介绍: 黑名单执行器监听器事件

@DeveloperApi
case class SparkListenerNodeBlacklistedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent
介绍: 节点stage黑名单监听事件

@DeveloperApi
case class SparkListenerExecutorUnblacklisted(time: Long, executorId: String)
  extends SparkListenerEvent
介绍: 解除执行器黑名单监听事件

@DeveloperApi
case class SparkListenerNodeBlacklisted(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent
介绍: 节点添加到黑名单事件

@DeveloperApi
case class SparkListenerNodeUnblacklisted(time: Long, hostId: String)
  extends SparkListenerEvent
介绍: 节点解除黑名单事件

@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent
介绍: 数据块更新事件

@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
    execId: String,
    accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
    executorUpdates: Map[(Int, Int), ExecutorMetrics] = Map.empty)
extends SparkListenerEvent
介绍: 更新执行器度量信息事件

@DeveloperApi
case class SparkListenerStageExecutorMetrics(
    execId: String,
    stageId: Int,
    stageAttemptId: Int,
    executorMetrics: ExecutorMetrics)
extends SparkListenerEvent
介绍: 执行器度量监听器

@DeveloperApi
case class SparkListenerApplicationStart(
    appName: String,
    appId: Option[String],
    time: Long,
    sparkUser: String,
    appAttemptId: Option[String],
    driverLogs: Option[Map[String, String]] = None,
    driverAttributes: Option[Map[String, String]] = None) extends SparkListenerEvent
介绍: 应用开始监听事件

@DeveloperApi
case class SparkListenerApplicationEnd(time: Long) extends SparkListenerEvent
介绍: 应用结束事件

@DeveloperApi
case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent
介绍: 事件日志启动事件

private[spark] trait SparkListenerInterface {
    介绍: 监听器接口
    操作集:
    def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit
    功能: stage完成处理
    
    def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit
    功能: stage提交处理
    
    def onTaskStart(taskStart: SparkListenerTaskStart): Unit
    功能: 任务启动处理
    
    def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit
    功能: 获取任务执行结果处理
    
    def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit
    功能: 任务结束处理
    
    def onJobStart(jobStart: SparkListenerJobStart): Unit
    功能: 任务开始处理
    
    def onJobEnd(jobEnd: SparkListenerJobEnd): Unit
    功能: 任务结束处理
    
    def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit
    功能: 更新环境变量处理
    
    def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit
    功能: 添加块管理器处理
    
    def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit
    功能: 移除块管理器处理
    
    def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit
    功能: 去除RDD的持久化功能
    
    def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit
    功能: 启动应用处理
    
    def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit
    功能: 应用结束处理
    
    def onExecutorMetricsUpdate(executorMetricsUpdate:
                                SparkListenerExecutorMetricsUpdate): Unit
    功能: 执行器度量信息更新处理
    
    def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit
    功能: stage度量信息更新处理
    
    def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit
    功能: 添加执行器
    
    def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit
    功能: 移除执行器
    
    def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit
    功能: 设置执行器黑名单
    
    def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit
    功能: 设置stage上执行器黑名单
    
    def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit
    功能: 设置节点上stage的黑名单信息
    
    def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit
    功能: 解除执行器的黑名单状态
    
    def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit
    功能: 解除节点的黑名单状态
    
    def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit
    功能: 数据块更新
    
    def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit
    功能: 解除节点的黑名单状态
    
    def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit
    功能: 处理推测任务提交
    
    def onOtherEvent(event: SparkListenerEvent): Unit
    功能: 处理其他事件
}
```

```scala
@DeveloperApi
abstract class SparkListener extends SparkListenerInterface {
    介绍: 监听接口的默认实现
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = { }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = { }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = { }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = { }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onStageExecutorMetrics(
      executorMetrics: SparkListenerStageExecutorMetrics): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }

  def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = { }

  def onNodeBlacklistedForStage(
      nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = { }

  override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }

  override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }

  override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onSpeculativeTaskSubmitted(
      speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = { }
}
```

```scala
private[spark] trait SparkListenerBus
extends ListenerBus[SparkListenerInterface, SparkListenerEvent] {
 	介绍: spark监听总线
    操作集:
    def doPostEvent(
      listener: SparkListenerInterface,
      event: SparkListenerEvent): Unit
    功能: 发送事件(使用模式匹配分别事件类型)
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        listener.onStageSubmitted(stageSubmitted)
      case stageCompleted: SparkListenerStageCompleted =>
        listener.onStageCompleted(stageCompleted)
      case jobStart: SparkListenerJobStart =>
        listener.onJobStart(jobStart)
      case jobEnd: SparkListenerJobEnd =>
        listener.onJobEnd(jobEnd)
      case taskStart: SparkListenerTaskStart =>
        listener.onTaskStart(taskStart)
      case taskGettingResult: SparkListenerTaskGettingResult =>
        listener.onTaskGettingResult(taskGettingResult)
      case taskEnd: SparkListenerTaskEnd =>
        listener.onTaskEnd(taskEnd)
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        listener.onEnvironmentUpdate(environmentUpdate)
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        listener.onBlockManagerAdded(blockManagerAdded)
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        listener.onBlockManagerRemoved(blockManagerRemoved)
      case unpersistRDD: SparkListenerUnpersistRDD =>
        listener.onUnpersistRDD(unpersistRDD)
      case applicationStart: SparkListenerApplicationStart =>
        listener.onApplicationStart(applicationStart)
      case applicationEnd: SparkListenerApplicationEnd =>
        listener.onApplicationEnd(applicationEnd)
      case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
        listener.onExecutorMetricsUpdate(metricsUpdate)
      case stageExecutorMetrics: SparkListenerStageExecutorMetrics =>
        listener.onStageExecutorMetrics(stageExecutorMetrics)
      case executorAdded: SparkListenerExecutorAdded =>
        listener.onExecutorAdded(executorAdded)
      case executorRemoved: SparkListenerExecutorRemoved =>
        listener.onExecutorRemoved(executorRemoved)
      case executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage =>
        listener.onExecutorBlacklistedForStage(executorBlacklistedForStage)
      case nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage =>
        listener.onNodeBlacklistedForStage(nodeBlacklistedForStage)
      case executorBlacklisted: SparkListenerExecutorBlacklisted =>
        listener.onExecutorBlacklisted(executorBlacklisted)
      case executorUnblacklisted: SparkListenerExecutorUnblacklisted =>
        listener.onExecutorUnblacklisted(executorUnblacklisted)
      case nodeBlacklisted: SparkListenerNodeBlacklisted =>
        listener.onNodeBlacklisted(nodeBlacklisted)
      case nodeUnblacklisted: SparkListenerNodeUnblacklisted =>
        listener.onNodeUnblacklisted(nodeUnblacklisted)
      case blockUpdated: SparkListenerBlockUpdated =>
        listener.onBlockUpdated(blockUpdated)
      case speculativeTaskSubmitted: SparkListenerSpeculativeTaskSubmitted =>
        listener.onSpeculativeTaskSubmitted(speculativeTaskSubmitted)
      case _ => listener.onOtherEvent(event)
    }
}
```

#### SplitInfo

```scala
@DeveloperApi
class SplitInfo(
    val inputFormatClazz: Class[_],
    val hostLocation: String,
    val path: String,
    val length: Long,
    val underlyingSplit: Any) {
    介绍: 分配信息
    构造器参数:
        inputFormatClazz	输入类型
        hostLocation	主机地址
        path	地址
        length	分片长度
        underlyingSplit	底层数据
    操作集:
    def toString(): String
    功能: 信息显示
    val= 
    "SplitInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz +
      ", hostLocation : " + hostLocation + ", path : " + path +
      ", length : " + length + ", underlyingSplit " + underlyingSplit
    
    def hashCode(): Int
    功能: 求取hash值
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + hostLocation.hashCode
    hashCode = hashCode * 31 + path.hashCode
    hashCode = hashCode * 31 + (length & 0x7fffffff).toInt
    val= hashCode
    
    def equals(other: Any): Boolean
    功能: 确定两个实例是否相等
}
```

```scala
object SplitInfo {
    操作集:
    def toSplitInfo(inputFormatClazz: Class[_], path: String,
                  mapredSplit: org.apache.hadoop.mapred.InputSplit): Seq[SplitInfo]
    功能: 获取分片信息列表
    val retval = new ArrayBuffer[SplitInfo]()
    val length = mapredSplit.getLength
    for (host <- mapredSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapredSplit)
    }
    val= retval
    
    def toSplitInfo(inputFormatClazz: Class[_], path: String,
                  mapreduceSplit: org.apache.hadoop.mapreduce.InputSplit): Seq[SplitInfo]
    功能: 获取分配信息列表
    val retval = new ArrayBuffer[SplitInfo]()
    val length = mapreduceSplit.getLength
    for (host <- mapreduceSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapreduceSplit)
    }
    val= retval
}
```

#### stage

```markdown
介绍:
	stage是一系列并行的任务,用于计算同一个功能,作为spark job的一部分.所有的任务都需要有相同的shuffle依赖.每个任务的DAG使用调度器运行,DAGScheduler按照拓扑关系运行这些stage.
	每个stage既可以时shuffle map 的stage(任务结果为其他stage的输入).在这种情况下,任务直接计算spark动作(count(),save())--> 通过运行RDD函数，对于shuffle map的stage，可以定位每个输出所在的节点信息。
	每个stage都有第一个job编号@firstJobId，表示job首次提交这个stage。使用FIFO调度时，允许之前的job优先计算，或者是优先失败快速恢复。
	最后，在默认恢复模式下，单个stage可能被多次重复执行。在这种情况下，stage对象会定位多个@StageInfo信息，将其送给监听器，或者WEB UI。最新的可以使用@latestInfo
	构造器参数:
	id	stage编号
	rdd	stage所运行的RDD
	numTasks	stage的任务数量
	parents	父stage列表
	firstJobId	stage的首个jobID
	callSite	用户调用
```

```scala
private[scheduler] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite)
extends Logging {
    属性:
    #name @numPartitions = rdd.partitions.length	分区数量
    #name @jobIds = new HashSet[Int]	stage所属的jobID列表
    #name @nextAttemptId: Int = 0	这个stage下次请求编号
    #name @name: String = callSite.shortForm	stage名称
    #name @details: String = callSite.longForm	stage细节描述
    #name @_latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)
    最新stage信息
    #name @failedAttemptIds = new HashSet[Int]	失败的stage请求id列表
    操作集:
    def clearFailures() : Unit = failedAttemptIds.clear()
    功能: 清除失败请求
    
    def latestInfo: StageInfo = _latestInfo
    功能: 获取最新的stage信息
    
    final def hashCode(): Int = id
    功能: 计算hash值
    
    final def equals(other: Any): Boolean
    功能: 确定两个stage是否相等
    val= other match {
        case stage: Stage => stage != null && stage.id == id
        case _ => false
      }
    
    def findMissingPartitions(): Seq[Int]
    功能: 获取丢失的分区列表
    
    def isIndeterminate: Boolean 
    功能: 确定信息是否不确定
    val= rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
    
    def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit
    功能: 创建新的stage请求(为当前stage)
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), 
        metrics, taskLocalityPreferences)
    	nextAttemptId += 1
}
```

#### StageInfo

```scala
@DeveloperApi
class StageInfo(
    val stageId: Int,	// stage 编号
    private val attemptId: Int, // stage请求编号
    val name: String, // stage名称
    val numTasks: Int, // 任务数量
    val rddInfos: Seq[RDDInfo], // rdd信息
    val parentIds: Seq[Int], // 父stage列表
    val details: String, // stage描述
    val taskMetrics: TaskMetrics = null, // 任务度量器
    // 任务位置列表
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
    private[spark] val shuffleDepId: Option[Int] = None) // shuffle 依赖编号
{
    介绍: stage信息
    属性:
    #name @submissionTime: Option[Long] = None	提交时间(DAGScheduler-->TaskScheduler时间)
    #name @completionTime: Option[Long] = None	完成时间(所有任务完成,或者stage取消)
    #name @failureReason: Option[String] = None	失败原因
    #name @accumulables = HashMap[Long, AccumulableInfo]()	度量信息表
    操作集:
    def attemptNumber(): Int = attemptId
    功能: 获取请求编号
    
    def stageFailed(reason: String): Unit
    功能: 处理stage失败
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
    
    def getStatusString: String
    功能: 获取状态信息
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
    
}
```

```scala
private[spark] object StageInfo {
    属性:
    #name @INVALID_STAGE_ID = -1	非法stageID
    #name @INVALID_ATTEMPT_ID = -1	非法请求ID
    操作集:
    def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
    ): StageInfo
    功能: 由一个stage构造一个@StageInfo,每个stage与一个或者多个RDD相关,使用shuffle依赖标记stage界限.因此,所有本stageRDD的祖先RDD需要与这个stage相联系.
    1. 获取祖先RDD信息
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    val shuffleDepId = stage match {
      case sms: ShuffleMapStage => Option(sms.shuffleDep).map(_.shuffleId)
      case _ => None
    }
    val= new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences,
      shuffleDepId)
}
```

#### StatsReportListener

```scala
@DeveloperApi
class StatsReportListener extends SparkListener with Logging {
    属性:
    #name @taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()	任务信息度量器
    操作集:
    def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit 
    功能: 任务结束处理
    1. 获取任务信息
    val info = taskEnd.taskInfo
    2. 更新度量信息
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      taskInfoMetrics += ((info, metrics))
    }
    
    def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit
    功能: 处理stage完成
    1. 处理shuffle写出
    showBytesDistribution("shuffle bytes written:",
      (_, metric) => metric.shuffleWriteMetrics.bytesWritten, taskInfoMetrics)
    2. 处理获取等待时间,IO信息
    showMillisDistribution("fetch wait time:",
      (_, metric) => metric.shuffleReadMetrics.fetchWaitTime, taskInfoMetrics)
    showBytesDistribution("remote bytes read:",
      (_, metric) => metric.shuffleReadMetrics.remoteBytesRead, taskInfoMetrics)
    showBytesDistribution("task result size:",
      (_, metric) => metric.resultSize, taskInfoMetrics)
    3. 处理运行时宕机
    val runtimePcts = taskInfoMetrics.map { case (info, metrics) =>
      RuntimePercentage(info.duration, metrics)
    }
    showDistribution("executor (non-fetch) time pct: ",
      Distribution(runtimePcts.map(_.executorPct * 100)), "%2.0f %%")
    showDistribution("fetch wait time pct: ",
      Distribution(runtimePcts.flatMap(_.fetchPct.map(_ * 100))), "%2.0f %%")
    showDistribution("other time pct: ", Distribution(runtimePcts.map(_.other * 100)), "%2.0f %%")
    taskInfoMetrics.clear()
    
    def getStatusDetail(info: StageInfo): String
    功能: 获取状态描述信息
    val failureReason = info.failureReason.map("(" + _ + ")").getOrElse("")
    val timeTaken = info.submissionTime.map(
      x => info.completionTime.getOrElse(System.currentTimeMillis()) - x
    ).getOrElse("-")
	val= s"Stage(${info.stageId}, ${info.attemptNumber}); Name: '${info.name}'; " +
      s"Status: ${info.getStatusString}$failureReason; numTasks: ${info.numTasks}; " +
      s"Took: $timeTaken msec"    
}
```

```scala
private case class RuntimePercentage(executorPct: Double, fetchPct: Option[Double], other: Double)
结束: 运行时状态信息(执行百分比)

private object RuntimePercentage {
    操作集:
    def apply(totalTime: Long, metrics: TaskMetrics): RuntimePercentage 
    功能: 获取实例
    val denom = totalTime.toDouble
    val fetchTime = Some(metrics.shuffleReadMetrics.fetchWaitTime)
    val fetch = fetchTime.map(_ / denom)
    val exec = (metrics.executorRunTime - fetchTime.getOrElse(0L)) / denom
    val other = 1.0 - (exec + fetch.getOrElse(0d))
    val= RuntimePercentage(exec, fetch, other)
}
```

```scala
private[spark] object StatsReportListener extends Logging {
    介绍: 状态汇报监听器
    属性:
    #name @probabilities = percentiles.map(_ / 100.0) 百分比列表
    #name @percentiles = Array[Int](0, 5, 10, 25, 50, 75, 90, 95, 100)	百分数(分子)
    #name @seconds = 1000L			秒
    #name @minutes = seconds * 60 	分
    #name @hours = minutes * 60		时
    操作集:
    def extractDoubleDistribution(
        taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
        getMetric: (TaskInfo, TaskMetrics) => Double): Option[Distribution]
    功能: 抓取分布情况
    val= Distribution(taskInfoMetrics.map { case (info, metric) => 
        getMetric(info, metric) })
    
    def extractLongDistribution(
        taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
        getMetric: (TaskInfo, TaskMetrics) => Long): Option[Distribution]
    功能: 同上
    val= xtractDoubleDistribution(
      taskInfoMetrics,
      (info, metric) => { getMetric(info, metric).toDouble })
    
    def showDistribution(
      heading: String,
      format: String,
      getMetric: (TaskInfo, TaskMetrics) => Double,
      taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit
    功能: 显示分布信息
    showDistribution(heading, extractDoubleDistribution(
        taskInfoMetrics, getMetric), format)
    
    def showBytesDistribution(
      heading: String,
      getMetric: (TaskInfo, TaskMetrics) => Long,
      taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit
    功能: 显示分布
    showBytesDistribution(heading, extractLongDistribution(taskInfoMetrics, getMetric))
    
    def showBytesDistribution(heading: String, dOpt: Option[Distribution]): Unit
    功能: 显示分布
    dOpt.foreach { dist => showBytesDistribution(heading, dist) }
    
    def showBytesDistribution(heading: String, dist: Distribution): Unit
    功能: 显示分布
    showDistribution(heading, dist, (d => Utils.bytesToString(d.toLong)):
                     Double => String)
    
    def showMillisDistribution(heading: String, dOpt: Option[Distribution]): Unit
    功能: 显示分布
    showDistribution(heading, dOpt,
      (d => StatsReportListener.millisToString(d.toLong)): Double => String)
    
    def showMillisDistribution(
      heading: String,
      getMetric: (TaskInfo, TaskMetrics) => Long,
      taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit
    功能: 显示分布
    showMillisDistribution(heading, extractLongDistribution(taskInfoMetrics, getMetric))
    
    def millisToString(ms: Long): String
    功能: 毫秒字符串显示
    val (size, units) =
      if (ms > hours) {
        (ms.toDouble / hours, "hours")
      } else if (ms > minutes) {
        (ms.toDouble / minutes, "min")
      } else if (ms > seconds) {
        (ms.toDouble / seconds, "s")
      } else {
        (ms.toDouble, "ms")
      }
    val= "%.1f %s".format(size, units)
}
```

#### Task

```markdown
介绍:
	执行单元,spark中含有两种类型
	1. shuffle Map 任务@ShuffleMapTask
	2. 结果任务@TaskResult
	spark job包含一个或者多个stage,非常大的stage会包含多个@ResultTask,之前的stage包含有@ShuffleMapTask.一个结果任务@ResultTask 执行任务,并将结果发送到驱动器中.
	@ShuffleMapTask执行任务,并将任务输出分发到多个桶中(基于任务分区器)
	构造器参数:
	stageId	stage编号
	stageAttemptId	stage请求编号
	partitionId	分区编号
	localProperties	本地属性
	serializedTaskMetrics	序列化任务度量器
	jobId	job编号
	appId	app编号
	appAttemptId	app请求编号
	isBarrier	是否为屏蔽任务
```

```scala
private[spark] abstract class  Task[T](
    val stageId: Int,
    val stageAttemptId: Int,
    val partitionId: Int,
    @transient var localProperties: Properties = new Properties,
    serializedTaskMetrics: Array[Byte] =    SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    val jobId: Option[Int] = None,
    val appId: Option[String] = None,
    val appAttemptId: Option[String] = None,
    val isBarrier: Boolean = false) extends Serializable {
    属性:
    #name @metrics	#type @TaskMetrics lazy @transient	任务度量器
    #name @taskMemoryManager: TaskMemoryManager = _	任务内存管理器
    #name @epoch: Long = -1	端点
    #name @context: TaskContext = _	transient 任务上下文
    #name @taskThread: Thread = _	任务线程
    #name @_reasonIfKilled: String = null	任务中断原因
    #name @_executorDeserializeTimeNs: Long = 0	任务反序列化时间
    #name @_executorDeserializeCpuTime: Long = 0	任务反序列化CPU时间
    操作集:
    def executorDeserializeTimeNs: Long = _executorDeserializeTimeNs
    def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime
    功能: 获取执行器反序列化时间/CPU时间
    
    def reasonIfKilled: Option[String] = Option(_reasonIfKilled)
    功能: 获取任务中断原因
    
    def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] 
    功能: 收集任务中最新的累加器值
    if (context != null) {
      context.taskMetrics.nonZeroInternalAccums() ++
        context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
    } else {
      Seq.empty
    }
    
    def kill(interruptThread: Boolean, reason: String): Unit
    功能: 中断任务,并指定中断原因.需要上层spark程序或者用户程序妥善的处理中断标记位@interruptThread
    require(reason != null)
    _reasonIfKilled = reason
    if (context != null) {
      context.markInterrupted(reason)
    }
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
    
    def runTask(context: TaskContext): T
    功能: 运行任务,获取任务执行结果
    
    def preferredLocations: Seq[TaskLocation] = Nil
    功能: 获取任务最佳执行位置
    
    def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit
    功能: 设置任务内存管理器
    this.taskMemoryManager = taskMemoryManager
    
    final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem,
      resources: Map[String, ResourceInformation]): T 
    功能: 使用执行器,调用这个任务
    输入参数:
        taskAttemptId	任务请求编号
        attemptNumber	请求数量
        metricsSystem	度量系统
        resources	资源列表
    1. 注册任务
    SparkEnv.get.blockManager.registerTask(taskAttemptId)
    2. 获取任务上下文@context
    val taskContext = new TaskContextImpl(
      stageId,
      stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics,
      resources)
    context = if (isBarrier) {
      new BarrierTaskContext(taskContext)
    } else {
      taskContext
    }
    3. 获取调用上下文
    InputFileBlockHolder.initialize()
    TaskContext.setTaskContext(context)
    taskThread = Thread.currentThread()
	// 处理任务中断
    if (_reasonIfKilled != null) {
      kill(interruptThread = false, _reasonIfKilled)
    }
    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()
    4. 运行任务
	try {
      runTask(context)
    } catch {
      case e: Throwable =>
        try {
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        context.markTaskCompleted(Some(e))
        throw e
    } finally {
      try {
        context.markTaskCompleted(None)
      } finally {
        try {
          Utils.tryLogNonFatalError {      SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(
              MemoryMode.OFF_HEAP)
            val memoryManager = SparkEnv.get.memoryManager
            memoryManager.synchronized { memoryManager.notifyAll() }
          }
        } finally {
          TaskContext.unset()
          InputFileBlockHolder.unset()
        }
      }
    }
}
```

####  TaskDescription

```markdown
介绍:
	任务描述，传递到执行器上，用于执行，通常使用@TaskSetManager.resourceOffe 创建。
	任务描述与任务相关，序列化时需要注意下述两点。
	1. 任务描述由执行器接受，执行器需要首先受到jar包和文件列表，并将其添加到类路径中，且设置属性，这些工作需要在反序列Task前进行处理。这就是为什么@Properties 包含在@TaskDescription 内，尽管它们也在反序列化任务中。
	2. 由于任务描述是发送到执行器，用于执行每个任务执行，因此有效的序列化很重要。所以序列化时需要自己提供序列化方法@TaskDescription.encode和 @TaskDescription.decode。 使得序列化结果更小。
```

```scala
private[spark] class TaskDescription(
    val taskId: Long,
    val attemptNumber: Int,
    val executorId: String,
    val name: String,
    val index: Int,   
    val partitionId: Int,
    val addedFiles: Map[String, Long],
    val addedJars: Map[String, Long],
    val properties: Properties,
    val resources: immutable.Map[String, ResourceInformation],
    val serializedTask: ByteBuffer) {
    构造器参数:
        taskId	任务编号
        attemptNumber	请求数量
        executorId	执行器ID
        name	任务名称
        index	索引
        partitionId	分区ID
        addedFiles	文件列表
        addedJars	jar包列表
        properties	属性
        resources	资源列表
        serializedTask	序列化任务
    操作集:
    def toString: String = "TaskDescription(TID=%d, index=%d)".format(taskId, index)
    功能: 信息显示
}
```

```scala
private[spark] object TaskDescription {
    操作集:
    def serializeStringLongMap(map: Map[String, Long], dataOut: DataOutputStream): Unit
    功能: 序列化map
    dataOut.writeInt(map.size)
    map.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      dataOut.writeLong(value)
    }
    
    def serializeResources(map: immutable.Map[String, ResourceInformation],
      dataOut: DataOutputStream): Unit
    功能: 序列化资源列表
    dataOut.writeInt(map.size)
    map.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      dataOut.writeUTF(value.name)
      dataOut.writeInt(value.addresses.size)
      value.addresses.foreach(dataOut.writeUTF(_))
    }
    
    def deserializeStringLongMap(dataIn: DataInputStream): HashMap[String, Long]
    功能: 反序列化Map
    val map = new HashMap[String, Long]()
    val mapSize = dataIn.readInt()
    var i = 0
    while (i < mapSize) {
      map(dataIn.readUTF()) = dataIn.readLong()
      i += 1
    }
    val= map
    
    def deserializeResources(dataIn: DataInputStream):
      immutable.Map[String, ResourceInformation]
    功能: 反序列化资源列表
    val map = new HashMap[String, ResourceInformation]()
    val mapSize = dataIn.readInt()
    var i = 0
    while (i < mapSize) {
      val resType = dataIn.readUTF()
      val name = dataIn.readUTF()
      val numIdentifier = dataIn.readInt()
      val identifiers = new ArrayBuffer[String](numIdentifier)
      var j = 0
      while (j < numIdentifier) {
        identifiers += dataIn.readUTF()
        j += 1
      }
      map(resType) = new ResourceInformation(name, identifiers.toArray)
      i += 1
    }
    val= map.toMap
    
    def decode(byteBuffer: ByteBuffer): TaskDescription
    功能: 解码(数据缓冲区数据 --> 任务描述)
    val dataIn = new DataInputStream(new ByteBufferInputStream(byteBuffer))
    val taskId = dataIn.readLong()
    val attemptNumber = dataIn.readInt()
    val executorId = dataIn.readUTF()
    val name = dataIn.readUTF()
    val index = dataIn.readInt()
    val partitionId = dataIn.readInt()
    val taskFiles = deserializeStringLongMap(dataIn)
    val taskJars = deserializeStringLongMap(dataIn)
    val properties = new Properties()
    val numProperties = dataIn.readInt()
    for (i <- 0 until numProperties) {
      val key = dataIn.readUTF()
      val valueLength = dataIn.readInt()
      val valueBytes = new Array[Byte](valueLength)
      dataIn.readFully(valueBytes)
      properties.setProperty(key, new String(valueBytes, StandardCharsets.UTF_8))
    }
    val resources = deserializeResources(dataIn)
    val serializedTask = byteBuffer.slice()
    new TaskDescription(taskId, attemptNumber, executorId, name, index, partitionId, taskFiles,taskJars, properties, resources, serializedTask)
    
    def encode(taskDescription: TaskDescription): ByteBuffer
    功能: 对任务描述进行加密
    val bytesOut = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(bytesOut)
    // 写出任务描述信息
    dataOut.writeLong(taskDescription.taskId)
    dataOut.writeInt(taskDescription.attemptNumber)
    dataOut.writeUTF(taskDescription.executorId)
    dataOut.writeUTF(taskDescription.name)
    dataOut.writeInt(taskDescription.index)
    dataOut.writeInt(taskDescription.partitionId)
    serializeStringLongMap(taskDescription.addedFiles, dataOut)
    serializeStringLongMap(taskDescription.addedJars, dataOut)
    dataOut.writeInt(taskDescription.properties.size())
    taskDescription.properties.asScala.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      val bytes = value.getBytes(StandardCharsets.UTF_8)
      dataOut.writeInt(bytes.length)
      dataOut.write(bytes)
    }
    serializeResources(taskDescription.resources, dataOut)
    Utils.writeByteBuffer(taskDescription.serializedTask, bytesOut)
    dataOut.close()
    bytesOut.close()
    bytesOut.toByteBuffer
}
```

#### TaskInfo

```scala
@DeveloperApi
class TaskInfo(
    val taskId: Long, // 任务ID
    val index: Int, // 任务集中任务的索引
    val attemptNumber: Int, // 请求数量
    val launchTime: Long, // 运行时间
    val executorId: String, // 执行器Id
    val host: String, // 主机名称
    val taskLocality: TaskLocality.TaskLocality, // 任务位置
    val speculative: Boolean) { // 是否为推测任务
    属性:
    #name @gettingResultTime: Long = 0	获取结果的时间
    #name @_accumulables: Seq[AccumulableInfo] = Nil	累加属性列表
    #name @finishTime: Long = 0	结束时间
    #name @failed = false	任务是否失效
    #name @killed = false	任务是否kill
    操作集:
    def accumulables: Seq[AccumulableInfo] = _accumulables
    功能: 获取累加属性列表
    
    def setAccumulables(newAccumulables: Seq[AccumulableInfo]): Unit
    功能: 设置累加信息
    _accumulables = newAccumulables
    
    def markGettingResult(time: Long): Unit
    功能: 标记获取结果
    gettingResultTime = time
    
    def markFinished(state: TaskState, time: Long): Unit
    功能: 标记任务完成
    assert(time > 0)
    finishTime = time
    if (state == TaskState.FAILED) {
      failed = true
    } else if (state == TaskState.KILLED) {
      killed = true
    }
    
    def gettingResult: Boolean = gettingResultTime != 0
    功能: 确认是否获取过结果
    
    def finished: Boolean = finishTime != 0
    功能: 确认是否完成
    
    def successful: Boolean = finished && !failed && !killed
    功能: 确认是否成功执行
    
    def running: Boolean = !finished
    功能: 确认运行状态
    
    def status: String
    功能: 获取状态信息
    val= if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (killed) {
      "KILLED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
    
    def id: String = s"$index.$attemptNumber"
    功能: 获取任务ID
    
    def duration: Long
    功能: 获取持续时间
    val= if (!finished) {
      throw new UnsupportedOperationException("duration() called on unfinished task")
    } else {
      finishTime - launchTime
    }
    
    def timeRunning(currentTime: Long): Long = currentTime - launchTime
    功能: 获取运行时间
}
```

#### TaskLocality

```scala
@DeveloperApi
object TaskLocality extends Enumeration {
    #name @PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY = Value
    本地进程需要在管理器@TaskSetManager中使用Only标记
    type TaskLocality = Value	任务位置
    操作集:
    def isAllowed(constraint: TaskLocality, condition: TaskLocality): Boolean 
    功能: 确认@constraint 是否允许称为一个位置
    val= condition <= constraint
}
```

#### TaskLocation

```scala
private[spark] sealed trait TaskLocation {
    介绍: 任务运行的位置,既可以时主机,也可以是主机+执行器编号组成的键值对.在后者情况下,会运行指定执行器的任务,但是次优位置会是同一个主机上的执行器.
    def host: String
}

private [spark]
case class ExecutorCacheTaskLocation(override val host: String, executorId: String)
  extends TaskLocation {
  override def toString: String = s"${TaskLocation.executorLocationTag}${host}_$executorId"
}
介绍: 执行器缓存任务位置,包含主机和执行器ID的位置信息

private [spark] case class HostTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = host
}
介绍: 执行器位置

private [spark] case class HDFSCacheTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = TaskLocation.inMemoryLocationTag + host
}
介绍: HDFS主机位置
```

```scala
private[spark] object TaskLocation {
    属性:
    #name @inMemoryLocationTag = "hdfs_cache_"	内存位置标签
    	参考RFC 952 和 RFC 1123获取更多信息
    #name @executorLocationTag = "executor_"	执行器位置前缀
    操作集:
    def apply(host: String, executorId: String): TaskLocation 
    功能: 获取主机执行器式任务位置
    val= new ExecutorCacheTaskLocation(host, executorId)
    
    def apply(str: String): TaskLocation 
    功能: 根据输入串确定最佳运行位置
    val hstr = str.stripPrefix(inMemoryLocationTag)
    if (hstr.equals(str)) {
      if (str.startsWith(executorLocationTag)) {
        val hostAndExecutorId = str.stripPrefix(executorLocationTag)
        val splits = hostAndExecutorId.split("_", 2)
        require(splits.length == 2, "Illegal executor location format: " + str)
        val Array(host, executorId) = splits
        new ExecutorCacheTaskLocation(host, executorId)
      } else {
        new HostTaskLocation(str)
      }
    } else {
      new HDFSCacheTaskLocation(hstr)
    }
}
```

#### TaskResult

```scala
private[spark] sealed trait TaskResult[T]
介绍: 任务结果

private[spark] case class IndirectTaskResult[T](blockId: BlockId, size: Int)
  extends TaskResult[T] with Serializable
介绍: 间接任务结果(存储到worker的块管理器中)

private[spark] class DirectTaskResult[T](
    var valueBytes: ByteBuffer,
    var accumUpdates: Seq[AccumulatorV2[_, _]],
    var metricPeaks: Array[Long])
extends TaskResult[T] with Externalizable {
    介绍: 直接任务结果
    构造器参数:
        valueBytes	数据值
        accumUpdates	更新度量器
        metricPeaks	度量参数列表
    属性:
    #name @valueObjectDeserialized = false	值对象是否反序列化
    #name @valueObject: T = _	值对象
    操作集:
    def writeExternal(out: ObjectOutput): Unit
    功能: 外部写出
    Utils.tryOrIOException {
        out.writeInt(valueBytes.remaining)
        Utils.writeByteBuffer(valueBytes, out)
        out.writeInt(accumUpdates.size)
        accumUpdates.foreach(out.writeObject)
        out.writeInt(metricPeaks.length)
        metricPeaks.foreach(out.writeLong)
      }
    
    def readExternal(in: ObjectInput): Unit
    功能: 读取外部数据
    Utils.tryOrIOException {
        val blen = in.readInt()
        val byteVal = new Array[Byte](blen)
        in.readFully(byteVal)
        valueBytes = ByteBuffer.wrap(byteVal)

        val numUpdates = in.readInt
        if (numUpdates == 0) {
          accumUpdates = Seq.empty
        } else {
          val _accumUpdates = new ArrayBuffer[AccumulatorV2[_, _]]
          for (i <- 0 until numUpdates) {
            _accumUpdates += in.readObject.asInstanceOf[AccumulatorV2[_, _]]
          }
          accumUpdates = _accumUpdates
        }

        val numMetrics = in.readInt
        if (numMetrics == 0) {
          metricPeaks = Array.empty
        } else {
          metricPeaks = new Array[Long](numMetrics)
          (0 until numMetrics).foreach { i =>
            metricPeaks(i) = in.readLong
          }
        }
        valueObjectDeserialized = false
    }
    
    def value(resultSer: SerializerInstance = null): T 
    功能: 获取反序列化@valueBytes 值
    首次执行,由@valueBytes 反序列化得到 @valueObject,之后就直接获取@valueObject
    val= if (valueObjectDeserialized) {
      valueObject
    } else {
      val ser =if(resultSer == null) SparkEnv.get.serializer.newInstance() else resultSer
      valueObject = ser.deserialize(valueBytes)
      valueObjectDeserialized = true
      valueObject
    }
}
```

#### TaskResultGetter

```scala
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
extends Logging {
    介绍: 任务结果获取器,运行一个反序列化的线程池,远端获取任务执行结果
    属性:
    #name @THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4) 线程数量
    #name @getTaskResultExecutor: ExecutorService 获取任务执行器（测试使用）
    val= ThreadUtils.newDaemonFixedThreadPool(THREADS, "task-result-getter")
    #name @serializer #type @ThreadLocal[SerializerInstance]	序列化器(测试使用)
    val= new ThreadLocal[SerializerInstance] {
        override def initialValue(): SerializerInstance = {
          sparkEnv.closureSerializer.newInstance()
        }
      }
    #name @taskResultSerializer #type @ThreadLocal[SerializerInstance] 任务结果序列化器(测试)
    val= new ThreadLocal[SerializerInstance] {
        override def initialValue(): SerializerInstance = {
          sparkEnv.serializer.newInstance()
        }	
      }
    操作集:
    def stop(): Unit = getTaskResultExecutor.shutdownNow()
    功能: 停止结果获取器
    
    def enqueuePartitionCompletionNotification(stageId: Int, partitionId: Int): Unit 
    功能: 该方法异步调用@TaskSchedulerImpl.handlePartitionCompleted,不会需要DAG调度器直接使用@TaskSchedulerImpl.handlePartitionCompleted,因为这个方法是同步的,可能使得调度器丧失生成能力.
    getTaskResultExecutor.execute(() => Utils.logUncaughtExceptions {
      scheduler.handlePartitionCompleted(stageId, partitionId)
    })
    
    def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,serializedData: ByteBuffer): Unit
    功能: 入队失败任务
    serializedData: ByteBuffer): Unit = {
    var reason : TaskFailedReason = UnknownReason
    try {
          getTaskResultExecutor.execute(() => Utils.logUncaughtExceptions {
            val loader = Utils.getContextOrSparkClassLoader
            try {
              if (serializedData != null && serializedData.limit() > 0) {
                reason = serializer.get().deserialize[TaskFailedReason](
                  serializedData, loader)
              }
            } catch {
              case _: ClassNotFoundException =>
                logError(
                  "Could not deserialize TaskEndReason: ClassNotFound 
                  with classloader " + loader)
              case _: Exception => // No-op
            } finally {
              scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
            }
          })
        } catch {
          case e: RejectedExecutionException if sparkEnv.isStopped =>
        }
    }
    
    def enqueueSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      serializedData: ByteBuffer): Unit 
    功能: 入队成功执行任务信息
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
            1. 获取任务执行结果
          val (result, size) = 
            serializer.get().deserialize[TaskResult[_]]	(serializedData) match {
            case directResult: DirectTaskResult[_] =>
              if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                scheduler.handleFailedTask(
                    taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }
              directResult.value(taskResultSerializer.get())
              (directResult, serializedData.limit())
            case IndirectTaskResult(blockId, size) =>
              if (!taskSetManager.canFetchMoreResults(size)) {
                sparkEnv.blockManager.master.removeBlock(blockId)
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled("Tasks result size has exceeded maxResultSize"))
                return
              }
              logDebug("Fetching indirect task result for TID %s".format(tid))
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (serializedTaskResult.isEmpty) {
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, TaskResultLost)
                return
              }
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get.toByteBuffer)
              // force deserialization of referenced value
              deserializedResult.value(taskResultSerializer.get())
              sparkEnv.blockManager.master.removeBlock(blockId)
              (deserializedResult, size)
          }
            2. 处理累加器更新
          result.accumUpdates = result.accumUpdates.map { a =>
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              val acc = a.asInstanceOf[LongAccumulator]
              assert(acc.sum == 0L, "task result size should not have 
              been set on the executors")
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }
            3. 处理为任务成功执行
          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            taskSetManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })  
}
```

#### TaskScheduler

```markdown
介绍: 
	底层任务调度器接口,使用@TaskSchedulerImpl 实现,这个接口允许阻塞多个不同的任务调度器.每个任务调度器调度单个@SparkContext 的任务.这些调度器获取一个任务集合,这个任务集合对于每个stage提交@DAGScheduler.调度器需要发送任务到达集群,并运行它们,如果失败需要进行重试(容错性).通过DAG任务调度器@DAGScheduler 返回事件.
```

```scala
private[spark] trait TaskScheduler {
    #name @appId = "spark-application-" + System.currentTimeMillis	应用编号
    操作集:
    def rootPool: Pool
    功能: 获取根调度池
    
    def schedulingMode: SchedulingMode
    功能: 获取调度模式
    
    def start(): Unit
    功能: 开启调度器
    
    def postStartHook(): Unit = { }
    功能: 系统成功初始化之后调用,yarn使用这个启动基于最近位置的资源分配.等待子注册器
    
    def stop(): Unit
    功能: 停止集群的任务调度器
    
    def submitTasks(taskSet: TaskSet): Unit
    功能: 提交运行任务集
	
    def cancelTasks(stageId: Int, interruptThread: Boolean): Unit
    功能: 放弃指定stage的任务
    
    def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean
    功能: 中断指定任务
    
    def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit
    功能: 清除所有任务请求
    
    def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit
    功能: 提示分区执行完成
    
    def setDAGScheduler(dagScheduler: DAGScheduler): Unit
    功能: 设置DAG调度器
    
    def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: Map[(Int, Int), ExecutorMetrics]): Boolean
    功能: 执行器接受心跳
    
    def applicationId(): String = appId
    功能: 获取应用编号
    
    def executorLost(executorId: String, reason: ExecutorLossReason): Unit
    功能: 执行器丢失处理
    
    def workerRemoved(workerId: String, host: String, message: String): Unit
    功能: 移除worker
    
    def applicationAttemptId(): Option[String]
    功能: 获取应用请求编号
}
```

#### TaskSchedulerImpl

```markdown
介绍:
	通过对调度器后端的操作,调度集群的多个类型任务,可以使用本地调度客户端创建.处理通用逻辑(例如,job的调度顺序,推测任务的执行等等).客户端需要调用初始化@initialize() 和开始@start() 通过提交任务方法提交任务.
 	威胁: 调度器后端@SchedulerBackend 和任务提交客户端可以多线程调用这个类.所以需要公用API锁去维护这个状态,此外,调度器后端在它发送事件的时候同步自身,然后获取这里的锁.所以需要确定获取自身锁的时候,没有获取调度器后端的锁.这个类可能被如下几种线程调用
 	1. DAGScheduler	事件环
 	2. RPC处理线程,响应执行器的状态更新
 	3. 周期性复活粒度集群后端,用于延时调度
 	4. 任务结果获取线程
```

```scala
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
extends TaskScheduler with Logging {
    属性:
    #name @conf = sc.conf	spark配置
    #name @blacklistTrackerOpt = maybeCreateBlacklistTracker(sc)	黑名单定位属性
    #name @SPECULATION_INTERVAL_MS = conf.get(SPECULATION_INTERVAL)	推测任务检查周期
    #name @MIN_TIME_TO_SPECULATION = 100	最小推测时间
    #name @speculationScheduler	推测调度器
    val= ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")
    #name @STARVATION_TIMEOUT_MS	互斥等待上限(防止死锁)
    conf.getTimeAsMs("spark.starvation.timeout", "15s")
    #name @CPUS_PER_TASK = conf.get(config.CPUS_PER_TASK)	每个任务的CPU数量
    #name @resourcesReqsPerTask	每个任务的资源需求列表
    #name @taskSetsByStageIdAndAttempt	任务集-->stage编号映射表
    val= new HashMap[Int, HashMap[Int, TaskSetManager]]
    #name @taskIdToTaskSetManager	任务ID与任务集管理器映射关系
    val= new ConcurrentHashMap[Long, TaskSetManager]
    #name @taskIdToExecutorId = new HashMap[Long, String]	任务ID与执行器ID映射表
    #name @hasReceivedTask = false	是否接受任务标志
    #name @hasLaunchedTask = false	是否启动任务标志
    #name @nextTaskId = new AtomicLong(0)	下一个任务编号
    #name @executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]
    	执行器--> 执行器运行的任务列表映射关系
    #name @hostToExecutors = new HashMap[String, HashSet[String]]	
    	主机 --> 执行器列表映射关系
    #name @hostsByRack = new HashMap[String, HashSet[String]]
    	机架 --> 主机映射关系
    #name @executorIdToHost = new HashMap[String, String]
    	执行器ID --> 主机映射关系
    #name @abortTimer = new Timer(true)	计数器(抛弃的时候使用)
    #name @clock = new SystemClock	系统时钟
    #name @unschedulableTaskSetToExpiryTime = new HashMap[TaskSetManager, Lon]
    	未调度的任务集 --> 到期时间映射表
    #name @dagScheduler: DAGScheduler = null	DAG调度器
    #name @backend: SchedulerBackend = null	调度器后端
    #name @mapOutputTracker	MAP输出定位器
    val= SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    #name @schedulableBuilder: SchedulableBuilder = null	调度构建器
    #name @schedulingModeConf = conf.get(SCHEDULER_MODE)	调度模式配置(默认FIFO)
    #name @schedulingMode #type @SchedulingMode	调度模式
    val= try {
      SchedulingMode.withName(schedulingModeConf.toUpperCase(Locale.ROOT))
    } catch {
      case e: java.util.NoSuchElementException =>
        throw new SparkException(s"Unrecognized $SCHEDULER_MODE_PROPERTY:
        $schedulingModeConf")
    }
    #name @rootPool: Pool = new Pool("", schedulingMode, 0, 0)	根调度池
    #name @taskResultGetter = new TaskResultGetter(sc.env, this)	任务结果获取器
    #name @barrierSyncTimeout = conf.get(config.BARRIER_SYNC_TIMEOUT)	屏蔽同步超时时间
    操作集:
    def runningTasksByExecutors: Map[String, Int] 
    功能: 获取执行器-->运行任务映射表
    val= synchronized {
        executorIdToRunningTaskIds.toMap.mapValues(_.size)
      }
    
    def maybeInitBarrierCoordinator(): Unit
    功能: 进行可能的屏蔽协调器RPC端点的初始化工作
    if (barrierCoordinator == null) {
      // 创建屏蔽协调者
      barrierCoordinator = new BarrierCoordinator(barrierSyncTimeout, sc.listenerBus,
        sc.env.rpcEnv)
      // 向spark环境中注册当前屏蔽协调者
      sc.env.rpcEnv.setupEndpoint("barrierSync", barrierCoordinator)
      logInfo("Registered BarrierCoordinator endpoint")
    }
    
    def setDAGScheduler(dagScheduler: DAGScheduler): Unit 
    功能: 设置DAG调度器
    this.dagScheduler = dagScheduler
    
    def newTaskId(): Long = nextTaskId.getAndIncrement()
    功能: 获取下一个任务ID
    
    def initialize(backend: SchedulerBackend): Unit
    功能: 初始化任务调度器@TaskScheduler
    1. 设置调度器后端
    this.backend = backend
    2. 获取调度构建器(FIFO/FAIR)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
          s"$schedulingMode")
      }
    }
    3. 创建调度池
    schedulableBuilder.buildPools()
    
    def start(): Unit
    功能: 启动任务调度器
    1. 启动调度器后端
    backend.start()
    2. 处理远端推测执行的情况(延时调度)
    if (!isLocal && conf.get(SPECULATION_ENABLED)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(
        () => Utils.tryOrStopSparkContext(sc) { checkSpeculatableTasks() },
        SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
    
    def postStartHook(): Unit
    功能: 系统初始化完毕,等待从节点注册
    waitBackendReady()
    
    def submitTasks(taskSet: TaskSet): Unit
    功能: 提交任务
    1. 获取任务集
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    2. 提交任务
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      // 获取当前stage的任务集
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(
            stage, new HashMap[Int, TaskSetManager]))
      // 设置任务集中的任务为僵尸任务,提交完毕之后会被kill
      stageTaskSets.foreach { case (_, ts) =>
        ts.isZombie = true
      }
      // 设置任务集管理器
      stageTaskSets(taskSet.stageAttemptId) = manager
      // 添加任务管理器
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
`	  // 处理远端未接受任务处理,如果没有运行任务则会打印出日志
      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run(): Unit = {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    3. 后端发送恢复提供(offer)请求
    backend.reviveOffers()
    
    def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager
    功能: 创建任务集管理器
    val= new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt)
    
    def cancelTasks(stageId: Int, interruptThread: Boolean): Unit
    功能: 放弃指定stage下的所有任务@stageId
    1. 中断stage下的所有任务
    killAllTaskAttempts(stageId, interruptThread, reason = "Stage cancelled")
    2. 放弃当前stage的所有请求
    val= taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
    
    def killTaskAttempt(
      taskId: Long,
      interruptThread: Boolean,
      reason: String): Boolean
    功能: kill任务请求
    val=  synchronized {
    logInfo(s"Killing task $taskId: $reason")
    val execId = taskIdToExecutorId.get(taskId)
        if (execId.isDefined) {
          backend.killTask(taskId, execId.get, interruptThread, reason)
          true
        } else {
          logWarning(s"Could not kill task $taskId because no task 
          with that ID was found.")
          false
        }
      }
    
    def killAllTaskAttempts(
      stageId: Int,
      interruptThread: Boolean,
      reason: String): Unit 
    功能: 中断执行stage下的所有任务
    synchronized {
        logInfo(s"Killing all running tasks in stage $stageId: $reason")
        taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
          attempts.foreach { case (_, tsm) =>
            tsm.runningTasksSet.foreach { tid =>
              taskIdToExecutorId.get(tid).foreach { execId =>
                backend.killTask(tid, execId, interruptThread, reason)
              }
            }
          }
        }
      }
    
    def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit
    功能: 提示指定分区@partitionId执行完毕
    val= taskResultGetter.enqueuePartitionCompletionNotification(stageId, partitionId)
    
    def taskSetFinished(manager: TaskSetManager): Unit
    功能: 调用表示指定任务集管理器@manager 中的所有任务执行完成,所以与@TaskSetManager 相关的状态需要被清除
    synchronized {
       	//从当前stage任务集中去掉@manager执行完的任务，如果当前stage中没有任务了，
        // 直接移除这个stage的记录
        taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { 
          taskSetsForStage =>
          taskSetsForStage -= manager.taskSet.stageAttemptId
          if (taskSetsForStage.isEmpty) {
            taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
          }
        }
        // 移除当前任务集管理器
        manager.parent.removeSchedulable(manager)
        logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed,
        from pool" +
        s" ${manager.parent.name}")
    }
    
    def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      availableResources: Array[Map[String, Buffer[String]]],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]],
      addressesWithDescs: ArrayBuffer[(String, TaskDescription)]) : Boolean 
    功能: 获取单个任务集的资源空间
    1. 设置当前任务运行状态
    var launchedTask = false
    2. 为每个任务(不处于黑名单节点或者执行器上的)分配资源
    for (i <- 0 until shuffledOffers.size) {
      // 获取执行器ID,主机名称(不含黑名单列表中的)
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      if (availableCpus(i) >= CPUS_PER_TASK &&
        resourcesMeetTaskRequirements(availableResources(i))) {
        // 分配任务资源
        try {
          for (task <- taskSet.resourceOffer(
              execId, host, maxLocality, availableResources(i))) {
            tasks(i) += task // 分配任务+1
            val tid = task.taskId 
            // 注册当前任务到各个映射表中
            taskIdToTaskSetManager.put(tid, taskSet)
            taskIdToExecutorId(tid) = execId
            executorIdToRunningTaskIds(execId).add(tid)
            // 更新CPU剩余量
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            task.resources.foreach { case (rName, rInfo) =>
              availableResources(i).getOrElse(rName,
                throw new SparkException(s"Try to acquire resource 
                $rName that doesn't exist."))
                .remove(0, rInfo.addresses.size)
            }
            // 处理任务处于屏蔽状态下的情况
            if (taskSet.isBarrier) {
              addressesWithDescs += (shuffledOffers(i).address.get -> task)
            }
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} 
            was not serializable")
            return launchedTask
        }
      }
    }
    
    def resourcesMeetTaskRequirements(resources: Map[String, Buffer[String]]): Boolean
    功能: 检查任务运行的资源要求
    val resourcesFree = resources.map(r => r._1 -> r._2.length)
    ResourceUtils.resourcesMeetRequirements(resourcesFree, resourcesReqsPerTask)
    
    def createUnschedulableTaskSetAbortTimer(
      taskSet: TaskSetManager,
      taskIndex: Int): TimerTask
    功能: 创建不可调度的任务集定时器,弃用任务集时使用
    val= new TimerTask() {
      override def run(): Unit = TaskSchedulerImpl.this.synchronized {
        if (unschedulableTaskSetToExpiryTime.contains(taskSet) &&
            unschedulableTaskSetToExpiryTime(taskSet) <= clock.getTimeMillis()) {
          logInfo("Cannot schedule any task because of complete blacklisting. " +
            s"Wait time for scheduling expired. Aborting $taskSet.")
          taskSet.abortSinceCompletelyBlacklisted(taskIndex)
        } else {
          this.cancel()
        }
      }
    }
    
    def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] 
    功能: 获取用于shuffle的空闲资源
    val= Random.shuffle(offers)
    
    def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer): Unit 
    功能: 状态更新
    1. 设置失败任务统计数据结构
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    2. 
    synchronized {
      try {
        Option(taskIdToTaskSetManager.get(tid)) match {
          case Some(taskSet) =>
           	// 处理任务丢失，将其视作失败任务处理
            if (state == TaskState.LOST) {
              val execId = taskIdToExecutorId.getOrElse(
                  tid, throw new IllegalStateException(
                "taskIdToTaskSetManager.contains(tid) 
                <=> taskIdToExecutorId.contains(tid)"))
              if (executorIdToRunningTaskIds.contains(execId)) {
                reason = Some(
                  SlaveLost(s"Task $tid was lost, so marking the 
                  executor as lost as well."))
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
           // 处理执行完成任务,获取任务执行的结果
            if (TaskState.isFinished(state)) {
              cleanupTaskState(tid)
              taskSet.removeRunningTask(tid)
              if (state == TaskState.FINISHED) { // 任务成功
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED,
                             TaskState.LOST).contains(state)) { // 任务失败
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s 
              because its task set is gone (this is " +
              "likely the result of receiving duplicate task finished
              status updates) or its " +
              "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // 更新DAG调度器,不使用锁,可能会导致死锁
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      backend.reviveOffers()
    }
    
    def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean
    功能: 执行器心跳接受
    1. 获取更新累加器四元组 (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        Option(taskIdToTaskSetManager.get(id)).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    2. 执行器调度器接受心跳信息
    dagScheduler.executorHeartbeatReceived(
        execId, accumUpdatesWithTaskIds, blockManagerId,executorUpdates)
    
    def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit
    功能: 处理任务获取结果
    taskSetManager.handleTaskGettingResult(tid)
    
    def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit
    功能: 处理执行成功的任务
    synchronized {
        taskSetManager.handleSuccessfulTask(tid, taskResult)
      }
    
    def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit
    功能: 处理失败的任务
    synchronized {
        taskSetManager.handleFailedTask(tid, taskState, reason)
        // 恢复成功申请且没有被设置为僵尸任务的任务
        if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
          backend.reviveOffers()
    	}
    }
    
    def handlePartitionCompleted(stageId: Int, partitionId: Int)
    功能: 处理指定分区完成情况
    synchronized {
    taskSetsByStageIdAndAttempt.get(stageId).foreach(
        _.values.filter(!_.isZombie).foreach { tsm =>
          tsm.markPartitionCompleted(partitionId)
        })
      }
    
    def error(message: String): Unit
    功能: 错误信息处理
    
    def stop(): Unit
    功能: 停止任务调度器
    1. 停止推测调度器
    speculationScheduler.shutdown()
    2. 停止调度器后台
    if (backend != null) {
      backend.stop()
    }
    3. 获取任务结果获取器
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    4. 获取屏蔽协调者
    if (barrierCoordinator != null) {
      barrierCoordinator.stop()
    }
    5. 重置计时器
    starvationTimer.cancel()
    abortTimer.cancel()
    
    def defaultParallelism(): Int = backend.defaultParallelism()
    功能: 获取默认并行度
    
    def checkSpeculatableTasks(): Unit
    功能: 检查推测任务
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
    
    def executorLost(executorId: String, reason: ExecutorLossReason): Unit
    功能: 标记指定执行器@executorId 丢失
    1. 设置记录失败执行器的数据结构
    var failedExecutor: Option[String] = None
    2. 标记执行器丢失 
    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)
          case None =>
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    3. DAG调度器标记执行器丢失
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      backend.reviveOffers()
    }
    
    def workerRemoved(workerId: String, host: String, message: String): Unit
    功能: 移除指定worker
    logInfo(s"Handle removed worker $workerId: $message")
    dagScheduler.workerRemoved(workerId, host, message)
    
    def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit
    功能: 记录执行器丢失
    reason match {
        case LossReasonPending => // 原因待定
          logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
        case ExecutorKilled => // 中断
          logInfo(s"Executor $executorId on $hostPort killed by driver.")
        case _ =>
          logError(s"Lost executor $executorId on $hostPort: $reason")
      }
    
    def cleanupTaskState(tid: Long): Unit
    功能: 清除任务状态
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
    
    def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit
    功能: 移除指定执行器
    1. 从运行任务映射表中移除指定执行器的条目
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      taskIds.foreach(cleanupTaskState)
    }
    2. 从执行器所属主机上移除执行器条目,若是为空,则从机架上移除这个节点
    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }
    3. 汇报执行器丢失
    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
    4. 从黑名单追踪器上移除这个执行器
    blacklistTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
    
    def executorAdded(execId: String, host: String): Unit
    功能: 添加执行器
    dagScheduler.executorAdded(execId, host)
    
    def getExecutorsAliveOnHost(host: String): Option[Set[String]] 
    功能: 获取主机上存活的执行器
    val= synchronized {
        hostToExecutors.get(host).map(_.toSet)
      }
    
    def hasExecutorsAliveOnHost(host: String): Boolean
    功能: 确定主机上是否有存活的执行器
    synchronized {
        hostToExecutors.contains(host)
      }
    
    def hasHostAliveOnRack(rack: String): Boolean
    功能: 确定机架上是否存在存活的主机
    val= synchronized {
        hostsByRack.contains(rack)
      }
    
    def isExecutorAlive(execId: String): Boolean 
    功能: 确定当前执行器是否存活
    val= synchronized { executorIdToRunningTaskIds.contains(execId) }
    
    def isExecutorBusy(execId: String): Boolean
    功能: 确定当前执行器是否处于繁忙状态
    val= synchronized {
        executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
      }
    
    def applicationId(): String = backend.applicationId()
    功能: 获取应用编号
    
    def applicationAttemptId(): Option[String] = backend.applicationAttemptId()
    功能: 获取任务请求编号
    
    def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager]
    功能: 获取指定stage的指定请求的任务集管理器,测试使用
    val= synchronized {
        for {
          attempts <- taskSetsByStageIdAndAttempt.get(stageId)
          manager <- attempts.get(stageAttemptId)
        } yield {
          manager
        }
      }
    
    def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] 
    功能: 获取任务描述表
    输入: 
    	offers	可使用的资源列表
    返回:
    	任务描述表
    1. 注册可以资源的信息到各个信息映射表中
    var newExecAvail = false
    for (o <- offers) {
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        newExecAvail = true
      }
    }
    val hosts = offers.map(_.host).toSet.toSeq
    // 注册主机机架信息表
    for ((host, Some(rack)) <- hosts.zip(getRacksForHosts(hosts))) {
      hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += host
    }
    2. 在进行空闲资源分配之前,需要移除黑名单中的执行器或者节点,并进行过滤
    blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())
    val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
      offers.filter { offer =>
        !blacklistTracker.isNodeBlacklisted(offer.host) &&
          !blacklistTracker.isExecutorBlacklisted(offer.executorId)
      }
    }.getOrElse(offers)
    3. 获取shuffleOffer
    val shuffledOffers = shuffleOffers(filteredOffers)
    4. 获取任务及任务分配的资源
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](
        o.cores / CPUS_PER_TASK))
    val availableResources = shuffledOffers.map(_.resources).toArray
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    5. 新建执行器
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }
    6. 按照调度顺序将每个任务集加入,然后按照位置升序的顺序排序,以便于可以运行本地任务去运行.
    	注意: 优先位置为@PROCESS_LOCAL 本地进程 > @NODE_LOCAL 本地节点
    	> @NO_PREF 无最优位置 > @RACK_LOCAL 本地机架 > ANY 任意
    for (taskSet <- sortedTaskSets) {
      val availableSlots = availableCpus.map(c => c / CPUS_PER_TASK).sum
      if (taskSet.isBarrier && availableSlots < taskSet.numTasks) {
        logInfo(s"Skip current round of resource o
        ffers for barrier stage ${taskSet.stageId} " +
          s"because the barrier taskSet requires ${taskSet.numTasks} 
          slots, while the total " +
          s"number of available slots is $availableSlots.")
      } else {
        var launchedAnyTask = false
        val addressesWithDescs = ArrayBuffer[(String, TaskDescription)]()
        for (currentMaxLocality <- taskSet.myLocalityLevels) {
          var launchedTaskAtCurrentMaxLocality = false
          do {
            launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(taskSet,
              currentMaxLocality, shuffledOffers, availableCpus,
              availableResources, tasks, addressesWithDescs)
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality
          } while (launchedTaskAtCurrentMaxLocality)
        }

        if (!launchedAnyTask) {
          taskSet.getCompletelyBlacklistedTaskIfAny(hostToExecutors).foreach { taskIndex 				=>
              executorIdToRunningTaskIds.find(x => !isExecutorBusy(x._1)) match {
                case Some ((executorId, _)) =>
                  if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                    blacklistTrackerOpt.foreach(blt => blt.killBlacklistedIdleExecutor(executorId))
                    val timeout = conf.get(config.UNSCHEDULABLE_TASKSET_TIMEOUT) * 1000
                    unschedulableTaskSetToExpiryTime(taskSet) = 
                      clock.getTimeMillis() + timeout
                    logInfo(s"Waiting for $timeout ms for completely "
                      + s"blacklisted task to be schedulable 
                      again before aborting $taskSet.")
                    abortTimer.schedule(
                      createUnschedulableTaskSetAbortTimer(taskSet, taskIndex), timeout)
                  }
                case None => // Abort Immediately
                  logInfo("Cannot schedule any task because 
                  of complete blacklisting. No idle" +
                    s" executors can be found to kill. Aborting $taskSet." )
                  taskSet.abortSinceCompletelyBlacklisted(taskIndex)
              }
          }
        } else {
          if (unschedulableTaskSetToExpiryTime.nonEmpty) {
            logInfo("Clearing the expiry times for all unschedulable 
            taskSets as a task was " +
            "recently scheduled.")
            unschedulableTaskSetToExpiryTime.clear()
          }
        }

        if (launchedAnyTask && taskSet.isBarrier) {
          require(addressesWithDescs.size == taskSet.numTasks,
            s"Skip current round of resource offers for 
            barrier stage ${taskSet.stageId} " +
              s"because only ${addressesWithDescs.size} out of a total number of " +
              s"${taskSet.numTasks} tasks got resource offers. 
              The resource offers may have " +
              "been blacklisted or cannot fulfill task locality requirements.")
          maybeInitBarrierCoordinator()
          val addressesStr = addressesWithDescs
            .sortBy(_._2.partitionId)
            .map(_._1)
            .mkString(",")
          addressesWithDescs.foreach(_._2.properties.setProperty(
              "addresses", addressesStr))
          logInfo(s"Successfully scheduled all the ${addressesWithDescs.size} 
          tasks for barrier " +
          s"stage ${taskSet.stageId}.")
        }
      }
    }
    
}
```

```scala
private[spark] object TaskSchedulerImpl {
    属性:
    #name @SCHEDULER_MODE_PROPERTY = SCHEDULER_MODE.key	调度模式属性
    操作集：
    def maybeCreateBlacklistTracker(sc: SparkContext): Option[BlacklistTracker]
    功能: 创建可能的黑名单追踪器
    val= if (BlacklistTracker.isBlacklistEnabled(sc.conf)) {
      val executorAllocClient: Option[ExecutorAllocationClient] = sc.schedulerBackend
        match {
            case b: ExecutorAllocationClient => Some(b)
            case _ => None
          }
          Some(new BlacklistTracker(sc, executorAllocClient))
        } else {
          None
    }
    
    def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T]
    功能: 容器优先化
    接受一个主机-->资源列表映射的map,返回一个优先列表,按照使用空间大小排序.之所以这样排序是可以避免分配下一个节点的容器.如果节点宕机,可以减少伤害返回
    例如:
    @literal <h1, [o1, o2, o3]> @literal <h2, [o4]> @literal <h3, [o5, o6]>
    返回 @literal [o1, o5, o4, o2, o6, o3]
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )
    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true
    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }
    retval.toList
}
```

#### TaskSet

```scala
private[spark] class TaskSet(
    val tasks: Array[Task[_]], // 任务列表
    val stageId: Int, // stage编号
    val stageAttemptId: Int, // stage请求编号
    val priority: Int, // 优先级
    val properties: Properties) { // 属性值
    介绍: 提交的底层任务调度器@TaskScheduler 的任务，代表指定stage的确实分区信息
    #name @id: String = stageId + "." + stageAttemptId	stage唯一标识符
    操作集:
    def toString: String = "TaskSet " + id
    功能: 信息显示
}
```

#### TaskSetBlacklist

```markdown
介绍:
	使用任务集处理黑名单中的执行器和节点.包括任务-->执行器和任务--> 节点键值对.也可以完成整个任务集的黑名单执行器和节点.
	也会存储任务失败的高效信息,用于应用层级的黑名单处置,使用@BlacklistTracker 处理.注意到黑名单追踪器@BlacklistTracker不知道任务的失败,直到任务集全部成功完成.
```

```scala
private[scheduler] class TaskSetBlacklist(
    private val listenerBus: LiveListenerBus,
    val conf: SparkConf,
    val stageId: Int,
    val stageAttemptId: Int,
    val clock: Clock) extends Logging {
 	输入参数:
        listenBus	监听总线
        conf	spark配置
        stageId	stage编号
        stageAttemptId	stage请求编号
        clock	时钟
    属性:
    #name @MAX_TASK_ATTEMPTS_PER_EXECUTOR	单个执行器最大请求数量
    val= conf.get(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR)
    #name @MAX_TASK_ATTEMPTS_PER_NODE	单个节点最大请求数量
    val= conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)
    #name @MAX_FAILURES_PER_EXEC_STAGE	单个执行stage最大失败次数
    val= conf.get(config.MAX_FAILURES_PER_EXEC_STAGE)
    #name @MAX_FAILED_EXEC_PER_NODE_STAGE	带个节点stage最大失败次数
    val=conf.get(config.MAX_FAILED_EXEC_PER_NODE_STAGE)
    #name @nodeToExecsWithFailures = new HashMap[String, HashSet[String]]()	节点失败映射表
    #name @nodeToBlacklistedTaskIndexes = new HashMap[String, HashSet[Int]]()
    	节点黑名单任务索引映射表
    #name @blacklistedExecs = new HashSet[String]()	黑名单执行器列表
    #name @blacklistedNodes = new HashSet[String]()	黑名单节点列表
    #name @latestFailureReason: String = null	上一次失败原因
    操作集:
    def getLatestFailureReason: String = latestFailureReason
    功能: 获取最新的失败原因
    
    def isNodeBlacklistedForTask(node: String, index: Int): Boolean
    功能: 确定节点是否对于任务时黑名单状态
    val= nodeToBlacklistedTaskIndexes.get(node).exists(_.contains(index))
    
    def isExecutorBlacklistedForTask(executorId: String, index: Int): Boolean
    功能: 确定执行器是否对于当前任务处于黑名单状态
    val= execToFailures.get(executorId).exists { execFailures =>
      execFailures.getNumTaskFailures(index) >= MAX_TASK_ATTEMPTS_PER_EXECUTOR
    }
    
    def isExecutorBlacklistedForTaskSet(executorId: String): Boolean
    功能: 确定执行器对任务集是否是黑名单状态
    val= blacklistedExecs.contains(executorId)
    
    def isNodeBlacklistedForTaskSet(node: String): Boolean
    功能: 确定节点对于任务集是否处于黑名单状态
    val= blacklistedNodes.contains(node)
    
    def updateBlacklistForFailedTask(
      host: String,
      exec: String,
      index: Int,
      failureReason: String): Unit
    功能: 更新失败任务的数据块信息
    1. 更新执行器失败情况
    val execFailures = execToFailures.getOrElseUpdate(
        exec, new ExecutorFailuresInTaskSet(host))
    execFailures.updateWithFailure(index, clock.getTimeMillis())
    2. 获取节点上的执行器失败信息
    val execsWithFailuresOnNode = nodeToExecsWithFailures.getOrElseUpdate(
        host, new HashSet())
    execsWithFailuresOnNode += exec
    3. 获取主机上的失败信息
    val failuresOnHost = execsWithFailuresOnNode.toIterator.flatMap { exec =>
      execToFailures.get(exec).map { failures =>
        failures.getNumTaskFailures(index)
      }
    }.sum
    if (failuresOnHost >= MAX_TASK_ATTEMPTS_PER_NODE) {
      nodeToBlacklistedTaskIndexes.getOrElseUpdate(host, new HashSet()) += index
    }
    4. 获取失败次数
    val numFailures = execFailures.numUniqueTasksWithFailures
    if (numFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
      if (blacklistedExecs.add(exec)) {
        logInfo(s"Blacklisting executor ${exec} for stage $stageId")
        val blacklistedExecutorsOnNode =
          execsWithFailuresOnNode.filter(blacklistedExecs.contains(_))
        val now = clock.getTimeMillis()
        listenerBus.post(
          SparkListenerExecutorBlacklistedForStage(
              now, exec, numFailures, stageId, stageAttemptId))
        val numFailExec = blacklistedExecutorsOnNode.size
        if (numFailExec >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
          if (blacklistedNodes.add(host)) {
            logInfo(s"Blacklisting ${host} for stage $stageId")
            listenerBus.post(
              SparkListenerNodeBlacklistedForStage(
                  now, host, numFailExec, stageId, stageAttemptId))
          }
        }
      }
    }
}
```

#### TaskSetManager

```markdown
在@TaskSchedulerImpl 中使用单个任务集进行任务调度.这个类保证了对每个任务的追踪,如果失败的时候进行重试,且对任务集进行位置感应调度(通过调度延时).主要接口是@resourceOffer,询问任务集是否在一个节点上运行任务且处理成功任务/处理失败任务.
注意: 这个类需要使用@TaskScheduler 加锁进行访问，不能被其他线程访问。
构造器参数:
	sched	任务集管理器对应的任务调度器@TaskSchedulerImpl
	taskSet	管理器调度的任务集
	maxTaskFailures	最大任务失败次数
```

```scala
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,
    val maxTaskFailures: Int,
    blacklistTracker: Option[BlacklistTracker] = None,
    clock: Clock = new SystemClock()) extends Schedulable with Logging {
    属性:
    #name @conf = sched.sc.conf	spark配置
    #name @addedJars = HashMap[String, Long](sched.sc.addedJars.toSeq: _*)	添加jar包表
    #name @addedFiles = HashMap[String, Long](sched.sc.addedFiles.toSeq: _*) 添加文件表
    #name @maxResultSize = conf.get(config.MAX_RESULT_SIZE)	最大结果大小
    #name @env = SparkEnv.get	spark环境
    #name @ser = env.closureSerializer.newInstance()	序列化实例
    #name @tasks = taskSet.tasks	任务集任务列表
    #name @partitionToIndex	分区-->任务索引映射表
    val= tasks.zipWithIndex
    .map { case (t, idx) => t.partitionId -> idx }.toMap
    #name @numTasks = tasks.length	任务数量
    #name @copiesRunning = new Array[Int](numTasks)	运行副本?
    #name @speculationEnabled = conf.get(SPECULATION_ENABLED)	是否允许推测执行
    #name @speculationQuantile = conf.get(SPECULATION_QUANTILE)	推测执行分位点
    #name @speculationMultiplier = conf.get(SPECULATION_MULTIPLIER)	推测乘法器
    #name @minFinishedForSpeculation	推测执行最小截止点
    val=math.max((speculationQuantile * numTasks).floor.toInt, 1)
    #name @speculationTaskDurationThresOpt	推测任务容器(无论是否到达分位点)
    val=  conf.get(SPECULATION_TASK_DURATION_THRESHOLD)
    #name @speculationTasksLessEqToSlots	推测任务数量是否小于等于槽数
    val= numTasks <= (conf.get(EXECUTOR_CORES) / sched.CPUS_PER_TASK)
    	在满足条件的情况下,如果不长于给定指定周期,任务管理器会开启推测执行.
    	在这种情况下,不能过于激进的使用推测执行.但是需要处理一些基本情景.
    #name @successful = new Array[Boolean](numTasks)	任务执行成功状态列表
    #name @numFailures = new Array[Int](numTasks)	任务失败次数列表
    #name @killedByOtherAttempt = new HashSet[Long]	由于其他请求被kill的任务id列表
    #name @taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)	任务请求列表
    #name @tasksSuccessful = 0	任务成功数量
    #name @weight = 1	权重
    #name @minShare = 0	最小共享次数
    #name @priority = taskSet.priority	优先级
    #name @stageId = taskSet.stageId	stage编号
    #name @name = "TaskSet_" + taskSet.id	任务名称
    #name @parent: Pool = null	父调度池
    #name @totalResultSize = 0L	合计结果大小
    #name @calculatedTasks = 0	已计算的任务数量
    #name @runningTasksSet = new HashSet[Long]	运行中的任务集
    #name @taskSetBlacklistHelperOpt: Option[TaskSetBlacklist]	任务集黑名单
    val= blacklistTracker.map { _ =>
          new TaskSetBlacklist(sched.sc.listenerBus, conf, stageId,
                               taskSet.stageAttemptId, clock)
        }
    #name @isZombie = false	是否为僵尸任务集(没有可以运行的任务集)
  	#name @pendingTasks = new PendingTasksByLocality()	待定任务列表
    #name @speculatableTasks = new HashSet[Int]	推测任务表
    #name @pendingSpeculatableTasks = new PendingTasksByLocality()	待定推测任务表
    #name @taskInfos = new HashMap[Long, TaskInfo]	任务信息映射表(taskId-> TaskInfo)
    #name @successfulTaskDurations = new MedianHeap()	成功执行任务时间
    	设置为可以获取中位数的堆,只有当开启推测执行时才能使用
    #name @EXCEPTION_PRINT_INTERVAL	异常打印时间间隔
    val= conf.getLong("spark.logging.exceptionPrintInterval", 10000)
    #name @recentExceptions = HashMap[String, (Int, Long)]()	最近异常列表
    #name @epoch = sched.mapOutputTracker.getEpoch	mapout定位器所处的位置
    #name @myLocalityLevels = computeValidLocalityLevels()	当前位置等级
    #name @localityWaits = myLocalityLevels.map(getLocalityWait)	位置等待信息表
    #name @currentLocalityIndex = 0	当前位置等级索引
    #name @lastLaunchTime = clock.getTimeMillis() 	最新运行时间
    #name @emittedTaskSizeWarning = false	是否发出任务大小的warning信息
    初始化操作:
    for (t <- tasks) {
        t.epoch = epoch
    }
    功能: 设置每个任务的定位位置
    
    addPendingTasks()
    功能: 添加所有任务到待定任务中
    
    操作集:
    def runningTasks: Int = runningTasksSet.size
    功能: 获取正在运行的任务数量
    
    def someAttemptSucceeded(tid: Long): Boolean
    功能: 获取指定任务@tid 的执行成功状态
    val= successful(taskInfos(tid).index)
    
    def isBarrier = taskSet.tasks.nonEmpty && taskSet.tasks(0).isBarrier
    功能: 确定是否为屏蔽执行
    
    def addPendingTasks(): Unit
    功能: 添加待定任务(按照taskId 倒序,以便以编号小的可以优先运行)
    val (_, duration) = Utils.timeTakenMs {
      for (i <- (0 until numTasks).reverse) {
        addPendingTask(i, resolveRacks = false)
      }
      val (hosts, indicesForHosts) = pendingTasks.forHost.toSeq.unzip
      val racks = sched.getRacksForHosts(hosts)
      racks.zip(indicesForHosts).foreach {
        case (Some(rack), indices) => // 更新机架信息
          pendingTasks.forRack.getOrElseUpdate(rack, new ArrayBuffer) ++= indices
        case (None, _) => // no rack, nothing to do
      }
    }
    
    def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null
    功能: 获取调度队列
    
    def schedulingMode: SchedulingMode = SchedulingMode.NONE
    功能: 获取调度模式
    
    def addPendingTask(
      index: Int,
      resolveRacks: Boolean = true,
      speculatable: Boolean = false): Unit 
    功能: 添加一个任务到待定任务列表中
    1. 确定待定任务列表类型
    val pendingTaskSetToAddTo = 
    	if (speculatable) pendingSpeculatableTasks else pendingTasks
    2. 确定任务需要放置的最佳位置
    for (loc <- tasks(index).preferredLocations) {
      loc match {
        case e: ExecutorCacheTaskLocation =>
          pendingTaskSetToAddTo.forExecutor.getOrElseUpdate(
              e.executorId, new ArrayBuffer) += index
        case e: HDFSCacheTaskLocation =>
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) =>
              for (e <- set) {
                pendingTaskSetToAddTo.forExecutor.getOrElseUpdate(
                    e, new ArrayBuffer) += index
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            case None => logDebug(s"Pending task $index has a cached 
            location at ${e.host} " +
              ", but there are no executors alive there.")
          }
        case _ =>
      }
      // 更新主机和机架的任务信息映射表
      pendingTaskSetToAddTo.forHost.getOrElseUpdate(loc.host, new ArrayBuffer) += index
      if (resolveRacks) {
        sched.getRackForHost(loc.host).foreach { rack =>
          pendingTaskSetToAddTo.forRack.getOrElseUpdate(rack, new ArrayBuffer) += index
        }
      }
    }
    3. 缺省处理(无最佳位置)
    if (tasks(index).preferredLocations == Nil) {
      pendingTaskSetToAddTo.noPrefs += index
    }
    pendingTaskSetToAddTo.all += index
    
    def dequeueTaskFromList(
      execId: String,
      host: String,
      list: ArrayBuffer[Int],
      speculative: Boolean = false): Option[Int]
    功能: 将一个任务从待定任务列表中出列,并返回任务编号
    var indexOffset = list.size
    1. 找到最后一个可以出列的元素,出列
    while (indexOffset > 0) {
      indexOffset -= 1
      val index = list(indexOffset)
      if (!isTaskBlacklistedOnExecOrNode(index, execId, host) &&
          !(speculative && hasAttemptOnHost(index, host))) {
        list.remove(indexOffset)
        if (!successful(index)) {
          if (copiesRunning(index) == 0) {
            return Some(index)
          } else if (speculative && copiesRunning(index) == 1) {
            return Some(index)
          }
        }
      }
    }
    2. 缺省处理
    val= None
    
    def hasAttemptOnHost(taskIndex: Int, host: String): Boolean 
    功能: 确定指定任务@taskIndex是否可以运行在指定@host上
    val= taskAttempts(taskIndex).exists(_.host == host)
    
    def isTaskBlacklistedOnExecOrNode(index: Int, execId: String, host: String): Boolean
    功能: 确定任务在指定执行器或者节点上是否处于黑名单状态
    val= taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTask(host, index) ||
        blacklist.isExecutorBlacklistedForTask(execId, index)
    }
    
    def dequeueTask(
      execId: String,
      host: String,
      maxLocality: TaskLocality.Value): Option[(Int, TaskLocality.Value, Boolean)] 
    功能: 对于一个指定节点,出队一个待定任务,返回任务编号和位置等级信息及是否为推测执行
    val= dequeueTaskHelper(execId, host, maxLocality, false).orElse(
      dequeueTaskHelper(execId, host, maxLocality, true))
    
    def dequeueTaskHelper(
      execId: String,
      host: String,
      maxLocality: TaskLocality.Value,
      speculative: Boolean): Option[(Int, TaskLocality.Value, Boolean)]
    功能: 出队任务辅助器
    if (speculative && speculatableTasks.isEmpty) {
      return None
    }
    // 首先处理,当前进程的出队情况
    val pendingTaskSetToUse = if (speculative) pendingSpeculatableTasks else pendingTasks
    def dequeue(list: ArrayBuffer[Int]): Option[Int] = {
      val task = dequeueTaskFromList(execId, host, list, speculative)
      if (speculative && task.isDefined) {
        speculatableTasks -= task.get
      }
      task
    }
    dequeue(pendingTaskSetToUse.forExecutor.getOrElse(execId, ArrayBuffer())).foreach
    { index =>
      return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
    }
     // 本节点出队情况
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      dequeue(pendingTaskSetToUse.forHost.getOrElse(host, ArrayBuffer())).foreach { 
          index =>
        return Some((index, TaskLocality.NODE_LOCAL, speculative))
      }
    }
    // 无偏好出队情况
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      dequeue(pendingTaskSetToUse.noPrefs).foreach { index =>
        return Some((index, TaskLocality.PROCESS_LOCAL, speculative))
      }
    }
    // 本机架出队情况
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeue(pendingTaskSetToUse.forRack.getOrElse(rack, ArrayBuffer()))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, speculative))
      }
    }
    // 其他位置出队情况
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      dequeue(pendingTaskSetToUse.all).foreach { index =>
        return Some((index, TaskLocality.ANY, speculative))
      }
    }
    // 缺省
    val= None
    
    def maybeFinishTaskSet(): Unit 
    功能: 进行可能的结束任务集处理
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
      if (tasksSuccessful == numTasks) {
        blacklistTracker.foreach(_.updateBlacklistForSuccessfulTaskSet(
          taskSet.stageId,
          taskSet.stageAttemptId,
          taskSetBlacklistHelperOpt.get.execToFailures))
      }
    }
    
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean 
    功能: 确认任务是否需要被重新调度
    var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
          pendingTaskIds.remove(indexOffset)
        }
      }
      val= false
    
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean
    功能: 是否需要运行更多的任务
    遍历可以调度的任务列表,如果仍然有任务需要调度返回true。并使用懒加载的方式清除已经被调度的任务。
    1. 确定是否有未调度的任务
    val emptyKeys = new ArrayBuffer[String]
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            emptyKeys += id
            false
          }
      }
    2. 清除已经调度的任务
    emptyKeys.foreach(id => pendingTasks.remove(id))
    val= hasTasks
    
    def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality
    功能: 根据延时调度,获取运行任务的位置等级
    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      // 获取需要新增的任务
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasks.forExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasks.forHost)
        case TaskLocality.NO_PREF => pendingTasks.noPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasks.forRack)
      }
      if (!moreTasks) { // 无新增处理方式,无任务则表明没有等待位置延时的必要
        lastLaunchTime = curTime
        logDebug(s"No tasks for locality level 
        ${myLocalityLevels(currentLocalityIndex)}, " +
        s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        currentLocalityIndex += 1 // 位置指针移动
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) {
        lastLaunchTime += localityWaits(currentLocalityIndex) // 更新运行时间
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)}
        after waiting for " +
        s"${localityWaits(currentLocalityIndex)}ms")
        currentLocalityIndex += 1 // 移动位置指针
      } else {
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    val= myLocalityLevels(currentLocalityIndex) //获取最后一个的位置等级
    
    def getLocalityIndex(locality: TaskLocality.TaskLocality): Int
    功能: 获取位置索引位置,使用顺序查找
    对于给定@locality 找到在存储等级列表中的位置@myLocalityLevels
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    val= index
    
    def getCompletelyBlacklistedTaskIfAny(
      hostToExecutors: HashMap[String, HashSet[String]]): Option[Int]
    功能: 检查给定任务是否被设置了黑名单,以至于不可以再任何位置运行.在黑名单执行器数量小于最大失败次数的时候,最通用的方式是需要探测出这些,从而防止这些job被终止.尝试通过kill空载的黑名单执行器,从而获取新的执行器.
    这里设置一个交换规则:
    	确保所有任务都可以调度,但是会花费额外的时间,用于每个迭代器的调度环.这里假定,至少有一个不可调度的任务最终是可以调度的.这就意味着不能尽快的探测到终止信息,但是最终是可以探测到终止信息的,且方法快于传统方式.最差时间复杂度为O(maxTaskFailures + numTasks).但是,在无任何失败的情况下是更快的.
    1. 对于黑名单列表进行处理
    taskSetBlacklistHelperOpt.flatMap { taskSetBlacklist =>
      val appBlacklist = blacklistTracker.get
      if (hostToExecutors.nonEmpty) {
        val pendingTask: Option[Int] = {
          val indexOffset = pendingTasks.all.lastIndexWhere { indexInTaskSet =>
            copiesRunning(indexInTaskSet) == 0 && !successful(indexInTaskSet)
          }
          if (indexOffset == -1) {
            None
          } else {
            Some(pendingTasks.all(indexOffset))
          }
        }
        pendingTask.find { indexInTaskSet =>
          hostToExecutors.forall { case (host, execsOnHost) =>
            val nodeBlacklisted =
              appBlacklist.isNodeBlacklisted(host) ||
                taskSetBlacklist.isNodeBlacklistedForTaskSet(host) ||
                taskSetBlacklist.isNodeBlacklistedForTask(host, indexInTaskSet)
            if (nodeBlacklisted) {
              true
            } else {
              execsOnHost.forall { exec =>
                appBlacklist.isExecutorBlacklisted(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTaskSet(exec) ||
                  taskSetBlacklist.isExecutorBlacklistedForTask(exec, indexInTaskSet)
              }
            }
          }
        }
      } else {
        None
      }
    }
    
    def abortSinceCompletelyBlacklisted(indexInTaskSet: Int): Unit
    功能: 抛弃黑名单任务集中指定的任务@indexInTaskSet
    taskSetBlacklistHelperOpt.foreach { taskSetBlacklist =>
      val partition = tasks(indexInTaskSet).partitionId
      abort(s"""
         |Aborting $taskSet because task $indexInTaskSet (partition $partition)
         |cannot run anywhere due to node and executor blacklist.
         |Most recent failure:
         |${taskSetBlacklist.getLatestFailureReason}
         |
         |Blacklisting behavior can be configured via spark.blacklist.*.
         |""".stripMargin)
    }
    
    def handleTaskGettingResult(tid: Long): Unit
    功能: 处理任务获取结果
    1. 标记获取任务结果
    val info = taskInfos(tid)
    info.markGettingResult(clock.getTimeMillis())
    2. 获取任务结果
    sched.dagScheduler.taskGettingResult(info)
    
    def canFetchMoreResults(size: Long): Boolean
    功能: 确认是否可以获取更多结果,大小为size
    val= sched.synchronized {
        totalResultSize += size
        calculatedTasks += 1
        if (maxResultSize > 0 && totalResultSize > maxResultSize) {
          val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
            s"(${Utils.bytesToString(totalResultSize)}) is bigger than 
            ${config.MAX_RESULT_SIZE.key} " +
            s"(${Utils.bytesToString(maxResultSize)})"
          logError(msg)
          abort(msg)
          false
        } else {
          true
        }
      }
    
    def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit
    功能: 标记任务成功执行,提示DAG调度器当前任务已经结束
    1. 检查之前是否有执行成功但是没有进行处理的请求
    val info = taskInfos(tid)
    val index = info.index
    if (successful(index) && killedByOtherAttempt.contains(tid)) {
      calculatedTasks -= 1
      val resultSizeAcc = result.accumUpdates.find(a =>
        a.name == Some(InternalAccumulator.RESULT_SIZE))
      if (resultSizeAcc.isDefined) {
        totalResultSize -= resultSizeAcc.get.asInstanceOf[LongAccumulator].value
      }
      handleFailedTask(tid, TaskState.KILLED,
        TaskKilled("Finish but did not commit due to another attempt succeeded"))
      return
    }
    2. 标志当前任务结束,并移除当前任务
    info.markFinished(TaskState.FINISHED, clock.getTimeMillis())
    if (speculationEnabled) {
      successfulTaskDurations.insert(info.duration)
    }
    removeRunningTask(tid)
    3. 清除对于这个任务的其他请求
    for (attemptInfo <- taskAttempts(index) if attemptInfo.running) {
      logInfo(s"Killing attempt ${attemptInfo.attemptNumber} for task ${attemptInfo.id} " +
        s"in stage ${taskSet.id} (TID ${attemptInfo.taskId}) on ${attemptInfo.host} " +
        s"as the attempt ${info.attemptNumber} succeeded on ${info.host}")
      killedByOtherAttempt += attemptInfo.taskId
      sched.backend.killTask(
        attemptInfo.taskId,
        attemptInfo.executorId,
        interruptThread = true,
        reason = "another attempt succeeded")
    }
    4. 统计执行成功的任务数量
    if (!successful(index)) {
      tasksSuccessful += 1
      logInfo(s"Finished task ${info.id} in stage 
      ${taskSet.id} (TID ${info.taskId}) in" +
        s" ${info.duration} ms on ${info.host} (executor ${info.executorId})" +
        s" ($tasksSuccessful/$numTasks)")
      successful(index) = true
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    5. DAG调度器标记任务结束
    sched.dagScheduler.taskEnded(tasks(index), Success, result.value(),
                                 result.accumUpdates,result.metricPeaks, info)
    maybeFinishTaskSet()
    
    def markPartitionCompleted(partitionId: Int): Unit
    功能: 标记分区执行完毕
    partitionToIndex.get(partitionId).foreach { index =>
      if (!successful(index)) {
        tasksSuccessful += 1
        successful(index) = true
        if (tasksSuccessful == numTasks) {
          isZombie = true
        }
        // 任务全部执行完毕则关闭
        maybeFinishTaskSet()
      }
    }
    
    def abort(message: String, exception: Option[Throwable] = None): Unit 
    功能: 发送抛弃信息
    1. DAG调度器设置失败信息
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    2. 标记为僵尸任务
    isZombie = true
    3. 进行可能的任务集清理
    maybeFinishTaskSet()
    
    def addRunningTask(tid: Long): Unit 
    功能: 添加任务到运行中任务列表
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
    
    def removeRunningTask(tid: Long): Unit
    功能: 移除指定运行任务
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
    
    def getSchedulableByName(name: String): Schedulable = null
    功能: 按照名称获取调度组件
    
    def addSchedulable(schedulable: Schedulable): Unit = {}
    def removeSchedulable(schedulable: Schedulable): Unit = {}
    功能: 添加/删除调度实例
    
    def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager]
    功能: 获取排序完成的任务集队列
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    val= sortedTaskSetQueue
    
    def handleFailedTask(tid: Long, state: TaskState, reason: TaskFailedReason): Unit
    功能: 标记任务失败,将其重新添加到待定列表中,并提醒DAG调度器
    1. 标记任务完成,并移除任务
    val info = taskInfos(tid)
    if (info.failed || info.killed) {
      return
    }
    removeRunningTask(tid)
    info.markFinished(state, clock.getTimeMillis())
    2. 获取任务相关参数
    val index = info.index
    copiesRunning(index) -= 1
    var accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty
    var metricPeaks: Array[Long] = Array.empty
    val failureReason = s"Lost task ${info.id} in stage
    ${taskSet.id} (TID $tid, ${info.host}," +
      s" executor ${info.executorId}): ${reason.toErrorString}"
    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        isZombie = true
        if (fetchFailed.bmAddress != null) {
          blacklistTracker.foreach(_.updateBlacklistForFetchFailure(
            fetchFailed.bmAddress.host, fetchFailed.bmAddress.executorId))
        }
        None
      case ef: ExceptionFailure =>
        accumUpdates = ef.accums
        metricPeaks = ef.metricPeaks.toArray
        if (ef.className == classOf[NotSerializableException].getName) {
          logError("Task %s in stage %s (TID %d) had a not
          serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) had a not 
          serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        if (ef.className == classOf[TaskOutputFileAlreadyExistException].getName) {
          logError("Task %s in stage %s (TID %d) can not
          write to output file: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) can not
          write to output file: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} 
            (TID $tid) on ${info.host}, executor" +
             s" ${info.executorId}: ${ef.className} 
             (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception
      case tk: TaskKilled =>
        accumUpdates = tk.accums
        metricPeaks = tk.metricPeaks.toArray
        logWarning(failureReason)
        None
      case e: ExecutorLostFailure if !e.exitCausedByApp =>
        logInfo(s"Task $tid failed because while it was being computed, its executor " +
          "exited for a reason unrelated to the task.
          Not counting this failure towards the " +
          "maximum number of failures for the task.")
        None
      case e: TaskFailedReason =>  // TaskResultLost and others
        logWarning(failureReason)
        None
    }
    3. DAG调度器标志任务结束
    if (tasks(index).isBarrier) {
      isZombie = true
    }
    sched.dagScheduler.taskEnded(tasks(index), reason, null, 
                                 accumUpdates, metricPeaks, info)
    4. 失败任务重试
    if (!isZombie && reason.countTowardsTaskFailures) {
      assert (null != failureReason)
      taskSetBlacklistHelperOpt.foreach(_.updateBlacklistForFailedTask(
        info.host, info.executorId, index, failureReason))
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, 
        most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }
    5. 将失败任务添加到待定列表
    if (successful(index)) {
      logInfo(s"Task ${info.id} in stage ${taskSet.id} (TID $tid) 
      failed, but the task will not" +
        s" be re-executed (either because the task failed with 
        a shuffle data fetch failure," +
        s" so the previous stage needs to be re-run, 
        or because a different copy of the task" +
        s" has already succeeded).")
    } else {
      addPendingTask(index)
    }
    6. 进行可能的清理工作
    maybeFinishTaskSet()
    
    def executorLost(execId: String, host: String, reason: ExecutorLossReason): Unit
    功能: 处理执行器丢失的问题
    将允许再失效执行器的任务重新入队(如果是一个shuffle map 的stage且没有使用外部shuffle).原因是下一个stage无法获取这个死亡执行器上的数据.所以需要重新运行.
    1. 失败任务重新入队(待定列表)
    if (tasks(0).isInstanceOf[ShuffleMapTask] && !env.blockManager.externalShuffleServiceEnabled
        && !isZombie) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        if (successful(index) && !killedByOtherAttempt.contains(tid)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          sched.dagScheduler.taskEnded(
            tasks(index), Resubmitted, null, Seq.empty, Array.empty, info)
        }
      }
    }
    2. 确定退出原因,并处理失败任务
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      val exitCausedByApp: Boolean = reason match {
        case exited: ExecutorExited => exited.exitCausedByApp
        case ExecutorKilled => false
        case _ => true
      }
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(info.executorId, exitCausedByApp,
        Some(reason.toString)))
    }
    3. 重新计算位置信息
    recomputeLocality()
    
    def checkAndSubmitSpeculatableTask(
      tid: Long,
      currentTimeMillis: Long,
      threshold: Double): Boolean
    功能: 检查并提交推测任务
    val info = taskInfos(tid)
    val index = info.index
    if (!successful(index) && copiesRunning(index) == 1 &&
        info.timeRunning(currentTimeMillis) > threshold &&
        !speculatableTasks.contains(index)) {
      addPendingTask(index, speculatable = true)
      logInfo(
        ("Marking task %d in stage %s (on %s) as speculatable because it ran more" +
          " than %.0f ms(%d speculatable tasks in this taskset now)")
          .format(index, taskSet.id, info.host, threshold, speculatableTasks.size + 1))
      speculatableTasks += index
      sched.dagScheduler.speculativeTaskSubmitted(tasks(index))
      true
    } else {
      false
    }
    
    def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean
    功能: 检查推测任务,如果有则返回true,有任务调度器@TaskScheduler 周期性调用
    1. 初始化标志位
    if (isZombie || isBarrier 
        || (numTasks == 1 && !speculationTaskDurationThresOpt.isDefined)) {
      	// 这几种情况下,不需要检查推测任务
        return false
    }
    var foundTasks = false
    2. 检测推测任务
    val numSuccessfulTasks = successfulTaskDurations.size()
    if (numSuccessfulTasks >= minFinishedForSpeculation) {
      val time = clock.getTimeMillis()
      val medianDuration = successfulTaskDurations.median
      val threshold = max(speculationMultiplier * medianDuration, minTimeToSpeculation)

      logDebug("Task length threshold for speculation: " + threshold)
      for (tid <- runningTasksSet) {
        foundTasks |= checkAndSubmitSpeculatableTask(tid, time, threshold)
      }
    } else if (speculationTaskDurationThresOpt.isDefined && speculationTasksLessEqToSlots) {
      val time = clock.getTimeMillis()
      val threshold = speculationTaskDurationThresOpt.get
      logDebug(s"Tasks taking longer time than provided 
      speculation threshold: $threshold")
      for (tid <- runningTasksSet) {
        foundTasks |= checkAndSubmitSpeculatableTask(tid, time, threshold)
      }
    }
    val= foundTasks
    
    def getLocalityWait(level: TaskLocality.TaskLocality): Long
    功能: 获取位置等待时间
    1. 获取等待时间
    val localityWait = level match {
      case TaskLocality.PROCESS_LOCAL => config.LOCALITY_WAIT_PROCESS
      case TaskLocality.NODE_LOCAL => config.LOCALITY_WAIT_NODE
      case TaskLocality.RACK_LOCAL => config.LOCALITY_WAIT_RACK
      case _ => null
    }
    val= if (localityWait != null) {
      conf.get(localityWait)
    } else {
      0L
    }
    
    def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality]
    功能: 计算有效位置等级
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (!pendingTasks.forExecutor.isEmpty &&
        pendingTasks.forExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }
    if (!pendingTasks.forHost.isEmpty &&
        pendingTasks.forHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    if (!pendingTasks.noPrefs.isEmpty) {
      levels += NO_PREF
    }
    if (!pendingTasks.forRack.isEmpty &&
        pendingTasks.forRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
    
    def recomputeLocality(): Unit
    功能: 重新计算位置信息
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    myLocalityLevels = computeValidLocalityLevels()
    localityWaits = myLocalityLevels.map(getLocalityWait)
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
    
    def executorAdded(): Unit
    功能: 添加执行器
    recomputeLocality()
    
    @throws[TaskNotSerializableException]
    def resourceOffer(
        execId: String,
        host: String,
        maxLocality: TaskLocality.TaskLocality,
        availableResources: Map[String, Seq[String]] = Map.empty)
    : Option[TaskDescription]
    功能: 调度器的执行器通过查找一个任务给与资源回应
    输入参数:
    	execId	资源执行器编号
    	host	资源主机编号
    	maxLocality	调度任务的最大位置
    1. 获取黑名单信息
    val offerBlacklisted = taskSetBlacklistHelperOpt.exists { blacklist =>
      blacklist.isNodeBlacklistedForTaskSet(host) ||
        blacklist.isExecutorBlacklistedForTaskSet(execId)
    }
    2. 获取分配的任务资源描述信息
    if (!isZombie && !offerBlacklisted) {
      val curTime = clock.getTimeMillis()
      var allowedLocality = maxLocality
      if (maxLocality != TaskLocality.NO_PREF) {
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          allowedLocality = maxLocality
        }
      }
      dequeueTask(execId, host, allowedLocality).map { case ((index, taskLocality, speculative)) =>
        val task = tasks(index)
        val taskId = sched.newTaskId()
        copiesRunning(index) += 1
        val attemptNum = taskAttempts(index).size
        val info = new TaskInfo(taskId, index, attemptNum, curTime,
          execId, host, taskLocality, speculative)
        taskInfos(taskId) = info
        taskAttempts(index) = info :: taskAttempts(index)
        if (maxLocality != TaskLocality.NO_PREF) {
          currentLocalityIndex = getLocalityIndex(taskLocality)
          lastLaunchTime = curTime
        }
        val serializedTask: ByteBuffer = try {
          ser.serialize(task)
        } catch {
          case NonFatal(e) =>
            val msg = s"Failed to serialize task $taskId, not attempting to retry it."
            logError(msg, e)
            abort(s"$msg Exception during serialization: $e")
            throw new TaskNotSerializableException(e)
        }
        if (serializedTask.limit() > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024 &&
          !emittedTaskSizeWarning) {
          emittedTaskSizeWarning = true
          logWarning(s"Stage ${task.stageId} contains a task of very large size " +
            s"(${serializedTask.limit() / 1024} KiB). The
            maximum recommended task size is " +
            s"${TaskSetManager.TASK_SIZE_TO_WARN_KIB} KiB.")
        }
        addRunningTask(taskId)
        val taskName = s"task ${info.id} in stage ${taskSet.id}"
        logInfo(s"Starting $taskName (TID $taskId, $host,
        executor ${info.executorId}, " +
        s"partition ${task.partitionId}, $taskLocality, 
        ${serializedTask.limit()} bytes)")
        val extraResources = sched.resourcesReqsPerTask.map { taskReq =>
          val rName = taskReq.resourceName
          val count = taskReq.amount
          val rAddresses = availableResources.getOrElse(rName, Seq.empty)
          assert(rAddresses.size >= count, s"Required $count $rName 
          addresses, but only " +
            s"${rAddresses.size} available.")
          val allocatedAddresses = rAddresses.take(count)
          (rName, new ResourceInformation(rName, allocatedAddresses.toArray))
        }.toMap

        sched.dagScheduler.taskStarted(task, info)
        new TaskDescription(
          taskId,
          attemptNum,
          execId,
          taskName,
          index,
          task.partitionId,
          addedFiles,
          addedJars,
          task.localProperties,
          extraResources,
          serializedTask)
      }
    } else {
      None
    }
}
```

```scala
private[spark] object TaskSetManager {
  val TASK_SIZE_TO_WARN_KIB = 1000 // 任务数量警告值
}
```

```scala
private[scheduler] class PendingTasksByLocality {
    介绍: 携带位置新的待定任务
    属性:
    #name @forExecutor = new HashMap[String, ArrayBuffer[Int]]	执行器待定任务信息表
    #name @forHost = new HashMap[String, ArrayBuffer[Int]]	主机待定任务信息表
    #name @noPrefs = new ArrayBuffer[Int]	无参考任务信息表
    #name @forRack = new HashMap[String, ArrayBuffer[Int]]	机架待定任务信息表
    #name @all = new ArrayBuffer[Int]	任意位置的待定任务表
}
```



#### WorkerOffer

```scala
private[spark]
case class WorkerOffer(
    executorId: String,
    host: String,
    cores: Int,
    address: Option[String] = None,
    resources: Map[String, Buffer[String]] = Map.empty)
介绍: 代表释放执行器上的资源
```

