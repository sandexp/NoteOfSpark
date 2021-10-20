#### 关于资源的申请和分配

当我们在spark-submit 脚本中为spark 应用分配了过多的资源量, 包括内存，虚拟内核等. 在集群资源紧张的时候运行不动. 现在从源码的角度上来探讨这个问题.

首先, spark一般会使用yarn 作为资源管理器. spark 提交的时候, 任务启动的AM会向RM申请资源, RM把AM申请的资源抽象成容器，并返回给AM. 这样Spark应用就可以在容器上运行了.

首先来看一下, AM提交RM上的逻辑:

```scala
def submitApplication(): ApplicationId = {
    // 验证spark on yarn相关的参数配置, 这主要是说在配置中写入了spark.yarn.am.resource.amount的参数key值
    ResourceRequestHelper.validateResources(sparkConf)

    var appId: ApplicationId = null
    try {
      // 初始化并启动yarn
      launcherBackend.connect()
      yarnClient.init(hadoopConf)
      yarnClient.start()

      logInfo("Requesting a new application from cluster with %d NodeManagers"
        .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

      // 在yarn上创建一个新的RM应用,应用中包含了这个应用的响应和提交信息的上下文
      val newApp = yarnClient.createApplication()
      // RM发送给客户端的响应信息，主要是用来查找应用的应用编号{@code ApplicationId},这个ID由RM分配
      val newAppResponse = newApp.getNewApplicationResponse()
      appId = newAppResponse.getApplicationId()


      // 获取应用的yarn staging 目录，没有配置的话使用用户的家目录
      val appStagingBaseDir = sparkConf.get(STAGING_DIR)
        .map { new Path(_, UserGroupInformation.getCurrentUser.getShortUserName) }
        .getOrElse(FileSystem.get(hadoopConf).getHomeDirectory())
      stagingDirPath = new Path(appStagingBaseDir, getAppStagingDir(appId))
      
	 // 设置Yarn上下文信息
      new CallerContext("CLIENT", sparkConf.get(APP_CALLER_CONTEXT),
        Option(appId.toString)).setCurrentContext()

      // 验证集群资源是否足够, 如果不够会快速失败
      verifyClusterResources(newAppResponse)

      // 为AM设置合适的上下文信息
      val containerContext = createContainerLaunchContext(newAppResponse)
      val appContext = createApplicationSubmissionContext(newApp, containerContext)
	  
      // 提交应用到RM上,并且将监控信息汇报给后台{@code LauncherBackend}
      logInfo(s"Submitting application $appId to ResourceManager")
      // 阻塞式调用,确保一定会提交到RM上
      yarnClient.submitApplication(appContext)
      launcherBackend.setAppId(appId.toString)
      reportLauncherState(SparkAppHandle.State.SUBMITTED)

      appId
    } catch {
      case e: Throwable =>
        if (stagingDirPath != null) {
          cleanupStagingDir()
        }
        throw e
    }
  }
```

RM接收到AM的请求信息之后，需要对AM进行资源分配，具体逻辑如下

```scala
def allocateResources(): Unit = synchronized {
    updateResourceRequests()

    val progressIndicator = 0.1f
	// 向RM申请container
    val allocateResponse = amClient.allocate(progressIndicator)
    val allocatedContainers = allocateResponse.getAllocatedContainers()
    allocatorNodeHealthTracker.setNumClusterNodes(allocateResponse.getNumClusterNodes)

    if (allocatedContainers.size > 0) {
      logDebug(("Allocated containers: %d. Current executor count: %d. " +
        "Launching executor count: %d. Cluster resources: %s.")
        .format(
          allocatedContainers.size,
          getNumExecutorsRunning,
          getNumExecutorsStarting,
          allocateResponse.getAvailableResources))
	 // 从RM分配到containers之后
      handleAllocatedContainers(allocatedContainers.asScala.toSeq)
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))
      processCompletedContainers(completedContainers.asScala.toSeq)
      logDebug("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, getNumExecutorsRunning))
    }
  }
```

分配完资源之后，即可在containers上运行executor任务

```scala
private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]): Unit = synchronized {
    // 对每个exector都执行任务
    for (container <- containersToUse) {
      // 对容器进行解析,获取资源信息
      val rpId = getResourceProfileIdFromPriority(container.getPriority)
      executorIdCounter += 1
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      val executorId = executorIdCounter.toString
      val yarnResourceForRpId = rpIdToYarnResource.get(rpId)
      assert(container.getResource.getMemory >= yarnResourceForRpId.getMemory)
      logInfo(s"Launching container $containerId on host $executorHostname " +
        s"for executor with ID $executorId for ResourceProfile Id $rpId")
	
      def updateInternalState(): Unit = synchronized {
        getOrUpdateRunningExecutorForRPId(rpId).add(executorId)
        getOrUpdateNumExecutorsStartingForRPId(rpId).decrementAndGet()
        executorIdToContainer(executorId) = container
        containerIdToExecutorIdAndResourceProfileId(container.getId) = (executorId, rpId)

        val localallocatedHostToContainersMap = getOrUpdateAllocatedHostToContainersMapForRPId(rpId)
        val containerSet = localallocatedHostToContainersMap.getOrElseUpdate(executorHostname,
          new HashSet[ContainerId])
        containerSet += containerId
        allocatedContainerToHostMap.put(containerId, executorHostname)
      }
      // 根据资源信息ID查找到资源信息rp, 并解析得出内存和虚拟内核的使用量 作为容器的资源状况
      val rp = rpIdToResourceProfile(rpId)
      val defaultResources = ResourceProfile.getDefaultProfileExecutorResources(sparkConf)
      val containerMem = rp.executorResources.get(ResourceProfile.MEMORY).
        map(_.amount).getOrElse(defaultResources.executorMemoryMiB).toInt
      // 获取容器中应当分配核心数和executor数量信息
      val containerCores = rp.getExecutorCores.getOrElse(defaultResources.cores)
      val rpRunningExecs = getOrUpdateRunningExecutorForRPId(rpId).size
      if (rpRunningExecs < getOrUpdateTargetNumExecutorsForRPId(rpId)) {
        getOrUpdateNumExecutorsStartingForRPId(rpId).incrementAndGet()
        if (launchContainers) {
          // 资源足够且 已经允许运行容器, 异步执行executor任务
          launcherPool.execute(() => {
            try {
              new ExecutorRunnable(
                Some(container),
                conf,
                sparkConf,
                driverUrl,
                executorId,
                executorHostname,
                containerMem,
                containerCores,
                appAttemptId.getApplicationId.toString,
                securityMgr,
                localResources,
                rp.id
              updateInternalState()
              ).run()
            } catch {
              case e: Throwable =>
                getOrUpdateNumExecutorsStartingForRPId(rpId).decrementAndGet()
                if (NonFatal(e)) {
                  logError(s"Failed to launch executor $executorId on container $containerId", e)
                  // Assigned container should be released immediately
                  // to avoid unnecessary resource occupation.
                  amClient.releaseAssignedContainer(containerId)
                } else {
                  throw e
                }
            }
          })
        } else {
          // For test only
          updateInternalState()
        }
      } else {
          // 资源不足 不足以运行
        logInfo(("Skip launching executorRunnable as running executors count: %d " +
          "reached target executors count: %d.").format(rpRunningExecs,
          getOrUpdateTargetNumExecutorsForRPId(rpId)))
      }
    }
  }
```

#### 调度

Spark on yarn的调度由`YarnScheduler`实现,这个类的主体实现在`TaskSchedulerImpl`中.

这个类主要包含如下方法签名:

```scala
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false,
    clock: Clock = new SystemClock){
	// 调度器初始化    
    def initialize(backend: SchedulerBackend): Unit
    
    // 调度器启动
    def start(): Unit
    
    // 前向处理函数, 用于启动时候的前置处理
    def postStartHook(): Unit
    
    // 任务提交过程
    def submitTasks(taskSet: TaskSet): Unit
    
    // 放弃某一组任务
    def cancelTasks(stageId: Int, interruptThread: Boolean): Unit
     
    // 强制退出某个任务
    def killTaskAttempt(
      taskId: Long,
      interruptThread: Boolean,
      reason: String): Boolean
	
    // 清除一组task任务
    def killAllTaskAttempts(
      stageId: Int,
      interruptThread: Boolean,
      reason: String): Unit
    
    // 分区完成处理函数 后置逻辑
    def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit
    
    // 校验任务资源是否足够
    def resourcesMeetTaskRequirements(
      taskSet: TaskSetManager,
      availCpus: Int,
      availWorkerResources: Map[String, Buffer[String]]
      ): Option[Map[String, ResourceInformation]]
    
    // 数据位置最优选择 用于数据本地化
    def minTaskLocality(
      l1: Option[TaskLocality],
      l2: Option[TaskLocality]) : Option[TaskLocality]
    
    // 后置函数
    def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit
    def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit
    def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit
    def handlePartitionCompleted(stageId: Int, partitionId: Int)
    
    
    ...
}
```

现在着重探讨任务提交的过程

```scala
def submitTasks(taskSet: TaskSet): Unit = {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks "
            + "resource profile " + taskSet.resourceProfileId)
    this.synchronized {
        val manager = createTaskSetManager(taskSet, maxTaskFailures)
        val stage = taskSet.stageId
        val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
        stageTaskSets.foreach { case (_, ts) =>
            ts.isZombie = true
        }
        stageTaskSets(taskSet.stageAttemptId) = manager
        schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

        if (!isLocal && !hasReceivedTask) {
            // 如果数据不在本地且没有接收到任务,则定期的去查看是否后台有任务启动,如果任务一直不来就会造成一直在等待,而任务的生成有资源决定,也就是资源不足的情况下,这个位置会一直等待运行时间拉长
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
    backend.reviveOffers()
}
```



 