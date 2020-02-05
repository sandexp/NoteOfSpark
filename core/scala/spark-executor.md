## **spark-executor**

---

1.  [CoarseGrainedExecutorBackend.scala](# CoarseGrainedExecutorBackend)
2.  [CommitDeniedException.scala](# CommitDeniedException)
3.  [Executor.scala](# Executor)
4.  [ExecutorBackend.scala](# ExecutorBackend)
5.  [ExecutorExitCode.scala](# ExecutorExitCode)
6.  [ExecutorLogUrlHandler.scala](# ExecutorLogUrlHandler)
7.  [ExecutorMetrics.scala](# ExecutorMetrics)
8.  [ExecutorMetricsPoller.scala](# ExecutorMetricsPoller)
9.  [ExecutorMetricsSource.scala](# ExecutorMetricsSource)
10.  [ExecutorSource.scala](# ExecutorSource)
11.  [InputMetrics.scala](# InputMetrics)
12.  [OutputMetrics.scala](# OutputMetrics)
13.  [ProcfsMetricsGetter.scala](# ProcfsMetricsGetter)
14.  [ShuffleReadMetrics.scala](# ShuffleReadMetrics)
15.  [ShuffleWriteMetrics.scala](# ShuffleWriteMetrics)
16.  [TaskMetrics.scala](# TaskMetrics)
17.  [基础拓展](# 基础拓展)

---

#### CoarseGrainedExecutorBackend

```markdown
private[spark] class CoarseGrainedExecutorBackend(override val rpcEnv: RpcEnv,driverUrl: String,
    executorId: String,bindAddress: String,hostname: String,cores: Int,userClassPath: Seq[URL],
    env: SparkEnv,resourcesFileOpt: Option[String]){
	关系: father --> IsolatedRpcEndpoint
    	sibling --> ExecutorBackend --> Logging
    介绍: 粗糙粒度的执行器后端
    构造器属性:
    	rpcEnv	RPC环境
    	driverUrl	驱动器地址
    	executorId	执行器ID
    	bindAddress	绑定地址
    	hostname	主机名称
    	cores	核心数量
    	userClassPath	用户类路径列表
    	env	spark环境
    	resourcesFileOpt	资源文件
    属性:
    #name @formats = DefaultFormats implicit	格式
    #name @stopping = new AtomicBoolean(false)	执行器状态位
    #name @executor=null #type @Executor	执行器
    #name @driver=None #type @Option[RpcEndpointRef] volatile	驱动器
    #name @ser #type @SerializerInstance	序列化实例
    	ser=env.closureSerializer.newInstance()
    	由于后端是支持多线程的,所以这里需要做出改变,以便于线程之间不共享序列化实例
    #name @taskResources=new mutable.HashMap[Long, Map[String, ResourceInformation]]	任务资源列表
    	映射taskId与其分配的资源关系
	操作集:
	def onStart(): Unit
	功能: 启动处理
	1. 获取资源
	val resources = parseOrFindResources(resourcesFileOpt)
	2. 根据driver的地址检索RPC地址,获取dirver,并向driver注册执行器信息
	rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      driver = Some(ref) // 获取driver
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
        extractAttributes, resources)) // 向driver发送注册执行器消息
    }(ThreadUtils.sameThread).onComplete {
      case Success(msg) =>
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    }(ThreadUtils.sameThread)
    
    def parseOrFindResources(resourcesFileOpt: Option[String]): Map[String, ResourceInformation]
    功能: 转换或者查找资源(只有当任务需求时才转换资源)
    val =parseResourceRequirements(env.conf, SPARK_TASK_PREFIX).nonEmpty ?
    	{
    		val resources = getOrDiscoverAllResources(env.conf, SPARK_EXECUTOR_PREFIX,
            	resourcesFileOpt) // 获取执行器资源
            if(resources.isEmpty) throw SparkException 
            else logResourceInfo(SPARK_EXECUTOR_PREFIX, resources)
    		val= resources
    	} :
    	{
    		// 不需要加载资源时返回空
    		val= Map.empty[String, ResourceInformation]
    	}
    
    def extractLogUrls: Map[String, String]
    功能: 提取日志URL(配置中以SPARK_LOG_URL_ 开头的配置信息)
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2))
      
   def extractAttributes: Map[String, String]
   功能: 抓取属性值
   val prefix = "SPARK_EXECUTOR_ATTRIBUTE_"
   sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toUpperCase(Locale.ROOT), e._2))
   
   def onDisconnected(remoteAddress: RpcAddress): Unit
   功能: 失去与指定RPC端点@remoteAddress连接的处理
   if (stopping.get()) {
      logInfo(s"Driver from $remoteAddress disconnected during shutdown")
    } else if (driver.exists(_.address == remoteAddress)) {
      // 指定的RPC端点恰好是驱动器的,则从驱动器中退出注册信息
      exitExecutor(1, s"Driver $remoteAddress disassociated! Shutting down.", null,
        notifyDriver = false)
    } else {
      logWarning(s"An unknown ($remoteAddress) driver disconnected.")
    }
    
    def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit
    功能: 更新指定任务@taskId的状态@state
    1. 获取任务资源
    val resources = taskResources.getOrElse(taskId, Map.empty[String, ResourceInformation])
    val msg = StatusUpdate(executorId, taskId, state, data, resources)
	2. 检查任务是否完成,完成则从任务资源表中移除当前taskId的表目
	if (TaskState.isFinished(state)) { taskResources.remove(taskId) }
	3. 驱动器发送执行器任务更新消息@msg
	driver match {
      case Some(driverRef) => driverRef.send(msg)
      case None => logWarning(s"Drop $msg because has not yet connected to driver")
    }
	
	def exitExecutor(code: Int,reason: String,throwable: Throwable = null,
		notifyDriver: Boolean = true): Unit
	功能: 执行器退出,这个函数可以被子类重载,以应对不同类型执行器的退出方案.比如当执行器崩溃时,后端不希望其父进	程退出执行
	val message = "Executor self-exiting due to : " + reason
    if (throwable != null) {
      logError(message, throwable)
    } else {
      logError(message)
    }
    if (notifyDriver && driver.nonEmpty) {
      driver.get.send(RemoveExecutor(executorId, new ExecutorLossReason(reason)))
    }
    System.exit(code)

	def receive: PartialFunction[Any, Unit]
	功能: 接受消息处理
	主要处理以下几种请求
	1. 注册执行器
	case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
      	// 创建执行器
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)
        // 驱动器通过RPC发送启动对应执行器消息
        driver.get.send(LaunchedExecutor(executorId))
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }
    2. 注册执行器失败
    case RegisterExecutorFailed(message) =>
      exitExecutor(1, "Slave registration failed: " + message)
	3. 运行任务
	case LaunchTask(data) =>
      if (executor == null) { // 没有可以执行的执行器,退出执行
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else { 
        val taskDesc = TaskDescription.decode(data.value) // 获取任务描述
        logInfo("Got assigned task " + taskDesc.taskId)
        taskResources(taskDesc.taskId) = taskDesc.resources // 注册任务的资源信息
        executor.launchTask(this, taskDesc) // 执行器运行任务
      }
   4. 解除任务
   case KillTask(taskId, _, interruptThread, reason) =>
      if (executor == null) {
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        executor.killTask(taskId, interruptThread, reason)
      }
   5. 停止执行器
   case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // 不能在这里直接关闭,因为还需要获取回调信息,所以使用self(在生命周期内)发起关闭信息,这样可以拿到回调信息
      self.send(Shutdown)
   6. 关闭(启动一个线程,关闭执行器)
   case Shutdown =>
      stopping.set(true)
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          // 执行器关闭executor.stop() 会调用@SparkEnv.stop(),这个东西会等到RPC环境完全关闭为之.
          // 然而executor.stop() 可能运行在RpcEnv线程中,然而RpcEnv 在停止之前无法调用@executor.stop(),这		   // 样就会引起死锁的发生(Spark-14180),一次这条指令需要放到一个新的线程中,进而解除死锁.(解除资源互斥)
          executor.stop()
        }
      }.start()
   7. 更新授权指令(添加授权指令到spark环境中)
   case UpdateDelegationTokens(tokenBytes) =>
      logInfo(s"Received tokens of ${tokenBytes.length} bytes")
      SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)
}
```

```markdown
private[spark] object CoarseGrainedExecutorBackend extends Logging {
	样例类:
	case class Arguments(driverUrl: String,executorId: String,bindAddress: String,hostname: String,
      cores: Int,appId: String, workerUrl: Option[String], userClassPath: mutable.ListBuffer[URL],
      resourcesFileOpt: Option[String])
     介绍: 参数样例
     
     操作集:
     def parseArguments(args: Array[String], classNameForEntry: String): Arguments
     功能: 将给定参数转化为参数集合@Arguments
	1. 初始化参数集合
	var driverUrl: String = null
    var executorId: String = null
    var bindAddress: String = null
    var hostname: String = null
    var cores: Int = 0
    var resourcesFileOpt: Option[String] = None
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()
    2. 展开参数列表@args 设置指定参数值
    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--bind-address") :: value :: tail =>
          bindAddress = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--resourcesFile") :: value :: tail =>
          resourcesFileOpt = Some(value)
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit(classNameForEntry)
      }
    }
    
    def printUsageAndExit(classNameForEntry: String): Unit
	功能: 打印使用情况,并退出(非正常退出)
	System.err.println(
      s"""
      |Usage: $classNameForEntry [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --bind-address <bindAddress>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --resourcesFile <fileWithJSONResourceInformation>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --user-class-path <url>
      |""".stripMargin)
    System.exit(1)
    
     def run(arguments: Arguments,
      backendCreateFn: (RpcEnv, Arguments, SparkEnv) => CoarseGrainedExecutorBackend): Unit 
	功能: 运行执行器后端
	输入参数:
		arguments	参数集合
		backendCreateFn	执行器后台形成函数
			((RpcEnv, Arguments, SparkEnv) => CoarseGrainedExecutorBackend)
	1. 初始化监视状态
	Utils.initDaemon(log)
	2. 运行执行器后端线程
	SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(arguments.hostname)
      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        arguments.bindAddress,
        arguments.hostname,
        -1,
        executorConf,
        new SecurityManager(executorConf),
        numUsableCores = 0,
        clientMode = true)
      var driver: RpcEndpointRef = null
      val nTries = 3
      for (i <- 0 until nTries if driver == null) {
        try {
          driver = fetcher.setupEndpointRefByURI(arguments.driverUrl)
        } catch {
          case e: Throwable => if (i == nTries - 1) {
            throw e
          }
        }
      }
      val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig)
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", arguments.appId))
      fetcher.shutdown()
      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }
      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }
      driverConf.set(EXECUTOR_ID, arguments.executorId)
      val env = SparkEnv.createExecutorEnv(driverConf, arguments.executorId, arguments.bindAddress,
        arguments.hostname, arguments.cores, cfg.ioEncryptionKey, isLocal = false)
      env.rpcEnv.setupEndpoint("Executor", backendCreateFn(env.rpcEnv, arguments, env))
      arguments.workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
      }
      env.rpcEnv.awaitTermination()
    }
    
    启动:
    def main(args: Array[String]): Unit
    1. 获取创建函数
    val createFn: (RpcEnv, Arguments, SparkEnv) =>
      CoarseGrainedExecutorBackend = { case (rpcEnv, arguments, env) =>
      new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
        arguments.bindAddress, arguments.hostname, arguments.cores, arguments.userClassPath, env,
        arguments.resourcesFileOpt)
    }
    2. 运行启动器后端
    run(parseArguments(args, this.getClass.getCanonicalName.stripSuffix("$")), createFn)
    3. 正常退出
    System.exit(0)
}
```

#### CommitDeniedException

```markdown
private[spark] class CommitDeniedException(msg: String,jobID: Int,splitID: Int,attemptNumber: Int){
	关系: father --> Exception(msg)
	介绍: 当任务请求提交到HDFS中,被driver质疑权限时.抛出这个异常
	操作集:
	def toTaskCommitDeniedReason: TaskCommitDenied = TaskCommitDenied(jobID, splitID, attemptNumber)
	功能: 获取任务被拒绝提交的原因@TaskCommitDenied,任务号@jobID,分片编号@splitID,请求编号@attemptNumber
}
```

#### Executor

```markdown
介绍:
	spark 执行器,由一个线程池返回,用与运行任务
	这个可以使用在mesos,yarn和独立运行状态的调度器下,通过远端RPC接口与驱动器交换数据.(处理Mesos的
	fine-grained模式)
```

```markdown
private[spark] class Executor(executorId: String,executorHostname: String,env: SparkEnv,
    userClassPath: Seq[URL] = Nil,isLocal: Boolean = false,
    uncaughtExceptionHandler: UncaughtExceptionHandler = new SparkUncaughtExceptionHandler){
	构造器属性:
    	executorId	执行器编号
    	executorHostname	执行器名称
    	env	spark环境
    	userClassPath	用户类路径列表
    	uncaughtExceptionHandler	未捕捉异常处理器
   	关系: father --> Logging
   	属性:
   	#name @executorShutdown = new AtomicBoolean(false) 	执行器关闭状态
   	#name @currentFiles: HashMap[String, Long] = new HashMap[String, Long]()	当前时刻依赖文件列表
   		当前该节点,使用@SparkContext 添加的任务依赖列表 [文件名称 --> 添加时间戳]
   	#name @currentJars=new HashMap[String, Long]()	当前时刻依赖Jar列表
   		当前该节点,使用@SparkContext 添加的任务依赖列表	[jar名称 --> 添加时间戳]
	#name @EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))	空字节缓冲区
	#name @conf=env.conf	spark配置信息
	#name @threadPool #type @ExecutorService	线程池
	开启worker的线程池,使用非中断类型线程去运行任务,这样运行程序时就不会被@Thread.interrupt() 中断.其他情况,比如说KAFKA-1894, HADOOP-10622,那样的话,某些方法被中断了就会一直处于挂起状态,不会再被调度.
	val= {
        val threadFactory = new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("Executor task launch worker-%d")
          .setThreadFactory((r: Runnable) => new UninterruptibleThread(r, "unused"))
          .build()
        Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
      }
	#name @executorSource = new ExecutorSource(threadPool, executorId) 执行器资源
	#name @taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper") 任务收获池
		用于监视kill和取消任务的线程池
	#name @taskReaperForTask= HashMap[Long, TaskReaper]() 任务未执行完毕映射表
		会获取最新的获取的任务@TaskReaper.所有对这个map的操作都需要同步,至于为什么不使用@ConcurrentHashMap
		是因为同步的目的仅仅是为了保护外部状态的完整性.设置它的目的是阻止给定任务每次调用@killTask 都要创建一		个回收对象@TaskReaper,相反这个map允许定位之前已经创建的回收对象,key对应于任务id
	#name @executorMetricsSource	执行器度量资源
		val= if (conf.get(METRICS_EXECUTORMETRICS_SOURCE_ENABLED)) Some(new ExecutorMetricsSource)
			else None
	#name @userClassPathFirst = conf.get(EXECUTOR_USER_CLASS_PATH_FIRST)	是否选择先加载用户jar包
		这个为true，则加载顺序先与spark jar包
	#name @taskReaperEnabled = conf.get(TASK_REAPER_ENABLED)	是否允许监控kill或者中断任务
	#name @urlClassLoader = createClassLoader()		URL加载器(sparkEnv创建之后获取,以便获取安全管理器)
	#name @replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)	回应加载器
		(sparkEnv创建之后获取,以便获取安全管理器)
	#name @plugins=Utils.withContextClassLoader(replClassLoader) {PluginContainer(env)}	插件管理器
		插件需要加载包含有执行器用户类路径的类加载器
	#name @maxDirectResultSize=Math.min(conf.get(TASK_MAX_DIRECT_RESULT_SIZE),
    	RpcUtils.maxMessageSizeBytes(conf))	最大直接结果大小(结果规模限制)
    	直接结果最大大小，如果任务结果值大于这个数，使用块管理器将结果发送回去
    #name @maxResultSize = conf.get(MAX_RESULT_SIZE)	最大结果大小(结果规模限制)
	#name @runningTasks = new ConcurrentHashMap[Long, TaskRunner]	运行任务列表
	#name @HEARTBEAT_MAX_FAILURES = conf.get(EXECUTOR_HEARTBEAT_MAX_FAILURES)	心跳允许最大失败次数
    	当执行器超过规定时间没有发送心跳信息给执行器时，需要自己kill掉。默认为60,且心跳周期为10.所以超过600s		就会停止掉该执行器。
    #name @HEARTBEAT_DROP_ZEROES = conf.get(EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES)
    	是否将空累加器发送到驱动器(空累加器可能使得心跳信息过多)
    #name @HEARTBEAT_INTERVAL_MS = conf.get(EXECUTOR_HEARTBEAT_INTERVAL)	心跳周期(ms)
    #name @METRICS_POLLING_INTERVAL_MS = conf.get(EXECUTOR_METRICS_POLLING_INTERVAL) 度量数据轮询周期
    #name @pollOnHeartbeat = if (METRICS_POLLING_INTERVAL_MS > 0) false else true 是否对心跳进行轮询
    #name @metricsPoller #type @ExecutorMetricsPoller	内存度量轮询器
        val= new ExecutorMetricsPoller(env.memoryManager,
            METRICS_POLLING_INTERVAL_MS,executorMetricsSource)
	#name @heartbeater #type @Heartbeater	执行器心跳机构
		val=new Heartbeater(() => Executor.this.reportHeartBeat(),
		"executor-heartbeater",HEARTBEAT_INTERVAL_MS)
	#name @heartbeatReceiverRef
		RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)
		心跳接受器参考(必须要在启动driver侧心跳@startDriverHeartbeat()之前初始化完成)
	#name @heartbeatFailures = 0	心跳失败计数器
	
	初始化操作:
	logInfo(s"Starting executor ID $executorId on host $executorHostname")
	功能: 登记启动信息
	
	Utils.checkHost(executorHostname)
	功能: 检查(断言)执行器主机名
	
	assert (0 == Utils.parseHostPort(executorHostname)._2)
	功能: 断言端口信息，保证不会出现已经分配的端口
	
	Utils.setCustomHostname(executorHostname)
	功能: 设置客户端主机名
		确保本地主机名与集群调度器上的这个主机名称要匹配
	
	if (!isLocal) Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
	功能: 设置非本地模式下未捕捉异常为指定@uncaughtExceptionHandler，确保线程是由于这个异常而kill整个执行器		进程
	
	if (!isLocal) {
        env.blockManager.initialize(conf.getAppId)
        env.metricsSystem.registerSource(executorSource)
        env.metricsSystem.registerSource(new JVMCPUSource())
        executorMetricsSource.foreach(_.register(env.metricsSystem))
        env.metricsSystem.registerSource(env.blockManager.shuffleMetricsSource)
  	}
  	功能: 非本地模式下，需要初始化块管理器，向度量系统中注册执行器资源@executorSource,@JVM CPU资源，shuffle	度量资源
	
	env.serializer.setDefaultClassLoader(replClassLoader)
	功能: 设置类加载器，用于序列化
	
	env.serializerManager.setDefaultClassLoader(replClassLoader)
	功能: 设置类加载器，用于序列化
    序列化管理器的内部实例Kryo,使用在netty线程中，用于获取远端缓存的RDD,所以也需要保证获取了正确的类加载器
	
	heartbeater.start()
	功能: 启动心跳机构
	
	metricsPoller.start()
	功能: 启动度量轮询器
	
	操作集:
	def numRunningTasks: Int = runningTasks.size()
	功能: 获取任务数量
	
	def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit
	功能: 运行任务
	输入参数:
		context	执行器后端
		taskDescription 任务描述
     1. 新建一个任务运行线程,将任务及其描述的信息插入到@runningTasks映射表中
     val tr = new TaskRunner(context, taskDescription)
     runningTasks.put(taskDescription.taskId, tr)
     2. 从线程池执行这个任务
     threadPool.execute(tr)
     
     def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit
     功能: kill 任务
     输入参数:
     	taskId	需要kill的任务
     	interruptThread	是否中断线程执行
     	reason	kill原因
     1. 获取需要kill的线程
     val taskRunner = runningTasks.get(taskId)
     2. 根据该任务是否可以被回收分为两种情况
     + 可以回收
     if(taskReaperEnabled){
     	// 获取新任务回收器
     	val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
          	// 从回收列表中查找,确认是否需要创建一个回收副本
            case None => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
          	// 需要回收，创建一个回收副本
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // 回收池执行线程，回收任务
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
     }
     + 不可回收，直接kill任务
     taskRunner.kill(interruptThread = interruptThread, reason = reason)
	
	def killAllTasks(interruptThread: Boolean, reason: String) : Unit
	功能: kill所有任务
	runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
      
    def stop(): Unit
    功能: 停止执行器
    if (!executorShutdown.getAndSet(true)) {
      env.metricsSystem.report() // 先想度量器汇报
      try { metricsPoller.stop() } // 停止轮询器
      catch {
        case NonFatal(e) =>
          logWarning("Unable to stop executor metrics poller", e)
      }
      try { heartbeater.stop() } // 停止心跳机构
      catch {
        case NonFatal(e) =>
          logWarning("Unable to stop heartbeater", e)
      }
      threadPool.shutdown() // 关闭线程池
      // 关闭所有插件
      Utils.withContextClassLoader(replClassLoader) { plugins.foreach(_.shutdown()) }
      if (!isLocal) env.stop() // 关闭非本地的spark Env
    }
    
    def computeTotalGcTime(): Long
    功能: 计算GC总时间
    val= ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
	
	def createClassLoader(): MutableURLClassLoader
	功能: 创建类加载器
	1. jar后缀设置
	val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }
    2. 将jar包集合中的jar包加入到类加载器，假定每个文件以及被获取
    val currentLoader = Utils.getContextOrSparkClassLoader
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri => // 获取地址集合
      new File(uri.split("/").last).toURI.toURL
    }
    3. 获取类加载器
    val= userClassPathFirst ? new ChildFirstURLClassLoader(urls, currentLoader) :
    	new MutableURLClassLoader(urls, currentLoader)
    
    def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader
    功能: 添加回应类加载器
    	如果REPL正在使用中，添加另一个类加载器，使用用户类型代码读取REPL定义的新类
    1. 获取类地址
    val classUri = conf.get("spark.repl.class.uri", null)
    2. 获取类加载器
    if(classUri == null) parent // REPL 不在使用中
    else{
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        // 反射获取一个实例
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    }
    
    def updateDependencies(newFiles: Map[String, Long], newJars: Map[String, Long]): Unit
   	功能: 更新依赖
   		如果收到一个新来自于@SparkContext的jar包和文件集合，则会下载丢失的依赖。添加新的jar到类加载器中。
   	输入参数:
   		newFiles	新文件映射表
   		newJars		新jar包映射表
   	1. 获取hadoop配置
   	lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
   	2. 获取丢失的文件依赖，便将其注册到当前文件映射表中
   	synchronized {
   		for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // 使用用户缓存代码获取文件，关闭本地模式下的缓存
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
   	}
   	3. 获取当前丢失的jar依赖，将丢失的部分注册到当前jar包映射器
   	for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // 使用用户缓存代码获取文件，关闭本地模式下的缓存
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
         // 将jar注册到类加载器中
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    
    def reportHeartBeat(): Unit
    功能: 汇报心跳机制
    1. 获取度量值
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]() //获取累加器度量列表
    val curGCTime = computeTotalGcTime() // 获取GC时间
    2. 对心跳机构进行轮询(有必要的情况下)
    if (pollOnHeartbeat) {
      metricsPoller.poll() }
    3. 获取执行器度量值更新
    val executorUpdates = metricsPoller.getExecutorUpdates()
    4. 每个任务将度量信息汇报给远端Driver的RPC端点
    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics() // 合并shuffle度量信息到driver端
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime) // 设定当前任务GC时间
        val accumulatorsToReport =
          if (HEARTBEAT_DROP_ZEROES) {
            taskRunner.task.metrics.accumulators().filterNot(_.isZero)
          } else {
            taskRunner.task.metrics.accumulators()
          }
          // 向汇报表中添加一条记录
        accumUpdates += ((taskRunner.taskId, accumulatorsToReport))
      }
    }
    5. 生成消息
    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId,
      executorUpdates)
    6. 发送消息给driver端，并获取driver端的回应
    try {
      // driver端同步请求该任务的回应
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
        message, new RpcTimeout(HEARTBEAT_INTERVAL_MS.millis, EXECUTOR_HEARTBEAT_INTERVAL.key))
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
}
```

#subclass @TaskRunner

```markdown
class TaskRunner(execBackend: ExecutorBackend,private val taskDescription: TaskDescription){
	关系: father --> Runnable
	构造器属性:
		execBackend	执行器后端
		taskDescription	任务描述
	属性:
	#name @taskId = taskDescription.taskId	任务ID
	#name @threadName = s"Executor task launch worker for task $taskId"	线程名称
	#name @taskName = taskDescription.name	任务名称
	#name @reasonIfKilled=None #type @Option[String] volatile	取消任务原因
	#name @threadId=-1 volatile	线程ID
	#name @finished = false GuardedBy("TaskRunner.this")	结束标志位
	#name @startGCTime=_ volatile	任务开始运行时GC的时间
	#name @task #type @Task[Any] volatile 运行的任务
		反序列化来自于driver的任务，一旦设置就不会改变
	操作集:
	def hasFetchFailure: Boolean
	功能: 确定是否获取失败
	val= task != null && task.context != null && task.context.fetchFailed.isDefined
	
	def setTaskFinishedAndClearInterruptStatus(): Unit
	功能: 设置任务完成，且清除中断状态
	synchronized {
		this.finished = true
		// spark-14234  重置线程中断状态，避免执行@execBackend.statusUpdate 时出现中断关闭异常
		// @ClosedByInterruptException
		Thread.interrupted()
		// 唤醒等待的任务回收线程
		notifyAll()
	}
	
	def collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs: Long)(Seq(),Seq())
	功能: 
    1. 报告执行器运行时和JVM GC时间
    Option(task).foreach(t => {
        t.metrics.setExecutorRunTime(TimeUnit.NANOSECONDS.toMillis(
          System.nanoTime() - taskStartTimeNs))
        t.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
      })
    2. 更新累加器
    val accums: Seq[AccumulatorV2[_, _]] =
    Option(task).map(_.collectAccumulatorUpdates(taskFailed = true)).getOrElse(Seq.empty)
    val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))
    3. 设置结束标志位及清除中断状态
    setTaskFinishedAndClearInterruptStatus()
    val= (accums, accUpdates)
    
    def run(): Unit 
    功能: 线程运行体逻辑
    Part 1: 参数初始化
    1. 获取线程相关信息
    threadId = Thread.currentThread.getId // 获取线程ID
    Thread.currentThread.setName(threadName) // 获取线程名称
    val threadMXBean = ManagementFactory.getThreadMXBean // 获取JVM的线程系统管理Bean
    Thread.currentThread.setContextClassLoader(replClassLoader) // 设置类加载器
    2. 获取相关资源参数
    // 获取任务内存管理器
    val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
    // 获取序列化/CPU开始时间
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
    	threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = env.closureSerializer.newInstance() // 获取序列化实例
    execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER) // 执行器后端更新任务状态
    var taskStartTimeNs: Long = 0	// 任务开始时间
    var taskStartCpu: Long = 0 // 任务开始的CPU时间
    startGCTime = computeTotalGcTime() // 起始时GC时间
    var taskStarted: Boolean = false // 任务开始标志
    Part 2: 线程运行过程
    1. 更新依赖
    // 设置反序列化参数,必须要在更新依赖@updateDependencies 之前调用，获取依赖需要它的属性
    Executor.taskDeserializationProps.set(taskDescription.properties)  
    updateDependencies(taskDescription.addedFiles, taskDescription.addedJars) // 更新依赖
    2. 获取任务相关信息
    task = ser.deserialize[Task[Any]](taskDescription.serializedTask,
    	Thread.currentThread.getContextClassLoader) // 获取任务
	task.localProperties = taskDescription.properties // 获取任务贝蒂属性
	task.setTaskMemoryManager(taskMemoryManager) // 设置任务的任务内存管理器
	3. kill 处理(当任务被kill,则应当停止,否则继续执行任务)
    val killReason = reasonIfKilled
    if (killReason.isDefined) { throw new TaskKilledException(killReason.get)}
    4. 处理非本地模式下的Map输出定位器@MapOutputTrackerMaster 
    // 这里更新时间点(epoch)是要在获取失败之后使得执行器map输出状态失效。本地模式下，@env.mapOutputTracker会	// 变成@MapOutputTrackerMaster
    if (!isLocal) {
        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].updateEpoch(task.epoch)
    }
    5. 启动轮询器
    metricsPoller.onTaskStart(taskId, task.stageId, task.stageAttemptId)
    taskStarted = true
    6. 获取开始时间参数
    taskStartTimeNs = System.nanoTime()
    taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
  	  threadMXBean.getCurrentThreadCpuTime
    } else 0L
    var threwException = true
    7. 运行任务@task，获取任务执行结果value
    val value = Utils.tryWithSafeFinally {
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem,
            resources = taskDescription.resources)
          threwException = false
          res
        } {
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          if (freedMemory > 0 && !threwException) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.get(UNSAFE_EXCEPTION_ON_MEMORY_LEAK)) {
              throw new SparkException(errMsg)
            } else {
              logWarning(errMsg)
            }
          }

          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
                releasedLocks.mkString("[", ", ", "]")
            if (conf.get(STORAGE_EXCEPTION_PIN_LEAK)) {
              throw new SparkException(errMsg)
            } else {
              logInfo(errMsg)
            }
          }
        }
    8. 获取失败的处理方案
    task.context.fetchFailed.foreach { fetchFailure =>
          logError(s"TID ${taskId} completed successfully though internally it encountered " +
            s"unrecoverable fetch failures!  Most likely this means user code is incorrectly " +
            s"swallowing Spark's internal ${classOf[FetchFailedException]}", fetchFailure) }
    9. 获取结束时时间参数
    val taskFinishNs = System.nanoTime()
    val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime} else 0L
    10. 任务被kill，直接使得其失败
    task.context.killTaskIfInterrupted()
    11. 获取序列化实例，并对，获取任务执行结果value序列化
    val resultSer = env.serializer.newInstance()
    val beforeSerializationNs = System.nanoTime()
    val valueBytes = resultSer.serialize(value)
    val afterSerializationNs = System.nanoTime()
    12. 反序列化度量参数(发生在两种情况:序列化任务对象@Task,Task.run()反序列化RDD)
    task.metrics.setExecutorDeserializeTime(TimeUnit.NANOSECONDS.toMillis(
          (taskStartTimeNs - deserializeStartTimeNs) + task.executorDeserializeTimeNs))
    task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
    // 去除Task.run()的反序列化，避免二次计算
    task.metrics.setExecutorRunTime(TimeUnit.NANOSECONDS.toMillis(
          (taskFinishNs - taskStartTimeNs) - task.executorDeserializeTimeNs))
    task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
    task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
    task.metrics.setResultSerializationTime(TimeUnit.NANOSECONDS.toMillis(
          afterSerializationNs - beforeSerializationNs))
    13. 更新度量计数器
    executorSource.METRIC_CPU_TIME.inc(task.metrics.executorCpuTime)
    executorSource.METRIC_RUN_TIME.inc(task.metrics.executorRunTime)
    executorSource.METRIC_JVM_GC_TIME.inc(task.metrics.jvmGCTime)
    executorSource.METRIC_DESERIALIZE_TIME.inc(task.metrics.executorDeserializeTime)
    executorSource.METRIC_DESERIALIZE_CPU_TIME.inc(task.metrics.executorDeserializeCpuTime)
    executorSource.METRIC_RESULT_SERIALIZE_TIME.inc(task.metrics.resultSerializationTime)
    executorSource.METRIC_SHUFFLE_FETCH_WAIT_TIME
    	.inc(task.metrics.shuffleReadMetrics.fetchWaitTime)
    executorSource.METRIC_SHUFFLE_WRITE_TIME.inc(task.metrics.shuffleWriteMetrics.writeTime)
    executorSource.METRIC_SHUFFLE_TOTAL_BYTES_READ
    	.inc(task.metrics.shuffleReadMetrics.totalBytesRead)
    executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ
    	.inc(task.metrics.shuffleReadMetrics.remoteBytesRead)
    executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK
    	.inc(task.metrics.shuffleReadMetrics.remoteBytesReadToDisk)
    executorSource.METRIC_SHUFFLE_LOCAL_BYTES_READ
    	.inc(task.metrics.shuffleReadMetrics.localBytesRead)
    executorSource.METRIC_SHUFFLE_RECORDS_READ
    	.inc(task.metrics.shuffleReadMetrics.recordsRead)
    executorSource.METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED
    	.inc(task.metrics.shuffleReadMetrics.remoteBlocksFetched)
    executorSource.METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED
    	.inc(task.metrics.shuffleReadMetrics.localBlocksFetched)
    executorSource.METRIC_SHUFFLE_BYTES_WRITTEN
    	.inc(task.metrics.shuffleWriteMetrics.bytesWritten)
    executorSource.METRIC_SHUFFLE_RECORDS_WRITTEN
    	.inc(task.metrics.shuffleWriteMetrics.recordsWritten)
    executorSource.METRIC_INPUT_BYTES_READ
    	.inc(task.metrics.inputMetrics.bytesRead)
    executorSource.METRIC_INPUT_RECORDS_READ
    	.inc(task.metrics.inputMetrics.recordsRead)
    executorSource.METRIC_OUTPUT_BYTES_WRITTEN
    	.inc(task.metrics.outputMetrics.bytesWritten)
    executorSource.METRIC_OUTPUT_RECORDS_WRITTEN
    	.inc(task.metrics.outputMetrics.recordsWritten)
    executorSource.METRIC_RESULT_SIZE.inc(task.metrics.resultSize)
    executorSource.METRIC_DISK_BYTES_SPILLED.inc(task.metrics.diskBytesSpilled)
    executorSource.METRIC_MEMORY_BYTES_SPILLED.inc(task.metrics.memoryBytesSpilled)
    14. 获取度量值
    val accumUpdates = task.collectAccumulatorUpdates() // 更新任务累加器
    val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId) // 获取任务峰值度量值列表
    15. 获取结果
    // 获取任务直接返回结果
    val directResult = new DirectTaskResult(valueBytes, accumUpdates, metricPeaks)
    val serializedDirectResult = ser.serialize(directResult) // 反序列化结果
    val resultSize = serializedDirectResult.limit() // 获取结果规模大小
    16. 获取发送到driver的数据(resultSize< MAX_resultSize)
    	val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }
    17. 传输数据，并返回标志位
    executorSource.SUCCEEDED_TASKS.inc(1L) // 成功任务+1
    setTaskFinishedAndClearInterruptStatus() // 设置任务结束，并清除中断标志
    execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult) //后端更新任务状态
    0. 异常处理
    + 任务kill失败
    case t: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId), reason: ${t.reason}")
          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
          val metricPeaks = WrappedArray.make(metricsPoller.getTaskMetricPeaks(taskId))
          val serializedTK = ser.serialize(TaskKilled(t.reason, accUpdates, accums, metricPeaks))
          execBackend.statusUpdate(taskId, TaskState.KILLED, serializedTK)
     + 中断异常
     case _: InterruptedException | NonFatal(_) if
            task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId), reason: $killReason")
          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
          val metricPeaks = WrappedArray.make(metricsPoller.getTaskMetricPeaks(taskId))
          val serializedTK = ser.serialize(TaskKilled(killReason, accUpdates, accums, metricPeaks))
          execBackend.statusUpdate(taskId, TaskState.KILLED, serializedTK)
   	+ 存在有获取数据失败的情况
   	case t: Throwable if hasFetchFailure && !Utils.isFatalError(t) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            val fetchFailedCls = classOf[FetchFailedException].getName
            logWarning(s"TID ${taskId} encountered a ${fetchFailedCls} and " +
              s"failed, but the ${fetchFailedCls} was hidden by another " +
              s"exception.  Spark is handling this like a fetch failure and ignoring the " +
              s"other exception: $t")
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))	
    + 提交权限不足
    case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskCommitDeniedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))
    + 任务停止
    case t: Throwable if env.isStopped =>
          logError(s"Exception in $taskName (TID $taskId): ${t.getMessage}")
  	+ 其他异常/错误
  	logError(s"Exception in $taskName (TID $taskId)", t)
  	if (!ShutdownHookManager.inShutdown()) {
            val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
            val metricPeaks = WrappedArray.make(metricsPoller.getTaskMetricPeaks(taskId))
            val serializedTaskEndReason = {
              try {
                val ef = new ExceptionFailure(t, accUpdates).withAccums(accums)
                  .withMetricPeaks(metricPeaks)
                ser.serialize(ef)
              } catch {
                case _: NotSerializableException =>
                  // t is not serializable so just send the stacktrace
                  val ef = new ExceptionFailure(t, accUpdates, false).withAccums(accums)
                    .withMetricPeaks(metricPeaks)
                  ser.serialize(ef)
              }
            }
            setTaskFinishedAndClearInterruptStatus()
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }
     if (!t.isInstanceOf[SparkOutOfMemoryError] && Utils.isFatalError(t)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)}
     -1. 结束处理
     从运行任务列表中移除当前任务条目
     runningTasks.remove(taskId)
     if (taskStarted) { metricsPoller.onTaskCompletion(taskId, task.stageId, task.stageAttemptId)}
}
```

#subclass @TaskReaper

```
介绍:
	通过发送中断标志位,监督被kill以及放弃的任务.发送@Thread.interrupt(),监视任务直到其完成。
	spark当前任务的取消或者kill原理遵循"尽最大努力"的原则。因为有一些任务可能不是被中断的，或者是没有设置killed标志。如果集群任务槽重要部分被那些标记为killed的任务，这些依然在运行，且会导致新的job或者task出现"饥饿"资源的情况，出现这些资源分配不当时因为这些任务的资源没有归还给系统。
	@TaskReaper 在SPARK-18761中作为一个机理去监视和清理这些"行尸走肉"的任务。对于"僵尸"任务，这个组件默认会取消作用，可以通过设置@spark.task.reaper.enabled=true 开启清理"僵尸"任务的功能。
	@TaskReaper 当任务发送kill或者取消任务时会创建。典型的来说，一个任务会仅仅有一个@TaskReaper，但是对于一个任务来说,可能有超过两个任务需要去清理，这种情况下，kill会被调用两次，但是中断参数却是不同的。
	一旦创建，这个回收任务便会监视，直到主任务都结束了。如果@TaskReaper 没有配置去kill JVM在一个指定时限内(timeout)，那么只要监视任务不退出运行，这个回收任务会一直运行。
```
```markdown
private class TaskReaper(taskRunner: TaskRunner,val interruptThread: Boolean,val reason: String){
	构造器属性:
		taskRunner	运行任务的线程
		interruptThread	线程是否可以被中断
		reason	中断原因
	属性:
	#name @taskId=taskRunner.taskId		任务ID
	#name @killPollingIntervalMs=conf.get(TASK_REAPER_POLLING_INTERVAL)	kill的轮询时间间隔
	#name @killTimeoutNs=TimeUnit.MILLISECONDS.toNanos(conf.get(TASK_REAPER_KILL_TIMEOUT))
		kill任务的时间限制
	#name @takeThreadDump=takeThreadDump	是否抛弃线程
	操作集:
	def run(): Unit
	功能: 线程运行体逻辑
	1. 计算经过时间，并判断是否超过时限
	val startTimeNs = System.nanoTime()
    def elapsedTimeNs = System.nanoTime() - startTimeNs
	def timeoutExceeded(): Boolean = killTimeoutNs > 0 && elapsedTimeNs > killTimeoutNs
	2. kill任务
	taskRunner.kill(interruptThread = interruptThread, reason = reason)
	3. 结束任务，清除中断状态位
	while (!finished && !timeoutExceeded()) {
		taskRunner.synchronized {
		   // 检查并关闭运行中任务@taskRepear
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          
          if (taskRunner.isFinished) {
            finished = true
          } else {
          // 运行任务@taskRunner没有结束，计算用时，如果允许执行thread-dump，则获取thread-dump信息
            val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)
            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
            if (takeThreadDump) {
              try {
                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
         // 任务没有结束，且运行超时，则 抛出错误信息(本地日志形式，在远端则抛出异常)
         if (!taskRunner.isFinished && timeoutExceeded()) {
          val killTimeoutMs = TimeUnit.NANOSECONDS.toMillis(killTimeoutNs)
          if (isLocal) {
            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
              "not killing JVM because we are running in local mode.")
          } else {
            throw new SparkException(
              s"Killing executor JVM because killed task $taskId could not be stopped within " +
                s"$killTimeoutMs ms.")
          }
        }
	}
	4. 最后将这个运行任务的记录从@taskReaperForTask 任务回收映射表中移除
	taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
            }
          }
    }
}
```

```markdown
private[spark] object Executor {
	属性:
	#name @taskDeserializationProps=new ThreadLocal[Properties]	任务反序列化属性
}
```

#### ExecutorBackend

```markdown
private[spark] trait ExecutorBackend {
	介绍: 执行器后端
		一个可插入的接口,执行器使用它去设置更新集群调度器.
	操作集:
	def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit
	输入参数:
		taskId	任务id
		state	任务状态
		data	数据
}
```

#### ExecutorExitCode

```markdown
介绍:
	执行器退出码,执行器需要提供信息给master,这些执行器执行失败信息假定在集群管理框架能够捕捉退出码(可能没有日志文件).退出码常数与正常退出码(JVM或者用户代码引起的)不应当起冲突.特殊的,退出码在128以上出现在类Unix上,作为信号的结果,OpenJdk的JVM使用退出码1在其"最后一次尝试"代码中.
```

```markdown
private[spark] object ExecutorExitCode {
	属性:
	#name @DISK_STORE_FAILED_TO_CREATE_DIR = 53	退出码: 磁盘存储不能在多次请求之后创建本地临时目录
	#name @EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE = 54 退出码: 多次请求之后外部块存储不能初始化
	#name @EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR=55 退出码: 多次请求之后,外部块存储不能创建临时目录
	#name @HEARTBEAT_FAILURE = 56	退出码: 心跳失败
		执行器不能够发送心跳给驱动器的次数超过了@spark.executor.heartbeat.maxFailures 设置的次数
	操作集:
	def explainExitCode(exitCode: Int): String
	功能: 解释退出码
	exitCode match {
      case UNCAUGHT_EXCEPTION => "Uncaught exception" // 未捕捉异常
      case UNCAUGHT_EXCEPTION_TWICE => "Uncaught exception, and logging the exception failed"
      case OOM => "OutOfMemoryError" // OOM
      case DISK_STORE_FAILED_TO_CREATE_DIR => 
        "Failed to create local directory (bad spark.local.dir?)"
      case EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE => "ExternalBlockStore failed to initialize."
      case EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR =>
        "ExternalBlockStore failed to create a local temporary directory."
      case HEARTBEAT_FAILURE =>
        "Unable to send heartbeats to driver."
      case _ => // 其他情况
        "Unknown executor exit code (" + exitCode + ")" + (
          if (exitCode > 128) {
            " (died from signal " + (exitCode - 128) + "?)"
          } else {
            ""
          }
        )
    }
}
```

#### ExecutorLogUrlHandler

```markdown
private[spark] class ExecutorLogUrlHandler(logUrlPattern: Option[String]){
	关系: father --> Logging
	介绍: 执行器日志URI处理器
	构造器属性:
		logUrlPattern	日志Url格式
	属性:
	#name @informedForMissingAttributes = new AtomicBoolean(false) 错过属性是否要提示
	操作集:
	def applyPattern(logUrls: Map[String, String],
      attributes: Map[String, String]): Map[String, String]
	功能: 对属性表@attributes 应用格式@logUrls
	val= logUrlPattern match {
      case Some(pattern) => doApplyPattern(logUrls, attributes, pattern)
      case None => logUrls }
    
    def doApplyPattern(logUrls: Map[String, String],attributes: Map[String, String],
      	urlPattern: String): Map[String, String]
    输入参数:
    	logUrls: 日志URI表
    	attributes	属性表
    	urlPattern	URI格式
    功能: 给定不知道类型的日志文件,,可能来自于资源管理器,需要资源管理器去提供可用的日志文件.这里建议使用与原始	日志URL相同类型的日志形式.一旦获取了日志文件列表,需要将其暴露给用户,以便于用户可以构成包含日志文件的用户日	志地址.
	1. 获取格式参数
	val allPatterns = CUSTOM_URL_PATTERN_REGEX.findAllMatchIn(urlPattern).map(_.group(1)).toSet
    val allPatternsExceptFileName = allPatterns.filter(_ != "FILE_NAME")
    2. 获取属性key值
    val allAttributeKeys = attributes.keySet
    val allAttributeKeysExceptLogFiles = allAttributeKeys.filter(_ != "LOG_FILES")
    3. 获取日志URL映射表
    val= allPatternsExceptFileName.diff(allAttributeKeysExceptLogFiles).nonEmpty ?
    {
    	logFailToRenewLogUrls("some of required attributes are missing in app's event log.",
        	allPatternsExceptFileName, allAttributeKeys)
        logUrls
    } : allPatterns.contains("FILE_NAME") && !allAttributeKeys.contains("LOG_FILES") ?
    {
    	logFailToRenewLogUrls("'FILE_NAME' parameter is provided, but file information is " +
        	"missing in app's event log.", allPatternsExceptFileName, allAttributeKeys)
      	logUrls
    } :
    {
    	val updatedUrl = allPatternsExceptFileName.foldLeft(urlPattern) { case (orig, patt) =>
        orig.replace(s"{{$patt}}", attributes(patt))
      	}
        if (allPatterns.contains("FILE_NAME")) {
          attributes("LOG_FILES").split(",").map { file =>
          	file -> updatedUrl.replace("{{FILE_NAME}}", file)
          }.toMap
        } else {
       	 Map("log" -> updatedUrl)
        }
    }
  
  	def logFailToRenewLogUrls(reason: String,allPatterns: Set[String],
      allAttributes: Set[String]): Unit 
    功能: 更新日志映射失败
        if (informedForMissingAttributes.compareAndSet(false, true)) {
          logInfo(s"Fail to renew executor log urls: $reason. Required: $allPatterns / " +
            s"available: $allAttributes. Falling back to show app's original log urls.")
        }
}
```

```markdown
private[spark] object ExecutorLogUrlHandler {
	val CUSTOM_URL_PATTERN_REGEX: Regex = "\\{\\{([A-Za-z0-9_\\-]+)\\}\\}".r
	用户URL正则形式
}
```

#### ExecutorMetrics

```markdown
@DeveloperApi
class ExecutorMetrics private[spark] extends Serializable {
	介绍: 执行器/驱动器的度量器定位
	执行器等级度量器发送于每个执行器,发送到驱动器,作为心跳@Heartbeat 的一部分信息
	属性:
	#name @metrics = new Array[Long](ExecutorMetricType.numMetrics) 计量值列表
		计量值由@ExecutorMetricType.metricToOffset 编号
	初始化:
	metrics(0) = -1
	功能: 初始化度量值列表计数值
	
	构造器:
	private[spark] def this(metrics: Array[Long]) {
        this()
        Array.copy(metrics, 0, this.metrics, 0, Math.min(metrics.size, this.metrics.size))
  	}
  	功能: 根据@metrics 度量器列表设置构造器
  	
  	private[spark] def this(metrics: AtomicLongArray) {
    	this()
    	ExecutorMetricType.metricToOffset.foreach { case (_, i) =>
      		this.metrics(i) = metrics.get(i)
    	}
  	}
	
	private[spark] def this(executorMetrics: Map[String, Long]) {
        this()
        ExecutorMetricType.metricToOffset.foreach { case (name, idx) =>
          metrics(idx) = executorMetrics.getOrElse(name, 0L)
        }
      }
    功能: 使用给定映射关系@executorMetrics 创建执行器度量器  
     
	操作集:
	def getMetricValue(metricName: String): Long
	功能: 获取指定度量器名称@metricName 的计量值
	val= metrics(ExecutorMetricType.metricToOffset(metricName))
	
	def isSet(): Boolean = metrics(0) > -1
	功能: 检查是否开始度量
	
	def compareAndUpdatePeakValues(executorMetrics: ExecutorMetrics): Boolean
	功能: 比较并更新峰值
		比较指定执行器度量值与当前度量器度量值,当新的度量值更大的时候,更新度量值
	输入参数: executorMetrics	需要做比较的执行器度量器,如果更新则返回true
	var updated = false
    (0 until ExecutorMetricType.numMetrics).foreach { idx =>
      if (executorMetrics.metrics(idx) > metrics(idx)) {
        updated = true
        metrics(idx) = executorMetrics.metrics(idx)
      }
    }
    updated
}
```

```markdown
private[spark] object ExecutorMetrics{
	操作集:
	def getCurrentMetrics(memoryManager: MemoryManager): Array[Long]
	功能: 获取当前度量器列表
	1. 确定维度
	val currentMetrics = new Array[Long](ExecutorMetricType.numMetrics)
	var offset = 0 // 初始化偏移量
	2. 设置度量值
	ExecutorMetricType.metricGetters.foreach { metricType =>
      val metricValues = metricType.getMetricValues(memoryManager)
      Array.copy(metricValues, 0, currentMetrics, offset, metricValues.length)
      offset += metricValues.length
    }
    val= currentMetrics
}
```

#### ExecutorMetricsPoller

```markdown
private[spark] class ExecutorMetricsPoller(memoryManager: MemoryManager,pollingInterval: Long,
    executorMetricsSource: Option[ExecutorMetricsSource]){
	关系: father --> Logging
    介绍: 这个类用于轮询执行器度量器，对于每个stage每个任务定位其峰值(peak).每个执行器维护了一个这个类的实例。
    轮询方法轮询执行器度量器，既运行在自己的线程上，又根据spark配置被执行器心跳线程调用.
    这个类维护了两个@ConcurrentHashMap 通过执行器线程并发的使用轮询线程获取。
    一个线程在另一个线程正在读取时进行更新的工作，所以读取线程可能不会获取最新的度量信息，但是这个是OK的。
    在每个stage/task中，去定位执行器度量的峰值信息，每个stage的峰值信息会通过执行器心跳发送给驱动器。这样在任	务运行的时候使用增量更新去更新度量值。且如果执行器挂了，仍就是有一些度量信息的。每个任务的峰值信息会在任务	结束时，放在任务结果中。对于短任务来说是相当有效的。如果在任务运行期间，没有心跳的存在，仍然需要通过轮询去获	  取度量信息。
    构造器属性:
    	memoryManager	内存管理器
    	pollingInterval	轮询间隔
  	样例类:
  	case class TCMP(count: AtomicLong, peaks: AtomicLongArray)
  	介绍: TCMP协议，携带信息@count 任务计数信息 @peaks 峰值列表
  	
  	属性：
  	#name @stageTCMP = new ConcurrentHashMap[StageKey, TCMP]	stage计量值映射表
  		处理(stageId, stageAttemptId) 与 (count of running tasks, executor metric peaks)的映射
  	#name @taskMetricPeaks = new ConcurrentHashMap[Long, AtomicLongArray] 任务计量值映射表
  		映射关系 taskId --> 执行器度量峰值列表
  	#name @poller= pollingInterval > 0 ? 
  		Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-metrics-poller"))
  		: None
  		轮询器
  	操作集:
  	def start(): Unit 
  	功能: 启动轮询线程
  	val= {
        poller.foreach { exec =>
          val pollingTask: Runnable = () => Utils.logUncaughtExceptions(poll())
          exec.scheduleAtFixedRate(pollingTask, 0L, pollingInterval, TimeUnit.MILLISECONDS)
        }
      }
  		
  	def stop(): Unit
  	功能: 停止轮询线程
  	val= {
        poller.foreach { exec =>
          exec.shutdown()
          exec.awaitTermination(10, TimeUnit.SECONDS)
        }
      }
    
    def onTaskStart(taskId: Long, stageId: Int, stageAttemptId: Int): Unit
    功能: 任务开始时操作
    1. 初始化该任务的度量信息
    taskMetricPeaks.put(taskId, new AtomicLongArray(ExecutorMetricType.numMetrics))
    2. 初始化指定stage@stageId的度量信息
    val countAndPeaks = stageTCMP.computeIfAbsent((stageId, stageAttemptId),
      _ => TCMP(new AtomicLong(0), new AtomicLongArray(ExecutorMetricType.numMetrics)))
    val stageCount = countAndPeaks.count.incrementAndGet() // count信息+1(由于stage的put操作)
    
    def onTaskCompletion(taskId: Long, stageId: Int, stageAttemptId: Int): Unit
    功能: 任务完成的操作--> 减少task计数，如果stage的count值到0移除这个键值对
    操作条件: 之前调用过@onTaskStart 且参数相同
    1. 减少计数值操作
    def decrementCount(stage: StageKey, countAndPeaks: TCMP): TCMP = {
      val countValue = countAndPeaks.count.decrementAndGet()
      if (countValue == 0L) {
        logDebug(s"removing (${stage._1}, ${stage._2}) from stageTCMP")
        null
      } else {
        logDebug(s"stageTCMP: (${stage._1}, ${stage._2}) -> " + countValue)
        countAndPeaks
      }
    }
    2. 处理stage计量信息,decrementCount不为零则重新映射，否则移除
    stageTCMP.computeIfPresent((stageId, stageAttemptId), decrementCount)
    3. 移除task计量信息
    taskMetricPeaks.remove(taskId)
    
    def getTaskMetricPeaks(taskId: Long): Array[Long]
    功能: 获取任务计量参数列表
    1. 获取度量信息
    val currentPeaks = taskMetricPeaks.get(taskId) // 可能为空
    val metricPeaks = new Array[Long](ExecutorMetricType.numMetrics)
    2. 装入度量信息
    if (currentPeaks != null) {
      ExecutorMetricType.metricToOffset.foreach { case (_, i) =>
        metricPeaks(i) = currentPeaks.get(i)
      }
    }
    val= currentPeaks
    
    def getExecutorUpdates(): HashMap[StageKey, ExecutorMetrics]
    功能: 执行器中的@reportHeartBeat 函数调用，用于传输计量信息给执行器的心跳@Heartbeater
    在返回执行器更新前重置了@stageTCMP 中的计量峰值列表。因此，执行器更新包含了从上次心跳开始每个stage的计量	峰值信息(增量式获取峰值信息)
    1. 获取上一次的stage计量峰值列表信息
    val executorUpdates = new HashMap[StageKey, ExecutorMetrics]
    2. 重置stage峰值列表
    def getUpdateAndResetPeaks(k: StageKey, v: TCMP): TCMP = {
      executorUpdates.put(k, new ExecutorMetrics(v.peaks))
      TCMP(v.count, new AtomicLongArray(ExecutorMetricType.numMetrics))
    }
    3. 替换原先的@stageTCMP
    stageTCMP.replaceAll(getUpdateAndResetPeaks)
	val= executorUpdates
	
	def poll(): Unit
	功能: 轮询执行器的计量信息，开始，轮询间隔(周期)为正，按照这个时间为周期定期调度。否则使用执行器中的
    @reportHeartBeat 将计量信息传入心跳中。注意到线程可以并发的更新和读取计量峰值信息。
    1. 获取最新计量信息，并更新计量信息的快照信息
    val latestMetrics = ExecutorMetrics.getCurrentMetrics(memoryManager)
    executorMetricsSource.foreach(_.updateMetricsSnapshot(latestMetrics))
    2. 更新计量峰值列表
    def updatePeaks(metrics: AtomicLongArray): Unit = {
      (0 until metrics.length).foreach { i =>
        metrics.getAndAccumulate(i, latestMetrics(i), math.max)
      }
    }
    3. 更新stage的计量信息列表
    stageTCMP.forEachValue(LONG_MAX_VALUE, v => updatePeaks(v.peaks))
    4. 更新task计量信息列表
    taskMetricPeaks.forEachValue(LONG_MAX_VALUE, updatePeaks)
}
```

#### ExecutorMetricsSource

```markdown
介绍:
	使用后端度量系统暴露来自@ExecutorMetricsType 的执行器度量器
	与内存系统相关的度量参数收集起来开销很大.因此做出了如下的优化方案:
	1. 缓存度量参数值,每次心跳(heartbeat 周期为10s)更新.使用可供选择的,更快的轮询策略.通过设置
	spark.executor.metrics.pollingInterval=<interval in ms>启动轮询.
	2. Procfs度量系统收集所有单向度量参数,只有当proc系统存在,且允许处理树状度量信息
		spark.eventLog.logStageExecutorProcessTreeMetrics.enabled=true且
		spark.eventLog.logStageExecutorMetrics.enabled=true.
```

```markdown
private[spark] class ExecutorMetricsSource extends Source {
	属性:
	#name @metricRegistry = new MetricRegistry()	度量注册器
	#name @sourceName = "ExecutorMetrics"	度量源名称
	#name @metricsSnapshot #type @Array[Long]	度量快照列表
		val= Array.fill(ExecutorMetricType.numMetrics)(0L)
	操作集:
	def updateMetricsSnapshot(metricsUpdates: Array[Long]): Unit
	功能: 更新度量快照列表,由@ExecutorMetricsPoller 调用
		metricsSnapshot = metricsUpdates
	
	def register(metricsSystem: MetricsSystem): Unit
	功能: 注册度量系统@metricsSystem
	1. 获取计量值列表
	val gauges: IndexedSeq[ExecutorMetricGauge] = (0 until ExecutorMetricType.numMetrics).map {
      idx => new ExecutorMetricGauge(idx)
    }.toIndexedSeq
    2. 注册度量信息
    ExecutorMetricType.metricToOffset.foreach {
      case (name, idx) =>	// 度量参数名称-> 度量值
        metricRegistry.register(MetricRegistry.name(name), gauges(idx))
    }
	metricsSystem.registerSource(this)
	
    内部类:
    private class ExecutorMetricGauge(idx: Int) extends Gauge[Long]
    介绍: 执行器计量器
    操作集:
    def getValue: Long = metricsSnapshot(idx)
    功能: 从快照中获取指定@idx的度量值
}
```

#### ExecutorSource

```markdown
private[spark] class ExecutorSource(threadPool: ThreadPoolExecutor, executorId: String){
	关系: father --> Source
	构造器属性:
	threadPool	线程池执行器
	executorId	执行器Id
	属性:
	#name @metricRegistry=new MetricRegistry()	度量值注册器
	#name @sourceName="executor"	资源名称
	#name @SUCCEEDED_TASKS = metricRegistry.counter(MetricRegistry.name("succeededTasks"))
		成功执行任务数量
	#name @METRIC_CPU_TIME=metricRegistry.counter(MetricRegistry.name("cpuTime"))	
		CPU时间计数
	#name @METRIC_RUN_TIME=etricRegistry.counter(MetricRegistry.name("runTime"))
		运行时间计数
	#name @METRIC_JVM_GC_TIME=metricRegistry.counter(MetricRegistry.name("jvmGCTime"))
		JVM GC时间计数
	#name @METRIC_DESERIALIZE_TIME
		= metricRegistry.counter(MetricRegistry.name("shuffleFetchWaitTime"))
		反序列化时间计数
	#name @METRIC_SHUFFLE_WRITE_TIME
		= metricRegistry.counter(MetricRegistry.name("shuffleWriteTime"))
		shuffle写出时间计数
	#name @METRIC_SHUFFLE_TOTAL_BYTES_READ
		= metricRegistry.counter(MetricRegistry.name("shuffleTotalBytesRead"))
		shuffle总计读取字节数量
	#name @METRIC_SHUFFLE_REMOTE_BYTES_READ
		=metricRegistry.counter(MetricRegistry.name("shuffleRemoteBytesRead"))
		shuffle远端读取字节数量
	#name @METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK
		= metricRegistry.counter(MetricRegistry.name("shuffleRemoteBytesReadToDisk"))
		shuffle远端读取到磁盘上字节数量
	#name @METRIC_SHUFFLE_LOCAL_BYTES_READ
		=metricRegistry.counter(MetricRegistry.name("shuffleLocalBytesRead"))
		shuffle 本地读取字节数量
	#name @METRIC_SHUFFLE_RECORDS_READ
		=metricRegistry.counter(MetricRegistry.name("shuffleRecordsRead"))
		shuffle读取记录数量
	#name @METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED
		=metricRegistry.counter(MetricRegistry.name("shuffleRemoteBlocksFetched"))
		shuffle获取远端数据块数量
	#name @METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED
		=metricRegistry.counter(MetricRegistry.name("shuffleLocalBlocksFetched"))
		shuffle获取本地数据块数量
	#name @METRIC_SHUFFLE_BYTES_WRITTEN
		=metricRegistry.counter(MetricRegistry.name("shuffleBytesWritten"))
		shuffle写出字节数量
	#name @METRIC_SHUFFLE_RECORDS_WRITTEN
		=metricRegistry.counter(MetricRegistry.name("shuffleRecordsWritten"))
		shuffle写出记录数量
	#name @METRIC_INPUT_BYTES_READ
		=metricRegistry.counter(MetricRegistry.name("bytesRead"))
		读取输入字节数量
	#name @METRIC_INPUT_RECORDS_READ
		=metricRegistry.counter(MetricRegistry.name("recordsRead"))
		读取输入记录数量
	#name @METRIC_OUTPUT_BYTES_WRITTEN
		=metricRegistry.counter(MetricRegistry.name("bytesWritten"))
		输出字节数量
	#name @METRIC_OUTPUT_RECORDS_WRITTEN
		=metricRegistry.counter(MetricRegistry.name("recordsWritten"))
		输出记录数量
	#name @METRIC_RESULT_SIZE
		= metricRegistry.counter(MetricRegistry.name("resultSize"))
		结果数量
	#name @METRIC_DISK_BYTES_SPILLED
		=metricRegistry.counter(MetricRegistry.name("diskBytesSpilled"))
		溢写到磁盘上的字节数量
	#name @METRIC_MEMORY_BYTES_SPILLED
		=metricRegistry.counter(MetricRegistry.name("memoryBytesSpilled"))
		溢写到内存的字节数量
	初始化操作:
	metricRegistry.register(MetricRegistry.name("threadpool", "activeTasks"), new Gauge[Int] {
    	override def getValue: Int = threadPool.getActiveCount()
  	})
  	功能: 注册执行器线程池中处于激活状态的计量值
  	
  	metricRegistry.register(MetricRegistry.name("threadpool", "completeTasks"), new Gauge[Long] {
    	override def getValue: Long = threadPool.getCompletedTaskCount()
  	})
  	功能: 注册执行器线程池中完成的计量值
  	
  	metricRegistry.register(MetricRegistry.name("threadpool", "startedTasks"), new Gauge[Long] {
    	override def getValue: Long = threadPool.getTaskCount()
  	})
  	功能: 注册执行器线程池中已经开启任务的计量值
  	
  	metricRegistry.register(MetricRegistry.name("threadpool", "currentPool_size"), new Gauge[Int] {
    	override def getValue: Int = threadPool.getPoolSize()
  	})
  	功能: 注册执行器线程池的大小
  	
  	for (scheme <- Array("hdfs", "file")) {
        registerFileSystemStat(scheme, "read_bytes", _.getBytesRead(), 0L)
        registerFileSystemStat(scheme, "write_bytes", _.getBytesWritten(), 0L)
        registerFileSystemStat(scheme, "read_ops", _.getReadOps(), 0)
        registerFileSystemStat(scheme, "largeRead_ops", _.getLargeReadOps(), 0)
        registerFileSystemStat(scheme, "write_ops", _.getWriteOps(), 0)
  	}
  	功能: 注册每个schema对应的记录值初始值
}
```

#### InputMetrics

```markdown
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {	
	介绍: 输入数据读取的方法数据通过网络从远端块管理器中读取,这个块管理器含有很多磁盘和内存中的数据.操作是线程		不安全的.
	type DataReadMethod = Value
	val Memory, Disk, Hadoop, Network = Value 
}
```

```markdown
@DeveloperApi
class InputMetrics private[spark] () extends Serializable {
	属性:
	#name @_bytesRead = new LongAccumulator	读取字节量
	#name @_recordsRead= new LongAccumulator 读取记录数量
	操作集:
	def bytesRead: Long = _bytesRead.sum
	功能: 获取读取的字节量
	
	def recordsRead: Long = _recordsRead.sum
	功能: 读取记录数量
	
	def incBytesRead(v: Long): Unit = _bytesRead.add(v)
	功能: 读取字节量+=v
	
	def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
	功能: 读取记录数量+=v
	
	def setBytesRead(v: Long): Unit = _bytesRead.setValue(v)
	功能: 设置读取字节数量
}
```

#### OutputMetrics

```markdown
@DeveloperApi
object DataWriteMethod extends Enumeration with Serializable {
	介绍: 输出数据写出的方法,操作是线程不安全的.
      type DataWriteMethod = Value
      val Hadoop = Value
}
```

```markdown
@DeveloperApi
class OutputMetrics private[spark] () extends Serializable {
	介绍: 累加器集合,代表写出到外部系统的度量参数
	属性:
	#name @_bytesWritten = new LongAccumulator	写出字节数量
	#name @_recordsWritten= new LongAccumulator
	操作集:
	def bytesWritten: Long = _bytesWritten.sum
	功能: 获取写出字节总数
	
	def recordsWritten: Long = _recordsWritten.sum
	功能: 获取写出记录数量
	
	def setBytesWritten(v: Long): Unit = _bytesWritten.setValue(v)
	功能: 设置写出字节数量
	
	def setRecordsWritten(v: Long): Unit = _recordsWritten.setValue(v)
	功能: 设置写出记录数量
}
```

#### ProcfsMetricsGetter

```markdown
private[spark] case class ProcfsMetrics(jvmVmemTotal: Long,jvmRSSTotal: Long,
    pythonVmemTotal: Long,pythonRSSTotal: Long,
    otherVmemTotal: Long,otherRSSTotal: Long)
介绍: 进程文件系统度量器
构造器参数:
	jvmVmemTotal	JVM内存使用量
	jvmRSSTotal		JVM RSS总量
	pythonVmemTotal	python内存使用量
	pythonRSSTotal	python RSS总量
	otherVmemTotal	其他类型虚拟机内存使用总量
	otherRSSTotal	其他类型RSS 使用总量
	
private[spark] object ProcfsMetricsGetter
介绍: 进程文件系统度量器获取器实例
参数: 
	#name @pTreeInfo = new ProcfsMetricsGetter	进程文件系统树状信息
```

```markdown
private[spark] class ProcfsMetricsGetter(procfsDir: String = "/proc/"){
	关系: father --> Logging
	构造器属性:
		procfsDir: String = "/proc/"	进程文件系统目录
	属性:
	#name @procfsStatFile="stat"	进程文件系统状态文件
	#name @testing=Utils.isTesting	测试状态
	#name @pageSize=computePageSize()	页长
	#name @isAvailable #type @Boolean	是否可以使用进程文件系统
		val= isProcfsAvailable
	#name @pid = computePid()	进程ID
	#name @isProcfsAvailable #type @Boolean	进程文件系统是否可以获取
		val= if (testing) true // 测试状态下可以使用
			else{
				val procDirExists = Try(Files.exists(Paths.get(procfsDir))).recover {
					// 进程文件系统目录不存在，则不允许使用
        			case ioe: IOException =>
          			logWarning("Exception checking for procfs dir", ioe)
          			false
     			 }
     			 // 其余情况，需要开启stage执行器计量日志和进程树计量日志才可以进行使用
      			val shouldLogStageExecutorMetrics =
        			SparkEnv.get.conf.get(config.EVENT_LOG_STAGE_EXECUTOR_METRICS)
      			val shouldLogStageExecutorProcessTreeMetrics =
        			SparkEnv.get.conf.get(config.EVENT_LOG_PROCESS_TREE_METRICS)
     			 procDirExists.get && shouldLogStageExecutorProcessTreeMetrics &&
                 shouldLogStageExecutorMetrics
			}
	操作集:
    def computePid(): Int
    功能: 计算PID编号(在可用且非测试状态下才有合法的pid编号)
    0.排出非常情况
    if (!isAvailable || testing) {
      return -1; }
   	1. 执行cmd语句，获取进程编号
   	val cmd = Array("bash", "-c", "echo $PPID")
    val out = Utils.executeAndGetOutput(cmd)
    val= Integer.parseInt(out.split("\n")(0))
    
    def computePageSize(): Long
    功能: 执行cmd命令获取页长，测试状态下长度为4096B
    if(testing) return 4096
    try {
      val cmd = Array("getconf", "PAGESIZE")
      val out = Utils.executeAndGetOutput(cmd)
      val=Integer.parseInt(out.split("\n")(0))
    }catch{
    	case e:Exception =>
    		isAvailable = false
        	val= 0
    }
    
    def computeProcessTree(): Set[Int]
    功能: 计算进程树，测试情况或者不可使用状态下返回空集合
    if (!isAvailable || testing) return set()
    var ptree: Set[Int] = Set() // 初始化进程树
    ptree += pid // 进程树添加新节点
    val queue = mutable.Queue.empty[Int] // 初始化进程队列
    queue += pid // 添加第一个节点
    // 使用BFS向进行树集合@ptree中添加节点 最终集合是按照层序结构输出
    while ( !queue.isEmpty ) {
      val p = queue.dequeue()
      val c = getChildPids(p)
      if (!c.isEmpty) {
        queue ++= c
        ptree ++= c.toSet
      }
    }
    val= ptree
    
    def getChildPids(pid: Int): ArrayBuffer[Int]
    功能: 获取指定进程节点@pid的子节点
    1. 初始化子节点列表
    val builder = new ProcessBuilder("pgrep", "-P", pid.toString)
    val process = builder.start()
    val childPidsInInt = mutable.ArrayBuffer.empty[Int]
    2. 添加子节点
    def appendChildPid(s: String): Unit = {
        if (s != "") {
          logTrace("Found a child pid:" + s)
          childPidsInInt += Integer.parseInt(s)
        }
     }
    3. 调用操作系统接口,从底层流式读取进程ID号.运行两个线程，一个线程处理正确处理逻辑(添加到子节点集合中)，拎一个线程处理异常逻辑(收集异常原因)
    val stdoutThread = Utils.processStreamByLine("read stdout for pgrep",process.getInputStream, 			appendChildPid)
    val errorStringBuilder = new StringBuilder()
    val stdErrThread = Utils.processStreamByLine("stderr for pgrep",process.getErrorStream,
        line => errorStringBuilder.append(line))
    val exitCode = process.waitFor()
    stdoutThread.join()
    stdErrThread.join()
    val errorString = errorStringBuilder.toString()
   	4. 抛出相关异常(退出码为1时表示pgrep有多个子进程，不应该抛出异常)
    if (exitCode != 0 && exitCode > 2) {
        val cmd = builder.command().toArray.mkString(" ")
        logWarning(s"Process $cmd exited with code $exitCode and stderr: $errorString")
        throw new SparkException(s"Process $cmd exited with code $exitCode")
    }
    val= childPidsInInt
    
    def addProcfsMetricsFromOneProcess(allMetrics: ProcfsMetrics,pid: Int): ProcfsMetrics 
    功能: 从一个进程中添加进程文件计量信息
    输入参数: 
    	allMetrics	进程文件计量系统
    	pid	进程id
    注: 进程的RSS和Vmem计算请参照 <http://man7.org/linux/man-pages/man5/proc.5.html>
    1. 获取并读取文件目录
    val pidDir = new File(procfsDir, pid.toString)
    def openReader(): BufferedReader = {
        val f = new File(new File(procfsDir, pid.toString), procfsStatFile)
        new BufferedReader(new InputStreamReader(new FileInputStream(f), UTF_8)) }
    2. 计算进程文件计量系统的RSS和Veme参数值
    Utils.tryWithResource(openReader) { in =>
        val procInfo = in.readLine
        val procInfoSplit = procInfo.split(" ")
        val vmem = procInfoSplit(22).toLong
        val rssMem = procInfoSplit(23).toLong * pageSize
        if (procInfoSplit(1).toLowerCase(Locale.US).contains("java")) {
          allMetrics.copy(
            jvmVmemTotal = allMetrics.jvmVmemTotal + vmem,
            jvmRSSTotal = allMetrics.jvmRSSTotal + (rssMem)
          )
        }
        else if (procInfoSplit(1).toLowerCase(Locale.US).contains("python")) {
          allMetrics.copy(
            pythonVmemTotal = allMetrics.pythonVmemTotal + vmem,
            pythonRSSTotal = allMetrics.pythonRSSTotal + (rssMem)
          )
        }
        else {
          allMetrics.copy(
            otherVmemTotal = allMetrics.otherVmemTotal + vmem,
            otherRSSTotal = allMetrics.otherRSSTotal + (rssMem)
          )
        }
        
   	def computeAllMetrics(): ProcfsMetrics
   	功能: 计算进程
   	0. 考虑特殊值
   	if(!isAvailable) return ProcfsMetrics(0, 0, 0, 0, 0, 0)
   	1. 获取进程树和进程文件度量系统
   	val pids = computeProcessTree
    var allMetrics = ProcfsMetrics(0, 0, 0, 0, 0, 0)
    2. 将进程度量值放入度量系统中
    for (p <- pids) {
      allMetrics = addProcfsMetricsFromOneProcess(allMetrics, p)
      // 如果度量有误，不会返回部分结果，而是返回默认输出
      if (!isAvailable) {
        return ProcfsMetrics(0, 0, 0, 0, 0, 0)
      }
    }
    val= allMetrics
}
```

#### ShuffleReadMetrics

```markdown
@DeveloperApi
class ShuffleReadMetrics private[spark] () extends Serializable {
	介绍: 累加器集合,表示shuffle读取数据的的度量参数.这些操作是线程不安全的.
	属性:
	#name @_remoteBlocksFetched = new LongAccumulator	远端获取块数量
	#name @_localBlocksFetched = new LongAccumulator	本地获取块数量
	#name @_remoteBytesRead	=new LongAccumulator 远端读取字节数量
	#name @_remoteBytesReadToDisk = new LongAccumulator	远端读取到磁盘的字节数量
	#name @_localBytesRead = new LongAccumulator	本地读取字节数量
	#name @_fetchWaitTime = new LongAccumulator	获取等待时间
	#name @_recordsRead = new LongAccumulator	读取记录数量
	操作集:
	def remoteBlocksFetched: Long = _remoteBlocksFetched.sum
	功能: 该任务在这个shuffle中获取远端块的数量
	
	def localBlocksFetched: Long = _localBlocksFetched.sum
	功能: 该任务在这个shuffle中本地读取块的数量
	
	def remoteBytesRead: Long = _remoteBytesRead.sum
	功能: 本任务从这个shuffle读取的远端字节数量
	
	def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk.sum
	功能: 本任务从这个shuffle中读取写到磁盘的远端字节数量
	
	def localBytesRead: Long = _localBytesRead.sum
	功能: 读取本地磁盘的shuffle的字节数量
	
	def fetchWaitTime: Long = _fetchWaitTime.sum
	功能: 计算等待远端shuffle块的时间.
	
	def recordsRead: Long = _recordsRead.sum
	功能: 获取本任务该shuffle读取的总记录数量
	
	def totalBytesRead: Long = remoteBytesRead + localBytesRead
	功能: 本任务该shuffle读取字节总数(包括本地和远端)
	
	def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched
	功能: 本任务该shuffle读取块总数(包括本地和远端)
	
	def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
	def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
	功能: 远端/本地块数量+=v
	
	def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
 	def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.add(v)
 	def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
	功能: 远端读取字节数量(读取到磁盘字节数量)/本地读取字节数量+=v
	
	def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
	功能: 等待时间+=v
	
	def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
	功能: 读取记录数量+=v
	
	def setRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.setValue(v)
	def setLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.setValue(v)
	def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)
	def setRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.setValue(v)
	def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)
	def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)
	def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)
	功能: 设置远端获取块数量/本地获取块数量/远端读取字节数量/远端读取的磁盘字节数量/本地读取字节数/等待时间/
	读取记录数量
	
	def setMergeValues(metrics: Seq[TempShuffleReadMetrics]): Unit
	功能: 充值累加器参数
         _remoteBlocksFetched.setValue(0)
        _localBlocksFetched.setValue(0)
        _remoteBytesRead.setValue(0)
        _remoteBytesReadToDisk.setValue(0)
        _localBytesRead.setValue(0)
        _fetchWaitTime.setValue(0)
        _recordsRead.setValue(0)
        metrics.foreach { metric =>
          _remoteBlocksFetched.add(metric.remoteBlocksFetched)
          _localBlocksFetched.add(metric.localBlocksFetched)
          _remoteBytesRead.add(metric.remoteBytesRead)
          _remoteBytesReadToDisk.add(metric.remoteBytesReadToDisk)
          _localBytesRead.add(metric.localBytesRead)
          _fetchWaitTime.add(metric.fetchWaitTime)
          _recordsRead.add(metric.recordsRead)
        }
}
```

```markdown
private[spark] class TempShuffleReadMetrics {
	关系: father --> ShuffleReadMetricsReporter
	功能: 临时shuffle读取度量器,用于收集每个shuffle依赖的读取计量参数,所有的临时shuffle读取计量参数都会在最	后累积到最终的@ShuffleReadMetrics中
	属性:
	#name @_remoteBlocksFetched = 0L	远端块文件数量
	#name @_localBlocksFetched = 0L		本地块文件数量
	#name @_remoteBytesRead = 0L		远端读取字节数量
	#name @_remoteBytesReadToDisk = 0L	远端读取到磁盘的字节数量
	#name @_localBytesRead = 0L			本地读取字节数量
	#name @_fetchWaitTime = 0L			等待时间
	#name @_recordsRead = 0L			记录数量
	操作集:
	 def remoteBlocksFetched: Long = _remoteBlocksFetched
     def localBlocksFetched: Long = _localBlocksFetched
     def remoteBytesRead: Long = _remoteBytesRead
     def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk
     def localBytesRead: Long = _localBytesRead
     def fetchWaitTime: Long = _fetchWaitTime
     def recordsRead: Long = _recordsRead
	功能: 获取相应参数值
	
	def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched += v
	def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched += v
	def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead += v
	def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk += v
	def incLocalBytesRead(v: Long): Unit = _localBytesRead += v
	def incFetchWaitTime(v: Long): Unit = _fetchWaitTime += v
	def incRecordsRead(v: Long): Unit = _recordsRead += v
	功能: 参数度量值累加
}
```

#### ShuffleWriteMetrics

```markdown
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends ShuffleWriteMetricsReporter with Serializable {
	介绍: 累加器集合,表示shuffle写出数据的度量参数,操作是线程不安全的
	属性:
	#name @_bytesWritten = new LongAccumulator	写出字节数量
	#name @_recordsWritten = new LongAccumulator 写出记录数量
	#name @_writeTime = new LongAccumulator	写出时间
	操作集:
	def bytesWritten: Long = _bytesWritten.sum
	功能: 获取本任务该shuffle写出字节数量
	
	def recordsWritten: Long = _recordsWritten.sum
	功能: 获取本任务该shuffle写出记录数量
	
	def writeTime: Long = _writeTime.sum
	功能: 获取写出时间
	
	def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
	功能: 写出字节数量+=v
	
	def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
	功能: 写出记录数量+=v
	
	def incWriteTime(v: Long): Unit = _writeTime.add(v)
	功能: 写出时间+=v
	
	def decBytesWritten(v: Long): Unit= _bytesWritten.setValue(bytesWritten - v)
	功能: 写出字节数量-=v
	
	def decRecordsWritten(v: Long): Unit = _recordsWritten.setValue(recordsWritten - v)
	功能: 写出记录数量-=v
}
```

#### TaskMetrics

```markdown
@DeveloperApi
class TaskMetrics private[spark] () extends Serializable {
	介绍: 任务度量器，任务执行期间用于对一个任务的计量
	这个类内部包装了外部累加器集合，代表与一个任务相关的计量信息。任务完成时,累加器的本地数值会从执行器发送到驱	动器上，这些数值会被合并到驱动器上之前登记的累加器上。
	累加器也会周期性的发送到驱动器上(通过执行器的心跳(heartbeat))
	属性:
	#name @_executorDeserializeTime = new LongAccumulator	执行器反序列化时间
	#name @_executorDeserializeCpuTime = new LongAccumulator	执行器反序列化CPU时间
	#name @_executorRunTime = new LongAccumulator	执行器运行时间
	#name @_executorCpuTime = new LongAccumulator	执行器CPU执行时间
	#name @_resultSize = new LongAccumulator	结果大小
	#name @_jvmGCTime = new LongAccumulator	JVM GC时间
	#name @_resultSerializationTime = new LongAccumulator	结果序列化时间
	#name @_memoryBytesSpilled = new LongAccumulator	内存溢写字节数量
	#name @_diskBytesSpilled = new LongAccumulator	磁盘溢写字节数量
	#name @_peakExecutionMemory = new LongAccumulator 峰值执行器内存大小
	#name @_updatedBlockStatuses = new CollectionAccumulator[(BlockId, BlockStatus)]	
		更新块状态信息
	#name @inputMetrics = new InputMetrics()	输入度量器
		读取来自@org.apache.spark.rdd.HadoopRDD的持久化数据仅仅在任务中定义
	#name @outputMetrics=new OutputMetrics()	输出度量器
		写出数据到外部系统(分布式系统)，在任务中定义为输出
	#name @shuffleReadMetrics=new ShuffleReadMetrics()	shuffle读取度量器
		从所有shuffle依赖中读取聚合结果，只有在存在有shuffle依赖的任务中才有这个定义
	#name @shuffleWriteMetrics	shuffle写度量器
		只有存在shuffle map阶段的任务才有定义
	#name @tempShuffleReadMetrics=new ArrayBuffer[TempShuffleReadMetrics] transient lazy
		临时shuffle读取度量器
		每个shuffle依赖存在有一个临时shuffle读取度量器@TempShuffleReadMetrics，为了避免来自不同线程的读取
        同步问题,正在进行中的任务使用@TempShuffleReadMetrics 给每个shuffle依赖。且合并这些度量结果，并汇报		给驱动器。
    #name @testAccum=sys.props.get(IS_TESTING.key).map(_ => new LongAccumulator)	
    	测试使用的累加器
	#name @nameToAccums #type @LinkedHashMap	名称映射器
	LinkedHashMap(
        EXECUTOR_DESERIALIZE_TIME -> _executorDeserializeTime,
        EXECUTOR_DESERIALIZE_CPU_TIME -> _executorDeserializeCpuTime,
        EXECUTOR_RUN_TIME -> _executorRunTime,
        EXECUTOR_CPU_TIME -> _executorCpuTime,
        RESULT_SIZE -> _resultSize,
        JVM_GC_TIME -> _jvmGCTime,
        RESULT_SERIALIZATION_TIME -> _resultSerializationTime,
        MEMORY_BYTES_SPILLED -> _memoryBytesSpilled,
        DISK_BYTES_SPILLED -> _diskBytesSpilled,
        PEAK_EXECUTION_MEMORY -> _peakExecutionMemory,
        UPDATED_BLOCK_STATUSES -> _updatedBlockStatuses,
        shuffleRead.REMOTE_BLOCKS_FETCHED -> shuffleReadMetrics._remoteBlocksFetched,
        shuffleRead.LOCAL_BLOCKS_FETCHED -> shuffleReadMetrics._localBlocksFetched,
        shuffleRead.REMOTE_BYTES_READ -> shuffleReadMetrics._remoteBytesRead,
        shuffleRead.REMOTE_BYTES_READ_TO_DISK -> shuffleReadMetrics._remoteBytesReadToDisk,
        shuffleRead.LOCAL_BYTES_READ -> shuffleReadMetrics._localBytesRead,
        shuffleRead.FETCH_WAIT_TIME -> shuffleReadMetrics._fetchWaitTime,
        shuffleRead.RECORDS_READ -> shuffleReadMetrics._recordsRead,
        shuffleWrite.BYTES_WRITTEN -> shuffleWriteMetrics._bytesWritten,
        shuffleWrite.RECORDS_WRITTEN -> shuffleWriteMetrics._recordsWritten,
        shuffleWrite.WRITE_TIME -> shuffleWriteMetrics._writeTime,
        input.BYTES_READ -> inputMetrics._bytesRead,
        input.RECORDS_READ -> inputMetrics._recordsRead,
        output.BYTES_WRITTEN -> outputMetrics._bytesWritten,
        output.RECORDS_WRITTEN -> outputMetrics._recordsWritten
      ) ++ testAccum.map(TEST_ACCUM -> _)
    #name @internalAccums #type @Seq[AccumulatorV2[_, _]] lazy transient	内部累加器
		val= nameToAccums.values.toIndexedSeq
	#name @externalAccums #type @ArrayBuffer[AccumulatorV2[_, _]] lazy transient	外部累加器
		
	操作集:
	def executorDeserializeTime: Long = _executorDeserializeTime.sum
	功能: 获取执行器在该任务上反序列化的时间
	
	def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime.sum
	功能: 该任务执行器上反序列化花费的CPU时间
	
	def executorRunTime: Long = _executorRunTime.sum
	功能: 获取执行器花费运行任务的时间(包括获取shuffle数据的时间)
	
	def executorCpuTime: Long = _executorCpuTime.sum
	功能: 执行器花费的运行CPU时间(包括shuffle数据获取的时间)
	
	def resultSize: Long = _resultSize.sum
	功能: 获取结果大小
	
	def jvmGCTime: Long = _jvmGCTime.sum
	功能: 获取jvm GC时间
	
	def resultSerializationTime: Long = _resultSerializationTime.sum
	功能: 花费在序列化任务结果的时间
	
	def diskBytesSpilled: Long = _diskBytesSpilled.sum
	功能: 获取溢写到磁盘上的字节数量
	
	def peakExecutionMemory: Long = _peakExecutionMemory.sum
	功能: 获取执行器峰值内存使用量
	
	def updatedBlockStatuses: Seq[(BlockId, BlockStatus)]= _updatedBlockStatuses.value.asScala
	功能: 获取块状态信息列表
		驱动器侧调用，所有累加器都有一个固定值。所以使用asScala是安全的
	
	def setExecutorDeserializeTime(v: Long): Unit =
    	_executorDeserializeTime.setValue(v)
	功能: 设置执行器反序列化时间
	
	def setExecutorDeserializeCpuTime(v: Long): Unit =
    	_executorDeserializeCpuTime.setValue(v)
	功能: 设置执行器反序列化所需要的CPU时间
	
	def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
    功能: 设置执行器运行时间
    
    def setExecutorCpuTime(v: Long): Unit = _executorCpuTime.setValue(v)
    功能: 设置执行器CPU运行时间
    
    def setResultSize(v: Long): Unit = _resultSize.setValue(v)
    功能: 设置任务结果规模大小
    
    def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
    功能: 设置JVM GC时间
    
    def setResultSerializationTime(v: Long): Unit
    功能: 设置结果反序列化时间
    
    def setPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.setValue(v)
    功能: 设置峰值内存使用量
    
    def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
    功能: 内存溢写字节量 += v
    
    def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
    功能: 溢写到磁盘的字节数量 +=v
    
    def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
    功能: 执行器内存峰值使用量 += v
    
    def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit
    	= _updatedBlockStatuses.add(v)
    功能: 增加一条块信息映射关系
    
    def setUpdatedBlockStatuses(v: java.util.List[(BlockId, BlockStatus)]): Unit =
    	_updatedBlockStatuses.setValue(v)
	功能: 设置块信息
	
	def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    	_updatedBlockStatuses.setValue(v.asJava)
    功能: 设置块信息
    
    def register(sc: SparkContext): Unit
    功能: 注册名称映射器@nameToAccums 中的累加器
    nameToAccums.foreach {
      case (name, acc) => acc.register(sc, name = Some(name), countFailedValues = true)
    }
    
    def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {externalAccums += a }
    功能: 注册指定累加器@a 到外部累加器中
    
    def accumulators(): Seq[AccumulatorV2[_, _]] = internalAccums ++ externalAccums
    功能: 获取累加器列表
    
    def nonZeroInternalAccums(): Seq[AccumulatorV2[_, _]]
    功能: 获取非零值内部累加器
    val= nternalAccums.filter(a => !a.isZero || a == _resultSize)
    
    def createTempShuffleReadMetrics(): TempShuffleReadMetrics
    功能: 创建一个临时shuffle读取度量器@TempShuffleReadMetrics
    	创建完成之后，需要注册到临时shuffle读取度量器列表中
    val= synchronized {
    	val readMetrics = new TempShuffleReadMetrics
    	tempShuffleReadMetrics += readMetrics
    	val=readMetrics
    }
    
    def mergeShuffleReadMetrics(): Unit
    功能: 合并shuffle读取度量器到@_shuffleReadMetrics 中。需要在执行器上调用，通过心跳机制在任务最后发送到驱		动器端。
	synchronized {
    	if (tempShuffleReadMetrics.nonEmpty) {
      		shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics)
    	}
  	}
}
```

```markdown
private[spark] object TaskMetrics extends Logging {
	操作集:
	def empty: TaskMetrics 
	功能: 获取一个空的任务度量器，没有注册任何累加器
    val tm = new TaskMetrics
    tm.nameToAccums.foreach { case (name, acc) =>
    	acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), true)
	val= tm
	
	def registered: TaskMetrics
	功能: 获取注册的任务度量器
	val tm = empty
    tm.internalAccums.foreach(AccumulatorContext.register)
    val=tm
	
	def fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics 
	功能: 构造由指定累加器列表@accums 形成的任务度量器,只能在驱动器侧调用
    val tm = new TaskMetrics
    for (acc <- accums) {
      val name = acc.name
      if (name.isDefined && tm.nameToAccums.contains(name.get)) {
        val tmAcc = tm.nameToAccums(name.get).asInstanceOf[AccumulatorV2[Any, Any]]
        tmAcc.metadata = acc.metadata
        tmAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
      } else {
        tm.externalAccums += acc
      }
    }
    val= tm
    
    def fromAccumulatorInfos(infos: Seq[AccumulableInfo]): TaskMetrics
    功能: 构造由指定累加器列表@infos 形成的任务度量器,只能在驱动器侧调用
		获取的任务度量器@TaskMetrics 只能用于获取内部度量器，不需要在乎传入的外部累加器
	val tm = new TaskMetrics
    infos.filter(info => info.name.isDefined && info.update.isDefined).foreach { info =>
      val name = info.name.get
      val value = info.update.get
      if (name == UPDATED_BLOCK_STATUSES) {
        tm.setUpdatedBlockStatuses(value.asInstanceOf[java.util.List[(BlockId, BlockStatus)]])
      } else {
        tm.nameToAccums.get(name).foreach(
          _.asInstanceOf[LongAccumulator].setValue(value.asInstanceOf[Long])
        )
      }
    }
    val= tm
}
```

#### 基础拓展

1.   polling 轮询
2.   TCMP : Tangosol Cluster Management Protocol
3.   增量式更新
4.   RSS
5.   进程树(进程有向树)