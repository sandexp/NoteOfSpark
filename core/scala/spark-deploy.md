### **spark-deploy**

---

1.  [client](#  client)
2.  [history](# history)
3.  [master](# master)
4. [rest](# rest)
5. [security](# security)
6. [worker](# worker)
7. [ApplicationDescription.scala](# ApplicationDescription)
8. [Client.scala](# Client.scala)
9. [ClientArguments.scala](# ClientArguments)
10. [Command.scala](# Command)
11. [DependencyUtils.scala](# DependencyUtils)
12. [DeployMessage.scala](# DeployMessage)
13. [DriverDescription.scala](# DriverDescription)
14. [ExecutorDescription.scala](# ExecutorDescription)
15. [ExecutorState.scala](# ExecutorState)
16. [ExternalShuffleService.scala](# ExternalShuffleService)
17. [ExternalShuffleServiceSource.scala](# ExternalShuffleServiceSource)
18. [FaultToleranceTest.scala](# FaultToleranceTest)
19. [JsonProtocol.scala](# JsonProtocol)
20. [LocalSparkCluster.scala](# LocalSparkCluster)
21. [PythonRunner.scala](# PythonRunner)
22. [RPackageUtils.scala](# RPackageUtils)
23. [RRunner.scala](# RRunner)
24. [SparkApplication.scala](# SparkApplication)
25. [SparkCuratorUtil.scala](# SparkCuratorUtil)
26. [SparkHadoopUtil.scala](# SparkHadoopUtil)
27. [SparkSubmit.scala](# SparkSubmit)
28. [SparkSubmitArguments.scala](# SparkSubmitArguments)
29. [StandaloneResourceUtils.scala](# StandaloneResourceUtils)

---

#### client

#####  StandaloneAppClient

```scala
private[spark] class StandaloneAppClient(
    rpcEnv: RpcEnv,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: StandaloneAppClientListener,
    conf: SparkConf)
extends Logging {
    介绍: 允许应用可以与spark独立集群管理器交互的接口。
    获取master的URL地址,应用描述,集群时间监听器
    master地址的形式为: spark://host:port
    属性:
    #name @masterRpcAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))	masterRPC地址
    #name @REGISTRATION_TIMEOUT_SECONDS = 20	注册时延
    #name @REGISTRATION_RETRIES = 3	注册重试次数
    #name @endpoint = new AtomicReference[RpcEndpointRef]	RPC端点
    #name @appId = new AtomicReference[String]	应用编号
    #name @registered = new AtomicBoolean(false)	客户端是否注册
    操作集:
    def start(): Unit
    功能: 启动客户端(只需要运行RPC端点即可)
    endpoint.set(rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)))
    
    def stop(): Unit
    功能: 关闭客户端(去除RPC端点即可)
    if (endpoint.get != null) {
      try {
        val timeout = RpcUtils.askRpcTimeout(conf)
        timeout.awaitResult(endpoint.get.ask[Boolean](StopAppClient))
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      endpoint.set(null)
    }
    
    def requestTotalExecutors(requestedTotal: Int): Future[Boolean]
    功能: 向master请求执行器,数量为@requestedTotal 包括待定以及运行的执行器,申请成功返回true
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](RequestExecutors(appId.get, requestedTotal))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
    
    def killExecutors(executorIds: Seq[String]): Future[Boolean]
    功能: kill执行器
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](KillExecutors(appId.get, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
}
```

ClientEndPoint

```scala
private class ClientEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
with Logging {
    属性:
    #name @master: Option[RpcEndpointRef] = None	master端点
    #name @alreadyDisconnected = false	是否已经断开连接(避免多次调用断开连接)
    #name @alreadyDead = new AtomicBoolean(false)	是否客户端以及死亡
    #name @registerMasterFutures = new AtomicReference[Array[JFuture[_]]] 注册master的线程
    #name @registrationRetryTimer = new AtomicReference[JScheduledFuture[_]]
    	注册调度线程(用于对重试进行计时)
    #name @registerMasterThreadPool	使用master注册的线程池
    由于使用master创建客户端时阻塞的,因此线程池容量至少是masterRpcAddresses.size,这样可以同时注册
    val= ThreadUtils.newDaemonCachedThreadPool(
      "appclient-register-master-threadpool",
      masterRpcAddresses.length 
    )
    #name @registrationRetryThread	用于调度注册动作的执行器
    val= ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "appclient-registration-retry-thread")
    
    操作集:
    def onStart(): Unit
    功能: 启动客户端
    try {
        registerWithMaster(1)
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          stop()
      }
    
    def tryRegisterAllMasters(): Array[JFuture[_]]
    功能: 异步注册所有的master,返回用于取消的线程`Future`
    for (masterAddress <- masterRpcAddresses) yield {
        registerMasterThreadPool.submit(new Runnable {
          override def run(): Unit = try {
            if (registered.get) {
              return
            }
            logInfo("Connecting to master " + masterAddress.toSparkURL + "...")
            val masterRef = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect
            to master $masterAddress", e)
          }
        })
      }
    
    def registerWithMaster(nthRetry: Int): Unit
    功能: 异步注册所有master,每隔@REGISTRATION_TIMEOUT_SECONDS 时间调用@registerWithMaster,直到到达了重试次数上限.一旦连接成功,就会取消调度工作.
    输入参数:
    	nthRetry	第n次请求注册master
    registerMasterFutures.set(tryRegisterAllMasters())
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
        override def run(): Unit = {
          if (registered.get) {
            registerMasterFutures.get.foreach(_.cancel(true))
            registerMasterThreadPool.shutdownNow() // 注册成功,放弃注册调度
          } else if (nthRetry >= REGISTRATION_RETRIES) { // 放弃注册
            markDead("All masters are unresponsive! Giving up.")
          } else {
            registerMasterFutures.get.foreach(_.cancel(true))
            registerWithMaster(nthRetry + 1) // 重试
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
    
    def sendToMaster(message: Any): Unit
    功能: 发送消息到master
    master match {
        case Some(masterRef) => masterRef.send(message)
        case None => logWarning(s"Drop $message because has not yet connected to master")
      }
    
    def isPossibleMaster(remoteAddress: RpcAddress): Boolean
    功能: 检查RPC地址@remoteAddress 是否为master
    val= masterRpcAddresses.contains(remoteAddress)
    
    def receive: PartialFunction[Any, Unit]
    功能: 接受RPC消息
    case RegisteredApplication(appId_, masterRef) => // 处理应用注册
        appId.set(appId_)
        registered.set(true)
        master = Some(masterRef)
        listener.connected(appId.get)
    case ApplicationRemoved(message) => // 处理删除应用
        markDead("Master removed our application: %s".format(message))
        stop()

    case ExecutorAdded(id: Int, workerId: String, hostPort: String, 
                       cores: Int, memory: Int) => // 处理添加执行器消息
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) 
        with %d core(s)".format(fullId, workerId, hostPort,
          cores))
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

    case ExecutorUpdated(id, state, message, exitStatus, workerLost) => // 更新执行器消息
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus, workerLost)
        }

      case WorkerRemoved(id, host, message) =>
        logInfo("Master removed worker %s: %s".format(id, message))
        listener.workerRemoved(id, host, message)

      case MasterChanged(masterRef, masterWebUiUrl) =>
        logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
        master = Some(masterRef)
        alreadyDisconnected = false
        masterRef.send(MasterChangeAcknowledged(appId.get))
}
```

#####  StandaloneAppClientListener

```markdown
介绍:
	当各种事情发生的时候,客户端调用回调函数.当前对5种信息的回调.
	1. 连接到集群
	2. 断开连接
	3. 给定执行器
	4. 移除执行器
	5. 移除worker
	在回调方法内部,用户API不应当阻塞.
```

```scala
private[spark] trait StandaloneAppClientListener {
    操作集:
    def connected(appId: String): Unit
    功能: 连接事件
    
    def disconnected(): Unit
    功能: 断开连接,可能断开连接是一个暂时的状态,因为会切换到一个新的master
    
    def dead(reason: String): Unit
    功能: 由于不可恢复的失败,导致的应用死亡
    
    def executorAdded(
      fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int): Unit
    功能: 添加执行器事件
    
    def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerLost: Boolean): Unit
    功能: 移除执行器事件
    
    def workerRemoved(workerId: String, host: String, message: String): Unit
    功能: 移除worker事件
}
```

#### history

#### master

##### UI

```markdown
包含三个WEB相关的页面
1. 应用程序页	@ApplicationPage
2. master页	@MasterPage
3. master webUI	@MasterWebUI
```

##### ApplicationInfo

```scala
private[spark] class ApplicationInfo(
    val startTime: Long,
    val id: String,
    val desc: ApplicationDescription,
    val submitDate: Date,
    val driver: RpcEndpointRef,
    defaultCores: Int)
extends Serializable {
    介绍: 应用信息
    构造器参数:
    	startTime	开始时间
    	id	应用编号
    	desc	应用描述
    	submitDate	提交日期
    	driver	驱动器RPC引用
    	defaultCores	默认核心数量
    属性:
    #name @state: ApplicationState.Value = _	transient	应用状态
    #name @executors: mutable.HashMap[Int, ExecutorDesc]	transient	执行器描述映射表
    #name @removedExecutors: ArrayBuffer[ExecutorDesc] = _	transient	移除的执行器列表
    #name @coresGranted: Int = _	transient	授权的核心数量
    #name @endTime: Long = _	transient	结束时间
    #name @appSource: ApplicationSource = _	transient	应用资源
    #name @_retryCount = 0	重试计数器
    #name @requestedCores = desc.maxCores.getOrElse(defaultCores)	请求核心数量
    #name @nextExecutorId: Int = _	transient	执行器编号
    #name @executorLimit: Int = _	执行器上限数量
    操作集:
    def readObject(in: java.io.ObjectInputStream): Unit
    功能: 读取配置文件
    Utils.tryOrIOException {
        in.defaultReadObject()
        init()
      }
    
    def init(): Unit
    功能: 初始化
    state = ApplicationState.WAITING
    executors = new mutable.HashMap[Int, ExecutorDesc]
    coresGranted = 0
    endTime = -1L
    appSource = new ApplicationSource(this)
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
    executorLimit = desc.initialExecutorLimit.getOrElse(Integer.MAX_VALUE)
    
    def newExecutorId(useID: Option[Int] = None): Int
    功能: 新建执行器编号
    val= useID match {
      case Some(id) => // 移动执行器编号指针
        nextExecutorId = math.max(nextExecutorId, id + 1)
        id
      case None => // 新建并移动执行器指针
        val id = nextExecutorId
        nextExecutorId += 1
        id
    }
    
    def addExecutor(
      worker: WorkerInfo,
      cores: Int,
      resources: Map[String, ResourceInformation],
      useID: Option[Int] = None): ExecutorDesc
    功能: 添加指定执行器@userID
    1. 获取执行描述
    val exec = new ExecutorDesc(newExecutorId(useID), this, worker, cores,
      desc.memoryPerExecutorMB, resources)
    2. 更新执行器列表,以及CPU数量信息
    executors(exec.id) = exec
    coresGranted += cores
    val= exec
    
    def removeExecutor(exec: ExecutorDesc): Unit 
    功能: 移除执行器
    if (executors.contains(exec.id)) {
      removedExecutors += executors(exec.id)
      executors -= exec.id
      coresGranted -= exec.cores
    }
    
    def coresLeft: Int = requestedCores - coresGranted
    功能: 计算剩余的核心数量
    
    def retryCount = _retryCount
    功能: 计算重试次数
    
    def incrementRetryCount() 
    功能: 增加重试计数次数
    _retryCount += 1
    val= _retryCount
    
    def resetRetryCount() = _retryCount = 0
    功能: 重置重试计数器
    
    def markFinished(endState: ApplicationState.Value): Unit
    功能: 标记应用完成
    state = endState
    endTime = System.currentTimeMillis()
    
    def isFinished: Boolean
    功能: 确定任务是否完成
    val= state != ApplicationState.WAITING && state != ApplicationState.RUNNING
    
    def getExecutorLimit: Int = executorLimit
    功能: 获取执行器上限数量
    
    def duration: Long
    功能: 确定任务持续时间
    val= if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
}
```

##### ApplicationSource

```scala
private[master] class ApplicationSource(val application: ApplicationInfo) extends Source {
    介绍: 应用资源
    构造器参数:
    	application	应用信息
    属性:
    #name @metricRegistry = new MetricRegistry()	度量值注册器
    #name @sourceName	资源名称
    val= "%s.%s.%s".format("application", application.desc.name,
    System.currentTimeMillis())
    初始化操作:
    metricRegistry.register(MetricRegistry.name("status"), new Gauge[String] {
        override def getValue: String = application.state.toString
      })
    功能: 注册状态信息
    
    metricRegistry.register(MetricRegistry.name("runtime_ms"), new Gauge[Long] {
        override def getValue: Long = application.duration
      })
    功能: 注册运行时间信息
    
    metricRegistry.register(MetricRegistry.name("cores"), new Gauge[Int] {
        override def getValue: Int = application.coresGranted
      })
    功能: 注册核心数量信息
}
```

##### ApplicationState

```scala
private[master] object ApplicationState extends Enumeration {
    介绍: 应用状态
    type ApplicationState = Value
    val WAITING, RUNNING, FINISHED, FAILED, KILLED, UNKNOWN = Value
    状态类型:
    	1. 等待
    	2. 运行中
    	3. 完成
    	4. 失败
    	5. 中断(Kill)
    	6. 未知
}
```

##### DriverInfo

```scala
private[deploy] class DriverInfo(
    val startTime: Long,
    val id: String,
    val desc: DriverDescription,
    val submitDate: Date)
extends Serializable {
    介绍: 驱动器信息
    构造器参数:
    	startTime	开始时间
    	id	驱动器ID
    	desc	驱动器描述
    	submitDate	提交日期
    属性:
    #name @state: DriverState.Value = DriverState.SUBMITTED	驱动器状态
    #name @exception: Option[Exception] = None	异常
    #name @worker: Option[WorkerInfo] = None	worker信息
    #name @_resources: Map[String, ResourceInformation] = _	资源信息表(gpu,fpga等等)
    操作集:
    def readObject(in: java.io.ObjectInputStream): Unit
    功能: 读取外部配置
    in.defaultReadObject()
    init()
    
    def init(): Unit
    功能: 初始化
    state = DriverState.SUBMITTED
    worker = None
    exception = None
    
    def withResources(r: Map[String, ResourceInformation]): Unit = _resources = r
    功能: 设置资源列表
    
    def resources: Map[String, ResourceInformation] = _resources
    功能: 获取驱动器资源列表
}
```

##### DriverState

```scala
private[deploy] object DriverState extends Enumeration {
    介绍: 驱动器状态
    type DriverState = Value
    val SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR = Value
    状态列表:
    	SUBMITTED	提交但是没有在worker上调用
    	RUNNING	已经分配运行的worker
    	FINISHED	运行完毕
    	RELAUCHING	重新运行
    	UNKNOWN	未知状态
    	KILLED	kill状态
    	FAILED	运行失败
    	ERROR	运行错误
}
```

##### ExecutorDesc

```scala
private[master] class ExecutorDesc(
    val id: Int,
    val application: ApplicationInfo,
    val worker: WorkerInfo,
    val cores: Int,
    val memory: Int,
    val resources: Map[String, ResourceInformation]) {
    功能: 执行器描述
    构造器参数:
    	id	执行器编号
    	application	应用信息
    	worker	worker信息
    	cores	核心数量
    	memory	内存占用大小
    	resources	执行器分配的资源列表(gpu/resources)
    属性:
    #name @state = ExecutorState.LAUNCHING	执行器状态
    操作集:
    def fullId: String = application.id + "/" + id
    功能: 获取全局唯一标识符
    
    def equals(other: Any): Boolean
    功能: 相等判定
    val= other match {
      case info: ExecutorDesc =>
        fullId == info.fullId &&
        worker.id == info.worker.id &&
        cores == info.cores &&
        memory == info.memory
      case _ => false
    }
    
    def toString: String = fullId
    功能: 信息显示
    
    def hashCode: Int = toString.hashCode()
    功能: 求解hashcode
    
    def copyState(execDesc: ExecutorDescription): Unit
    功能: 从制定的执行器描述@execDesc中拷贝所有状态变量
}
```

##### FileSystemPersistenceEngine

```scala
private[master] class FileSystemPersistenceEngine(
    val dir: String,
    val serializer: Serializer)
extends PersistenceEngine with Logging {
    介绍: 文件系统持久化引擎
    存储数据到磁盘目录上,按照每个worker每个应用存储一个文件的原则存储.
    构造器参数:
    	dir	目录名称
    	serializer	序列化器
    操作集:
    def persist(name: String, obj: Object): Unit
    功能: 将指定对象@obj 持久化为文件名称为dir+name的文件
    serializeIntoFile(new File(dir + File.separator + name), obj)
    
    def unpersist(name: String): Unit
    功能: 去除指定dir+name持久化
    val f = new File(dir + File.separator + name)
    if (!f.delete()) {
      logWarning(s"Error deleting ${f.getPath()}")
    }
    
    def read[T: ClassTag](prefix: String): Seq[T]
    功能: 读取目录dir中前缀为@prefix的文件
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T])
    
    def serializeIntoFile(file: File, value: AnyRef): Unit
    功能: 序列化为文件
    val created = file.createNewFile()
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }
    val fileOut = new FileOutputStream(file)
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      out = serializer.newInstance().serializeStream(fileOut)
      out.writeObject(value)
    } {
      fileOut.close()
      if (out != null) {
        out.close()
      }
    }
    
    def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T
    功能: 反序列化指定文件
    val fileIn = new FileInputStream(file)
    var in: DeserializationStream = null
    try {
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
    
    初始化操作:
    new File(dir).mkdir()
    功能: 创建指定目录
}
```

##### LeaderElectionAgent

```scala
@DeveloperApi
trait LeaderElectionAgent {
	介绍: leader选举代理,定位当前master,对于所有选举代理来说是一个通用接口
    val masterInstance: LeaderElectable 
    def stop(): Unit = {} // to avoid noops in implementations.
}

@DeveloperApi
trait LeaderElectable {
    介绍: 可以选择leader
    def electedLeader(): Unit
    功能: 选举leader
    def revokedLeadership(): Unit
    功能: 取消leader
}

private[spark] class MonarchyLeaderAgent(val masterInstance: LeaderElectable)
extends LeaderElectionAgent {
    介绍: leader选举代理的单节点实现
    masterInstance.electedLeader()
}
```

##### master

```scala
private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {
    构造器属性:
    	rpcEnv	rpc环境
    	address	RPC地址
    	webUIPort	web端口
    	securityMgr	安全管理器
    	conf	spark配置
    属性:
    #name @forwardMessageThread	转发消息的线程
    val= ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "master-forward-message-thread")
    #name @hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)	hadoop配置
    #name @workerTimeoutMs = conf.get(WORKER_TIMEOUT) * 1000	worker超时时间
    #name @retainedApplications = conf.get(RETAINED_APPLICATIONS)	保存的应用数量
    #name @retainedDrivers = conf.get(RETAINED_DRIVERS)	保存的driver数量
    #name @reaperIterations = conf.get(REAPER_ITERATIONS)	弃用的worker数量
    #name @recoveryMode = conf.get(RECOVERY_MODE)	恢复模式
    #name @maxExecutorRetries = conf.get(MAX_EXECUTOR_RETRIES)	最大尝试次数
    #name @workers = new HashSet[WorkerInfo]	worker列表
    #name @idToApp = new HashMap[String, ApplicationInfo]	id与应用映射表
    #name @waitingApps = new ArrayBuffer[ApplicationInfo]	等待应用列表
    #name @apps = new HashSet[ApplicationInfo]	应用数量
    #name @idToWorker = new HashMap[String, WorkerInfo]	id与worker的映射表
    #name @addressToWorker = new HashMap[RpcAddress, WorkerInfo]	worker的RPC地址表
    #name @endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]	端点引用-->应用映射
    #name @addressToApp = new HashMap[RpcAddress, ApplicationInfo]	RPC地址与应用的映射表
    #name @completedApps = new ArrayBuffer[ApplicationInfo]	完成的应用列表
    #name @nextAppNumber = 0	应用编号
    #name @drivers = new HashSet[DriverInfo]	驱动器列表
    #name @completedDrivers = new ArrayBuffer[DriverInfo]	已经完成的驱动器
    #name @waitingDrivers = new ArrayBuffer[DriverInfo]	正在等待的驱动器
    #name @nextDriverNumber = 0	驱动器编号
    #name @masterMetricsSystem	master度量系统
    val= MetricsSystem.createMetricsSystem(
        MetricsSystemInstances.MASTER, conf, securityMgr)
    #name @applicationMetricsSystem	应用度量系统
    val= MetricsSystem.createMetricsSystem(
        MetricsSystemInstances.APPLICATIONS, conf, securityMgr)
    #name @masterSource = new MasterSource(this)	master资源
    #name @webUi: MasterWebUI = null	web端口,启动时会设置
    #name @masterPublicAddress	master公用地址
    val= {
        val envVar = conf.getenv("SPARK_PUBLIC_DNS")
        if (envVar != null) envVar else address.host
      }
    #name @masterUrl = address.toSparkURL	master地址
    #name @masterWebUiUrl: String = _	webUI地址
    #name @state = RecoveryState.STANDBY	恢复状态(备用)
    #name @persistenceEngine: PersistenceEngine = _	持久化引擎
    #name @leaderElectionAgent: LeaderElectionAgent = _	选举代理
    #name @recoveryCompletionTask: ScheduledFuture[_] = _	恢复完成的任务
    #name @checkForWorkerTimeOutTask: ScheduledFuture[_] = _	检查worker超时的线程
    #name @spreadOutApps = conf.get(SPREAD_OUT_APPS)	传递的应用
    #name @defaultCores = conf.get(DEFAULT_CORES)	默认核心数量
    #name @reverseProxy = conf.get(UI_REVERSE_PROXY)	UI反向代理
    #name @restServerEnabled = conf.get(MASTER_REST_SERVER_ENABLED)	rest服务器启动标记
    #name @restServer: Option[StandaloneRestServer] = None	rest服务器
    #name @restServerBoundPort: Option[Int] = None	rest服务器绑定端口
    初始化操作:
    Utils.checkHost(address.host)
    功能: 检查主机信息
    
    if (defaultCores < 1) {
        throw new SparkException(s"${DEFAULT_CORES.key} must be positive")
      }
    功能: 核心数量校验
    
    {
    val authKey = SecurityManager.SPARK_AUTH_SECRET_CONF
    require(conf.getOption(authKey).isEmpty || !restServerEnabled,
      s"The RestSubmissionServer does not support authentication via 
      ${authKey}.  Either turn " +
      "off the RestSubmissionServer with spark.master.rest.enabled=false,
      or do not use " +
      "authentication.")
    }
    功能: 授权密钥断言

    
    操作集:
    def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
    功能: 创建日志形式
    
    def onStart(): Unit
    功能: 启动master
    1. 设置并绑定webUI
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    webUi = new MasterWebUI(this, webUiPort)
    webUi.bind()
    masterWebUiUrl = s"${webUi.scheme}$masterPublicAddress:${webUi.boundPort}"
    2. 进行可能的反向代理配置
    if (reverseProxy) {
      masterWebUiUrl = conf.get(UI_REVERSE_PROXY_URL).orElse(Some(masterWebUiUrl)).get
      webUi.addProxy()
      logInfo(s"Spark Master is acting as a reverse proxy. Master, Workers and " +
       s"Applications UIs are available at $masterWebUiUrl")
    }
    3. 设置检查worker超时线程的执行内容
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError { self.send(CheckForWorkerTimeOut) },
      0, workerTimeoutMs, TimeUnit.MILLISECONDS)
    4. 启动rest服务器
    if (restServerEnabled) {
      val port = conf.get(MASTER_REST_SERVER_PORT)
      restServer = Some(new StandaloneRestServer(
          address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())
    5. 设置度量系统参数
    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    6. 设置持久化和master选举信息
    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = recoveryMode match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get(RECOVERY_MODE_FACTORY))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
    
    def onStop(): Unit
    功能: 停止master
    1. 汇报度量信息
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    2. 取消辅助线程
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    3. 关闭WEB,REST,度量系统
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
    
    def electedLeader(): Unit= self.send(ElectedLeader)
    功能: 通过RPC发送leader选举消息@ElectedLeader
    
    def revokedLeadership(): Unit = self.send(RevokedLeadership)
    功能: 撤销leader,通过发送取消消息@RevokedLeadership
    
    def receive: PartialFunction[Any, Unit]
    功能: 接受并处理RPC消息
    case ElectedLeader => // 处理leader的选举的消息
      val (storedApps, storedDrivers, storedWorkers) =
    	persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(CompleteRecovery)
          }
        }, workerTimeoutMs, TimeUnit.MILLISECONDS)
      }
    case CompleteRecovery => completeRecovery() // 处理完全恢复消息
    case RevokedLeadership => // 取消leader消息
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
    case RegisterWorker( // 注册worker的消息
      id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl,
      masterAddress, resources) =>
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      if (state == RecoveryState.STANDBY) {
        workerRef.send(MasterInStandby)
      } else if (idToWorker.contains(id)) {
        workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress, true))
      } else {
        val workerResources = resources.map(r => r._1 -> WorkerResourceInfo(
            r._1, r._2.addresses))
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerWebUiUrl, workerResources)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress, false))
          schedule()
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to 
          re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register 
          worker at same address: "
            + workerAddress))
        }
      }
    
    case RegisterApplication(description, driver) => // 注册应用的消息
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        val app = createApplication(description, driver)
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        persistenceEngine.addApplication(app)
        driver.send(RegisteredApplication(app.id, self))
        schedule()
      }
    
    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      // 执行器状态改变的消息
      1. 获取执行器属性
   	  val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) =>
          val appInfo = idToApp(appId)
          val oldState = exec.state
          exec.state = state
          if (state == ExecutorState.RUNNING) {
            assert(oldState == ExecutorState.LAUNCHING,
              s"executor $execId state transfer from $oldState to RUNNING is illegal")
            appInfo.resetRetryCount()
          }
          exec.application.driver.send(ExecutorUpdated(execId, state, message,
                                                       exitStatus, false))
          if (ExecutorState.isFinished(state)) {
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            if (!appInfo.isFinished) {
              appInfo.removeExecutor(exec)
            }
            exec.worker.removeExecutor(exec)
            val normalExit = exitStatus == Some(0)
            if (!normalExit
                && appInfo.incrementRetryCount() >= maxExecutorRetries
                && maxExecutorRetries >= 0) {
              val execs = appInfo.executors.values
              if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                logError(s"Application ${appInfo.desc.name} 
                with ID ${appInfo.id} failed " +
                  s"${appInfo.retryCount} times; removing it")
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }
          schedule()
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }
    
    case DriverStateChanged(driverId, state, exception) => // 驱动器状态改变
      state match {
        case DriverState.ERROR | DriverState.FINISHED | 
          DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update 
          for driver $driverId: $state")
      }
    
    case Heartbeat(workerId, worker) => // 接受worker发送过来的心跳信息
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }
    
    case MasterChangeAcknowledged(appId) => // 接受或者master的消息
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }
      if (canCompleteRecovery) { completeRecovery() }
    
    case WorkerSchedulerStateResponse(workerId, execResponses, driverResponses) =>
    	// 接受worker调度状态响应消息
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE
          val validExecutors = execResponses.filter(
            exec => idToApp.get(exec.desc.appId).isDefined)
          for (exec <- validExecutors) {
            val (execDesc, execResources) = (exec.desc, exec.resources)
            val app = idToApp(execDesc.appId)
            val execInfo = app.addExecutor(
              worker, execDesc.cores, execResources, Some(execDesc.execId))
            worker.addExecutor(execInfo)
            worker.recoverResources(execResources)
            execInfo.copyState(execDesc)
          }
          for (driver <- driverResponses) {
            val (driverId, driverResource) = (driver.driverId, driver.resources)
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              driver.withResources(driverResource)
              worker.recoverResources(driverResource)
              worker.addDriver(driver)
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }
      if (canCompleteRecovery) { completeRecovery() }
    
    case UnregisterApplication(applicationId) => // 解除应用的注册消息
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)
    
    case CheckForWorkerTimeOut => // 检查worker超时消息
      timeOutDeadWorkers()
    
    case WorkerLatestState(workerId, executors, driverIds) =>
    // 接受worker最新状态的消息
      idToWorker.get(workerId) match {
        case Some(worker) =>
          for (exec <- executors) {
            val executorMatches = worker.executors.exists {
              case (_, e) => e.application.id == exec.appId && e.id == exec.execId
            }
            if (!executorMatches) {
              worker.endpoint.send(KillExecutor(masterUrl, exec.appId, exec.execId))
            }
          }
          for (driverId <- driverIds) {
            val driverMatches = worker.drivers.exists { case (id, _) => id == driverId }
            if (!driverMatches) {
              // master doesn't recognize this driver. So just tell worker to kill it.
              worker.endpoint.send(KillDriver(driverId))
            }
          }
        case None =>
          logWarning("Worker state from unknown worker: " + workerId)
      }
    
    def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
    功能: 接受并回应RPC消息
    case RequestSubmitDriver(description) =>
    // master请求提交驱动器的消息
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()
        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }
    
    case RequestKillDriver(driverId) =>
    	// 请求kill驱动器的消息
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
            } else {
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg))
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }
    
    case RequestDriverStatus(driverId) =>
    	// 回应请求驱动器状态的消息
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None,
                               Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }
    
    case RequestMasterState => // 请求master状态的消息
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state))
    
    case BoundPortsRequest =>  // 请求绑定端口的消息
      context.reply(
          BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))
    
    case RequestExecutors(appId, requestedTotal) => // 请求执行器消息
      context.reply(handleRequestExecutors(appId, requestedTotal))
    
    case KillExecutors(appId, executorIds) => // kill执行器消息
      val formattedExecutorIds = formatExecutorIds(executorIds)
      context.reply(handleKillExecutors(appId, formattedExecutorIds))
    
    def onDisconnected(address: RpcAddress): Unit
    功能: 断开连接处理
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker(_, s"${address} 
    got disassociated"))
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
    
    def canCompleteRecovery
    功能: 确定是否能够完全恢复
    val= workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0
    
    def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]): Unit
    功能: 启动恢复工作
    1. 恢复应用信息
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }
    2. 恢复执行器和驱动器
    for (driver <- storedDrivers) {
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + 
                                     " had exception on reconnect")
      }
    }
    
    def completeRecovery(): Unit
    功能: 完全恢复master
    1. 使用短期同步,确保只有一次恢复
    if (state != RecoveryState.RECOVERING) { return }
    state = RecoveryState.COMPLETING_RECOVERY
    2. kill所有不回应的worker和应用
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(
      removeWorker(_, "Not responding for recovery"))
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)
    3. 更新恢复的应用状态为RUNNING
    apps.filter(_.state == ApplicationState.WAITING).foreach(
        _.state = ApplicationState.RUNNING)
    4. 重新调度没有执行器的驱动器
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }
    5. 修改恢复状态，并启动调度
    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
    
    def startExecutorsOnWorkers(): Unit
    功能: 启动worker上的执行器
    1. 使用FIFO调度策略，使用等待队列中的应用
    for (app <- waitingApps) {
      val coresPerExecutor = app.desc.coresPerExecutor.getOrElse(1)
      if (app.coresLeft >= coresPerExecutor) {
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(canLaunchExecutor(_, app.desc))
          .sortBy(_.coresFree).reverse
        if (waitingApps.length == 1 && usableWorkers.isEmpty) {
          logWarning(s"App ${app.id} requires more resource than 
          any of Workers could have.")
        }
        val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)
        for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
          allocateWorkerResourceToExecutors(
            app, assignedCores(pos), app.desc.coresPerExecutor, usableWorkers(pos))
        }
      }
    }
    
    def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit
    功能： 分配worker的资源到执行器上
    如果执行器的核心数量已经分配了，那么对剩余的执行器进行均分
    1. 获取执行数量，确定需要分配的核心数量
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    2. 分配执行器资源
    for (i <- 1 to numExecutors) {
       // 分配资源--> 添加到执行器列表中 --> 运行执行器 --> 设置应用状态
      val allocated = worker.acquireResources(app.desc.resourceReqsPerExecutor)
      val exec = app.addExecutor(worker, coresToAssign, allocated)
      launchExecutor(worker, exec)
      app.state = ApplicationState.RUNNING
    }
    
    def canLaunch(
      worker: WorkerInfo,
      memoryReq: Int,
      coresReq: Int,
      resourceRequirements: Seq[ResourceRequirement])
    : Boolean
    功能: 确定master是否可以被启动
    输入参数:
    	worker	worker信息
    	memoryReq	内存请求量
    	coresReq	请求核心数量
    	resourceRequirements	资源列表
    1. 确定内存是否足够
    val enoughMem = worker.memoryFree >= memoryReq
    2. 确定核心数量是否足够
    val enoughCores = worker.coresFree >= coresReq
    3. 确定资源是否足够
    val enoughResources = ResourceUtils.resourcesMeetRequirements(
      worker.resourcesAmountFree, resourceRequirements)
    val=  enoughMem && enoughCores && enoughResources
    
    def canLaunchDriver(worker: WorkerInfo, desc: DriverDescription): Boolean
    功能: 确定是否可以运行驱动器
    val= canLaunch(worker, desc.mem, desc.cores, desc.resourceReqs)
    
    def canLaunchExecutor(worker: WorkerInfo, desc: ApplicationDescription): Boolean
    功能: 确定是否可以运行执行器
    val= canLaunch(
      worker,
      desc.memoryPerExecutorMB,
      desc.coresPerExecutor.getOrElse(1),
      desc.resourceReqsPerExecutor)
    
    def schedule(): Unit 
    功能: 调度当前可用资源,用于分配给等待的应用.当新的应用添加的时候或者有新的可用资源的时候就会调用.
    1. 状态校验
    if (state != RecoveryState.ALIVE) {
      return
    }
    2. worker顺序随机化
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(
        _.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    3. 对驱动器中的执行器分配资源,假设等待驱动器的执行器按照round-robin的格式进行等待,对于每个驱动器来说,开始于上一个分配驱动器的worker,继续直到所有的存活worker全部访问完毕.
    for (driver <- waitingDrivers.toList) {
      var launched = false
      var isClusterIdle = true
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        isClusterIdle = worker.drivers.isEmpty && worker.executors.isEmpty
        numWorkersVisited += 1
        if (canLaunchDriver(worker, driver.desc)) {
            // 满足分配条件,分配内存等资源,并更新资源列表
          val allocated = worker.acquireResources(driver.desc.resourceReqs)
          driver.withResources(allocated)
          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        // round-robin 访问下一个驱动器
        curPos = (curPos + 1) % numWorkersAlive
      }
      if (!launched && isClusterIdle) {
        logWarning(s"Driver ${driver.id} requires more
        resource than any of Workers could have.")
      }
    }
    4. 启动worker上的执行器
    startExecutorsOnWorkers()
    
    def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit
    功能: 运行执行器
    1. 添加执行器
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    2. worker端发送运行执行器消息
    worker.endpoint.send(LaunchExecutor(masterUrl, exec.application.id, exec.id,
      exec.application.desc, exec.cores, exec.memory, exec.resources))
    3. 驱动器发送添加当前执行器的消息
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
    
    def registerWorker(worker: WorkerInfo): Boolean
    功能: 注册worker(注意在一个节点上可能有一个或者多个指向死亡worker的应用,需要移除)
    1. 过滤死亡的worker
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }
    2. 处理旧worker
    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        removeWorker(oldWorker, "Worker replaced by a new worker with same address")
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }
    3. 设置并注册新的worker
    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    val= true
    
    def removeWorker(worker: WorkerInfo, msg: String): Unit
    功能: 移除指定worker
    1. 修改指定worker的状态为dead,并从注册表中移除
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address
    2. 告知应用执行器丢失
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
        // 发送执行器状态更新消息
      exec.application.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None, workerLost = true))
        // 修改并移除执行器
      exec.state = ExecutorState.LOST
      exec.application.removeExecutor(exec)
    }
    3. 处理驱动器,驱动器处于监视状态下,则重启驱动器即可,否则需要移除驱动器
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    4. 告知应用丢失的worker
    logInfo(s"Telling app of lost worker: " + worker.id)
    apps.filterNot(completedApps.contains(_)).foreach { app =>
      app.driver.send(WorkerRemoved(worker.id, worker.host, msg))
    }
    5. 从持久化引擎中移除丢失worker
    persistenceEngine.removeWorker(worker)
    
    def relaunchDriver(driver: DriverInfo): Unit
    功能: 重新运行driver
    必须要使用新的driver编号创建driver,因为原始的驱动器可能仍旧在运行.考虑到worker与master是网络连接,master重启使用drverID1,需要使用driverID2,然后worker就可以连接到master.如果ID1=ID2,master就无法分辨出是状态更新还是重启一个驱动器了.请参考SPARK-19900.
    1. 移除原来的驱动器
    removeDriver(driver.id, DriverState.RELAUNCHING, None)
    2. 新建驱动器,并注册到注册表中
    val newDriver = createDriver(driver.desc)
    persistenceEngine.addDriver(newDriver)
    drivers.add(newDriver)
    waitingDrivers += newDriver
    3. 重新调度
    schedule()
    
    def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo
    功能: 创建应用
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    val= new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
    
    def registerApplication(app: ApplicationInfo): Unit
    功能: 注册应用
    val appAddress = app.driver.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }
    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
    
    def finishApplication(app: ApplicationInfo): Unit
    功能: 结束应用
    removeApplication(app, ApplicationState.FINISHED)
    
    def removeApplication(app: ApplicationInfo, state: ApplicationState.Value): Unit
    功能: 移除应用
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address
      if (completedApps.size >= retainedApplications) {
        val toRemove = math.max(retainedApplications / 10, 1)
        completedApps.take(toRemove).foreach { a =>
          applicationMetricsSystem.removeSource(a.appSource)
        }
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      for (exec <- app.executors.values) {
        killExecutor(exec)
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver.send(ApplicationRemoved(state.toString))
      }
      persistenceEngine.removeApplication(app)
      schedule()
      // worker告知master应用以及结束
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
    
    def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean
    功能: 处理请求执行器
    输入参数:
    	appId	应用编号
    	requestTotal	请求执行器总量
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requested to 
        set total executors to $requestedTotal.")
        appInfo.executorLimit = requestedTotal
        schedule()
        true
      case None =>
        logWarning(s"Unknown application $appId requested 
        $requestedTotal total executors.")
        false
    }
    
    def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean
    功能: 处理执行器的kill
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requests to kill executors:
        " + executorIds.mkString(", "))
        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
        known.foreach { executorId =>
          val desc = appInfo.executors(executorId)
          appInfo.removeExecutor(desc)
          killExecutor(desc)
        }
        if (unknown.nonEmpty) {
          logWarning(s"Application $appId attempted to kill non-existent executors: "
            + unknown.mkString(", "))
        }
        schedule()
        true
      case None =>
        logWarning(s"Unregistered application $appId requested us to kill executors!")
        false
    }
    
    def formatExecutorIds(executorIds: Seq[String]): Seq[Int]
    功能: 格式化执行器ID
    val= executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
    
    def killExecutor(exec: ExecutorDesc): Unit 
    功能: kill执行器,询问worker去kill指定执行器
    exec.worker.removeExecutor(exec)
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    exec.state = ExecutorState.KILLED
    
    def newApplicationId(submitDate: Date): String
    功能: 先进应用ID
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    val= appId
    
    def timeOutDeadWorkers(): Unit 
    功能: 检查,移除任何超时的worker
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime -
                                  workerTimeoutMs).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        val workerTimeoutSecs = TimeUnit.MILLISECONDS.toSeconds(workerTimeoutMs)
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, workerTimeoutSecs))
        removeWorker(worker, s"Not receiving heartbeat for $workerTimeoutSecs seconds")
      } else {
        if (worker.lastHeartbeat < currentTime - 
            ((reaperIterations + 1) * workerTimeoutMs)) {
          workers -= worker 
        }
      }
    }
    
    def newDriverId(submitDate: Date): String
    功能: 新建驱动器ID
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate),
                                        nextDriverNumber)
    nextDriverNumber += 1
    val= appId
    
    def createDriver(desc: DriverDescription): DriverInfo
    功能: 创建驱动器
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val= new DriverInfo(now, newDriverId(date), desc, date)
    
    def launchDriver(worker: WorkerInfo, driver: DriverInfo): Unit
    功能: 运行驱动器
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    worker.addDriver(driver)
    driver.worker = Some(worker)
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc, driver.resources))
    driver.state = DriverState.RUNNING
    
    def removeDriver(
      driverId: String,
      finalState: DriverState,
      exception: Option[Exception]): Unit
    功能: 移除驱动器
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        drivers -= driver
        if (completedDrivers.size >= retainedDrivers) {
          val toRemove = math.max(retainedDrivers / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        completedDrivers += driver
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        driver.worker.foreach(w => w.removeDriver(driver))
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
    
    def canLaunchExecutorForApp(pos: Int): Boolean
    功能: 确定是否可以运行执行器
    val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(
          pos) >= minCoresPerExecutor
      val assignedExecutorNum = assignedExecutors(pos)
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutorNum == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutorNum * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory 
          >= memoryPerExecutor
        val assignedResources = resourceReqsPerExecutor.map {
          req => req.resourceName -> req.amount * assignedExecutorNum
        }.toMap
        val resourcesFree = usableWorkers(pos).resourcesAmountFree.map {
          case (rName, free) => rName -> (free - assignedResources.getOrElse(rName, 0))
        }
        val enoughResources = ResourceUtils.resourcesMeetRequirements(
          resourcesFree, resourceReqsPerExecutor)
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && enoughResources && underLimit
      } else {
        keepScheduling && enoughCores
      }
    
    def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int]
    功能: 调度worker上的执行器
    调度执行器,用于运行在worker上,返回一个数组,这个数组包含分配到每个worker上的核心的数量.
    有两种运行执行器的模式,第一种尝试尽可能多的传送应用的执行器.而第二种需要越少越好.前者适合与数据本地化且是默认配置.
    分配到执行器的核心数量已经配置,来自同一个应用的多个执行器可以运行在一个worker上.(只要这个worker拥有足够多的核心和内存).否则每个执行器按照默认配置获取核心数量.在这种情况下,每个应用的一个执行器可以在单个调度中运行.
    既然`spark.executor.cores`没有设置,人就在同一个worker上运行多个执行器,考虑到两个应用A,B.在worker1上,A的剩余核心数量大于0.B完成且释放所有worker1的核心.因此A运行的时候可以利用所有的核心.因此可以使得A在worker1上运行.
    同时为每个worker分配每个执行器的核心数量是重要的.考虑到下述案例:
   	用户需要3个执行器, (spark.cores.max = 48, spark.executor.cores = 16)
    集群含有4个worker,使用16个核心,需要同时分配,12个核心会被分配到执行器,因为12<16.没有执行器可以运行.
    1. 获取基本参数
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val resourceReqsPerExecutor = app.desc.resourceReqsPerExecutor
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) 
    val assignedExecutors = new Array[Int](numUsable) 
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
    2. 保持执行器的运行状态，直到worker中没有足够的执行器可用，或者达到应用的限制
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutorForApp)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutorForApp(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutorForApp)
    }
}
```

```scala
private[deploy] object Master extends Logging {
    属性:
    #name @SYSTEM_NAME = "sparkMaster"	系统名称
    #name @ENDPOINT_NAME = "Master"	端点名称
    操作集:
    def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int])
    功能: 启动master并返回三元组(master RPC环境,webUI端口,rest服务器)
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    val= (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
    
    def main(argStrings: Array[String]): Unit 
    功能: 启动函数
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(
        args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
}
```



##### MasterArguments

```scala
private[master] class MasterArguments(args: Array[String], conf: SparkConf) 
extends Logging {
    介绍: master属性
    构造器参数:
    	args	属性列表
    	conf	spark配置
    属性:
    #name @host = Utils.localHostName()	主机名称
    #name @port = 7077	端口号
    #name @webUiPort = 8080	web端口号
    #name @propertiesFile: String = null	属性文件
    操作集:
    @tailrec
    private def parse(args: List[String]): Unit
    功能: 指令转换
    args match {
        case ("--ip" | "-i") :: value :: tail => // 主机指令
          Utils.checkHost(value)
          host = value
          parse(tail)
        case ("--host" | "-h") :: value :: tail => // 主机指令
          Utils.checkHost(value)
          host = value
          parse(tail)
        case ("--port" | "-p") :: IntParam(value) :: tail => // 端口指令
          port = value
          parse(tail)
        case "--webui-port" :: IntParam(value) :: tail => // web端口指令
          webUiPort = value
          parse(tail)
        case ("--properties-file") :: value :: tail => // 指定配置文件
          propertiesFile = value
          parse(tail)
        case ("--help") :: tail => // 正常打印信息并退出
          printUsageAndExit(0)
        case Nil => // No-op
        case _ =>
          printUsageAndExit(1) // 打印信息,非正常退出
      }
    
    def printUsageAndExit(exitCode: Int): Unit
    功能: 打印使用信息,并退出
    System.err.println(
      "Usage: Master [options]\n" +
      "\n" +
      "Options:\n" +
      "  -i HOST, --ip HOST     Hostname to listen on 
      (deprecated, please use --host or -h) \n" +
      "  -h HOST, --host HOST   Hostname to listen on\n" +
      "  -p PORT, --port PORT   Port to listen on (default: 7077)\n" +
      "  --webui-port PORT      Port for web UI (default: 8080)\n" +
      "  --properties-file FILE Path to a custom Spark properties file.\n" +
      "                         Default is conf/spark-defaults.conf.")
    System.exit(exitCode)
    
    初始化操作:
    if (System.getenv("SPARK_MASTER_IP") != null) {
        logWarning("SPARK_MASTER_IP is deprecated, please use SPARK_MASTER_HOST")
        host = System.getenv("SPARK_MASTER_IP")
      }
      if (System.getenv("SPARK_MASTER_HOST") != null) {
        host = System.getenv("SPARK_MASTER_HOST")
      }
      if (System.getenv("SPARK_MASTER_PORT") != null) {
        port = System.getenv("SPARK_MASTER_PORT").toInt
      }
      if (System.getenv("SPARK_MASTER_WEBUI_PORT") != null) {
        webUiPort = System.getenv("SPARK_MASTER_WEBUI_PORT").toInt
      }
    功能: 检查环境变量
    
    parse(args.toList)
    功能: 转换参数列表中的参数
    
    propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)
    功能: 指定配置文件
    
    if (conf.contains(MASTER_UI_PORT.key)) {
        webUiPort = conf.get(MASTER_UI_PORT)
    }
    功能: 设定web端口
}
```

##### MasterMessages

```scala
private[master] object MasterMessages {
    介绍: master消息,内部包含只有master可见的信息
    
    ---
    使用选举代理
    
    case object ElectedLeader
    介绍: 选举的leader
    
    case object RevokedLeadership
    介绍: 取消leader
    
    ---
    自己管理自己
    case object CheckForWorkerTimeOut
    介绍: 检查超时时间
    
    case class BeginRecovery(storedApps: Seq[ApplicationInfo], 
                             storedWorkers: Seq[WorkerInfo])
    介绍: 开始恢复
    
    case object CompleteRecovery
    介绍: 完成恢复
    
    case object BoundPortsRequest
    介绍: 请求界限端口
    
    class BoundPortsResponse(rpcEndpointPort: Int, webUIPort: Int, restPort: Option[Int])
    介绍: 响应界限端口
}
```

##### MasterSource

```scala
private[spark] class MasterSource(val master: Master) extends Source {
    属性: 
    #name @metricRegistry = new MetricRegistry()	度量值注册器
    #name @sourceName = "master"	资源名称
    初始化操作:
    metricRegistry.register(MetricRegistry.name("workers"), new Gauge[Int] {
        override def getValue: Int = master.workers.size
    })
	功能: 注册worker信息
    
    metricRegistry.register(MetricRegistry.name("aliveWorkers"), new Gauge[Int]{
        override def getValue: Int = master.workers.count(_.state == WorkerState.ALIVE)
    })
    功能: 注册存活的worker信息
    
    metricRegistry.register(MetricRegistry.name("apps"), new Gauge[Int] {
        override def getValue: Int = master.apps.size
    })
    功能: 注册应用信息
    
    metricRegistry.register(MetricRegistry.name("waitingApps"), new Gauge[Int] {
        override def getValue: Int = master.apps.count(
            _.state == ApplicationState.WAITING)
    })
    功能: 注册等待应用信息
}
```

##### PersistenceEngine

```markdown
介绍:
 	运行master持久化任何需要的状态,用于从失败中恢复
 	需要下述语义:
 	1. 添加应用@addApplication和添加worker @addWorker 在完成新的应用/worker注册之前进行调用.
 	2. 移除应用和移除worker可以在任何时刻调用
 	满足上述两点前置需求,就可以将应用/worker进行持久化,但是不可以删除已经完成的应用/worker(恢复的时候必须要获得生产状态)
 	这个类的实现定义了命名对象键值对是如何进行存储和检索的.
```

```scala
@DeveloperApi
abstract class PersistenceEngine {
    操作集:
    def persist(name: String, obj: Object): Unit
    功能: 持久化指定对象@obj
    
    def unpersist(name: String): Unit
    功能: 解除指定名称的持久化
    
    def read[T: ClassTag](prefix: String): Seq[T]
    功能:  根据前缀信息@prefix 返回读取回来的数据
    
    final def addApplication(app: ApplicationInfo): Unit
    功能: 添加应用
    persist("app_" + app.id, app)
    
    final def removeApplication(app: ApplicationInfo): Unit
    功能: 移除应用@app
    unpersist("app_" + app.id)
    
    final def addWorker(worker: WorkerInfo): Unit
    功能: 添加worker
    persist("worker_" + worker.id, worker)
    
    final def removeWorker(worker: WorkerInfo): Unit 
    功能: 移除worker
    unpersist("worker_" + worker.id)
    
    final def addDriver(driver: DriverInfo): Unit
    功能: 添加驱动器
    persist("driver_" + driver.id, driver)
    
    final def removeDriver(driver: DriverInfo): Unit
    功能: 移除驱动器
    unpersist("driver_" + driver.id)
    
    def close(): Unit = {}
    功能: 关闭引擎
    
    final def readPersistedData(
      rpcEnv: RpcEnv): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo])
    功能: 读取持久化数据
    rpcEnv.deserialize { () =>
      (read[ApplicationInfo]("app_"), read[DriverInfo]("driver_"), 
       read[WorkerInfo]("worker_"))
    }
}
```

```scala
private[master] class BlackHolePersistenceEngine extends PersistenceEngine {
    def persist(name: String, obj: Object): Unit = {}
    功能: 持久化
    
    def unpersist(name: String): Unit = {}
    功能: 解除持久化
    
    def read[T: ClassTag](name: String): Seq[T] = Nil
    功能: 读取持久化数据
}
```

##### StandaloneRecoveryModeFactory

```scala
@DeveloperApi
abstract class StandaloneRecoveryModeFactory(conf: SparkConf, serializer: Serializer){
    介绍: 独立运行恢复模式工厂
    这个类的实现可以在spark独立模式下作为恢复模式进行插拔
    操作集:
    def createPersistenceEngine(): PersistenceEngine
    功能: 创建持久化引擎
    
    def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent
    功能: 创建leader引擎代理
}
```

```scala
private[master] class FileSystemRecoveryModeFactory(
    conf: SparkConf, serializer: Serializer)
extends StandaloneRecoveryModeFactory(conf, serializer) with Logging {
    介绍: 文件系统恢复模式工厂,这种情况下领导者代理@LeaderAgent 是无法操作的,由于是从文件系统进行恢复,所以leader永远是leader.
    属性:
    #name @recoveryDir = conf.get(RECOVERY_DIRECTORY)	恢复目录
    操作集:
    def createPersistenceEngine(): PersistenceEngine
    功能: 创建持久化引擎
    logInfo("Persisting recovery state to directory: " + recoveryDir)
    val= new FileSystemPersistenceEngine(recoveryDir, serializer)
    
    def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent
    功能: 创建leader代理
    val= new MonarchyLeaderAgent(master)
}
```

```scala
private[master] class ZooKeeperRecoveryModeFactory(
    conf: SparkConf, serializer: Serializer)
extends StandaloneRecoveryModeFactory(conf, serializer) {
    介绍: zookeeper恢复模式工厂
    操作集:
    def createPersistenceEngine(): PersistenceEngine
    功能: 创建持久化引擎
    val= new ZooKeeperPersistenceEngine(conf, serializer)
    
    def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent
    功能: 创建leader选举代理
    val= new ZooKeeperLeaderElectionAgent(master, conf)
}
```

##### RecoveryState

```scala
private[deploy] object RecoveryState extends Enumeration {
    type MasterState = Value
	
    val STANDBY, ALIVE, RECOVERING, COMPLETING_RECOVERY = Value
    状态列表:
    	standby		备用
    	alive	存活
    	recovering	恢复状态
    	complete_recovery	完全恢复
}
```

##### WorkerInfo

```scala
private[spark] case class WorkerResourceInfo(name: String, addresses: Seq[String])
extends ResourceAllocator {
    介绍: worker的资源信息
    构造器参数:
    	name 	worker名称
    	address	地址列表
    操作集:
    def resourceName = this.name
    功能: 获取资源名称
    
    def resourceAddresses = this.addresses
    功能: 获取资源地址
    
    def slotsPerAddress: Int = 1
    功能: 获取每个地址的槽数
    
    def acquire(amount: Int): ResourceInformation
    功能: 获取指定数量的资源信息
    val allocated = availableAddrs.take(amount)
    acquire(allocated)
    val= new ResourceInformation(resourceName, allocated.toArray)
}
```

```scala
private[spark] class WorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val cores: Int,
    val memory: Int,
    val endpoint: RpcEndpointRef,
    val webUiAddress: String,
    val resources: Map[String, WorkerResourceInfo])
extends Serializable {
    介绍: worker信息
    构造器参数:
    	id	worker编号
    	host	主机名称
    	port	端口号
    	cores	核心数量
    	memory	内存大小
    	endpoint	RPC端点
    	webUiAddress	webUI地址
    	resources	资源列表
    属性:
    #name @executors: mutable.HashMap[String, ExecutorDesc] = _	执行器描述映射表
    #name @drivers: mutable.HashMap[String, DriverInfo] = _	驱动器映射表
    #name @state: WorkerState.Value = _	状态信息
    #name @coresUsed: Int = _	核心使用量
    #name @memoryUsed: Int = _	内存使用量
    #name @lastHeartbeat: Long = _	上次心跳时间
    操作集:
    def coresFree: Int = cores - coresUsed
    功能: 获取空闲核心数量
    
    def memoryFree: Int = memory - memoryUsed
    功能: 获取内存释放量
    
    def resourcesAmountFree: Map[String, Int]
    功能: 获取资源释放量
    val= resources.map { case (rName, rInfo) =>
      rName -> rInfo.availableAddrs.length
    }
    
    def resourcesInfo: Map[String, ResourceInformation]
    功能: 获取资源信息表
    val= resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.addresses.toArray)
    }
    
    def resourcesInfoFree: Map[String, ResourceInformation]
    功能: 获取资源释放表
    val= resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.availableAddrs.toArray)
    }
    
    def resourcesInfoUsed: Map[String, ResourceInformation]
    功能: 获取资源使用情况表
    val= resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.assignedAddrs.toArray)
    }
    
    def readObject(in: java.io.ObjectInputStream): Unit
    功能: 读取参数信息
    val= Utils.tryOrIOException {
        in.defaultReadObject()
        init()
      }
    
    def init(): Unit
    功能: 初始化
    executors = new mutable.HashMap
    drivers = new mutable.HashMap
    state = WorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
    
    def removeExecutor(exec: ExecutorDesc): Unit
    功能: 移除执行器(移除度量信息+ 释放执行器所占有的内存)
    if (executors.contains(exec.fullId)) {
      executors -= exec.fullId
      coresUsed -= exec.cores
      memoryUsed -= exec.memory
      releaseResources(exec.resources)
    }
    
    def addExecutor(exec: ExecutorDesc): Unit
    功能: 添加执行器
    executors(exec.fullId) = exec
    coresUsed += exec.cores
    memoryUsed += exec.memory
    
    def addDriver(driver: DriverInfo): Unit
    功能: 添加驱动器
    drivers(driver.id) = driver
    memoryUsed += driver.desc.mem
    coresUsed += driver.desc.cores
    
    def removeDriver(driver: DriverInfo): Unit
    功能: 移除驱动器
    drivers -= driver.id
    memoryUsed -= driver.desc.mem
    coresUsed -= driver.desc.cores
    releaseResources(driver.resources)
    
    def setState(state: WorkerState.Value): Unit
    功能: 设置worker状态
    this.state = state
    
    def isAlive(): Boolean = this.state == WorkerState.ALIVE
    功能: 确定worker是否存活
    
    def acquireResources(resourceReqs: Seq[ResourceRequirement])
    : Map[String, ResourceInformation]
    功能: 获取指定的资源列表,返回一个资源映射表(执行器编号--> 资源信息)
    resourceReqs.map { req =>
      val rName = req.resourceName
      val amount = req.amount
      rName -> resources(rName).acquire(amount)
    }.toMap
    
    def recoverResources(expected: Map[String, ResourceInformation]): Unit
    功能: 恢复资源,在master恢复时使用
    expected.foreach { case (rName, rInfo) =>
      resources(rName).acquire(rInfo.addresses)
    }
    
    def releaseResources(allocated: Map[String, ResourceInformation]): Unit
    功能: 释放指定的资源信息表
    allocated.foreach { case (rName, rInfo) =>
      resources(rName).release(rInfo.addresses)
    }
}
```

##### WorkerState

```scala
private[master] object WorkerState extends Enumeration {
    type WorkerState = Value

    val ALIVE, DEAD, DECOMMISSIONED, UNKNOWN = Value
  	worker状态列表:
    	alive	存活
    	dead	死亡
    	decommissioned	退役
    	unknown	未知状态
}
```

##### ZooKeeperLeaderElectionAgent

```scala
private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
    conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent with Logging  {
    介绍: zookeeper leader选举代理
    属性:
    #name @workingDir 	工作目录
    val= conf.get(ZOOKEEPER_DIRECTORY).getOrElse("/spark") + "/leader_election"
    #name @zk: CuratorFramework = _	zk监视框架
    #name @leaderLatch: LeaderLatch = _	领导占有者
    在一组JMV(连接到zk集群)中的多个竞争者中选取一个leader,如果N个线程/进程竞争leader,其中一个会被随机的任命为leader,直到它释放了leader权限,才会任命其他竞争者作为leader
    #name @status = LeadershipStatus.NOT_LEADER	leader状态
    操作集:
    def start(): Unit
    功能: 启动zk leader选举代理
    zk = SparkCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, workingDir)
    leaderLatch.addListener(this)
    leaderLatch.start()
    
    def stop(): Unit
    功能: 停止代理
    leaderLatch.close()
    zk.close()
    
    def isLeader(): Unit
    功能: 确认是否为leader
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }
      logInfo("We have gained leadership") // 获取了leader
      updateLeadershipStatus(true)
    }
    
    def notLeader(): Unit
    功能: 没有leader的处理
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }
      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
    
    def updateLeadershipStatus(isLeader: Boolean): Unit
    功能: 更新leader状态
    if (isLeader && status == LeadershipStatus.NOT_LEADER) { // leader选举处理
      status = LeadershipStatus.LEADER
      masterInstance.electedLeader()
    } else if (!isLeader && status == LeadershipStatus.LEADER) { // leader释放处理
      status = LeadershipStatus.NOT_LEADER
      masterInstance.revokedLeadership()
    }
    
    内部类:
    private object LeadershipStatus extends Enumeration {
        type LeadershipStatus = Value
        val LEADER, NOT_LEADER = Value
      }
}
```

##### ZooKeeperPersistenceEngine

```scala
private[master] class ZooKeeperPersistenceEngine(conf: SparkConf, val serializer: Serializer) extends PersistenceEngine with Logging {
    介绍: zk 持久化引擎
    属性:
    #name @workingDir	工作目录
    val= conf.get(ZOOKEEPER_DIRECTORY).getOrElse("/spark") + "/master_status"
    #name @zk: CuratorFramework = SparkCuratorUtil.newClient(conf)	zk监视框架
    操作集:
    def persist(name: String, obj: Object): Unit
    功能: 持久化数据
    serializeIntoFile(workingDir + "/" + name, obj)
    
    def unpersist(name: String): Unit
    功能: 解除持久化
    zk.delete().forPath(workingDir + "/" + name)
    
    def read[T: ClassTag](prefix: String): Seq[T]
    功能: 读取工作目录中,指定前缀的数据
    val= zk.getChildren.forPath(workingDir).asScala
      .filter(_.startsWith(prefix)).flatMap(deserializeFromFile[T])
    
    def close(): Unit
    功能: 关闭zk
    zk.close()
    
    def serializeIntoFile(path: String, value: AnyRef): Unit
    功能: 序列化文件
    val serialized = serializer.newInstance().serialize(value)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes)
    
    def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T]
    功能: 从文件中反序列化
    val fileData = zk.getData().forPath(workingDir + "/" + filename)
    val= try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(workingDir + "/" + filename)
        None
    }
}
```

#### rest

##### RestSubmissionClient

```markdown
介绍:
	这是一个客户端,可以提交应用到Rest服务器上.
 	在v1版本的协议中,rest url 的格式采取http://[host:port]/v1/submissions/[action],其中action是create,kill,status中的一个.每种请求代表着不同种类的HTTP消息.
 	1. submit	POST --> /submission/create
 	2. kill		POST --> /submission/kill/[submissionId]
 	3. status	GET --> /submission/status/[submissionId]
 	在类型1的情况下,蚕食使用json的格式放入HTTP的请求体中.否则URL完全指定客户端需要的动作.由于协议需要获取稳定的spark版本信息,所以存在的属性不能被添加或者移除,尽管新的配置属性可以添加.很少事件的前后兼容性被打破,spark必须提供一套新的协议(v2).
 	客户端和服务器必须使用同样的协议才能够进行通信,如果不匹配,服务器会回应其支持的最高版本.会创建一个任务,这个异步任务描述的是客户端尝试使用这个版本与服务器进行通信.
```

```scala
private[spark] class RestSubmissionClient(master: String) extends Logging {
    属性:
    #name @masters: Array[String]	master列表
    val= if (master.startsWith("spark://")) Utils.parseStandaloneMasterUrls(master)?
    	else Array(master)
    #name @lostMasters = new mutable.HashSet[String]	丢失master列表(去重)
    操作集:
    def createSubmission(request: CreateSubmissionRequest): SubmitRestProtocolResponse
    功能: 创建一个提交,如果提交成功,轮询提交的状态位,并汇报给用户.否则汇报服务端的错误信息
    1. 初始化执行标记
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    2. 处理master列表中的每个未处理的消息
    for (m <- masters if !handled) {
      // 验证master的合法性
      validateMaster(m)
      // 获取提交地址
      val url = getSubmitUrl(m)
      try {
        // 获取request请求
        response = postJson(url, request.toJson)
        // 执行请求处理
        response match {
          case s: CreateSubmissionResponse =>
            if (s.success) {
              reportSubmissionStatus(s)
              handleRestResponse(s)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    
    def killSubmission(submissionId: String): SubmitRestProtocolResponse
    功能: 请求kill指定请求@submissionId
    1. 初始化处理标记
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    2. 处理master中未处理的信息列表
    for (m <- masters if !handled) {
        // 验证master合法性
      validateMaster(m)
        // 获取需要kill的位置地址@URL
      val url = getKillUrl(m, submissionId)
      try {
          // 发送请求,获取响应
        response = post(url)
          // 响应处理
        response match {
          case k: KillSubmissionResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    val= response
    
    def requestSubmissionStatus(
      submissionId: String,
      quiet: Boolean = false): SubmitRestProtocolResponse
    功能: 请求获取提交状态
    输入参数:
    	quiet 是否后台执行
    1. 初始化处理标记
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    2. 处理masters列表中未处理消息中相关请求
    for (m <- masters if !handled) {
        // 验证master信息
      validateMaster(m)
        // 获取状态地址
      val url = getStatusUrl(m, submissionId)
          if (!quiet) { // 处理非后台执行
            handleRestResponse(s)
          }
          handled = true
          try {
              // 获取状态响应
            response = get(url)
            response match {
              case s: SubmissionStatusResponse if s.success =>
        case unexpected =>
          handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    
    def constructSubmitRequest(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      sparkProperties: Map[String, String],
      environmentVariables: Map[String, String]): CreateSubmissionRequest
    功能: 构造提交请求
    输入参数:
    	appResource	应用资源信息
    	mainClass	主类
    	appArgs	应用参数
    	sparkProperties	spark参数表
    	environmentVariables	环境变量信息表
    val message = new CreateSubmissionRequest
    message.clientSparkVersion = sparkVersion
    message.appResource = appResource
    message.mainClass = mainClass
    message.appArgs = appArgs
    message.sparkProperties = sparkProperties
    message.environmentVariables = environmentVariables
    message.validate()
    val= message
    
    def get(url: URL): SubmitRestProtocolResponse
    功能: 发送GET请求到指定地址,获取响应
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    readResponse(conn)
    
    def post(url: URL): SubmitRestProtocolResponse
    功能: 发送POST请求到指定地址,获取响应
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    readResponse(conn)
    
    def postJson(url: URL, json: String): SubmitRestProtocolResponse
    功能: 使用指定json发送post请求到指定的url,并获取响应
    1. 获取链接信息
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(true)
    2. 写出json信息
    try {
      val out = new DataOutputStream(conn.getOutputStream)
      Utils.tryWithSafeFinally {
        out.write(json.getBytes(StandardCharsets.UTF_8))
      } {
        out.close()
      }
    } catch {
      case e: ConnectException =>
        throw new SubmitRestConnectionException("Connect Exception when connect to server", e)
    }
    3. 获取回应
    val= readResponse(conn)
    
    def readResponse(connection: HttpURLConnection): SubmitRestProtocolResponse
    功能: 读取服务器回应,并进行验证,之后获取@SubmitRestProtocolResponse,如果回应出错,则汇报一条嵌入式的消息给用户,可以暴露给测试
    1. 获取一个异步任务,用户处理服务器回应
    val responseFuture = Future {
        // 获取请求码,并进行校验,如果请求码不正确
        // 1. 服务器内部错误 --> 直接抛出异常
        // 2. 其他错误,则包装错误信息,返回给用户
      val responseCode = connection.getResponseCode
      if (responseCode != HttpServletResponse.SC_OK) {
        val errString = Some(Source.fromInputStream(connection.getErrorStream())
          .getLines().mkString("\n"))
        if (responseCode == HttpServletResponse.SC_INTERNAL_SERVER_ERROR &&
          !connection.getContentType().contains("application/json")) {
          throw new SubmitRestProtocolException(s"Server responded with exception:\n${errString}")
        }
        logError(s"Server responded with error:\n${errString}")
        val error = new ErrorResponse
        if (responseCode == RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION) {
          error.highestProtocolVersion = RestSubmissionServer.PROTOCOL_VERSION
        }
        error.message = errString.get
        error
      } else {
          // 读取请求发送过来的json数据,并对其做出响应
        val dataStream = connection.getInputStream
        if (dataStream == null) {
          throw new SubmitRestProtocolException("Server returned empty body")
        }
        val responseJson = Source.fromInputStream(dataStream).mkString
        logDebug(s"Response from the server:\n$responseJson")
        val response = SubmitRestProtocolMessage.fromJson(responseJson)
        response.validate()
        response match {
          case error: ErrorResponse =>
            logError(s"Server responded with error:\n${error.message}")
            error
          case response: SubmitRestProtocolResponse => response
          case unexpected =>
            throw new SubmitRestProtocolException(
              s"Message received from server was not a response:\n${unexpected.toJson}")
        }
      }
    }
    2. 周期性的获取异步任务执行结果
    try { Await.result(responseFuture, 10.seconds) } catch {
      case unreachable @ (_: FileNotFoundException | _: SocketException) =>
        throw new SubmitRestConnectionException("Unable to connect to server", unreachable)
      case malformed @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
        throw new SubmitRestProtocolException("Malformed response received from server"
                                              , malformed)
      case timeout: TimeoutException =>
        throw new SubmitRestConnectionException("No response from server", timeout)
      case NonFatal(t) =>
        throw new SparkException("Exception while waiting for response", t)
    }
    
    def getSubmitUrl(master: String): URL
    功能: 返回rest URL,用于创建新的提交
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/create")
    
    def getKillUrl(master: String, submissionId: String): URL
    功能: 用于创建url,用于kill已经存在的提交
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/kill/$submissionId")
    
    def getStatusUrl(master: String, submissionId: String): URL
    功能: 用于创建状态URL
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/status/$submissionId")
    
    def getBaseUrl(master: String): String
    功能: 获取与服务器交换的基本URL地址,包括协议版本
    var masterUrl = master
    supportedMasterPrefixes.foreach { prefix =>
      if (master.startsWith(prefix)) {
        masterUrl = master.stripPrefix(prefix)
      }
    }
    masterUrl = masterUrl.stripSuffix("/")
    val= s"http://$masterUrl/$PROTOCOL_VERSION/submissions"
    
    def validateMaster(master: String): Unit
    功能: master校验,如果在非独立部署的情况下,抛出异常
    val valid = supportedMasterPrefixes.exists { prefix => master.startsWith(prefix) }
    if (!valid) {
      throw new IllegalArgumentException(
        "This REST client only supports master URLs that start with " +
          "one of the following: " + supportedMasterPrefixes.mkString(","))
    }
    
    def reportSubmissionStatus(submitResponse: CreateSubmissionResponse): Unit
    功能: 汇报新创建提交的提交状态,成功则轮询并记录消息,失败则打印失败日志
    if (submitResponse.success) {
      val submissionId = submitResponse.submissionId
      if (submissionId != null) {
        logInfo(s"Submission successfully created as $submissionId. Polling submission state...")
        pollSubmissionStatus(submissionId)
      } else {
        // should never happen
        logError("Application successfully submitted, but submission ID was not provided!")
      }
    } else {
      val failMessage = Option(submitResponse.message).map { ": " + _ }.getOrElse("")
      logError(s"Application submission failed$failMessage")
    }
    
    def pollSubmissionStatus(submissionId: String): Unit
    功能: 轮询指定提交的状态,可以进行多次尝试
    (1 to REPORT_DRIVER_STATUS_MAX_TRIES).foreach { _ =>
      val response = requestSubmissionStatus(submissionId, quiet = true)
      val statusResponse = response match {
        case s: SubmissionStatusResponse => s
        case _ => return 
      }
        // 提交成功,则显示状态信息到日志中,并停止
      if (statusResponse.success) {
        val driverState = Option(statusResponse.driverState)
        val workerId = Option(statusResponse.workerId)
        val workerHostPort = Option(statusResponse.workerHostPort)
        val exception = Option(statusResponse.message)
        driverState match {
          case Some(state) => logInfo(s"State of driver $submissionId is now $state.")
          case _ => logError(s"State of driver $submissionId was not found!")
        }
        (workerId, workerHostPort) match {
          case (Some(id), Some(hp)) => logInfo(s"Driver is running on worker $id at $hp.")
          case _ =>
        }
        exception.foreach { e => logError(e) }
        return
      }
        // 汇报中断时间
      Thread.sleep(REPORT_DRIVER_STATUS_INTERVAL)
    }
    
    def handleRestResponse(response: SubmitRestProtocolResponse): Unit
    功能: 打印rest 服务器发送的信息,在rest应用提交协议中
    logInfo(s"Server responded with ${response.messageType}:\n${response.toJson}")
    
    def handleUnexpectedRestResponse(unexpected: SubmitRestProtocolResponse): Unit 
    功能: 如果服务器发送的响应类型不符合,打印日志
    logError(s"Error: Server responded with message of unexpected
    	type ${unexpected.messageType}.")
    
    def handleConnectionException(masterUrl: String): Boolean
   	功能: 当捕捉到异常的时候,返回true,注意到master由于在生命周期内的恢复不会考虑在内.这个假设没有影响,因为现在还不支持客户端的尝试提交.(SPAKR-6443).
    if (!lostMasters.contains(masterUrl)) {
      logWarning(s"Unable to connect to server ${masterUrl}.")
      lostMasters += masterUrl
    }
    val= lostMasters.size >= masters.length
}
```

```scala
private[spark] object RestSubmissionClient {
    属性:
    #name @supportedMasterPrefixes = Seq("spark://", "mesos://")	支持的master前缀
    #name @BLACKLISTED_SPARK_ENV_VARS = Set("SPARK_ENV_LOADED", "SPARK_HOME", "SPARK_CONF_DIR")
    	黑名单环境变量(这些变量在远端机器上会导致错误,SPARK-12345,SPARK-25934)
    #name @REPORT_DRIVER_STATUS_INTERVAL = 1000	汇报给读取器状态的时间间隔
    #name @REPORT_DRIVER_STATUS_MAX_TRIES = 10	汇报次数
    #name @PROTOCOL_VERSION = "v1"	最大尝试次数
    操作集:
    def filterSystemEnvironment(env: Map[String, String]): Map[String, String]
    功能: 获取过滤完毕的系统环境变量表
    val= env.filterKeys { k =>
      (k.startsWith("SPARK_") && !BLACKLISTED_SPARK_ENV_VARS.contains(k))
        || k.startsWith("MESOS_")
    }
    
    def supportsRestClient(master: String): Boolean
    功能: 确定是否支持REST客户端(检查是否存在这个服务器即可)
    val= supportedMasterPrefixes.exists(master.startsWith)
}
```

```scala
private[spark] class RestSubmissionClientApp extends SparkApplication {
    def run(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      conf: SparkConf,
      env: Map[String, String] = Map()): SubmitRestProtocolResponse
    功能: 提交请求,运行应用,返回响应,测试可见
    val master = conf.getOption("spark.master").getOrElse {
      throw new IllegalArgumentException("'spark.master' must be set.")
    }
    val sparkProperties = conf.getAll.toMap
    val client = new RestSubmissionClient(master)
    val submitRequest = client.constructSubmitRequest(
      appResource, mainClass, appArgs, sparkProperties, env)
    val= client.createSubmission(submitRequest)
    
    def start(args: Array[String], conf: SparkConf): Unit
    功能: 启动函数
    if (args.length < 2) {
      sys.error("Usage: RestSubmissionClient [app resource] [main class] [app args*]")
      sys.exit(1)
    }
    val appResource = args(0)
    val mainClass = args(1)
    val appArgs = args.slice(2, args.length)
    val env = RestSubmissionClient.filterSystemEnvironment(sys.env)
    run(appResource, mainClass, appArgs, conf, env)
}RestSubmissionServer
```

##### RestSubmissionServer

```markdown
介绍:
	REST提交服务器,用于回应由@RestSubmissionClient 提交的请求
	根据情况回应不同的响应码:
	200 	OK	请求成功的执行了
	400		BAD REQUEST		请求形式不正确,或者形式错误
	468		UNKNOWN PROTOCOL VERSION	位置协议版本,服务器不能识别
	500		INTERNAL SERVER ERROR 	服务器内部错误
 	服务从事涉及到一个json表示的响应@SubmitRestProtocolResponse,json信息在HTTP请求体中.但是如果发生了错误,服务器会包含一个错误响应@ErrorResponse,而非客户端所需要的响应.如果错误响应自己失败了,响应的HTTP请求体就为空.且携带有响应码,表示服务器内部发生了错误.
```

```scala
private[spark] abstract class RestSubmissionServer(
    val host: String,
    val requestedPort: Int,
    val masterConf: SparkConf) extends Logging {
    构造器参数:
    	host	主机名称
    	requestPort	请求端口号
    	masterConf	spark配置
    属性:
    #name @submitRequestServlet	#type @SubmitRequestServlet	提交的请求服务程序(servlet)
    #name @killRequestServlet: KillRequestServlet	kill的请求服务程序
    #name @statusRequestServlet: StatusRequestServlet	状态请求服务程序
    #name @_server: Option[Server] = None	服务器
    #name @baseContext = s"/${RestSubmissionServer.PROTOCOL_VERSION}/submissions"	
    	基本上下文信息
    #name @contextToServlet	URL前缀与服务程序的映射,可以暴露给测试
    val= Map[String, RestServlet](
        s"$baseContext/create/*" -> submitRequestServlet,
        s"$baseContext/kill/*" -> killRequestServlet,
        s"$baseContext/status/*" -> statusRequestServlet,
        "/*" -> new ErrorServlet // default handler
      )
    操作集:
    def start(): Int
    功能: 启动服务器,并返回端口号
    val (server, boundPort) = Utils.startServiceOnPort[Server](requestedPort, doStart, masterConf)
    _server = Some(server)
    logInfo(s"Started REST server for submitting applications on port $boundPort")
    boundPort
    
    def doStart(startPort: Int): (Server, Int)
    功能: 映射servlet到相应的上下文,并连接到服务器上.返回启动服务器与端口号的二元组
    1. 获取链接
    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    val server = new Server(threadPool)
    val connector = new ServerConnector(
      server,
      null,
      // Call this full constructor to set this, which forces daemon threads:
      new ScheduledExecutorScheduler("RestSubmissionServer-JettyScheduler", true),
      null,
      -1,
      -1,
      new HttpConnectionFactory())
    connector.setHost(host)
    connector.setPort(startPort)
    connector.setReuseAddress(!Utils.isWindows)
    server.addConnector(connector)
    2. 获取服务程序主处理器
    val mainHandler = new ServletContextHandler
    mainHandler.setServer(server)
    mainHandler.setContextPath("/")
    contextToServlet.foreach { case (prefix, servlet) =>
      mainHandler.addServlet(new ServletHolder(servlet), prefix)
    }
    3. 启动服务器
    server.setHandler(mainHandler)
    server.start()
    val boundPort = connector.getLocalPort
    val= (server, boundPort)
    
    def stop(): Unit
    功能: 停止服务器
}
```

```scala
private[rest] object RestSubmissionServer {
    #name @PROTOCOL_VERSION = RestSubmissionClient.PROTOCOL_VERSION	协议版本号
    #name @SC_UNKNOWN_PROTOCOL_VERSION = 468	未知版本号错误码
}
```

```scala
private[rest] abstract class RestServlet extends HttpServlet with Logging {
    介绍: 用于处理发送给服务器@RestSubmissionServer的REST服务程序
    操作集:
    def sendResponse(
      responseMessage: SubmitRestProtocolResponse,
      responseServlet: HttpServletResponse): Unit
    功能: 序列化给定响应信息为json形式,通过响应服务程序发送,这个在发送之前验证了请求的形式.
    1. 确定响应形式
    val message = validateResponse(responseMessage, responseServlet)
    2. 设置内容类型
    responseServlet.setContentType("application/json")
    responseServlet.setCharacterEncoding("utf-8")
    3. 写出消息的json形式
    responseServlet.getWriter.write(message.toJson)
    
    def findUnknownFields(
      requestJson: String,
      requestMessage: SubmitRestProtocolMessage): Array[String]
    功能: 返回客户端请求信息中,服务器不知道的信息
    val clientSideJson = parse(requestJson)
    val serverSideJson = parse(requestMessage.toJson)
    val Diff(_, _, unknown) = clientSideJson.diff(serverSideJson)
    unknown match {
      case j: JObject => j.obj.map { case (k, _) => k }.toArray
      case _ => Array.empty[String] // No difference
    }
    
    def formatException(e: Throwable): String
    功能: 返回认为可识别的格式异常
    val stackTraceString = e.getStackTrace.map { "\t" + _ }.mkString("\n")
    val= s"$e\n$stackTraceString"
    
    def handleError(message: String): ErrorResponse
    功能: 构建错误响应
    val e = new ErrorResponse
    e.serverSparkVersion = sparkVersion
    e.message = message
    val= e
    
    def parseSubmissionId(path: String): Option[String]
    功能: 从相关路径@path转换提交ID,假定这是路径的首个部分.例如,希望采取格式/[submission ID]/maybe/something/else,返回的提交ID不能为空,不适合返回None
    val=if (path == null || path.isEmpty) {
      None
    } else {
      path.stripPrefix("/").split("/").headOption.filter(_.nonEmpty)
    }
    
    def validateResponse(
      responseMessage: SubmitRestProtocolResponse,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse
    功能: 响应信息验证
    val= try {
      responseMessage.validate()
      responseMessage
    } catch {
      case e: Exception =>
        responseServlet.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        handleError("Internal server error: " + formatException(e))
    }
}
```

```scala
private[rest] abstract class KillRequestServlet extends RestServlet {
    介绍: kill请求的服务程序
    操作集:
    def handleKill(submissionId: String): KillSubmissionResponse
    功能: 处理kill指定提交的逻辑,并返回指定响应
    
    def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit
    功能: 如果提交ID在URL中指定了,如果master中含有这个信息,则kill对应的驱动器并返回合适的响应.否则返回错误信息.
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleKill).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in kill request.")
    }
    sendResponse(responseMessage, response)
}
```

```scala
private[rest] abstract class StatusRequestServlet extends RestServlet {
    介绍: 请求状态获取服务程序
    操作集:
    def handleStatus(submissionId: String): SubmissionStatusResponse
    功能: 处理指定提交任务的状态信息,并返回状态信息
    
    def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit
    功能: 如果提交的任务在URL中,从master中请求驱动器相关的状态,包装到响应中返回,如果没有则返回错误信息
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleStatus).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in status request.")
    }
    sendResponse(responseMessage, response)
}
```

```scala
private[rest] abstract class SubmitRequestServlet extends RestServlet {
    介绍: 用于将请求提交给服务器的服务程序
    操作集:
    def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse
    功能: 处理信息提交,返回服务器响应
    
    def doPost(
      requestServlet: HttpServletRequest,
      responseServlet: HttpServletResponse): Unit
    功能: 将消息提交到master,使用给定的请求参数
    val responseMessage =
      try {
        val requestMessageJson = Source.fromInputStream(requestServlet.getInputStream).mkString
        val requestMessage = SubmitRestProtocolMessage.fromJson(requestMessageJson)
        requestMessage.validate()
        handleSubmit(requestMessageJson, requestMessage, responseServlet)
      } catch {
        case e @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
          responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          handleError("Malformed request: " + formatException(e))
      }
    sendResponse(responseMessage, responseServlet)
}
```

```scala
private class ErrorServlet extends RestServlet {
    介绍: 错误服务程序,用于返回给用户缺省的错误信息
    def service(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit 
    功能: 返回合适的错误信息给用户
    val path = request.getPathInfo
    val parts = path.stripPrefix("/").split("/").filter(_.nonEmpty).toList
    var versionMismatch = false
    var msg =
      parts match {
        case Nil =>
          "Missing protocol version."
        case `serverVersion` :: Nil =>
          "Missing the /submissions prefix."
        case `serverVersion` :: "submissions" :: tail =>
          "Missing an action: please specify one of /create, /kill, or /status."
        case unknownVersion :: tail =>
          versionMismatch = true
          s"Unknown protocol version '$unknownVersion'."
        case _ =>
          "Malformed path."
      }
    msg += s" Please submit requests through http://[host]:[port]/$serverVersion/submissions/..."
    val error = handleError(msg)
    if (versionMismatch) {
      error.highestProtocolVersion = serverVersion
      response.setStatus(RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION)
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    }
    sendResponse(error, response)
}
```

##### StandaloneRestServer

```markdown
介绍:
 	独立REST服务器,用于响应@RestSubmissionClient 的响应,可以嵌入到独立的master中,且只能在集群模式情况下使用.这个模式根据不同的情况给出不同的响应码:
    200 	OK	请求成功的执行了
	400		BAD REQUEST		请求形式不正确,或者形式错误
	468		UNKNOWN PROTOCOL VERSION	位置协议版本,服务器不能识别
	500		INTERNAL SERVER ERROR 	服务器内部错误
	服务器总是包含一个json表示的相关于@SubmitRestProtocolResponse 的内容在请求体中,如果发送了错误,则会对错误信息进行包装,形成@ErrorResponse,将其置于请求体中,若果形成错误信息失败,请求体则会返回空,此时返回服务器内部错误的错误码.
	构造器参数
		host	主机地址
		requestedPort	请求端口
		masterConf	master的配置信息
		masterEndpoint	master的RPC端点引用
		masterUrl	master地址
```

```scala
private[deploy] class StandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterConf: SparkConf,
    masterEndpoint: RpcEndpointRef,
    masterUrl: String)
extends RestSubmissionServer(host, requestedPort, masterConf) {
    属性: 
    #Name @submitRequestServlet	提交请求服务程序
    val= new StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf)
    #name @killRequestServlet	kill任务服务程序
    val= new StandaloneKillRequestServlet(masterEndpoint, masterConf)
    #name @statusRequestServlet	状态请求服务器
    val= new StandaloneStatusRequestServlet(masterEndpoint, masterConf)
}
```

```scala
private[rest] class StandaloneKillRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
extends KillRequestServlet {
    介绍: 独立kill请求的服务程序
    def handleKill(submissionId: String): KillSubmissionResponse
    功能: 处理kill指定提交的处理方式,并返回响应
    val k = new KillSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success
    val= k
}
```

```scala
private[rest] class StandaloneStatusRequestServlet(
    masterEndpoint: RpcEndpointRef, conf: SparkConf)
extends StatusRequestServlet {
    介绍: 独立状态请求服务程序
    def handleStatus(submissionId: String): SubmissionStatusResponse 
    功能: 处理状态并获取响应
    val response = masterEndpoint.askSync[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(submissionId))
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new SubmissionStatusResponse
    d.serverSparkVersion = sparkVersion
    d.submissionId = submissionId
    d.success = response.found
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    val= d
}
```

```scala
private[rest] class StandaloneSubmitRequestServlet(
    masterEndpoint: RpcEndpointRef,
    masterUrl: String,
    conf: SparkConf)
extends SubmitRequestServlet {
    介绍: 独立提交请求服务程序
    操作集:
    def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse
    功能: 处理提交,并获取响应
    val= requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val driverDescription = buildDriverDescription(submitRequest)
        val response = masterEndpoint.askSync[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription))
        val submitResponse = new CreateSubmissionResponse
        submitResponse.serverSparkVersion = sparkVersion
        submitResponse.message = response.message
        submitResponse.success = response.success
        submitResponse.submissionId = response.driverId.orNull
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          submitResponse.unknownFields = unknownFields
        }
        submitResponse
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
    
    def buildDriverDescription(request: CreateSubmissionRequest): DriverDescription
    功能: 建立driver的描述
    1. 获取驱动器属性
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class is missing.")
    }
    val sparkProperties = request.sparkProperties
    val driverMemory = sparkProperties.get(config.DRIVER_MEMORY.key)
    val driverCores = sparkProperties.get(config.DRIVER_CORES.key)
    val driverDefaultJavaOptions = sparkProperties.get(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS)
    val driverExtraJavaOptions = sparkProperties.get(config.DRIVER_JAVA_OPTIONS.key)
    val driverExtraClassPath = sparkProperties.get(config.DRIVER_CLASS_PATH.key)
    val driverExtraLibraryPath = sparkProperties.get(config.DRIVER_LIBRARY_PATH.key)
    val superviseDriver = sparkProperties.get(config.DRIVER_SUPERVISE.key)
    val masters = sparkProperties.get("spark.master")
    val (_, masterPort) = Utils.extractHostPortFromSparkUrl(masterUrl)
    val masterRestPort = this.conf.get(config.MASTER_REST_SERVER_PORT)
    val updatedMasters = masters.map(
      _.replace(s":$masterRestPort", s":$masterPort")).getOrElse(masterUrl)
    val appArgs = request.appArgs
    val environmentVariables =
      request.environmentVariables.filterNot(x => x._1.matches("SPARK_LOCAL_(IP|HOSTNAME)"))
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", updatedMasters)
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val defaultJavaOpts = driverDefaultJavaOptions.map(Utils.splitCommandString)
      .getOrElse(Seq.empty)
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ defaultJavaOpts ++ extraJavaOpts
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ appArgs, // args to the DriverWrapper
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    val driverResourceReqs = ResourceUtils.parseResourceRequirements(conf,
      config.SPARK_DRIVER_PREFIX)
    val= new DriverDescription(
      appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver, command,
      driverResourceReqs)
}
```

##### SubmitRestProtocolException

```scala
private[rest] class SubmitRestProtocolException(message: String, cause: Throwable = null)
extends Exception(message, cause)
介绍: REST应用协议错误

private[rest] class SubmitRestMissingFieldException(message: String)
extends SubmitRestProtocolException(message)
介绍: @SubmitRestProtocolMessage	缺少属性引发的异常

private[deploy] class SubmitRestConnectionException(message: String, cause: Throwable)
extends SubmitRestProtocolException(message, cause)
介绍: 客户端接收不到服务端的信息异常
```

##### SubmitRestProtocolMessage

```scala
@JsonInclude(Include.NON_ABSENT)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
@JsonPropertyOrder(alphabetic = true)
private[rest] abstract class SubmitRestProtocolMessage {
    介绍: REST应用提交协议中用于抽象交换的消息
    在数据交换的过程中,按照json的形式进行序列化/反序列化,每个请求或是响应可以使用如下三个属性构成:
    1. 动作,用于指定消息的类型
    2. 客户端/服务器的spark版本号
    3. 可配置信息
    属性:
    #name @messageType = Utils.getFormattedClassName(this)	消息类型
    #name @action: String = messageType	动作名称
    #name @message: String = null	消息名称
    操作集:
    def setAction(a: String): Unit = { }
    功能: 设置动作,用于json的反序列化
    
    def toJson: String 
    功能: 转化为json(序列化信息为json)
    validate()
    SubmitRestProtocolMessage.mapper.writeValueAsString(this)
    
    final def validate(): Unit
    功能: 消息格式断言
    try {
      doValidate()
    } catch {
      case e: Exception =>
        throw new SubmitRestProtocolException(
            s"Validation of message $messageType failed!", e)
    }
    
    def doValidate(): Unit
    功能: 断言消息的可用性
    if (action == null) {
      throw new SubmitRestMissingFieldException(s"The action field 
      is missing in $messageType")
    }
    
    def assertFieldIsSet[T](value: T, name: String): Unit
    功能: 断言指定属性在消息中设定了
     if (value == null) {
      throw new SubmitRestMissingFieldException(s"'$name' is 
      missing in message $messageType.")
    }
    
    def assert(condition: Boolean, failMessage: String): Unit
    功能: 验证消息信息时断言条件
    if (!condition) { throw new SubmitRestProtocolException(failMessage) }
}
```

```scala
private[spark] object SubmitRestProtocolMessage {
    介绍: 提供一些辅助方法,用于启动序列化的@SubmitRestProtocolMessage
    属性:
    #name @packagePrefix = this.getClass.getPackage.getName	包前缀
    #name @mapper = new ObjectMapper()	对象映射器
    val= new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)
    
    操作集:
    def parseAction(json: String): String
    功能: 转换指定json对应的动作属性值,如果没有找到对应的动作,抛出@SubmitRestMissingFieldException
    val value: Option[String] = parse(json) match {
      case JObject(fields) =>
        fields.collectFirst { case ("action", v) => v }.collect { case JString(s) => s }
      case _ => None
    }
    val= value.getOrElse {
      throw new SubmitRestMissingFieldException(s"Action 
      field not found in JSON:\n$json")
    }
    
    def fromJson(json: String): SubmitRestProtocolMessage
    功能: 根据指定的json,构造一条REST提交消息
    方法首先转化json的动作,使用它去转换信息类型,注意到动作必须代表定义在包内的@SubmitRestProtocolMessage,否则会抛出@ClassNotFoundException
    val className = parseAction(json)
    val clazz = Utils.classForName(packagePrefix + "." + className)
      .asSubclass[SubmitRestProtocolMessage](classOf[SubmitRestProtocolMessage])
    fromJson(json, clazz)
    
    def fromJson[T <: SubmitRestProtocolMessage](json: String, clazz: Class[T]): T
    功能: 由指定的json数据构造出一个@SubmitRestProtocolMessage
    这个方法决定消息的类型,有利于反序列化数据
    val= mapper.readValue(json, clazz)
}
```

##### SubmitRestProtocolRequest

```scala
private[rest] abstract class SubmitRestProtocolRequest extends SubmitRestProtocolMessage {
    介绍: 代表着由客户端在REST提交协议中发送的请求
    属性:
    #name @clientSparkVersion: String = null	客户端spark版本
    操作集:
    def doValidate(): Unit
    功能: 参数验证
    super.doValidate()
    assertFieldIsSet(clientSparkVersion, "clientSparkVersion")
}
```

```scala
private[rest] class CreateSubmissionRequest extends SubmitRestProtocolRequest {
    介绍: 用于在REST应用中运行新的应用请求
    属性:
    #name @appResource: String = null	应用资源
    #name @mainClass: String = null	主类
    #name @appArgs: Array[String] = null	应用参数列表
    #name @sparkProperties: Map[String, String] = null	spark属性列表
    #name @environmentVariables: Map[String, String] = null	环境变量表
    操作集:
    def doValidate(): Unit
    功能: 参数合法性校验
    super.doValidate()
    assert(sparkProperties != null, "No Spark properties set!")
    assertFieldIsSet(appResource, "appResource")
    assertFieldIsSet(appArgs, "appArgs")
    assertFieldIsSet(environmentVariables, "environmentVariables")
    assertPropertyIsSet("spark.app.name")
    assertPropertyIsBoolean(config.DRIVER_SUPERVISE.key)
    assertPropertyIsNumeric(config.DRIVER_CORES.key)
    assertPropertyIsNumeric(config.CORES_MAX.key)
    assertPropertyIsMemory(config.DRIVER_MEMORY.key)
    assertPropertyIsMemory(config.EXECUTOR_MEMORY.key)
    
    def assertPropertyIsSet(key: String): Unit
    功能: 断言属性已经设置完毕
    assertFieldIsSet(sparkProperties.getOrElse(key, null), key)
    
    def assertPropertyIsBoolean(key: String): Unit
    功能: 断言属性是boolean类型
    assertProperty[Boolean](key, "boolean", _.toBoolean)
    
    def assertPropertyIsNumeric(key: String): Unit
    功能: 断言属性为数字类型
    assertProperty[Double](key, "numeric", _.toDouble)
    
    def assertPropertyIsMemory(key: String): Unit
    功能: 断言属性时内存计量值
    assertProperty[Int](key, "memory", Utils.memoryStringToMb)
    
    def assertProperty[T](key: String, valueType: String, convert: (String => T)): Unit
    功能: 属性断言
    sparkProperties.get(key).foreach { value =>
      Try(convert(value)).getOrElse {
        throw new SubmitRestProtocolException(
          s"Property '$key' expected $valueType value: actual was '$value'.")
      }
    }
}
```

##### SubmitRestProtocolResponse

```scala
private[rest] abstract class SubmitRestProtocolResponse extends SubmitRestProtocolMessage {
    介绍: 响应的抽象,在REST应用的提交协议中用于发送到客户端
    属性: 
    #name @serverSparkVersion: String = null	服务器spark版本
    #name @success: Boolean = null	响应成功标记
    #name @unknownFields: Array[String] = null	未知属性
    操作集:
    def doValidate(): Unit
    功能: 参数验证
    super.doValidate()
    assertFieldIsSet(serverSparkVersion, "serverSparkVersion")
}
```

```scala
private[spark] class CreateSubmissionResponse extends SubmitRestProtocolResponse{
    介绍: 发送给客户端的一个REST应用提交协议
    属性:
    #name @submissionId: String = null	提交ID
    操作集:
    def doValidate(): Unit
    功能: 参数验证
    super.doValidate()
    assertFieldIsSet(success, "success")
}
```

```scala
private[spark] class KillSubmissionResponse extends SubmitRestProtocolResponse {
    介绍: 在REST应用中kill请求的响应
    属性:
    #name @submissionId: String = null	提交ID
    操作集:
    def doValidate(): Unit
    功能: 参数验证
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
}
```

```scala
private[spark] class SubmissionStatusResponse extends SubmitRestProtocolResponse{
    介绍: REST应用中用于请求状态的响应
    属性:
    #name @submissionId: String = null	提交ID
    #name @driverState: String = null	driver状态
    #name @workerId: String = null	workerID
    #name @workerHostPort: String = null	worker主机端口号
    操作集:
    def doValidate(): Unit
    功能: 参数验证
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
}
```


#### security

1.  [HadoopDelegationTokenManager.scala](# HadoopDelegationTokenManager)

2.  [HadoopFSDelegationTokenProvider.scala](# HadoopFSDelegationTokenProvider)

3.  [HBaseDelegationTokenProvider.scala](# HBaseDelegationTokenProvider)

   ---

   #### HadoopDelegationTokenManager

   ```markdown
   介绍:
   	hadoop授权令牌管理器,管理spark应用程序的授权令牌.
   	当运行更新授权令牌时,这个管理器会确保长期运行,且获取安全服务时不会中断.周期性地向密钥分配中心产生日志,使用用户提供的证书与所有配置的安全服务交互,目的是获取授权令牌去分配到剩余的应用上.
   	新的授权令牌每次创建的时间为原始授权令牌更新时间的75%.新的令牌会发送的spark驱动器端点上.驱动器将令牌分配到需要它的进程中.
   	更新可以使用两种方式开启:
   	1. 提供转换准则和spark的密钥表
   	2. 允许基于本地证书缓存更新
   	后者有一个缺陷,spark自身无法生成TGT票据,因此用于必须手动更新Kerberos票据缓存.
    	这个类也可以仅仅用来创建授权令牌,通过调用@obtainDelegationTokens 方法,这个配置不需要使用@start方法或者提供驱动器属性,但是调用者需要自己分配产生的授权令牌.
    	构造器参数:
    		sparkConf	spark配置
    		hadoopConf	hadoop配置
    		schedulerRef	调度的RPC端点(驱动器)
   ```


```scala
   private[spark] class HadoopDelegationTokenManager(
       protected val sparkConf: SparkConf,
       protected val hadoopConf: Configuration,
       protected val schedulerRef: RpcEndpointRef) extends Logging {
       属性:
       #name @deprecatedProviderEnabledConfigs	弃用配置信息
       val= List(
       "spark.yarn.security.tokens.%s.enabled",
       "spark.yarn.security.credentials.%s.enabled")
       #name @providerEnabledConfig = "spark.security.credentials.%s.enabled"
       	提供的运行配置
       #name @principal = sparkConf.get(PRINCIPAL).orNull	Kerberos准则
       #name @keytab = sparkConf.get(KEYTAB).map { uri => new URI(uri).getPath() }.orNull
       	密钥表
       #name @delegationTokenProviders = loadProviders()	授权令牌提供者
       #name @renewalExecutor: ScheduledExecutorService = _	更新执行器
       操作集:
       def renewalEnabled: Boolean
       功能: 确认授权令牌是否可以更新
       val= sparkConf.get(KERBEROS_RENEWAL_CREDENTIALS) match {
           case "keytab" => principal != null
           case "ccache" => 
               UserGroupInformation.getCurrentUser().hasKerberosCredentials()
           case _ => false
         }
       
       def start(): Array[Byte]
       功能: 启动令牌刷新器,需要Kerberos准则和密钥表.启动时,刷新器会获取所有配置服务的授权令牌,并发送到驱动器上,创建任务,周期性的刷新令牌信息.
       这个方法需要给spark提供一个密钥表,在管理器可用的时候,会注册TGT的可用性.
       返回配置的Kerberos准则的授权令牌.
       1. 参数断言
       require(renewalEnabled, "Token renewal must be enabled to start the renewer.")
       require(schedulerRef != null, "Token renewal requires a scheduler endpoint.")
       2. 创建一个线程,用于定期刷新令牌
       renewalExecutor =
         ThreadUtils.newDaemonSingleThreadScheduledExecutor("Credential Renewal Thread")
       3. 获取用户组信息,进行可能的TGT检查
       val ugi = UserGroupInformation.getCurrentUser()
       if (ugi.isFromKeytab()) {
   	 /**
   	 在hadoop 2.x中密钥表的更新看起来是自动的,但是在hadoop 3.x中是可以进行配置的,具体请参考HADOOP-9567.
   	 使用@hadoop.kerberos.keytab.login.autorenewal.enabled 进行配置,这个任务会确保用户保证处于登录状态,而不需要处理配置值的情况.注意到当TGT不需要更新时,@checkTGTAndReloginFromKeytab()时no-op的.
   	 */
         val tgtRenewalTask = new Runnable() {
           override def run(): Unit = {
             ugi.checkTGTAndReloginFromKeytab()
           }
         }
         val tgtRenewalPeriod = sparkConf.get(KERBEROS_RELOGIN_PERIOD) //kerberos重新注册周期
         renewalExecutor.scheduleAtFixedRate(tgtRenewalTask, tgtRenewalPeriod, tgtRenewalPeriod,
           TimeUnit.SECONDS)
       }
       4. 更新令牌
       updateTokensTask()
       
       def stop(): Unit
       功能: 关闭管理器(关闭刷新的执行器即可)
       if (renewalExecutor != null) {
         renewalExecutor.shutdownNow()
       }
       
       def obtainDelegationTokens(creds: Credentials): Unit 
       功能: 获取授权密钥，存储在给定的证书中
       1. 获取当前用户组信息
       val currentUser = UserGroupInformation.getCurrentUser()
       2. 获取授权令牌，并将其存储到证书中
       val hasKerberosCreds = principal != null ||
         Option(currentUser.getRealUser()).getOrElse(currentUser).hasKerberosCredentials()
       if (hasKerberosCreds) {
         val freshUGI = doLogin()
         freshUGI.doAs(new PrivilegedExceptionAction[Unit]() {
           override def run(): Unit = {
             val (newTokens, _) = obtainDelegationTokens()
             creds.addAll(newTokens)
           }
         })
       }
       
       def obtainDelegationTokens(): (Credentials, Long)
       功能: 获取配置服务的授权令牌，并返回带有授权令牌的证书以及令牌刷新时间
       1. 获取证书
       val creds = new Credentials()
       2. 获取令牌刷新时间
       val nextRenewal = delegationTokenProviders.values.flatMap { provider =>
         if (provider.delegationTokensRequired(sparkConf, hadoopConf)) {
           provider.obtainDelegationTokens(hadoopConf, sparkConf, creds)
         } else {
           logDebug(s"Service ${provider.serviceName} does not require a token." +
             s" Check your configuration to see if security is disabled or not.")
           None
         }
       }.foldLeft(Long.MaxValue)(math.min)
       val= (creds, nextRenewal)
       
       def isProviderLoaded(serviceName: String): Boolean
       功能: 确定指定服务是否加载 （测试使用）
       val= delegationTokenProviders.contains(serviceName)
       
       def isServiceEnabled(serviceName: String): Boolean
       功能: 确定服务是否可用
       val key = providerEnabledConfig.format(serviceName)
       deprecatedProviderEnabledConfigs.foreach { pattern =>
         val deprecatedKey = pattern.format(serviceName)
         if (sparkConf.contains(deprecatedKey)) {
           logWarning(s"${deprecatedKey} is deprecated.  Please use ${key} instead.")
         }
       }
       val isEnabledDeprecated = deprecatedProviderEnabledConfigs.forall { pattern =>
         sparkConf
           .getOption(pattern.format(serviceName))
           .map(_.toBoolean)
           .getOrElse(true)
       }
       val= sparkConf
         .getOption(key)
         .map(_.toBoolean)
         .getOrElse(isEnabledDeprecated)
       
       def scheduleRenewal(delay: Long): Unit
       功能: 按照指定时间间隔进行调度延时
       1. 确定调度延时
       val _delay = math.max(0, delay)
       logInfo(s"Scheduling renewal in ${UIUtils.formatDuration(delay)}.")
       2. 确定调度任务体(更新令牌)
       val renewalTask = new Runnable() {
         override def run(): Unit = {
           updateTokensTask()
         }
       }
       3. 进行任务调度
       renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
       
       def updateTokensTask(): Array[Byte]
       功能: 周期性的调度,用于登录到密钥分配中心KDC,且创建授权密钥,重新调度会获取下一组密钥.
       try {
         // 获取令牌信息
         val freshUGI = doLogin()
         val creds = obtainTokensAndScheduleRenewal(freshUGI)
         val tokens = SparkHadoopUtil.get.serialize(creds)
         logInfo("Updating delegation tokens.")
         // 发送更新令牌消息到driver端 
         schedulerRef.send(UpdateDelegationTokens(tokens))
         tokens
       } catch {
         case _: InterruptedException =>
           null
         case e: Exception =>
           val delay = TimeUnit.SECONDS.toMillis(sparkConf.get(CREDENTIALS_RENEWAL_RETRY_WAIT))
           logWarning(s"Failed to update tokens, will try again in 
           ${UIUtils.formatDuration(delay)}!" +
             " If this happens too often tasks will fail.", e)
           scheduleRenewal(delay)
           null
       }
       
       def obtainTokensAndScheduleRenewal(ugi: UserGroupInformation): Credentials 
       功能: 获取令牌并调度更新,返回包含新的令牌的证书
       val= ugi.doAs(new PrivilegedExceptionAction[Credentials]() {
         override def run(): Credentials = {
           val (creds, nextRenewal) = obtainDelegationTokens()
           val now = System.currentTimeMillis
           val ratio = sparkConf.get(CREDENTIALS_RENEWAL_INTERVAL_RATIO)
           val delay = (ratio * (nextRenewal - now)).toLong
           scheduleRenewal(delay)
           creds
         }
       })
       
       def doLogin(): UserGroupInformation
       功能: 登录,并返回用户组信息
       if (principal != null) {
         logInfo(s"Attempting to login to KDC using principal: $principal")
         require(new File(keytab).isFile(), s"Cannot find keytab at $keytab.")
         val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
         logInfo("Successfully logged into KDC.")
         ugi
       } else if (!SparkHadoopUtil.get.isProxyUser(UserGroupInformation.getCurrentUser())) {
         logInfo(s"Attempting to load user's ticket cache.")
         val ccache = sparkConf.getenv("KRB5CCNAME")
         val user = Option(sparkConf.getenv("KRB5PRINCIPAL")).getOrElse(
           UserGroupInformation.getCurrentUser().getUserName())
         UserGroupInformation.getUGIFromTicketCache(ccache, user)
       } else {
         UserGroupInformation.getCurrentUser()
       }
       
       def loadProviders(): Map[String, HadoopDelegationTokenProvider]
       功能: 加载供应器,返回供应器表
       val loader = ServiceLoader.load(classOf[HadoopDelegationTokenProvider],
         Utils.getContextOrSparkClassLoader)
       val providers = mutable.ArrayBuffer[HadoopDelegationTokenProvider]()
       val iterator = loader.iterator
       while (iterator.hasNext) {
         try {
           providers += iterator.next
         } catch {
           case t: Throwable =>
             logDebug(s"Failed to load built in provider.", t)
         }
       }
   	providers
         .filter { p => isServiceEnabled(p.serviceName) }
         .map { p => (p.serviceName, p) }
         .toMap
   }
```

   #### HadoopFSDelegationTokenProvider

   ```scala
   private[deploy] class HadoopFSDelegationTokenProvider
   extends HadoopDelegationTokenProvider with Logging {
       介绍: Hadoop文件系统授权令牌提供器
       属性:
       #name @tokenRenewalInterval: Option[Long] = null	令牌更新周期
       #name @serviceName: String = "hadoopfs"	服务名称
       操作集:
       def obtainDelegationTokens(
         hadoopConf: Configuration,
         sparkConf: SparkConf,
         creds: Credentials): Option[Long]
       功能: 获取授权令牌,返回令牌下次刷新时间
       try {
         1. 获取证书
         val fileSystems = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(sparkConf, hadoopConf)
         val fetchCreds = fetchDelegationTokens(getTokenRenewer(hadoopConf), fileSystems, creds)
         2. 确定令牌刷新周期
         if (tokenRenewalInterval == null) {
           tokenRenewalInterval = getTokenRenewalInterval(hadoopConf, sparkConf, fileSystems)
         }
         val nextRenewalDate = tokenRenewalInterval.flatMap { interval =>
           val nextRenewalDates = fetchCreds.getAllTokens.asScala
             .filter(_.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier])
             .map { token =>
               val identifier = token
                 .decodeIdentifier()
                 .asInstanceOf[AbstractDelegationTokenIdentifier]
               identifier.getIssueDate + interval
             }
           if (nextRenewalDates.isEmpty) None else Some(nextRenewalDates.min)
         }
         nextRenewalDate
       } catch {
         case NonFatal(e) =>
           logWarning(s"Failed to get token from service $serviceName", e)
           None
       }
     
       def delegationTokensRequired(
         sparkConf: SparkConf,
         hadoopConf: Configuration): Boolean
       功能: 确认是否需要授权令牌
       val= UserGroupInformation.isSecurityEnabled
       
       def getTokenRenewer(hadoopConf: Configuration): String
       功能: 获取令牌更新器
       val tokenRenewer = Master.getMasterPrincipal(hadoopConf)
       logDebug("Delegation token renewer is: " + tokenRenewer)
       if (tokenRenewer == null || tokenRenewer.length() == 0) {
         val errorMessage = "Can't get Master Kerberos principal for use as renewer."
         logError(errorMessage)
         throw new SparkException(errorMessage)
       }
   	val= tokenRenewer
       
       def fetchDelegationTokens(
         renewer: String,
         filesystems: Set[FileSystem],
         creds: Credentials): Credentials
       功能: 获取带有授权令牌的证书
       filesystems.foreach { fs =>
         logInfo(s"getting token for: $fs with renewer $renewer")
         fs.addDelegationTokens(renewer, creds)
       }
       val= creds
       
       def getTokenRenewalInterval(
         hadoopConf: Configuration,
         sparkConf: SparkConf,
         filesystems: Set[FileSystem]): Option[Long]
       功能: 获取令牌更新周期
      	不能通过刷新yarn来产生令牌,所有通过更新者的身份登录并创建令牌.
       1. 确定更新者身份
       val renewer = UserGroupInformation.getCurrentUser().getUserName()
       2. 获取证书,并添加令牌
       val creds = new Credentials()
       fetchDelegationTokens(renewer, filesystems, creds)
       3. 确定更新周期
       val renewIntervals = creds.getAllTokens.asScala.filter {
         _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]
       }.flatMap { token =>
         Try {
           val newExpiration = token.renew(hadoopConf)
           val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
           val interval = newExpiration - identifier.getIssueDate
           logInfo(s"Renewal interval is $interval for token ${token.getKind.toString}")
           interval
         }.toOption
       }
       val= if (renewIntervals.isEmpty) None else Some(renewIntervals.min)
   }
   ```

   ```scala
   private[deploy] object HadoopFSDelegationTokenProvider {
       操作集:
       def hadoopFSsToAccess(
         sparkConf: SparkConf,
         hadoopConf: Configuration): Set[FileSystem]
       功能: 获取权限范围内的文件系统列表
       val defaultFS = FileSystem.get(hadoopConf)
       val filesystemsToAccess = sparkConf.get(KERBEROS_FILESYSTEMS_TO_ACCESS)
         .map(new Path(_).getFileSystem(hadoopConf))
         .toSet
       val master = sparkConf.get("spark.master", null)
       val stagingFS = if (master != null && master.contains("yarn")) {
         sparkConf.get(STAGING_DIR).map(new Path(_).getFileSystem(hadoopConf))
       } else {
         None
       }
       val= filesystemsToAccess ++ stagingFS + defaultFS
   }
   ```

   #### HBaseDelegationTokenProvider

   ```scala
   private[security] class HBaseDelegationTokenProvider
   extends HadoopDelegationTokenProvider with Logging {
       介绍: HBase 授权令牌提供器
       操作集:
       def serviceName: String = "hbase"
       功能: 获取服务名称
       
       def obtainDelegationTokens(
         hadoopConf: Configuration,
         sparkConf: SparkConf,
         creds: Credentials): Option[Long]
       功能: 获取授权令牌
       try {
         val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
         val obtainToken = mirror.classLoader.
           loadClass("org.apache.hadoop.hbase.security.token.TokenUtil")
           .getMethod("obtainToken", classOf[Configuration])
         logDebug("Attempting to fetch HBase security token.")
         val token = obtainToken.invoke(null, hbaseConf(hadoopConf))
           .asInstanceOf[Token[_ <: TokenIdentifier]]
         logInfo(s"Get token from HBase: ${token.toString}")
         creds.addToken(token.getService, token)
       } catch {
         case NonFatal(e) =>
           logWarning(s"Failed to get token from service $serviceName due to  " + e +
             s" Retrying to fetch HBase security token with hbase connection parameter.")
           obtainDelegationTokensWithHBaseConn(hadoopConf, creds)
       }
       val= None
       
       def delegationTokensRequired(
         sparkConf: SparkConf,
         hadoopConf: Configuration): Boolean
       功能: 确认是否需要授权令牌
       val= hbaseConf(hadoopConf).get("hbase.security.authentication") == "kerberos"
       
       def hbaseConf(conf: Configuration): Configuration
       功能: 获取hbase配置信息
       try {
         val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
         val confCreate = mirror.classLoader.
           loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
           getMethod("create", classOf[Configuration])
         confCreate.invoke(null, conf).asInstanceOf[Configuration]
       } catch {
         case NonFatal(e) =>
           logDebug("Unable to load HBaseConfiguration.", e)
           conf
       }
       
       def obtainDelegationTokensWithHBaseConn(
         hadoopConf: Configuration,
         creds: Credentials): Unit
       功能: 获取带有HBase连接的授权密钥
       @Token<AuthenticationTokenIdentifier> obtainToken(Configuration conf) 方法是一个弃用的方法没带HBase 2.0.0 这个方法以及被移除. HBase 客户端API使用下述方法(0.98.9)从@ConnectionFactory中检索出第一条链接,,这个链接会通过@Token<AuthenticationTokenIdentifier> obtainToken(Connection conn) 调用.
       输入参数:
       	hadoopConf	hadoop配置
       	creds	需要添加的证书
       var hbaseConnection : Closeable = null
       try {
         val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
         val connectionFactoryClass = mirror.classLoader
           .loadClass("org.apache.hadoop.hbase.client.ConnectionFactory")
           .getMethod("createConnection", classOf[Configuration])
         hbaseConnection = connectionFactoryClass.invoke(null, hbaseConf(hadoopConf))
           .asInstanceOf[Closeable]
         val connectionParamTypeClassRef = mirror.classLoader
           .loadClass("org.apache.hadoop.hbase.client.Connection")
         val obtainTokenMethod = mirror.classLoader
           .loadClass("org.apache.hadoop.hbase.security.token.TokenUtil")
           .getMethod("obtainToken", connectionParamTypeClassRef)
         logDebug("Attempting to fetch HBase security token.")
         val token = obtainTokenMethod.invoke(null, hbaseConnection)
           .asInstanceOf[Token[_ <: TokenIdentifier]]
         logInfo(s"Get token from HBase: ${token.toString}")
         creds.addToken(token.getService, token)
       } catch {
         case NonFatal(e) =>
           logWarning(s"Failed to get token from service $serviceName", e)
       } finally {
         if (null != hbaseConnection) {
           hbaseConnection.close()
         }
       }
   }
   ```

#### worker

1.  [ui](# ui)

2.  [CommandUtils.scala](# CommandUtils)

3.  [DriverRunner.scala](# DriverRunner)

4.  [DriverWrapper.scala](# DriverWrapper)

5.  [ExecutorRunner.scala](# ExecutorRunner)

6.  [Worker.scala](# Worker)

7.  [WorkerArguments.scala](# WorkerArguments)

8. [WorkerSource.scala](# WorkerSource)

9.  [WorkerWatcher.scala](# WorkerWatcher)

   ---

   #### ui

   ```scala
   介绍: 这个目录下是worker所属web UI内容
   提供了
   """
   1. LogPage	日志页面
   2. WorkerPage	worker页面
   3. WorkerWebUI	独立运行的worker web服务器
   """
   ```

   #### CommandUtils

   ```scala
   private[deploy]
   object CommandUtils extends Logging {
       介绍: spark类路径的运行指令
       操作集:
       def buildProcessBuilder(
         command: Command,
         securityMgr: SecurityManager,
         memory: Int,
         sparkHome: String,
         substituteArguments: String => String,
         classPaths: Seq[String] = Seq.empty,
         env: Map[String, String] = sys.env): ProcessBuilder
       功能: 基于给定参数构建进程构建器@ProcessBuilder，env参数可以暴露给测试
       1. 获取进程构建器
       val localCommand = buildLocalCommand(
         command, securityMgr, substituteArguments, classPaths, env)
       val commandSeq = buildCommandSeq(localCommand, memory, sparkHome)
       val builder = new ProcessBuilder(commandSeq: _*)
       val environment = builder.environment()
       2. 设置环境变量
       for ((key, value) <- localCommand.environment) {
         environment.put(key, value)
       }
       val= builder
       
       def buildCommandSeq(command: Command, memory: Int, sparkHome: String): Seq[String]
       功能: 构建命令序列
       注意: 不要调用run.cmd脚本,在windows上不能使用process.destroy() kill进程树
       val cmd = new WorkerCommandBuilder(sparkHome, memory, command).buildCommand()
       cmd.asScala ++ Seq(command.mainClass) ++ command.arguments
       
       def buildLocalCommand(
         command: Command,
         securityMgr: SecurityManager,
         substituteArguments: String => String,
         classPath: Seq[String] = Seq.empty,
         env: Map[String, String]): Command 
       功能: 基于给定命令@command构建本地命令,考虑到本地的环境变量
       1. 确定指令的库路径
       val libraryPathName = Utils.libraryPathEnvName
       val libraryPathEntries = command.libraryPathEntries
       val cmdLibraryPath = command.environment.get(libraryPathName)
       2. 获取环境变量(考虑到本地环境变量)
       var newEnvironment = if (libraryPathEntries.nonEmpty && libraryPathName.nonEmpty) {
         val libraryPaths = libraryPathEntries ++ cmdLibraryPath ++ env.get(libraryPathName)
         command.environment + ((libraryPathName, libraryPaths.mkString(File.pathSeparator)))
       } else {
         command.environment
       }
       3. 设置授权密码
       if (securityMgr.isAuthenticationEnabled) {
         newEnvironment += (SecurityManager.ENV_AUTH_SECRET -> securityMgr.getSecretKey)
       }
       val=Command(
         command.mainClass,
         command.arguments.map(substituteArguments),
         newEnvironment,
         command.classPathEntries ++ classPath,
         Seq.empty, 
         command.javaOpts.filterNot(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
       
       def redirectStream(in: InputStream, file: File): Unit
       功能: 创建一个线程,用于重定向输入流到文件中
       1. 确定输出流
       val out = new FileOutputStream(file, true)
       2. 重定向输出到指定文件中
       new Thread("redirect output to " + file) {
         override def run(): Unit = {
           try {
             Utils.copyStream(in, out, true)
           } catch {
             case e: IOException =>
               logInfo("Redirection to " + file + " closed: " + e.getMessage)
           }
         }
       }.start()
   }
   ```

   #### DriverRunner

   ```scala
   private[deploy] class DriverRunner(
       conf: SparkConf, // spark配置
       val driverId: String, // 驱动器编号
       val workDir: File, // 工作目录
       val sparkHome: File, // spark 目录
       val driverDesc: DriverDescription, // 驱动器描述
       val worker: RpcEndpointRef, //worker端点
       val workerUrl: String, // worker URL地址
       val securityManager: SecurityManager, // 安全管理器
       val resources: Map[String, ResourceInformation] = Map.empty) // 资源列表
   extends Logging {
       介绍: 管理driver的执行,自动包含驱动器的失败重启,只能使用于独立模式运行下的驱动器.
       属性:
       #name @process: Option[Process] = None	volatile	进程
       #name @killed = false	volatile	是否被kill
       #name @finalState: Option[DriverState] = None volatile	最终状态下驱动器状态
       #name @finalException: Option[Exception] = None volatile	最终状态下异常
       #name @driverTerminateTimeoutMs = conf.get(WORKER_DRIVER_TERMINATE_TIMEOUT)
       	驱动器结束等待时延
       #name @clock: Clock = new SystemClock()	系统时钟
       #name @sleeper #type @Sleeper 睡眠器
       val= new Sleeper {
           def sleep(seconds: Int): Unit = (0 until seconds).takeWhile { _ =>
             Thread.sleep(1000)
             !killed
           }
         }
       操作集:
       def setClock(_clock: Clock): Unit = clock = _clock
       功能: 设置时钟,用于测试
       
       def setSleeper(_sleeper: Sleeper): Unit = sleeper = _sleeper
       功能: 设置睡眠器
       
       def start()
       功能: 启动并管理驱动器
       new Thread("DriverRunner for " + driverId) {
         override def run(): Unit = {
           var shutdownHook: AnyRef = null
           try {
             // 设置停止处理函数
             shutdownHook = ShutdownHookManager.addShutdownHook { () =>
               logInfo(s"Worker shutting down, killing driver $driverId")
               kill()
             }
             // 获取最终状态
             val exitCode = prepareAndRunDriver()
             finalState = if (exitCode == 0) {
               Some(DriverState.FINISHED)
             } else if (killed) {
               Some(DriverState.KILLED)
             } else {
               Some(DriverState.FAILED)
             }
           } catch {
             case e: Exception =>
               kill()
               finalState = Some(DriverState.ERROR)
               finalException = Some(e)
           } finally {
             if (shutdownHook != null) {
               ShutdownHookManager.removeShutdownHook(shutdownHook)
             }
           }
           // worker发送驱动器状态已经发生改变
           worker.send(DriverStateChanged(driverId, finalState.get, finalException))
         }
       }.start()
       
       def kill(): Unit
       功能: 终止驱动器
       logInfo("Killing driver process!")
       killed = true
       synchronized {
         process.foreach { p =>
           val exitCode = Utils.terminateProcess(p, driverTerminateTimeoutMs)
           if (exitCode.isEmpty) {
             logWarning("Failed to terminate driver process: " + p +
                 ". This process will likely be orphaned.")
           }
         }
       }
       
       def createWorkingDirectory(): File
       功能: 创建工作目录
       val driverDir = new File(workDir, driverId)
       if (!driverDir.exists() && !driverDir.mkdirs()) {
         throw new IOException("Failed to create directory " + driverDir)
       }
       val= driverDir
       
       def downloadUserJar(driverDir: File): String
       功能: 将用户jar包下载到指定目录,并返回类路径
       val jarFileName = new URI(driverDesc.jarUrl).getPath.split("/").last
       val localJarFile = new File(driverDir, jarFileName)
       if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
         logInfo(s"Copying user jar ${driverDesc.jarUrl} to $localJarFile")
         Utils.fetchFile(
           driverDesc.jarUrl,
           driverDir,
           conf,
           securityManager,
           SparkHadoopUtil.get.newConfiguration(conf),
           System.currentTimeMillis(),
           useCache = false)
         if (!localJarFile.exists()) { // Verify copy succeeded
           throw new IOException(
             s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
         }
       }
       val= localJarFile.getAbsolutePath
       
       def substituteVariables(argument: String): String = argument match {
         case "{{WORKER_URL}}" => workerUrl
         case "{{USER_JAR}}" => localJarFilename
         case other => other
       }
       功能: 变量替换
       
       def prepareAndRunDriver(): Int
       功能: 进行准备并运行驱动器
       1. 准备运行参数
       val driverDir = createWorkingDirectory()
       val localJarFilename = downloadUserJar(driverDir)
       val resourceFileOpt = prepareResourcesFile(SPARK_DRIVER_PREFIX, resources, driverDir)
       val javaOpts = driverDesc.command.javaOpts ++ resourceFileOpt.map(f =>
         Seq(s"-D${DRIVER_RESOURCES_FILE.key}=${f.getAbsolutePath}")).getOrElse(Seq.empty)
       val builder = CommandUtils.buildProcessBuilder(driverDesc.command.copy(javaOpts = javaOpts),
         securityManager, driverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)
       2. 运行驱动器
       runDriver(builder, driverDir, driverDesc.supervise)
       
       def initialize(process: Process): Unit
       功能: 初始化进程
       1. 重定向标准输出到文件
       val stdout = new File(baseDir, "stdout")
       CommandUtils.redirectStream(process.getInputStream, stdout)
       2. 重定向标准错误到文件
       val stderr = new File(baseDir, "stderr")
       val redactedCommand = Utils.redactCommandLineArgs(conf, builder.command.asScala)
       .mkString("\"", "\" \"", "\"")
       val header = "Launch Command: %s\n%s\n\n".format(redactedCommand, "=" * 40)
       Files.append(header, stderr, StandardCharsets.UTF_8)
       CommandUtils.redirectStream(process.getErrorStream, stderr)
       
       def runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int
       功能: 启动驱动器
       builder.directory(baseDir) // 设置进程工作目录
       // 可重试的执行指令
       runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
       
       def runCommandWithRetry(
         command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Int
       功能: 可重试的运行指令
       1. 确定是否进行尝试
       var exitCode = -1
       var waitSeconds = 1
       val successfulRunDuration = 5
       var keepTrying = !killed
       val redactedCommand = Utils.redactCommandLineArgs(conf, command.command)
         .mkString("\"", "\" \"", "\"")
       2. 重复尝试执行指令
       while (keepTrying) {
         logInfo("Launch Command: " + redactedCommand)
         synchronized {
           if (killed) { return exitCode }
           process = Some(command.start())
           initialize(process.get)
         }
         val processStart = clock.getTimeMillis()
         exitCode = process.get.waitFor()
         keepTrying = supervise && exitCode != 0 && !killed
         if (keepTrying) {
           if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000L) {
             waitSeconds = 1
           }
           logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
           sleeper.sleep(waitSeconds)
           waitSeconds = waitSeconds * 2 // exponential back-off
         }
       }
       val= exitCode
   }
   ```

   ```scala
   private[deploy] trait Sleeper {
     def sleep(seconds: Int): Unit
   }
   
   private[deploy] trait ProcessBuilderLike {
     def start(): Process
     def command: Seq[String]
   }
   介绍: 类进程构建器
   
   private[deploy] object ProcessBuilderLike {
     def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
       override def start(): Process = processBuilder.start()
       override def command: Seq[String] = processBuilder.command().asScala
     }
   }
   ```

   #### DriverWrapper

   ```scala
   object DriverWrapper extends Logging {
       介绍: 驱动器包装器
       操作集:
       def setupDependencies(loader: MutableURLClassLoader, userJar: String): Unit
       功能: 创建依赖
       输入参数:
       	loader	类加载器
       	userJar	用户jar信息
       1. 获取依赖五元组
       val sparkConf = new SparkConf()
       val secMgr = new SecurityManager(sparkConf)
       val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)
       val Seq(packagesExclusions, packages, repositories, ivyRepoPath, ivySettingsPath) =
         Seq(
           "spark.jars.excludes",
           "spark.jars.packages",
           "spark.jars.repositories",
           "spark.jars.ivy",
           "spark.jars.ivySettings"
         ).map(sys.props.get(_).orNull)
       2. 处理maven依赖
       val resolvedMavenCoordinates = DependencyUtils.resolveMavenDependencies(packagesExclusions,
         packages, repositories, ivyRepoPath, Option(ivySettingsPath))
       3. 获取jar属性
       val jars = {
         val jarsProp = sys.props.get(config.JARS.key).orNull
         if (!StringUtils.isBlank(resolvedMavenCoordinates)) {
           DependencyUtils.mergeFileLists(jarsProp, resolvedMavenCoordinates)
         } else {
           jarsProp
         }
       }
       val localJars = DependencyUtils.resolveAndDownloadJars(
           jars, userJar, sparkConf, hadoopConf,secMgr)
       4. 添加jar到类路径中
       DependencyUtils.addJarsToClassPath(localJars, loader)
       
       def main(args: Array[String]): Unit
       功能: 启动函数
       case workerUrl :: userJar :: mainClass :: extraArgs =>
           1. 获取启动参数,处理依赖关系
           val conf = new SparkConf()
           val host: String = Utils.localHostName()
           val port: Int = sys.props.getOrElse(config.DRIVER_PORT.key, "0").toInt
           val rpcEnv = RpcEnv.create("Driver", host, port, conf, new SecurityManager(conf))
           logInfo(s"Driver address: ${rpcEnv.address}")
           rpcEnv.setupEndpoint("workerWatcher", new WorkerWatcher(rpcEnv, workerUrl))
           val currentLoader = Thread.currentThread.getContextClassLoader
           val userJarUrl = new File(userJar).toURI().toURL()
           val loader =
             if (sys.props.getOrElse(config.DRIVER_USER_CLASS_PATH_FIRST.key, "false").toBoolean) {
               new ChildFirstURLClassLoader(Array(userJarUrl), currentLoader)
             } else {
               new MutableURLClassLoader(Array(userJarUrl), currentLoader)
             }
           Thread.currentThread.setContextClassLoader(loader)
           setupDependencies(loader, userJar)
       	2. 启动main
           val clazz = Utils.classForName(mainClass)
           val mainMethod = clazz.getMethod("main", classOf[Array[String]])
           mainMethod.invoke(null, extraArgs.toArray[String])
           rpcEnv.shutdown()
   
         case _ =>
           System.err.println("Usage: DriverWrapper <workerUrl>
           <userJar> <driverMainClass> [options]")
           System.exit(-1)
   }
   ```

   #### ExecutorRunner

   ```scala
   private[deploy] class ExecutorRunner(
       val appId: String, // 应用ID
       val execId: Int, // 执行器ID
       val appDesc: ApplicationDescription, // 应用描述
       val cores: Int, //CPU数量
       val memory: Int, // 内存量
       val worker: RpcEndpointRef, // worker RPC通信端口
       val workerId: String, // workerID
       val webUiScheme: String, // webUI schema
       val host: String, // 主机名称
       val webUiPort: Int, // webUI端口
       val publicAddress: String, // 公共地址
       val sparkHome: File, // sparkHome
       val executorDir: File, // 执行器目录
       val workerUrl: String, // workerURL地址
       conf: SparkConf, // spark配置
       val appLocalDirs: Seq[String], // 应用本地目录
       @volatile var state: ExecutorState.Value, // 执行器状态
       val resources: Map[String, ResourceInformation] = Map.empty) // 资源列表
   extends Logging {
       介绍: 管理一个执行器进程的执行,只能在独立运行模式下使用.
       属性:
       #name @fullId = appId + "/" + execId	id全称
       #name @workerThread: Thread = null	worker线程
       #name @process: Process = null	进程
       #name @stdoutAppender: FileAppender = null	标准输出文件添加器
       #name @stderrAppender: FileAppender = null	标准错误文件添加器
       #name @EXECUTOR_TERMINATE_TIMEOUT_MS = 10 * 1000	执行器停止时延
       #name @shutdownHook: AnyRef = null	停止点
       操作集:
       def start(): Unit
       功能: 启动运行
       1. 创建并启动用户进程
       workerThread = new Thread("ExecutorRunner for " + fullId) {
         override def run(): Unit = { fetchAndRunExecutor() }
       }
       workerThread.start()
       2. 设定关闭处理函数
       shutdownHook = ShutdownHookManager.addShutdownHook { () =>
         if (state == ExecutorState.LAUNCHING) {
           state = ExecutorState.FAILED
         }
         killProcess(Some("Worker shutting down")) }
       
       def killProcess(message: Option[String]): Unit
       功能: kill进程
       1. 停止进程(停止文件添加器)
       var exitCode: Option[Int] = None
       if (process != null) {
         logInfo("Killing process!")
         if (stdoutAppender != null) {
           stdoutAppender.stop()
         }
         if (stderrAppender != null) {
           stderrAppender.stop()
         }
         exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
         if (exitCode.isEmpty) {
           logWarning("Failed to terminate process: " + process +
             ". This process will likely be orphaned.")
         }
       }
       2. 发送执行器状态更新消息
       try {
         worker.send(ExecutorStateChanged(appId, execId, state, message, exitCode))
       } catch {
         case e: IllegalStateException => logWarning(e.getMessage(), e)
       }
       
       def kill(): Unit
       功能: 停止执行器运行器,包括kill运行的进程
       if (workerThread != null) {
         workerThread.interrupt()
         workerThread = null
         state = ExecutorState.KILLED
         try {
           ShutdownHookManager.removeShutdownHook(shutdownHook)
         } catch {
           case e: IllegalStateException => None
         }
       }
       
       def substituteVariables(argument: String): String
       功能: 变量替换
       val= argument match {
           case "{{WORKER_URL}}" => workerUrl
           case "{{EXECUTOR_ID}}" => execId.toString
           case "{{HOSTNAME}}" => host
           case "{{CORES}}" => cores.toString
           case "{{APP_ID}}" => appId
           case other => other
         }
       
       def fetchAndRunExecutor(): Unit
       功能: 下载并运行应用描述中的执行器
       try {
         val resourceFileOpt = prepareResourcesFile(SPARK_EXECUTOR_PREFIX, resources, executorDir)
         val arguments = appDesc.command.arguments ++ resourceFileOpt.map(f =>
           Seq("--resourcesFile", f.getAbsolutePath)).getOrElse(Seq.empty)
         val subsOpts = appDesc.command.javaOpts.map {
           Utils.substituteAppNExecIds(_, appId, execId.toString)
         }
         val subsCommand = appDesc.command.copy(arguments = arguments, javaOpts = subsOpts)
         val builder = CommandUtils.buildProcessBuilder(subsCommand, new SecurityManager(conf),
           memory, sparkHome.getAbsolutePath, substituteVariables)
         val command = builder.command()
         val redactedCommand = Utils.redactCommandLineArgs(conf, command.asScala)
           .mkString("\"", "\" \"", "\"")
         logInfo(s"Launch command: $redactedCommand")
         builder.directory(executorDir)
         builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
         builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")
         val baseUrl =
           if (conf.get(UI_REVERSE_PROXY)) {
             s"/proxy/$workerId/logPage/?appId=$appId&executorId=$execId&logType="
           } else {
             s"$webUiScheme$publicAddress:$webUiPort/logPage/?
             appId=$appId&executorId=$execId&logType="
           }
         builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
         builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")
         process = builder.start()
         val header = "Spark Executor Command: %s\n%s\n\n".format(
           redactedCommand, "=" * 40)
         val stdout = new File(executorDir, "stdout")
         stdoutAppender = FileAppender(process.getInputStream, stdout, conf)
         val stderr = new File(executorDir, "stderr")
         Files.write(header, stderr, StandardCharsets.UTF_8)
         stderrAppender = FileAppender(process.getErrorStream, stderr, conf)
         state = ExecutorState.RUNNING
         worker.send(ExecutorStateChanged(appId, execId, state, None, None))
         val exitCode = process.waitFor()
         state = ExecutorState.EXITED
         val message = "Command exited with code " + exitCode
         worker.send(ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode)))
       } catch {
         case interrupted: InterruptedException =>
           logInfo("Runner thread for executor " + fullId + " interrupted")
           state = ExecutorState.KILLED
           killProcess(None)
         case e: Exception =>
           logError("Error running executor", e)
           state = ExecutorState.FAILED
           killProcess(Some(e.toString))
       }
   }
   ```

   #### Worker

   ```scala
private[deploy] class Worker(
       override val rpcEnv: RpcEnv, // rpc环境
       webUiPort: Int, // webUI端口
       cores: Int, // 核心数
       memory: Int, // 内存量
       masterRpcAddresses: Array[RpcAddress], // master的RPC地址列表
       endpointName: String, // 端点名称
       workDirPath: String = null, // 工作目录
       val conf: SparkConf, // spark配置
       val securityMgr: SecurityManager, // 安全管理器
       resourceFileOpt: Option[String] = None, // 资源文件名称
       // 外部shuffle服务
       externalShuffleServiceSupplier: Supplier[ExternalShuffleService] = null,
       pid: Int = Utils.getProcessId) // 进程ID
   extends ThreadSafeRpcEndpoint with Logging {
       属性:
       #name @host = rpcEnv.address.host	主机名称
       #name @port = rpcEnv.address.port	端口号
       #name @forwardMessageScheduler	发送消息的调度线程
       val= ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
       #name @cleanupThreadExecutor	清理完成的工作目录和完成应用的独立线程
       val= ThreadUtils.newDaemonSingleThreadExecutor("worker-cleanup-thread"))
       #name @HEARTBEAT_MILLIS = conf.get(WORKER_TIMEOUT) * 1000 / 4	心跳周期
       #name @INITIAL_REGISTRATION_RETRIES = 6	初始化注册尝试次数
       #name @TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10	总计注册的尝试次数
       #name @FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500	模数乘法间隔下限
       #name @REGISTRATION_RETRY_FUZZ_MULTIPLIER	模数乘法的注册尝试次数
       val= {
           val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
           randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
         }
       #name @INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS	初始化注册尝试时间间隔
       val= (math.round(10 *REGISTRATION_RETRY_FUZZ_MULTIPLIER))
       #name @PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS	延长注册尝试时间间隔
       val= (math.round(60* REGISTRATION_RETRY_FUZZ_MULTIPLIER))
       #name @CLEANUP_ENABLED = conf.get(WORKER_CLEANUP_ENABLED)	是否允许清理
       #name @CLEANUP_INTERVAL_MILLIS = conf.get(WORKER_CLEANUP_INTERVAL) * 1000	清理时间间隔
       #name @APP_DATA_RETENTION_SECONDS = conf.get(APP_DATA_RETENTION)	应用数据保留时间
       #name @CLEANUP_FILES_AFTER_EXECUTOR_EXIT	是否在执行器退出之后清理文件(非shuffle)
       val= conf.get(config.STORAGE_CLEANUP_FILES_AFTER_EXECUTOR_EXIT)
       #name @master: Option[RpcEndpointRef] = None	master RPC端点引用
       #name @preferConfiguredMasterAddress = conf.get(PREFER_CONFIGURED_MASTER_ADDRESS)
       	是否使用master的地址,选false,仅仅会使用master发送过来的地址,不会使用master RPC地址
       #name @masterAddressToConnect: Option[RpcAddress] = None	master需要连接的地址
       #name @activeMasterUrl: String = ""	激活的master地址
       #name @activeMasterWebUiUrl : String = ""	激活的webUI地址
       #name @workerWebUiUrl: String = ""	worker的webUI地址
       #name @workerUri = RpcEndpointAddress(rpcEnv.address, endpointName).toString worker的RPC地址
       #name @registered = false	worker是否注册
       #name @connected = false	worker是否已经连接
       #name @workerId = generateWorkerId()	workerID
       #name @sparkHome	sparkHome
       val= if (sys.props.contains(IS_TESTING.key)) {
         assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
         new File(sys.props("spark.test.home"))
       } else {
         new File(sys.env.getOrElse("SPARK_HOME", "."))
       }
       #name @workDir: File = null	工作目录
       #name @finishedExecutors = new LinkedHashMap[String, ExecutorRunner]	完成的执行器列表
       #name @drivers = new HashMap[String, DriverRunner]	驱动器列表
       #name @executors = new HashMap[String, ExecutorRunner]	执行器列表
       #name @finishedDrivers = new LinkedHashMap[String, DriverRunner]	完成的驱动器列表
       #name @appDirectories = new HashMap[String, Seq[String]]	应用目录表
       #name @finishedApps = new HashSet[String]	完成的应用列表
       #name @retainedExecutors = conf.get(WORKER_UI_RETAINED_EXECUTORS)	剩余的执行数量
       #name @retainedDrivers = conf.get(WORKER_UI_RETAINED_DRIVERS)	剩余的驱动器数量
       #name @shuffleService 外部shuffle服务
       val=  if (externalShuffleServiceSupplier != null) {
           externalShuffleServiceSupplier.get()
         } else {
           new ExternalShuffleService(conf, securityMgr)
         }
       #name @publicAddress	公用地址
       val= {
           val envVar = conf.getenv("SPARK_PUBLIC_DNS")
           if (envVar != null) envVar else host
         }
       #name @webUi: WorkerWebUI = null	worker的WEBUI
       #name @connectionAttemptCount = 0	连接请求计数器
       #name @metricsSystem	度量系统
       val= MetricsSystem.createMetricsSystem(MetricsSystemInstances.WORKER, conf, securityMgr)
       #name @workerSource = new WorkerSource(this)	worker的资源
       #name @reverseProxy = conf.get(UI_REVERSE_PROXY)	是否开启反向代理
       #name @registerMasterFutures: Array[JFuture[_]] = null	注册的master任务表
       #name @registrationRetryTimer: Option[JScheduledFuture[_]] = None	注册的调度任务(用于重试计时)
       #name @registerMasterThreadPool	注册的master线程池
       val= ThreadUtils.newDaemonCachedThreadPool(
           "worker-register-master-threadpool",
           masterRpcAddresses.length // Make sure we can register with all masters at the same time
         )
       #name @resources: Map[String, ResourceInformation] = Map.empty	资源列表(测试可见)
       #name @coresUsed = 0	核心使用量
       #name @memoryUsed = 0	内存使用量
       #name @resourcesUsed = new HashMap[String, MutableResourceInfo]()	使用资源表
       操作集:
       def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
       功能: 创建日期形式
       
       def coresFree: Int = cores - coresUsed
       功能: 获取空闲核心数量
       
       def memoryFree: Int = memory - memoryUsed
       功能: 获取空闲内存量
       
       def createWorkDir(): Unit
       功能: 创建工作目录
       workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
       if (!Utils.createDirectory(workDir)) {
         System.exit(1)
       }
       
       def onStart(): Unit
       功能: 启动worker
       1. 创建工作目录
       assert(!registered)
       logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
         host, port, cores, Utils.megabytesToString(memory)))
       logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
       logInfo("Spark home: " + sparkHome)
       createWorkDir()
       2. 启动外部shuffle服务,创建工作资源表
       startExternalShuffleService()
       releaseResourcesOnInterrupt()
       setupWorkerResources()
       3. 设置UI信息
       webUi = new WorkerWebUI(this, workDir, webUiPort)
       webUi.bind()
       workerWebUiUrl = s"${webUi.scheme}$publicAddress:${webUi.boundPort}"
       registerWithMaster()
       4. 设置度量系统
       metricsSystem.registerSource(workerSource)
    metricsSystem.start()
       metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
}
   ```
   
   
   
   #### WorkerArguments
   
   ```scala
   private[worker] class WorkerArguments(args: Array[String], conf: SparkConf){
       介绍: worker的命令行转换器
       属性:
       #name @host = Utils.localHostName()	主机名称
       #name @port = 0	端口号
       #name @webUiPort = 8081	webUI端口
       #name @cores = inferDefaultCores()	CPU数量
       #name @memory = inferDefaultMemory()	内存量
       #name @masters: Array[String] = null	master列表
       #name @workDir: String = null	工作目录
       #name @propertiesFile: String = null 属性文件
       初始化操作:
       if (System.getenv("SPARK_WORKER_PORT") != null) {
           port = System.getenv("SPARK_WORKER_PORT").toInt
       }
       if (System.getenv("SPARK_WORKER_CORES") != null) {
           cores = System.getenv("SPARK_WORKER_CORES").toInt
       }
       if (conf.getenv("SPARK_WORKER_MEMORY") != null) {
           memory = Utils.memoryStringToMb(conf.getenv("SPARK_WORKER_MEMORY"))
       }
       if (System.getenv("SPARK_WORKER_WEBUI_PORT") != null) {
           webUiPort = System.getenv("SPARK_WORKER_WEBUI_PORT").toInt
       }
       if (System.getenv("SPARK_WORKER_DIR") != null) {
           workDir = System.getenv("SPARK_WORKER_DIR")
       }
    介绍: 检查环境变量
       
    @tailrec
       private def parse(args: List[String]): Unit
       功能: 参数转换
       args match {
           case ("--ip" | "-i") :: value :: tail =>
             Utils.checkHost(value)
             host = value
             parse(tail)
           case ("--host" | "-h") :: value :: tail =>
             Utils.checkHost(value)
             host = value
             parse(tail)
           case ("--port" | "-p") :: IntParam(value) :: tail =>
             port = value
             parse(tail)
           case ("--cores" | "-c") :: IntParam(value) :: tail =>
             cores = value
             parse(tail)
           case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
             memory = value
             parse(tail)
           case ("--work-dir" | "-d") :: value :: tail =>
             workDir = value
             parse(tail)
           case "--webui-port" :: IntParam(value) :: tail =>
             webUiPort = value
             parse(tail)
           case ("--properties-file") :: value :: tail =>
             propertiesFile = value
             parse(tail)
           case ("--help") :: tail =>
             printUsageAndExit(0)
           case value :: tail =>
             if (masters != null) {  // Two positional arguments were given
               printUsageAndExit(1)
             }
             masters = Utils.parseStandaloneMasterUrls(value)
             parse(tail)
           case Nil =>
             if (masters == null) {  // No positional argument was given
               printUsageAndExit(1)
             }
           case _ =>
             printUsageAndExit(1)
         }
       
       def printUsageAndExit(exitCode: Int): Unit
       功能: 打印使用情况,并退出JVM
       System.err.println(
         "Usage: Worker [options] <master>\n" +
         "\n" +
         "Master must be a URL of the form spark://hostname:port\n" +
         "\n" +
         "Options:\n" +
         "  -c CORES, --cores CORES  Number of cores to use\n" +
         "  -m MEM, --memory MEM     Amount of memory to use (e.g. 1000M, 2G)\n" +
         "  -d DIR, --work-dir DIR   Directory to run apps in (default: SPARK_HOME/work)\n" +
         "  -i HOST, --ip IP         Hostname to listen on (deprecated, please use --host or -h)\n" +
         "  -h HOST, --host HOST     Hostname to listen on\n" +
         "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
         "  --webui-port PORT        Port for web UI (default: 8081)\n" +
         "  --properties-file FILE   Path to a custom Spark properties file.\n" +
         "                           Default is conf/spark-defaults.conf.")
       System.exit(exitCode)
       
       def inferDefaultCores(): Int = Runtime.getRuntime.availableProcessors()
       功能: 获取默认CPU数量
       
       def inferDefaultMemory(): Int
       功能: 获取默认内存量
       val= {
           val ibmVendor = System.getProperty("java.vendor").contains("IBM")
           var totalMb = 0
           try {
             val bean = ManagementFactory.getOperatingSystemMXBean()
             if (ibmVendor) {
               val beanClass = Class.forName("com.ibm.lang.management.OperatingSystemMXBean")
               val method = beanClass.getDeclaredMethod("getTotalPhysicalMemory")
               totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
             } else {
               val beanClass = Class.forName("com.sun.management.OperatingSystemMXBean")
               val method = beanClass.getDeclaredMethod("getTotalPhysicalMemorySize")
               totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
             }
           } catch {
             case e: Exception =>
               totalMb = 2*1024
               System.out.println("Failed to get total physical memory. Using " + totalMb + " MB")
           }
           math.max(totalMb - 1024, Utils.DEFAULT_DRIVER_MEM_MB)
         }
       
       def checkWorkerMemory(): Unit
       功能: 校验worker内存量
       if (memory <= 0) {
         val message = "Memory is below 1MB, or missing a M/G at the end of 
         the memory specification?"
         throw new IllegalStateException(message)
       }
   }
   ```
   
   #### WorkerSource
   
   ```scala
   private[worker] class WorkerSource(val worker: Worker) extends Source {
       介绍:
       	worker资源
       属性:
       #name @sourceName = "worker"	资源名称
       #name @metricRegistry = new MetricRegistry()	度量注册器
       初始化操作:
       metricRegistry.register(MetricRegistry.name("executors"), new Gauge[Int] {
           override def getValue: Int = worker.executors.size
         })
       功能: 注册执行器数量
       
       metricRegistry.register(MetricRegistry.name("coresUsed"), new Gauge[Int] {
           override def getValue: Int = worker.coresUsed
         })
       功能: 注册CPU数量
       
       metricRegistry.register(MetricRegistry.name("memUsed_MB"), new Gauge[Int] {
           override def getValue: Int = worker.memoryUsed
         })
       功能: 注册内存使用量
       
       metricRegistry.register(MetricRegistry.name("coresFree"), new Gauge[Int] {
           override def getValue: Int = worker.coresFree
         })
       功能: 注册空闲CPU数量
       
       metricRegistry.register(MetricRegistry.name("memFree_MB"), new Gauge[Int] {
           override def getValue: Int = worker.memoryFree
         })
       功能: 注册空闲内存量
   }
   ```
   
   #### WorkerWatcher
   
   ```scala
   private[spark] class WorkerWatcher(
       override val rpcEnv: RpcEnv, workerUrl: String, isTesting: Boolean = false)
   extends RpcEndpoint with Logging {
       介绍: worker监视器
       这是一个RPC端点,连接到一个进程,如果链接已经被占用了,则会终止JVM.提供worker与子进程的共享.
       属性:
       #name @isShutDown = false	关闭状态(用于避免测试时JVM关闭)
       #name @expectedAddress = RpcAddress.fromURIString(workerUrl)	期望地址
       操作集:
       def isWorker(address: RpcAddress) = expectedAddress == address
       功能: 确定是否为worker
       
       def exitNonZero() = if (isTesting) isShutDown = true else System.exit(-1)
       功能: 非正常退出
       
       def receive: PartialFunction[Any, Unit]
       功能: 接受RPC消息
       case e => logWarning(s"Received unexpected message: $e")
       
       def onConnected(remoteAddress: RpcAddress): Unit
       功能: 连接处理
       if (isWorker(remoteAddress)) {
         logInfo(s"Successfully connected to $workerUrl")
       }
       
       def onDisconnected(remoteAddress: RpcAddress): Unit
       功能: 断开连接处理
       if (isWorker(remoteAddress)) {
         logError(s"Lost connection to worker rpc endpoint $workerUrl. Exiting.")
         exitNonZero()
       }
       
       def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit
       功能: 处理网络错误
       if (isWorker(remoteAddress)) {
         logError(s"Could not initialize connection to worker $workerUrl. Exiting.")
         logError(s"Error was: $cause")
         exitNonZero()
       }
   }
   ```

#### ApplicationDescription

```scala
private[spark] case class ApplicationDescription(
    name: String,	// 应用名称
    maxCores: Option[Int], // 使用最大CPU数量
    memoryPerExecutorMB: Int, // 每个执行器内存大小MB
    command: Command, // 指令
    appUiUrl: String, // 应用Web端地址
    eventLogDir: Option[URI] = None, // 事件日志地址
    eventLogCodec: Option[String] = None, // 事件日志压缩方式
    coresPerExecutor: Option[Int] = None, // 每个执行器CPU数量
    initialExecutorLimit: Option[Int] = None, // 初始化执行器上限
    user: String = System.getProperty("user.name", "<unknown>"),// 用户信息
    resourceReqsPerExecutor: Seq[ResourceRequirement] = Seq.empty) {// 每个执行器资源请求
    def toString: String = "ApplicationDescription(" + name + ")"
    功能: 信息显示
}
```

#### Client.scala

```scala
private class ClientEndpoint(
    override val rpcEnv: RpcEnv,
    driverArgs: ClientArguments,
    masterEndpoints: Seq[RpcEndpointRef],
    conf: SparkConf)
extends ThreadSafeRpcEndpoint with Logging {
    介绍: 客户端RPC端点
    传达信息给驱动器的代理,暂时不支持子任务失败的重新提交.在HA模式下,客户端会提交请求到所有master,且需要探测哪个master可以执行.
    构造器参数:
    	rpcEnv	RPC环境
    	driverArgs	驱动器参数
    	masterEndpoints	master RPC端点列表
    	conf spark配置
    属性:
    #name @forwardMessageThread	用于在特定时间发送消息的调度执行器
    val= ThreadUtils.newDaemonSingleThreadScheduledExecutor("client-forward-message")
    #name @forwardMessageExecutionContext	用于提供`Future`方法的隐式参数
    val= ExecutionContext.fromExecutor(forwardMessageThread,
      t => t match {
        case ie: InterruptedException => // Exit normally
        case e: Throwable =>
          logError(e.getMessage, e)
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
      })
    #name @lostMasters = new HashSet[RpcAddress]	丢失的master列表
    #name @activeMasterEndpoint: RpcEndpointRef = null	激活的Master端点
    操作集:
    def getProperty(key: String, conf: SparkConf): Option[String]
    功能: 获取系统属性
    
    def onStart(): Unit
    功能: 启动客户端
    1. 根据驱动器侧属性@driverArgs发送启动或者中断指令
    driverArgs.cmd match {
      case "launch" => // 发起启动指令
        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"
        val classPathConf = config.DRIVER_CLASS_PATH.key
        val classPathEntries = getProperty(classPathConf, conf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }
        val libraryPathConf = config.DRIVER_LIBRARY_PATH.key
        val libraryPathEntries = getProperty(libraryPathConf, conf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }
        val extraJavaOptsConf = config.DRIVER_JAVA_OPTIONS.key
        val extraJavaOpts = getProperty(extraJavaOptsConf, conf)
          .map(Utils.splitCommandString).getOrElse(Seq.empty)
        val sparkJavaOpts = Utils.sparkJavaOpts(conf)
        val javaOpts = sparkJavaOpts ++ extraJavaOpts
        val command = new Command(mainClass,
          Seq("{{WORKER_URL}}", "{{USER_JAR}}", 
              driverArgs.mainClass) ++ driverArgs.driverOptions,
          sys.env, classPathEntries, libraryPathEntries, javaOpts)
        val driverResourceReqs = ResourceUtils.parseResourceRequirements(conf,
          config.SPARK_DRIVER_PREFIX)
        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.memory,
          driverArgs.cores,
          driverArgs.supervise,
          command,
          driverResourceReqs)
        asyncSendToMasterAndForwardReply[SubmitDriverResponse](
          RequestSubmitDriver(driverDescription))

      case "kill" => //发起中断指令
        val driverId = driverArgs.driverId
        asyncSendToMasterAndForwardReply[KillDriverResponse](RequestKillDriver(driverId))
    }
    
    def asyncSendToMasterAndForwardReply[T: ClassTag](message: Any): Unit 
    功能: 异步地发送消息到master,并将消息传回
    for (masterEndpoint <- masterEndpoints) {
      masterEndpoint.ask[T](message).onComplete { // 成功执行函数
        case Success(v) => self.send(v)
        case Failure(e) =>
          logWarning(s"Error sending messages to master $masterEndpoint", e)
      }(forwardMessageExecutionContext) 
    }
    
    def pollAndReportStatus(driverId: String): Unit
    功能: 轮询找到驱动器状态并退出JVM
    1. 由master发送RPC消息,询问driver端状态
    Thread.sleep(5000)
    val statusResponse =
      activeMasterEndpoint.askSync[DriverStatusResponse](RequestDriverStatus(driverId))
    2. 处理返回的状态响应,处理完毕退出JVM
    if (statusResponse.found) {
      logInfo(s"State of $driverId is ${statusResponse.state.get}")
      // driver侧状态正常
      (statusResponse.workerId, statusResponse.workerHostPort, statusResponse.state)
        match {
        case (Some(id), Some(hostPort), Some(DriverState.RUNNING)) =>
          logInfo(s"Driver running on $hostPort ($id)")
        case _ =>
      }
      // driver侧状态异常
      statusResponse.exception match {
        case Some(e) =>
          logError(s"Exception from cluster was: $e")
          e.printStackTrace()
          System.exit(-1)
        case _ =>
          System.exit(0)
      }
    } else {
      logError(s"ERROR: Cluster master did not recognize $driverId")
      System.exit(-1)
    }
    
    def receive: PartialFunction[Any, Unit]
    功能: 接受RPC消息,可以接受master发送的提交driver请求和中断(kill)driver请求
    case SubmitDriverResponse(master, success, driverId, message) =>
      // 提交driver回应
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
        pollAndReportStatus(driverId.get)
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }
    // 中断driver侧的回应
    case KillDriverResponse(master, driverId, success, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
        pollAndReportStatus(driverId)
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }
 
    def onDisconnected(remoteAddress: RpcAddress): Unit
    功能: 断开连接处理
    if (!lostMasters.contains(remoteAddress)) {
      logError(s"Error connecting to master $remoteAddress.")
      lostMasters += remoteAddress // 丢失master信息+1
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
    
    def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit
    功能: 网络错误处理
    if (!lostMasters.contains(remoteAddress)) {
      logError(s"Error connecting to master ($remoteAddress).")
      logError(s"Cause was: $cause")
      lostMasters += remoteAddress // 丢失master信息+1
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
    
    def onError(cause: Throwable): Unit
    功能: 错误处理
    logError(s"Error processing messages, exiting.")
    cause.printStackTrace()
    System.exit(-1)
    
    def onStop(): Unit= forwardMessageThread.shutdownNow()
    功能: 停止客户端处理(关闭发送消息的线程)
}
```

```scala
object Client {
    介绍: 在独立运行的集群中用于开启或者终止驱动器的可使用类
 	def main(args: Array[String]): Unit = {
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a future version of Spark")
      println("Use ./bin/spark-submit with \"--master spark://host:port\"")
    }
    new ClientApp().start(args, new SparkConf())
  }   
}
```

```scala
private[spark] class ClientApp extends SparkApplication {
    介绍: 客户端应用程序
    def start(args: Array[String], conf: SparkConf): Unit 
    功能: 启动客户端应用程序
    1. 设置启动参数
    val driverArgs = new ClientArguments(args)
    if (!conf.contains(RPC_ASK_TIMEOUT)) {
      conf.set(RPC_ASK_TIMEOUT, "10s")
    }
    Logger.getRootLogger.setLevel(driverArgs.logLevel)
    2. 创建RPC环境
    val rpcEnv =
      RpcEnv.create("driverClient", Utils.localHostName(),
                    0, conf, new SecurityManager(conf))
    3. 获取master端点信息
    val=rpcEnv.setupEndpoint("client", new ClientEndpoint(
        rpcEnv, driverArgs, masterEndpoints, conf))
    rpcEnv.awaitTermination()
}
```

#### ClientArguments

```scala
private[deploy] class ClientArguments(args: Array[String]) {
    介绍: 客户端参数,命令行参数转换器
    属性:
    #name @cmd: String = ""	命令(launch/kill)
    #name @logLevel = Level.WARN	日志等级
    #name @masters: Array[String] = null	master列表
    #name @jarUrl: String = ""	jar位置
    #name @mainClass: String = ""	主类名称
    #name @supervise: Boolean = DEFAULT_SUPERVISE	是否监管
    #name @memory: Int = DEFAULT_MEMORY	内存大小
    #name @cores: Int = DEFAULT_CORES	CPU数量
    #name @_driverOptions = ListBuffer[String]()	driver参数信息
    #name @driverId: String = ""	driver编号(kill参数)
    初始化操作:
    parse(args.toList)
    功能: 转换参数列表
    操作集:
    def printUsageAndExit(exitCode: Int): Unit
    功能: 打印可用性并退出JVM
    1. 使用信息
    val usage =
     s"""
      |Usage: DriverClient [options] launch
      <active-master> <jar-url> <main-class> [driver options]
      |Usage: DriverClient kill <active-master> <driver-id>
      |
      |Options:
      |   -c CORES, --cores CORES        
      Number of cores to request (default: $DEFAULT_CORES)
      |   -m MEMORY, --memory MEMORY     
      Megabytes of memory to request (default: $DEFAULT_MEMORY)
      |   -s, --supervise                Whether to restart the driver on failure
      |                                  (default: $DEFAULT_SUPERVISE)
      |   -v, --verbose                  Print more debugging output
     """.stripMargin
    2. 打印并退出JVM
    System.err.println(usage)
    System.exit(exitCode)
    
    @tailrec
    private def parse(args: List[String]): Unit 
    功能: 转换参数列表
    args match {
        case ("--cores" | "-c") :: IntParam(value) :: tail =>
          cores = value
          parse(tail)
        case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
          memory = value
          parse(tail)
        case ("--supervise" | "-s") :: tail =>
          supervise = true
          parse(tail)
        case ("--help" | "-h") :: tail =>
          printUsageAndExit(0)
        case ("--verbose" | "-v") :: tail =>
          logLevel = Level.INFO
          parse(tail)
        case "launch" :: _master :: _jarUrl :: _mainClass :: tail =>
          cmd = "launch"
          if (!ClientArguments.isValidJarUrl(_jarUrl)) {
            println(s"Jar url '${_jarUrl}' is not in valid format.")
            println(s"Must be a jar file path in URL format " +
              "(e.g. hdfs://host:port/XX.jar, file:///XX.jar)")
            printUsageAndExit(-1)
          }
          jarUrl = _jarUrl
          masters = Utils.parseStandaloneMasterUrls(_master)
          mainClass = _mainClass
          _driverOptions ++= tail
        case "kill" :: _master :: _driverId :: tail =>
          cmd = "kill"
          masters = Utils.parseStandaloneMasterUrls(_master)
          driverId = _driverId
        case _ =>
          printUsageAndExit(1)
      }
}
```

#### Command

```scala
private[spark] case class Command(
    mainClass: String,
    arguments: Seq[String],
    environment: Map[String, String],
    classPathEntries: Seq[String],
    libraryPathEntries: Seq[String],
    javaOpts: Seq[String]) {
}
介绍: 控制指令
参数:
	mainClass	主类名称
	arguments	参数列表
	environment	环境参数映射表
	classPathEntries	类路径列表
	libraryPathEntries	库路径列表
	javaOpts	java参数配置列表
```

#### DependencyUtils

```scala
private[deploy] object DependencyUtils extends Logging {
    介绍: 依赖处理工具类
    操作集:
    def resolveMavenDependencies(
      packagesExclusions: String,
      packages: String,
      repositories: String,
      ivyRepoPath: String,
      ivySettingsPath: Option[String]): String
    功能: 处理maven依赖
    参数:
    	packagesExclusions	包排除内容
    	packages	包名
    	repositories	库名称
    	ivyRepoPath	ivy库路径(ivy?)
    	ivySettingsPath	配置路径(ivy?)
    1. 获取排除信息的列表
    val exclusions: Seq[String] =
      if (!StringUtils.isBlank(packagesExclusions)) {
        packagesExclusions.split(",")
      } else {
        Nil
      }
    2. 获取ivy路径对应的ivy配置(既可以从文件加载也可以默认构建)
    val ivySettings = ivySettingsPath match {
      case Some(path) =>
        SparkSubmitUtils.loadIvySettings(path, Option(repositories), Option(ivyRepoPath))
      case None =>
        SparkSubmitUtils.buildIvySettings(Option(repositories), Option(ivyRepoPath))
    }
    3. 协调maven的依赖
    SparkSubmitUtils.resolveMavenCoordinates(
        packages, ivySettings, exclusions = exclusions)
    
    def resolveAndDownloadJars(
      jars: String,
      userJar: String,
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      secMgr: SecurityManager): String 
    功能: 解决和下载jar包
    1. 确定目标目录和用户jar名称
    val targetDir = Utils.createTempDir()
    val userJarName = userJar.split(File.separatorChar).last
    2. 检查是否有该配置,如果含有则下载jar包
    Option(jars)
      .map {
        resolveGlobPaths(_, hadoopConf)
          .split(",")
          .filterNot(_.contains(userJarName))
          .mkString(",")
      }
      .filterNot(_ == "")
      .map(downloadFileList(_, targetDir, sparkConf, hadoopConf, secMgr))
      .orNull
    
    def addJarsToClassPath(jars: String, loader: MutableURLClassLoader): Unit
    功能: 添加jar到类路径下
    if (jars != null) {
      for (jar <- jars.split(",")) {
        addJarToClasspath(jar, loader)
      }
    }
    
    def downloadFileList(
      fileList: String,
      targetDir: File,
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      secMgr: SecurityManager): String 
    功能: 下载文件列表中的文件
    require(fileList != null, "fileList cannot be null.")
    Utils.stringToSeq(fileList)
      .map(downloadFile(_, targetDir, sparkConf, hadoopConf, secMgr))
      .mkString(",")
    
    def downloadFile(
      path: String,
      targetDir: File,
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      secMgr: SecurityManager): String
    功能: 从远端下载文件到本地临时目录,如果输入目录指向本地目录.返回nop
    参数:
    	path	输入文件路径
    	targetDir	下载文件临时目录
    	sparkConf	spark配置
    	hadoopConf	hadoop配置
    	secMgr	安全管理器
    返回: 本地文件目录
    0. 路径合法性断言
    require(path != null, "path cannot be null.")
    1. 获取路径的URI
    val uri = Utils.resolveURI(path)
    2. 根据URI使用协议处理下载问题
    uri.getScheme match {
      case "file" | "local" => path // 本地文件处理
      case "http" | "https" | "ftp" if Utils.isTesting => // http/https/ftp协议处理
        val file = new File(uri.getPath)
        new File(targetDir, file.getName).toURI.toString
      case _ =>
        val fname = new Path(uri).getName()
        val localFile = Utils.doFetchFile(
            uri.toString(), targetDir, fname, sparkConf, secMgr,hadoopConf)
        localFile.toURI().toString()
    }
    
    def resolveGlobPaths(paths: String, hadoopConf: Configuration): String
    功能: 解决全局路径问题,返回全局路径名称
    0. 路径合法性断言
    require(paths != null, "paths cannot be null.")
    1. 处理每个路径的全局名称
    Utils.stringToSeq(paths).flatMap { path =>
      val (base, fragment) = splitOnFragment(path)
      (resolveGlobPath(base, hadoopConf), fragment) match {
        case (resolved, Some(_)) if resolved.length > 1 => throw new SparkException(
            s"${base.toString} resolves ambiguously to multiple files:
            ${resolved.mkString(",")}")
        case (resolved, Some(namedAs)) => resolved.map(_ + "#" + namedAs)
        case (resolved, _) => resolved
      }
    }.mkString(",")
    
    def addJarToClasspath(localJar: String, loader: MutableURLClassLoader): Unit 
    功能: 添加jar包到类路径下(localJar --> loader)
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          logWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        logWarning(s"Skip remote jar $uri.")
    }
    
    def mergeFileLists(lists: String*): String
    功能: 合并文件列表(过滤掉空名称的文件,将其连接成一个串)
    val merged = lists.filterNot(StringUtils.isBlank)
      .flatMap(Utils.stringToSeq)
    val= if (merged.nonEmpty) merged.mkString(",") else null
    
    def splitOnFragment(path: String): (URI, Option[String]) 
    功能: 路径分片
    val uri = Utils.resolveURI(path) // 获取一个组织完好的文件描述,例如: file://schema
    val withoutFragment = new URI(uri.getScheme, uri.getSchemeSpecificPart, null)
    val=(withoutFragment, Option(uri.getFragment))
    
    def resolveGlobPath(uri: URI, hadoopConf: Configuration): Array[String]
    功能: 解决全局路径问题
    val= uri.getScheme match {
      case "local" | "http" | "https" | "ftp" => Array(uri.toString)
      case _ =>
        val fs = FileSystem.get(uri, hadoopConf)
        Option(fs.globStatus(new Path(uri))).map { status =>
          status.filter(_.isFile).map(_.getPath.toUri.toString)
        }.getOrElse(Array(uri.toString))
    }
}
```

#### DeployMessage

```scala
private[deploy] sealed trait DeployMessage extends Serializable
功能: 部署消息
```

```scala
private[deploy] object DeployMessages {
    介绍: 部署消息,包含调度端点传递的消息
    
    ---
    Worker --> Master的消息集合
    
    case class RegisterWorker(
      id: String,
      host: String,
      port: Int,
      worker: RpcEndpointRef,
      cores: Int,
      memory: Int,
      workerWebUiUrl: String,
      masterAddress: RpcAddress,
      resources: Map[String, ResourceInformation] = Map.empty)
    extends DeployMessage{
        介绍: 注册worker消息
        参数校验:
        Utils.checkHost(host)
        assert (port > 0)
        功能: 校验主机和端口号
    }
    
    case class ExecutorStateChanged(
      appId: String,
      execId: Int,
      state: ExecutorState,
      message: Option[String],
      exitStatus: Option[Int])
    extends DeployMessage
    介绍: 执行器状态改变消息
    
    case class DriverStateChanged(
      driverId: String,
      state: DriverState,
      exception: Option[Exception])
    extends DeployMessage
    介绍: 驱动器状态改变消息
    
    case class WorkerExecutorStateResponse(
      desc: ExecutorDescription,
      resources: Map[String, ResourceInformation])
    介绍: worker执行器状态回应
    
    case class WorkerDriverStateResponse(
      driverId: String,
      resources: Map[String, ResourceInformation])
    介绍: worker驱动器状态回应
    
    case class WorkerSchedulerStateResponse(
      id: String,
      execResponses: List[WorkerExecutorStateResponse],
      driverResponses: Seq[WorkerDriverStateResponse])
    介绍: worker调度状态回应
    
    case class WorkerLatestState(
        id: String,
        executors: Seq[ExecutorDescription],
        driverIds: Seq[String]) extends DeployMessage
    介绍: worker最新的状态
    当注册到master的时候,会发送这条消息到master上.然后master会将其与master中存在的驱动器/执行器比较,并告知worker去kill未知的执行器/驱动器.
    
    case class Heartbeat(workerId: String, worker: RpcEndpointRef) extends DeployMessage
    功能: 心跳消息
    
    --- 
    
    Master --> Worker 的消息集合
    
    sealed trait RegisterWorkerResponse
    
    case class RegisteredWorker(
      master: RpcEndpointRef,
      masterWebUiUrl: String,
      masterAddress: RpcAddress,
      duplicate: Boolean) extends DeployMessage with RegisterWorkerResponse
    介绍: 注册worker
    
    case class RegisterWorkerFailed(message: String) extends DeployMessage 
    with RegisterWorkerResponse
    介绍: 注册worker失败消息
    
    case class ReconnectWorker(masterUrl: String) extends DeployMessage
    介绍: 重新连接worker
    
    case class KillExecutor(masterUrl: String, appId: String, execId: Int) 
    extends DeployMessage
    介绍: kill执行器
    
    case object MasterInStandby extends DeployMessage with RegisterWorkerResponse
    介绍: master处于standby状态消息
    
    case class LaunchExecutor(
      masterUrl: String,
      appId: String,
      execId: Int,
      appDesc: ApplicationDescription,
      cores: Int,
      memory: Int,
      resources: Map[String, ResourceInformation] = Map.empty)
    extends DeployMessage
    介绍: 启动执行器消息
    
    case class LaunchDriver(
      driverId: String,
      driverDesc: DriverDescription,
      resources: Map[String, ResourceInformation] = Map.empty) extends DeployMessage
    介绍: 启动驱动器消息
    
    case class KillDriver(driverId: String) extends DeployMessage
    介绍: kill驱动器消息
    
    case class ApplicationFinished(id: String)
    介绍: 应用结束
    
    ---
    
    worker内部
    
    case object WorkDirCleanup 
   	介绍: 工作目录清理,周期性的发送到worker端点,清除应用文件夹
    
    case object ReregisterWithMaster
    介绍: 使用master注册,当一个worker需要连接到master时使用
    
    ---
    
    应用客户端 --> Master
    
    case class RegisterApplication(
        appDescription: ApplicationDescription, driver: RpcEndpointRef) 
    extends DeployMessage
    介绍: 注册应用消息
    
    case class UnregisterApplication(appId: String)
    介绍: 解除应用的注册
    
    case class MasterChangeAcknowledged(appId: String)
    介绍: 告知收到master变化
    
    case class RequestExecutors(appId: String, requestedTotal: Int)
    介绍: 请求执行器
    
    case class KillExecutors(appId: String, executorIds: Seq[String])
    介绍: 中断执行器
    
    ---
    
    Master --> 应用客户端
    
    case class RegisteredApplication(appId: String, master: RpcEndpointRef) 
    extends DeployMessage
    介绍: 注册应用消息
    
    case class ExecutorAdded(
        id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) {
        介绍: 添加执行器
        Utils.checkHostPort(hostPort)
        功能: 参数校验
    }
    
    case class ExecutorUpdated(id: Int, state: ExecutorState, message: Option[String],
    exitStatus: Option[Int], workerLost: Boolean)
    介绍: 更新执行器
    
    case class ApplicationRemoved(message: String)
    介绍: 移除应用
    
    case class WorkerRemoved(id: String, host: String, message: String)
    介绍: 移除worker
    
    ---
    
    DriverClient <-> Master
    
    case class RequestSubmitDriver(driverDescription: DriverDescription) 
    extends DeployMessage
    介绍: 请求提交驱动器
    
    case class SubmitDriverResponse(
        master: RpcEndpointRef, success: Boolean, driverId: Option[String],
        message: String)
    extends DeployMessage
    介绍: 提交驱动器回应
    
    case class RequestKillDriver(driverId: String) extends DeployMessage
    介绍: 请求kill驱动器
    
    case class KillDriverResponse(
      master: RpcEndpointRef, driverId: String, success: Boolean, message: String)
    extends DeployMessage
    介绍: kill驱动器回应消息
    
    case class RequestDriverStatus(driverId: String) extends DeployMessage
    介绍: 请求驱动器状态
    
    case class DriverStatusResponse(
        found: Boolean, state: Option[DriverState],
        workerId: Option[String], workerHostPort: Option[String], 
        exception: Option[Exception])
    介绍: 驱动器状态回应
    
    ---
    
    case object StopAppClient
    介绍: 停止应用客户端
    
    case class MasterChanged(master: RpcEndpointRef, masterWebUiUrl: String)
    介绍: 改变master
    
    case object RequestMasterState
    介绍: 请求master状态
    
    case class MasterStateResponse(
      host: String,
      port: Int,
      restPort: Option[Int],
      workers: Array[WorkerInfo],
      activeApps: Array[ApplicationInfo],
      completedApps: Array[ApplicationInfo],
      activeDrivers: Array[DriverInfo],
      completedDrivers: Array[DriverInfo],
      status: MasterState) {
        介绍: master状态回应
        参数校验:
        Utils.checkHost(host)
        assert (port > 0)
        操作集:
        def uri: String = "spark://" + host + ":" + port
        功能: 获取URI地址
        
        def restUri: Option[String] = restPort.map { p => "spark://" + host + ":" + p }
        功能: 获取REST URI
    }
    
    case object RequestWorkerState
    介绍: 请求worker状态
    
    case class WorkerStateResponse(
        host: String, port: Int, workerId: String,
        executors: List[ExecutorRunner], finishedExecutors: List[ExecutorRunner],
        drivers: List[DriverRunner], finishedDrivers: List[DriverRunner], 
        masterUrl: String,
        cores: Int, memory: Int, coresUsed: Int, memoryUsed: Int, masterWebUiUrl: String,
        resources: Map[String, ResourceInformation] = Map.empty,
        resourcesUsed: Map[String, ResourceInformation] = Map.empty) {
        介绍: worker状态响应
        参数校验
        Utils.checkHost(host)
        assert (port > 0)
    }
    
    case object SendHeartbeat
    介绍: 发送心跳信息
}
```

#### DriverDescription

```scala
private[deploy] case class DriverDescription(
    jarUrl: String,
    mem: Int,
    cores: Int,
    supervise: Boolean,
    command: Command,
    resourceReqs: Seq[ResourceRequirement] = Seq.empty) {
    介绍: 驱动器描述
    构造器参数:
        jarUrl	jarUrl地址
        mem	内存使用量
        cores	CPU使用量
        supervise	是否监控
        command	控制指令
        resourceReqs	资源请求列表
    操作集:
    def toString: String = s"DriverDescription (${command.mainClass})"
    功能: 信息显示
}
```

#### ExecutorDescription

```scala
private[deploy] class ExecutorDescription(
    val appId: String,
    val execId: Int,
    val cores: Int,
    val state: ExecutorState.Value)
extends Serializable {
    介绍: 执行器描述
    worker发送执行器状态到Master上,对于master来说重构内部数据结构很高效.
    构造器参数:
    	appId	应用ID
    	execId	执行器编号
    	cores	CPU数量
    	state	执行器状态
    操作集:
    def toString: String
    功能: 信息显示
    val="ExecutorState(appId=%s, execId=%d, cores=%d, state=%s)".format(
        appId, execId, cores, state)
}
```

#### ExecutorState

```scala
private[deploy] object ExecutorState extends Enumeration {
    介绍: 执行器状态
    val LAUNCHING, RUNNING, KILLED, FAILED, LOST, EXITED = Value
    type ExecutorState = Value
    
    def isFinished(state: ExecutorState): Boolean = 
    	Seq(KILLED, FAILED, LOST, EXITED).contains(state)
    功能: 确定执行器是否处于结束状态
}
```

#### ExternalShuffleService

```scala
private[deploy]
class ExternalShuffleService(sparkConf: SparkConf, securityManager: SecurityManager)
extends Logging {
    介绍: 外部shuffle服务,提供一个服务器,执行器可以从其中读取shuffle文件(而非是从其他地方读取).提供非中断的文件获取方式(以应对执行器关闭或者被kill的情况)
    属性:
    #name @masterMetricsSystem	master度量系统
    val= MetricsSystem.createMetricsSystem(MetricsSystemInstances.SHUFFLE_SERVICE,
      sparkConf, securityManager)
    #name @enabled = sparkConf.get(config.SHUFFLE_SERVICE_ENABLED)	是否允许外部shuffle服务
    #name @port = sparkConf.get(config.SHUFFLE_SERVICE_PORT)	shuffle服务端口
    #name @registeredExecutorsDB = "registeredExecutors.ldb"	注册执行器数据库名
    #name @transportConf	传输配置
    val= SparkTransportConf.fromSparkConf(sparkConf, "shuffle", numUsableCores = 0)
    #name @blockHandler = newShuffleBlockHandler(transportConf)	块处理器
    #name @transportContext: TransportContext = _	传输上下文
    #name @server: TransportServer = _	传输服务器
    #name @shuffleServiceSource = new ExternalShuffleServiceSource	shuffle服务资源
    操作集:
    def findRegisteredExecutorsDBFile(dbName: String): File 
    功能: 查找注册的执行器数据库文件
    1. 获取本地目录
    val localDirs = sparkConf.getOption(
        "spark.local.dir").map(_.split(",")).getOrElse(Array())
    2. 返回查找到的文件目录
    val= if (localDirs.length >= 1) {
      new File(localDirs.find(
          new File(_, dbName).exists()).getOrElse(localDirs(0)), dbName)
    } else {
      logWarning(s"'spark.local.dir' should be set first when we use db in " +
        s"ExternalShuffleService. Note that this only affects standalone mode.")
      null
    }
    
    def getBlockHandler: ExternalBlockHandler= blockHandler
    功能: 获取数据块处理器
    
    def newShuffleBlockHandler(conf: TransportConf): ExternalBlockHandler
    功能: 新建数据块处理器
    val= if (sparkConf.get(config.SHUFFLE_SERVICE_DB_ENABLED) && enabled) {
      new ExternalBlockHandler(
          conf, findRegisteredExecutorsDBFile(registeredExecutorsDB))
    } else { 
      new ExternalBlockHandler(conf, null)
    }
    
    def startIfEnabled(): Unit= if (enabled) { start() }
    功能: 如果允许外部shuffle服务,则开启外部shuffle服务
    
    def start(): Unit
    功能: 启动外部shuffle服务
    0. 服务器状态断言
    require(server == null, "Shuffle server already started")
    1. 获取服务器启动器
    val authEnabled = securityManager.isAuthenticationEnabled()
    logInfo(s"Starting shuffle service on port $port (auth enabled = $authEnabled)")
    val bootstraps: Seq[TransportServerBootstrap] =
      if (authEnabled) {
        Seq(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        Nil
      }
    2. 创建服务器
    transportContext = new TransportContext(transportConf, blockHandler, true)
    server = transportContext.createServer(port, bootstraps.asJava)
    3. 初始化度量系统参数
    shuffleServiceSource.registerMetricSet(server.getAllMetrics)
    blockHandler.getAllMetrics.getMetrics.put("numRegisteredConnections",
        server.getRegisteredConnections)
    shuffleServiceSource.registerMetricSet(blockHandler.getAllMetrics)
    masterMetricsSystem.registerSource(shuffleServiceSource)
    masterMetricsSystem.start()
    
    def executorRemoved(executorId: String, appId: String): Unit
    功能: 清除执行器存在的非shuffle文件
    blockHandler.executorRemoved(executorId, appId)
    
    def stop(): Unit
    功能: 关闭shuffle服务
    if (server != null) {
      server.close()
      server = null
    }
    if (transportContext != null) {
      transportContext.close()
      transportContext = null
    }
}
```

```scala
object ExternalShuffleService extends Logging {
    介绍: 外部shuffle服务的主类
    属性:
    #name @server: ExternalShuffleService = _ @volatile	外部shuffle服务服务器
    #name @barrier = new CountDownLatch(1)	屏蔽计数值(计数值小于0抛出异常,可以当特殊信号量使用)
    操作集:
    private[spark] def main(
      args: Array[String],
      newShuffleService: (SparkConf, SecurityManager) => ExternalShuffleService): Unit
    功能: 辅助主函数
    输入参数:
    	newShuffleService	新建shuffle服务函数
    1. 启动外部shuffle服务器
    Utils.initDaemon(log)
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    val securityManager = new SecurityManager(sparkConf)
    sparkConf.set(config.SHUFFLE_SERVICE_ENABLED.key, "true")
    server = newShuffleService(sparkConf, securityManager)
    server.start()
    2. 设置关闭处理函数
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutting down shuffle service.")
      server.stop()
      barrier.countDown() // 降低latch值
    }
    3. 等待结束
    barrier.await() // latch值小于等于0即结束
    
    启动函数:
    def main(args: Array[String]): Unit = {
        main(args, (conf: SparkConf, sm: SecurityManager) => 
             new ExternalShuffleService(conf, sm))
    }
}
```

#### ExternalShuffleServiceSource

```scala
@ThreadSafe
private class ExternalShuffleServiceSource extends Source {
    介绍: 外部shuffle服务资源
    属性: 
    #name @metricRegistry = new MetricRegistry()	计量注册器
    #name @sourceName = "shuffleService"	资源名称
    操作集:
    def registerMetricSet(metricSet: MetricSet): Unit 
    功能: 注册度量集合
    metricRegistry.registerAll(metricSet)
}
```

#### FaultToleranceTest

```markdown
介绍:
	容错性测试,测试spark独立调度器的容错性,主要是Master.为了更好的模仿分布式系统,这里使用了docker.执行使用:  ./bin/spark-class org.apache.spark.deploy.FaultToleranceTest 来开启测试
	确保环境中配置了如下的配置(在SPARK_DAEMON_JAVA_OPTS内部)
	- spark.deploy.recoveryMode=ZOOKEEPER
	- spark.deploy.zookeeper.url=172.17.42.1:2181
	注意到172.17.42.1是docker的默认ip,而2181是zookeeper的默认端口
	失败的情况下需要在重启前kill之前的docker容器.docker kill $(docker ps -q)
	不幸的是,由于docker的依赖,这个条件不可以在没有安装docker的环境下自动运行.除了需要运行docker,还需要如下配置:
	1. 不使用sudo运行docker <http://docs.docker.io/en/latest/use/basics/>
	2. docker镜像标记了spark-test-master 和 spark-test-worker 都是建立在docker/上.运行docker/spark-test/build 产生这些内容
```

```scala
private object FaultToleranceTest extends App with Logging {
    属性:
    #name @conf = new SparkConf()	spark配置
    #name @zkDir = conf.get(config.Deploy.ZOOKEEPER_DIRECTORY).getOrElse("/spark")
    	zk目录
    #name @masters = ListBuffer[TestMasterInfo]()	master列表
    #name @workers = ListBuffer[TestWorkerInfo]()	worker列表
    #name @sc: SparkContext = _	spark上下文
    #name @zk = SparkCuratorUtil.newClient(conf)	zk客户端
    #name @numPassed = 0	跳过检查的数量
    #name @numFailed = 0	检查失败的数量
    #name @sparkHome = System.getenv("SPARK_HOME")	sparkHome
    #name @containerSparkHome = "/opt/spark"	容器spark Home
    #name @dockerMountDir = "%s:%s".format(sparkHome, containerSparkHome)
    	docker挂载目录
    初始化操作:
    test("sanity-basic") {
        addMasters(1)
        addWorkers(1)
        createClient()
        assertValidClusterState()
      }
    功能: 测试基本功能是否清晰
    
    test("sanity-many-masters") {
        addMasters(3)
        addWorkers(3)
        createClient()
        assertValidClusterState()
      }
    功能: 测试多个master情况下是否可用
    
    test("single-master-halt") {
        addMasters(3)
        addWorkers(2)
        createClient()
        assertValidClusterState()

        killLeader() // 停止一个master
        delay(30.seconds)
        assertValidClusterState()
        createClient()
        assertValidClusterState()
      }
    功能: 测试单个master停止
    
    test("single-master-restart") {
        addMasters(1)
        addWorkers(2)
        createClient()
        assertValidClusterState()
		
        // 单个master重启
        killLeader()
        addMasters(1)
        delay(30.seconds)
        assertValidClusterState()

        killLeader()
        addMasters(1)
        delay(30.seconds)
        assertValidClusterState()
      }
    功能: 测试单个master重启
    
    test("cluster-failure") {
        addMasters(2)
        addWorkers(2)
        createClient()
        assertValidClusterState()

        terminateCluster()
        addMasters(2)
        addWorkers(2)
        assertValidClusterState()
      }
    功能: 测试集群失败
    
    test("rolling-outage") {
        addMasters(1)
        delay()
        addMasters(1)
        delay()
        addMasters(1)
        addWorkers(2)
        createClient()
        assertValidClusterState()
        assertTrue(getLeader == masters.head)

        (1 to 3).foreach { _ =>
          killLeader()
          delay(30.seconds)
          assertValidClusterState()
          assertTrue(getLeader == masters.head)
          addMasters(1)
        }
      }
    功能: 测试滚动重启
    
    操作集:
    def afterEach(): Unit 
    功能: 收尾操作
    if (sc != null) {
      sc.stop()
      sc = null
    }
    terminateCluster()
    SparkCuratorUtil.deleteRecursive(zk, zkDir + "/spark_leader")
    SparkCuratorUtil.deleteRecursive(zk, zkDir + "/master_status")
    
    def test(name: String)(fn: => Unit): Unit
    功能: 测试
    输入参数:
    	name	测试名称
    	fn	测试过程
    try {
      fn
      numPassed += 1
      logInfo("==============================================")
      logInfo("Passed: " + name)
      logInfo("==============================================")
    } catch {
      case e: Exception =>
        numFailed += 1
        logInfo("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logError("FAILED: " + name, e)
        logInfo("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        sys.exit(1)
    }
    afterEach()
    
    def addMasters(num: Int): Unit
    功能: 添加指定数量个master
    logInfo(s">>>>> ADD MASTERS $num <<<<<")
    (1 to num).foreach { _ => masters += SparkDocker.startMaster(dockerMountDir) }
    
    def addWorkers(num: Int): Unit
    功能:添加指定数量worker
    logInfo(s">>>>> ADD WORKERS $num <<<<<")
    val masterUrls = getMasterUrls(masters)
    (1 to num).foreach { _ => workers += SparkDocker.startWorker(
        dockerMountDir, masterUrls) }
   
    def createClient()
    功能: 创建一个sparkContext,这个会创建一个客户端,与集群进行交互
    logInfo(">>>>> CREATE CLIENT <<<<<")
    if (sc != null) { sc.stop() }
    System.setProperty(config.DRIVER_PORT.key, "0")
    sc = new SparkContext(getMasterUrls(masters), "fault-tolerance", containerSparkHome)
    
    def getMasterUrls(masters: Seq[TestMasterInfo]): String
    功能: 获取master的URL地址(合并)
    val= "spark://" + masters.map(master => master.ip + ":7077").mkString(",")
    
    def getLeader: TestMasterInfo
    功能: 获取master中的leader
    val leaders = masters.filter(_.state == RecoveryState.ALIVE)
    assertTrue(leaders.size == 1)
    leaders(0)
    
    def killLeader(): Unit
    功能: kill leader
    logInfo(">>>>> KILL LEADER <<<<<")
    masters.foreach(_.readState())
    val leader = getLeader
    masters -= leader
    leader.kill() 
    
    def delay(secs: Duration = 5.seconds) = Thread.sleep(secs.toMillis)
    功能: 延时
    
    def terminateCluster(): Unit
    功能: 关闭集群(释放所有master/worker)
    logInfo(">>>>> TERMINATE CLUSTER <<<<<")
    masters.foreach(_.kill())
    workers.foreach(_.kill())
    masters.clear()
    workers.clear()
    
    def assertTrue(bool: Boolean, message: String = ""): Unit
    功能: 确保为true
    if (!bool) {
      throw new IllegalStateException("Assertion failed: " + message)
    }
    
    def assertUsable()
    功能: 确保集群的可用性,如果正在恢复中,则需要花费一点时间
    val f = Future {
      try {
        val res = sc.parallelize(0 until 10).collect()
        assertTrue(res.toList == (0 until 10).toList)
        true
      } catch {
        case e: Exception =>
          logError("assertUsable() had exception", e)
          e.printStackTrace()
          false
      }
    }
    // 避免无限等待(可以注册,但是一直获取不到执行器)
    assertTrue(ThreadUtils.awaitResult(f, 2.minutes))
    
    def assertValidClusterState()
    功能: 确保集群的可用状态,master/worker都存活
    1. 设置计量参数
    logInfo(">>>>> ASSERT VALID CLUSTER STATE <<<<<")
    assertUsable()
    var numAlive = 0
    var numStandby = 0
    var numLiveApps = 0
    var liveWorkerIPs: Seq[String] = List()
    2. 确定状态可用性(所有m/w都存活)
    def stateValid(): Boolean = {
      (workers.map(_.ip) -- liveWorkerIPs).isEmpty &&
        numAlive == 1 && numStandby == masters.size - 1 && numLiveApps >= 1
    }
    3. 周期性检测并统计master的状态(Alive/Standby)
    val f = Future {
      try {
        while (!stateValid()) {
          Thread.sleep(1000)
          numAlive = 0
          numStandby = 0
          numLiveApps = 0
          masters.foreach(_.readState())
          for (master <- masters) {
            master.state match {
              case RecoveryState.ALIVE =>
                numAlive += 1
                liveWorkerIPs = master.liveWorkerIPs
              case RecoveryState.STANDBY =>
                numStandby += 1
              case _ => // ignore
            }

            numLiveApps += master.numLiveApps
          }
        }
        true
      } catch {
        case e: Exception =>
          logError("assertValidClusterState() had exception", e)
          false
      }
    }
    4. 防止无限等待
    try {
      assertTrue(ThreadUtils.awaitResult(f, 2.minutes))
    } catch {
      case e: TimeoutException =>
        logError("Master states: " + masters.map(_.state))
        logError("Num apps: " + numLiveApps)
        logError("IPs expected: " + workers.map(_.ip) + " / found: " + liveWorkerIPs)
        throw new RuntimeException("Failed to get into 
        acceptable cluster state after 2 min.", e)
    }
    
}
```

```scala
private class TestMasterInfo(val ip: String, val dockerId: DockerId, val logFile: File)
extends Logging  {
    介绍: master测试信息
    构造器参数:
    	ip	master信息
    	dockerId	docker编号
    	logFile		日志文件
    属性:
    #name @formats = org.json4s.DefaultFormats	格式处理器
    #name @state: RecoveryState.Value = _	恢复状态
    #name @liveWorkerIPs: List[String] = _	存活worker的ip列表
    #name @numLiveApps = 0	存活应用数量
    操作集:
    def kill(): Unit = { Docker.kill(dockerId) }
    功能: kill master
    
    def toString: String
    功能: 信息显示
    val= "[ip=%s, id=%s, logFile=%s, state=%s]".
      format(ip, dockerId.id, logFile.getAbsolutePath, state)
    
    def readState(): Unit 
    功能: 读取状态
    try {
      val masterStream = new InputStreamReader(
        new URL("http://%s:8080/json".format(ip)).openStream, StandardCharsets.UTF_8)
      val json = JsonMethods.parse(masterStream)
      val workers = json \ "workers"
      val liveWorkers = workers.children.filter(w => (w \ "state").extract[String] == "ALIVE")
      liveWorkerIPs = liveWorkers.map {
        w => 	(w\"webuiaddress").extract[String].
          stripPrefix("http://").stripSuffix(":8081")
      }
      numLiveApps = (json \ "activeapps").children.size
      val status = json \\ "status"
      val stateString = status.extract[String]
      state = RecoveryState.values.filter(state => state.toString == stateString).head
    } catch {
      case e: Exception =>
        logWarning("Exception", e)
    }
}
```

```scala
private class TestWorkerInfo(val ip: String, val dockerId: DockerId, val logFile: File)
extends Logging {
    属性:
    #name @formats = org.json4s.DefaultFormats	格式处理器
    操作集：
    def kill(): Unit = { Docker.kill(dockerId) }
    功能: kill worker
    
    def toString: String
    功能: 信息显示
    val= "[ip=%s, id=%s, logFile=%s]".format(ip, dockerId, logFile.getAbsolutePath)
}
```

```scala
private class DockerId(val id: String) {
	介绍: docker ID
    def toString: String = id
    功能: 信息显示
}

private object Docker extends Logging {
    操作集:
    def makeRunCmd(
        imageTag: String, args: String = "", mountDir: String = ""): ProcessBuilder
    功能: 构建运行命令
    输入参数:
    	image	镜像标签
    	args	参数
    	mountDir	挂载目录
    val mountCmd = if (mountDir != "") { " -v " + mountDir } else ""
    val cmd = "docker run -privileged %s %s %s".format(mountCmd, imageTag, args)
    logDebug("Run command: " + cmd)
    val= cmd
    
    def kill(dockerId: DockerId) : Unit 
    功能: kill docker当前id
    "docker kill %s".format(dockerId.id).!
    
    def getLastProcessId: DockerId
    功能: 获取上一个进程编号
    var id: String = null
    "docker ps -l -q".!(ProcessLogger(line => id = line))
    val= new DockerId(id)
}
```

```scala
private object SparkDocker {
    操作集:
    def startMaster(mountDir: String): TestMasterInfo
    功能: 启动master节点
    val cmd = Docker.makeRunCmd("spark-test-master", mountDir = mountDir)
    val (ip, id, outFile) = startNode(cmd)
    val= new TestMasterInfo(ip, id, outFile)
    
    def startWorker(mountDir: String, masters: String): TestWorkerInfo 
    功能: 启动worker节点
    val cmd = Docker.makeRunCmd("spark-test-worker", args = masters, mountDir = mountDir)
    val (ip, id, outFile) = startNode(cmd)
    val= new TestWorkerInfo(ip, id, outFile)
    
    def startNode(dockerCmd: ProcessBuilder) : (String, DockerId, File)
    功能: 根据docker指令,获取启动节点的参数三元组
    val ipPromise = Promise[String]()
    val outFile = File.createTempFile("fault-tolerance-test", "", Utils.createTempDir())
    val outStream: FileWriter = new FileWriter(outFile)
    def findIpAndLog(line: String): Unit = {
      if (line.startsWith("CONTAINER_IP=")) {
        val ip = line.split("=")(1)
        ipPromise.success(ip)
      }

      outStream.write(line + "\n")
      outStream.flush()
    }
    dockerCmd.run(ProcessLogger(findIpAndLog _))
    val ip = ThreadUtils.awaitResult(ipPromise.future, 30.seconds)
    val dockerId = Docker.getLastProcessId
    val= (ip, dockerId, outFile)
}
```

#### JsonProtocol

```scala
private[deploy] object JsonProtocol {
    介绍: json协议
    操作集:
    def writeResourcesInfo(info: Map[String, ResourceInformation]): JObject
    功能: 写出资源信息表@info,使用抽象语法树将其组织为json对象
    1. 获取json属性
    val jsonFields = info.map {
      case (k, v) => JField(k, v.toJson)
    }
    2. 或json对象
    val= JObject(jsonFields.toList) 
    
    def writeResourceRequirement(req: ResourceRequirement): JObject
    功能: 写出资源需求
    val= ("name" -> req.resourceName) ~
    ("amount" -> req.amount)
    
    def writeWorkerInfo(obj: WorkerInfo): JObject
    功能: 以json形式写出worker的信息
    val=("id" -> obj.id) ~	// workerid
        ("host" -> obj.host) ~ // 主机名称
        ("port" -> obj.port) ~ // 端口号
        ("webuiaddress" -> obj.webUiAddress) ~ // webui地址
        ("cores" -> obj.cores) ~ // cpu数量
        ("coresused" -> obj.coresUsed) ~ // 使用的cpu数量
        ("coresfree" -> obj.coresFree) ~ // 空闲cpu数量
        ("memory" -> obj.memory) ~ // 内存量
        ("memoryused" -> obj.memoryUsed) ~ // 使用内存量
        ("memoryfree" -> obj.memoryFree) ~ // 释放内存量
        ("resources" -> writeResourcesInfo(obj.resourcesInfo)) ~ // 资源量
        ("resourcesused" -> writeResourcesInfo(obj.resourcesInfoUsed)) ~ // 已使用资源
        ("resourcesfree" -> writeResourcesInfo(obj.resourcesInfoFree)) ~ // 释放资源量
        ("state" -> obj.state.toString) ~ //worker 状态
        ("lastheartbeat" -> obj.lastHeartbeat) // 上次心跳时间
    
    def writeApplicationInfo(obj: ApplicationInfo): JObject
    功能: 以json形式写出应用信息
    ("id" -> obj.id) ~ // 应用id
    ("starttime" -> obj.startTime) ~ // 开始时间
    ("name" -> obj.desc.name) ~ // 名称
    ("cores" -> obj.coresGranted) ~ // cpu数量
    ("user" -> obj.desc.user) ~ // 用户名称
    ("memoryperslave" -> obj.desc.memoryPerExecutorMB) ~ // 每个执行器的内存量
    ("resourcesperslave" -> obj.desc.resourceReqsPerExecutor //每个执行器的资源量
      .toList.map(writeResourceRequirement)) ~
    ("submitdate" -> obj.submitDate.toString) ~ // 提交日期
    ("state" -> obj.state.toString) ~	// 状态
    ("duration" -> obj.duration) // 任务运行时间
    
    def writeApplicationDescription(obj: ApplicationDescription): JObject
    功能: 以json形式写出应用描述
    ("name" -> obj.name) ~	// 应用名称
    ("cores" -> obj.maxCores.getOrElse(0)) ~ // cpu数量
    ("memoryperslave" -> obj.memoryPerExecutorMB) ~ // 每个执行器的内存量
    ("resourcesperslave" ->   obj.resourceReqsPerExecutor.
     toList.map(writeResourceRequirement)) ~// 每个执行器的资源需求
    ("user" -> obj.user) ~ // 用户
    ("command" -> obj.command.toString) //提交指令
    
    def writeDriverInfo(obj: DriverInfo): JObject
    功能: 以json形式写出driver信息
    ("id" -> obj.id) ~	// driver编号
    ("starttime" -> obj.startTime.toString) ~ // 开始时间
    ("state" -> obj.state.toString) ~ // driver状态
    ("cores" -> obj.desc.cores) ~ // cpu数量
    ("memory" -> obj.desc.mem) ~ // 内存数量
    ("resources" -> writeResourcesInfo(obj.resources)) ~ // 资源需求
    ("submitdate" -> obj.submitDate.toString) ~ // 提交日期
    ("worker" -> obj.worker.map(_.id).getOrElse("None")) ~ // driver运行的worker名称
    ("mainclass" -> obj.desc.command.arguments(2)) // 主类
    
    def writeWorkerState(obj: WorkerStateResponse): JObject
    功能: 以json形式写出worker状态
    ("id" -> obj.workerId) ~ // worker id
    ("masterurl" -> obj.masterUrl) ~ // master地址
    ("masterwebuiurl" -> obj.masterWebUiUrl) ~ // master webui地址
    ("cores" -> obj.cores) ~ // cpu数量
    ("coresused" -> obj.coresUsed) ~ // 使用cpu数量
    ("memory" -> obj.memory) ~ // 内存量
    ("memoryused" -> obj.memoryUsed) ~ // 内存使用量
    ("resources" -> writeResourcesInfo(obj.resources)) ~ // 资源量
    ("resourcesused" -> writeResourcesInfo(obj.resourcesUsed)) ~ //使用资源信息
    ("executors" -> obj.executors.map(writeExecutorRunner)) ~ // 执行器信息
    ("finishedexecutors" -> obj.finishedExecutors.map(writeExecutorRunner)) // 完成的执行器
    
    def writeMasterState(obj: MasterStateResponse): JObject
    功能： 以json形式写出master状态
    1. 列出所有存活的worker
    val aliveWorkers = obj.workers.filter(_.isAlive())
    2. 形成json
    ("url" -> obj.uri) ~ // master地址
    ("workers" -> obj.workers.toList.map(writeWorkerInfo)) ~// worker列表
    ("aliveworkers" -> aliveWorkers.length) ~ // 存活worker
    ("cores" -> aliveWorkers.map(_.cores).sum) ~ // 核心数量
    ("coresused" -> aliveWorkers.map(_.coresUsed).sum) ~ // 使用的cpu数量
    ("memory" -> aliveWorkers.map(_.memory).sum) ~ // 内存量
    ("memoryused" -> aliveWorkers.map(_.memoryUsed).sum) ~ //内存
    // 资源表
    ("resources" -> aliveWorkers.map(_.resourcesInfo).toList.map(writeResourcesInfo)) ~
    ("resourcesused" -> aliveWorkers.map(_.resourcesInfoUsed).
     toList.map(writeResourcesInfo)) ~ // 已使用资源表
    ("activeapps" -> obj.activeApps.toList.map(writeApplicationInfo)) ~ //激活应用信息
    ("completedapps" -> obj.completedApps.toList.map(writeApplicationInfo)) ~//完成应用信息
    ("activedrivers" -> obj.activeDrivers.toList.map(writeDriverInfo)) ~ // 激活驱动器
    ("completeddrivers" -> obj.completedDrivers.toList.map(writeDriverInfo)) ~ //完成驱动器
    ("status" -> obj.status.toString) // 状态
}
```

#### LocalSparkCluster

```scala
private[spark]
class LocalSparkCluster(
    numWorkers: Int,
    coresPerWorker: Int,
    memoryPerWorker: Int,
    conf: SparkConf)
extends Logging {
    介绍: 本地spark集群,创建一个独立的spark集群,其中master和worker运行在同一个jvm中.worker运行的执行器仍然处于多个JVM中.
    构造器参数:
        numWorkers	worker的数量
        coresPerWorker	每个worker的cpu数量
        memoryPerWorker	每个worker的内存量
        conf	spark配置
    属性:
    #name @localHostname = Utils.localHostName()	本机名称
    #name @masterRpcEnvs = ArrayBuffer[RpcEnv]()	masterRPC环境
    #name @workerRpcEnvs = ArrayBuffer[RpcEnv]()	workerRPC环境
    #name @masterWebUIPort = -1	master web端口(用于测试)
    操作集:
    def start(): Array[String]
    功能: 启动集群
    1. 取消master上的REST服务器
    val _conf = conf.clone()
      .setIfMissing(config.MASTER_REST_SERVER_ENABLED, false)
      .set(config.SHUFFLE_SERVICE_ENABLED, false)
    2. 启动master
    val (rpcEnv, webUiPort, _) = Master.startRpcEnvAndEndpoint(
        localHostname, 0, 0, _conf)
    masterWebUIPort = webUiPort
    masterRpcEnvs += rpcEnv
    val masterUrl = "spark://" + Utils.localHostNameForURI() + ":" + rpcEnv.address.port
    val masters = Array(masterUrl)
    3. 启动workers
    for (workerNum <- 1 to numWorkers) {
      val workerEnv = Worker.startRpcEnvAndEndpoint(localHostname, 0, 0, coresPerWorker,
        memoryPerWorker, masters, null, Some(workerNum), _conf,
        conf.get(config.Worker.SPARK_WORKER_RESOURCE_FILE))
      workerRpcEnvs += workerEnv
    }
    val=master
    
    def stop(): Unit
    功能: 关闭集群(先关闭workers)
    workerRpcEnvs.foreach(_.shutdown())
    masterRpcEnvs.foreach(_.shutdown())
    workerRpcEnvs.foreach(_.awaitTermination())
    masterRpcEnvs.foreach(_.awaitTermination())
    masterRpcEnvs.clear()
    workerRpcEnvs.clear()
}
```

#### PythonRunner

```scala
介绍: 运行python应用程序的主类,在这里不做详细介绍
object PythonRunner {
    操作集:
    def formatPath(path: String, testWindows: Boolean = false): String
    功能: 格式化python文件路径,以至于可以添加到python路径中
    python不了解路径中的URI schema,再添加到python文件之前,首先需要根据URI抓取路径.由于只支持本地文件所以这样做是安全的.
    
    def formatPaths(paths: String, testWindows: Boolean = false): Array[String]
    功能: 格式化python文件路径,以至于可以添加到python路径中去
    
    def resolvePyFiles(pyFiles: Array[String]): Array[String]
    功能: 解决.py文件,该方法创建一个临时目录,将.py文件放入给定的路径中去
    
    def main(args: Array[String]): Unit
    功能: 启动函数
}
```

#### RPackageUtils

```scala
介绍: R语言的打包工具,不做详细介绍
private[deploy] object RPackageUtils extends Logging {
    属性:
    #name @hasRPackage = "Spark-HasRPackage"
    	MANIFEST.mf寻找的key,这个jar中含有R程序代码
    #name @baseInstallCmd = Seq("R", "CMD", "INSTALL", "-l")
    	基本shell指令,用于安装R包
    #name @RJarEntries = "R/pkg"	
    	R代码的具体位置
    #name @val RJarDoc R源文件在jar中的展示形式
    val= s"""In order for Spark to build R packages that are parts of 
    Spark Packages, there are a few
      |requirements. The R source code must be shipped in a jar, 
      with additional Java/Scala
      |classes. The jar must be in the following format:
      |  1- The Manifest (META-INF/MANIFEST.mf) must contain the key-value: 
      $hasRPackage: true
      |  2- The standard R package layout must be preserved under R/pkg/ 
      inside the jar. More
      |  information on the standard R package layout can be found in:
      |  http://cran.r-project.org/doc/contrib/Leisch-CreatingPackages.pdf
      |  An example layout is given below. After running `jar tf $$JAR_FILE | sort`:
      |
      |META-INF/MANIFEST.MF
      |R/
      |R/pkg/
      |R/pkg/DESCRIPTION
      |R/pkg/NAMESPACE
      |R/pkg/R/
      |R/pkg/R/myRcode.R
      |org/
      |org/apache/
      |...
    """.stripMargin.trim
    
    操作集:
    def print(
      msg: String,
      printStream: PrintStream,
      level: Level = Level.FINE,
      e: Throwable = null): Unit
    功能: 打印方法,用于debug
    
    def checkManifestForR(jar: JarFile): Boolean
    功能: 检查是否有R代码打包到jar包,用于测试
    
    def rPackageBuilder(
      dir: File,
      printStream: PrintStream,
      verbose: Boolean,
      libDir: String): Boolean
    功能: 运行R包标准安装代码,由资源构建jar包.
    def rPackageBuilder(
      dir: File,
      printStream: PrintStream,
      verbose: Boolean,
      libDir: String): Boolean
    
    def extractRFolder(jar: JarFile, printStream: PrintStream, verbose: Boolean): File
    功能: 抓取在/R 目录下的文件到一个临时文件中,用于构建
    
    def checkAndBuildRPackage(
      jars: String,
      printStream: PrintStream = null,
      verbose: Boolean = false): Unit 
    功能: 检查和构建R包
    
    def listFilesRecursively(dir: File, excludePatterns: Seq[String]): Set[File]
    功能: 迭代列举文件
    
    def zipRLibraries(dir: File, name: String): File
    功能: 归档所有R的库文件,用于分发到集群上
}
```

#### RRunner

```scala
介绍: R语言的运行类,这里不做详细介绍
object RRunner {
    def main(args: Array[String]): Unit
    功能: 启动函数
}
```

#### SparkApplication

```scala
private[spark] trait SparkApplication {
    介绍: spark应用入口点,必须提供无参构造器
    def start(args: Array[String], conf: SparkConf): Unit
    功能: 启动程序
}

private[deploy] class JavaMainApplication(klass: Class[_]) extends SparkApplication {
    介绍: SparkApplication的实现,通过系统参数将配置传入.在同一个JVM中运行多个配置会导致不确定的结果.
     val mainMethod = klass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the 
      given main class must be static")
    }
    val sysProps = conf.getAll.toMap
    sysProps.foreach { case (k, v) =>
      sys.props(k) = v
    }
    mainMethod.invoke(null, args)
}
```

#### SparkCuratorUtil

```scala
private[spark] object SparkCuratorUtil extends Logging {
    属性:
    #name @ZK_CONNECTION_TIMEOUT_MILLIS = 15000	zk连接时间上限
    #name @ZK_SESSION_TIMEOUT_MILLIS = 60000	zk会话超时时间
    #name @RETRY_WAIT_MILLIS = 5000	重试等待时间
    #name @MAX_RECONNECT_ATTEMPTS = 3	最大重连请求数
    操作集:
    def newClient(
      conf: SparkConf,
      zkUrlConf: String = ZOOKEEPER_URL.key): CuratorFramework
    功能: 创建zk客户端@CuratorFramework
    val ZK_URL = conf.get(zkUrlConf)
    val zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    zk.start()
    val= zk
    
    def mkdir(zk: CuratorFramework, path: String): Unit
    功能: 创建zk目录
    if (zk.checkExists().forPath(path) == null) {
      try {
        zk.create().creatingParentsIfNeeded().forPath(path)
      } catch {
        case nodeExist: KeeperException.NodeExistsException =>
        case e: Exception => throw e
      }
    }
    
    def deleteRecursive(zk: CuratorFramework, path: String): Unit
    功能: 迭代删除目录
    if (zk.checkExists().forPath(path) != null) {
      for (child <- zk.getChildren.forPath(path).asScala) {
        zk.delete().forPath(path + "/" + child)
      }
      zk.delete().forPath(path)
    }
}
```

#### SparkHadoopUtil

```scala
private[spark] class SparkHadoopUtil extends Logging {
    介绍: spark用于和hadoop进行交互的方法类
    属性:
    #name @sparkConf = new SparkConf(false).loadFromSystemProperties(true) spark配置
    #name @conf: Configuration = newConfiguration(sparkConf)	hadoop配置
    #name @HADOOP_CONF_PATTERN = "(\\$\\{hadoopconf-[^\\}\\$\\s]+\\})".r.unanchored
    	hadoop配置形式
    
    操作集:
    def runAsSparkUser(func: () => Unit): Unit
    功能: 使用hadoop用户组信息作为一个线程池变量,用于授权HDFS和YARN的调用
    注意: 如果函数需要在进程中重复的调用,请参考<https://issues.apache.org/jira/browse/HDFS-3545>,很有可能创建一个文件系统(通过调用FileSystem.closeAllForUGI ),主要是防止内存泄漏.
    createSparkUser().doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = func()
    })
    
    def createSparkUser(): UserGroupInformation
    功能: 创建spark user,返回hadoop的用户组信息
    val user = Utils.getCurrentUserName()
    logDebug("creating UGI for user: " + user)
    val ugi = UserGroupInformation.createRemoteUser(user)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    val= ugi
    
    def transferCredentials(source: UserGroupInformation, 
                            dest: UserGroupInformation): Unit
    功能: 转换证书(将源用户组的证书添加到目标组中)
    dest.addCredentials(source.getCredentials())
    
    def appendS3AndSparkHadoopHiveConfigurations(
      conf: SparkConf,
      hadoopConf: Configuration): Unit
    功能: 添加S3-specific,spark.hadoop.*,以及spark.buffer.size的配置到hadoop配置中
    SparkHadoopUtil.appendS3AndSparkHadoopHiveConfigurations(conf, hadoopConf)
    
    def appendSparkHadoopConfigs(conf: SparkConf, hadoopConf: Configuration): Unit
    功能: 添加spark.hadoop.*配置到hadoop配置中(来自spark配置)
    SparkHadoopUtil.appendSparkHadoopConfigs(conf, hadoopConf)
    
    def appendSparkHadoopConfigs(
      srcMap: Map[String, String],
      destMap: HashMap[String, String]): Unit
    功能: 从源配置@srcMap加入到目标位置在@destMap中
    for ((key, value) <- srcMap if key.startsWith("spark.hadoop.")) {
      destMap.put(key.substring("spark.hadoop.".length), value)
    }
    
    def appendSparkHiveConfigs(
      srcMap: Map[String, String],
      destMap: HashMap[String, String]): Unit
    功能: 添加spark.hive.foo=bar属性到hive.foo=bar中
    for ((key, value) <- srcMap if key.startsWith("spark.hive.")) {
      destMap.put(key.substring("spark.".length), value)
    }
    
    def newConfiguration(conf: SparkConf): Configuration 
    功能: 返回一个合适的配置,这个配置可以初始化hadoop子系统
    val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
    hadoopConf.addResource(SparkHadoopUtil.SPARK_HADOOP_CONF_FILE)
    val= hadoopConf
    
    def addCredentials(conf: JobConf): Unit
    功能: 添加证书
    val jobCreds = conf.getCredentials()
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())
    
    def addCurrentUserCredentials(creds: Credentials): Unit
    功能: 将指定证书@creds添加到当前用户的证书中
    UserGroupInformation.getCurrentUser.addCredentials(creds)
    
    def loginUserFromKeytab(principalName: String, keytabFilename: String): Unit
    功能: 从密钥表中登录用户
    输入参数:
    	principalName	原理名称
    	keytabFilename	密钥表文件名称
    if (!new File(keytabFilename).exists()) {
      throw new SparkException(s"Keytab file: ${keytabFilename} does not exist")
    } else {
      logInfo("Attempting to login to Kerberos " +
        s"using principal: ${principalName} and keytab: ${keytabFilename}")
      UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename)
    }
    
    def addDelegationTokens(tokens: Array[Byte], sparkConf: SparkConf): Unit
    功能: 添加授权密钥
    添加和覆盖当前用户证书(使用序列化的授权密钥),也会确认是否进行正确的hadoop配置.
    UserGroupInformation.setConfiguration(newConfiguration(sparkConf))
    val creds = deserialize(tokens)
    logInfo("Updating delegation tokens for current user.")
    logDebug(s"Adding/updating delegation tokens ${dumpTokens(creds)}")
    addCurrentUserCredentials(creds)
    
    def getFSBytesReadOnThreadCallback(): () => Long 
    功能: 获取可以寻找hadoop文件系统的函数
    1. 计算文件系统读取字节总量
    val f = () => FileSystem.getAllStatistics.asScala.map(
        _.getThreadStatistics.getBytesRead).sum
    val baseline = (Thread.currentThread().getId, f())
    // 这个函数在子线程和父线程中都可能调用,hadoop文件系统使用本地线程变量去追踪统计值,
    // 因此需要去追踪父子线程读取的字节量,计算任务读取字节量和
    val= new Function0[Long] {
      private val bytesReadMap = new mutable.HashMap[Long, Long]()
      override def apply(): Long = {
        bytesReadMap.synchronized {
          bytesReadMap.put(Thread.currentThread().getId, f())
          bytesReadMap.map { case (k, v) =>
            v - (if (k == baseline._1) baseline._2 else 0)
          }.sum
        }
      }
    }
    
    def getFSBytesWrittenOnThreadCallback(): () => Long 
    功能: 获取计算写入到hadoop文件系统的字节量
    val threadStats = FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics)
    val f = () => threadStats.map(_.getBytesWritten).sum
    val baselineBytesWritten = f()
    val= () => f() - baselineBytesWritten
    
    def listLeafStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus]
    功能: 获取给定路径@basePath的叶子节点的文件状态@FileStatus
    val= listLeafStatuses(fs, fs.getFileStatus(basePath))
    
    def recurse(status: FileStatus): Seq[FileStatus]
    功能: 迭代获取当前文件下叶子节点的文件状态
    val (directories, leaves) = fs.listStatus(status.getPath).partition(_.isDirectory)
    leaves ++ directories.flatMap(f => listLeafStatuses(fs, f))
    
    def listLeafStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus]
    功能: 获取给定路径@basePath的叶子节点的文件状态@FileStatus,如果给定目录指向的是一个文件,返回单个元素的集合
    val=  if (baseStatus.isDirectory) recurse(baseStatus) else Seq(baseStatus)
    
    def listLeafDirStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus]
    功能: 列举给出目录子目录的状态
    val= listLeafDirStatuses(fs, fs.getFileStatus(basePath))
    
    def listLeafDirStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus]
    功能: 列举叶子目录的状态信息
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, files) = fs.listStatus(status.getPath).partition(_.isDirectory)
      val leaves = if (directories.isEmpty) Seq(status) else Seq.empty[FileStatus]
      leaves ++ directories.flatMap(dir => listLeafDirStatuses(fs, dir))
    }
    assert(baseStatus.isDirectory)
    recurse(baseStatus)
    
    def isGlobPath(pattern: Path): Boolean
    功能: 确定是否为全局目录
    val= pattern.toString.exists("{}[]*?\\".toSet.contains)
    
    def globPath(pattern: Path): Seq[Path]
    功能： 获取全局路径
    val fs = pattern.getFileSystem(conf)
    globPath(fs, pattern)
    
    def globPath(fs: FileSystem, pattern: Path): Seq[Path]
    功能： 获取全局路径，指定文件系统@fs
    val= Option(fs.globStatus(pattern)).map { statuses =>
      statuses.map(_.getPath.makeQualified(fs.getUri, fs.getWorkingDirectory)).toSeq
    }.getOrElse(Seq.empty[Path])
    
    def globPathIfNecessary(pattern: Path): Seq[Path]
    功能: 获取可能的全局路径
    val= if (isGlobPath(pattern)) globPath(pattern) else Seq(pattern)
    
    def globPathIfNecessary(fs: FileSystem, pattern: Path): Seq[Path]
    功能: 获取可能的全局路径,指定文件系统
    val= if (isGlobPath(pattern)) globPath(fs, pattern) else Seq(pattern)
    
    def listFilesSorted(
      remoteFs: FileSystem,
      dir: Path,
      prefix: String,
      exclusionSuffix: String): Array[FileStatus]
    功能: 列举目录中所有带有指定前缀@prefix的文件,返回的文件状态列表是按照修改时间排序的
    try {
      // 获取指定文件系统下指定目录的文件状态列表
      val fileStatuses = remoteFs.listStatus(dir,
        new PathFilter {
          override def accept(path: Path): Boolean = {
            val name = path.getName
            name.startsWith(prefix) && !name.endsWith(exclusionSuffix)
          }
        })
      // 对文件状态列表进行排序
      Arrays.sort(fileStatuses, (o1: FileStatus, o2: FileStatus) =>
        Longs.compare(o1.getModificationTime, o2.getModificationTime))
      fileStatuses
    } catch {
      case NonFatal(e) =>
        logWarning("Error while attempting to list files 
        from application staging dir", e)
        Array.empty
    }
    
    def getSuffixForCredentialsPath(credentialsPath: Path): Int
    功能: 获取指定授权路径的前缀长度
    val fileName = credentialsPath.getName
    fileName.substring(
      fileName.lastIndexOf(SparkHadoopUtil.SPARK_YARN_CREDS_COUNTER_DELIM) + 1).toInt
    
    def substituteHadoopVariables(text: String, hadoopConf: Configuration): String 
    功能: 替代hadoop变量
    text match {
      case HADOOP_CONF_PATTERN(matched) =>
        logDebug(text + " matched " + HADOOP_CONF_PATTERN)
        val key = matched.substring(13, matched.length() - 1) 
        val eval = Option[String](hadoopConf.get(key))
          .map { value =>
            logDebug("Substituted " + matched + " with " + value)
            text.replace(matched, value)
          }
        if (eval.isEmpty) {
          text
        } else {
          substituteHadoopVariables(eval.get, hadoopConf)
        }
      case _ =>
        logDebug(text + " didn't match " + HADOOP_CONF_PATTERN)
        text
    }
    
    def dumpTokens(credentials: Credentials): Iterable[String]
    功能: 转储标记,转储证书标识符为string形式
    val= if (credentials != null) {
      credentials.getAllTokens.asScala.map(tokenToString)
    } else {
      Seq.empty
    }
    
    def tokenToString(token: Token[_ <: TokenIdentifier]): String 
    功能: 将标识符转换为string，用于标识
    如果是一个抽象的证书标识，会对数据进行解组并打印更多的细节，包括人可识别的形式
    val df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT,
                                            Locale.US)
    val buffer = new StringBuilder(128)
    buffer.append(token.toString)
    try {
      val ti = token.decodeIdentifier
      buffer.append("; ").append(ti)
      ti match {
        case dt: AbstractDelegationTokenIdentifier =>
          // include human times and the renewer, which the HDFS tokens toString omits
          buffer.append("; Renewer: ").append(dt.getRenewer)
          buffer.append("; Issued: ").append(df.format(new Date(dt.getIssueDate)))
          buffer.append("; Max Date: ").append(df.format(new Date(dt.getMaxDate)))
        case _ =>
      }
    } catch {
      case e: IOException =>
        logDebug(s"Failed to decode $token: $e", e)
    }
    val= buffer.toString
    
    def serialize(creds: Credentials): Array[Byte]
    功能: 序列化证书
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    creds.writeTokenStorageToStream(dataStream)
    val= byteStream.toByteArray
    
    def deserialize(tokenBytes: Array[Byte]): Credentials 
    功能: 反序列化证书
    val tokensBuf = new ByteArrayInputStream(tokenBytes)
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(tokensBuf))
    val= creds
    
    def isProxyUser(ugi: UserGroupInformation): Boolean
    功能: 确定是否为代理用户
    val= ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.PROXY
}
```

```scala
private[spark] object SparkHadoopUtil {
    属性:
    #name @instance=new SparkHadoopUtil	lazy	实例
    #name @SPARK_YARN_CREDS_TEMP_EXTENSION = ".tmp"	spark yarn临时证书扩展
    #name @SPARK_YARN_CREDS_COUNTER_DELIM = "-"	spark yarn证书计数分隔符
    #name @UPDATE_INPUT_METRICS_INTERVAL_RECORDS = 1000	读取HadoopRDD更新输入度量的间隔记录数
    #name @SPARK_HADOOP_CONF_FILE = "__spark_hadoop_conf__.xml"	spark hadoop配置文件
    	用于配置网关信息,覆盖在hadoop集群配置之上
    操作集:
    def get: SparkHadoopUtil = instance
    功能: 获取实例
    
    def newConfiguration(conf: SparkConf): Configuration
    功能: 基于指定spark配置@conf 创建一个hadoop配置
    val hadoopConf = new Configuration()
    appendS3AndSparkHadoopHiveConfigurations(conf, hadoopConf)
    val= hadoopConf
    
    def appendS3AndSparkHadoopHiveConfigurations(
      conf: SparkConf,
      hadoopConf: Configuration): Unit
    功能: 添加S3和spark配置到hadoop配置中,空值检查超过conf,原因是由于代码的旧实现的原因,为了向后兼容.
    if (conf != null) {
      val keyId = System.getenv("AWS_ACCESS_KEY_ID")
      val accessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
      if (keyId != null && accessKey != null) {
        hadoopConf.set("fs.s3.awsAccessKeyId", keyId)
        hadoopConf.set("fs.s3n.awsAccessKeyId", keyId)
        hadoopConf.set("fs.s3a.access.key", keyId)
        hadoopConf.set("fs.s3.awsSecretAccessKey", accessKey)
        hadoopConf.set("fs.s3n.awsSecretAccessKey", accessKey)
        hadoopConf.set("fs.s3a.secret.key", accessKey)
        val sessionToken = System.getenv("AWS_SESSION_TOKEN")
        if (sessionToken != null) {
          hadoopConf.set("fs.s3a.session.token", sessionToken)
        }
      }
      appendSparkHadoopConfigs(conf, hadoopConf)
      appendSparkHiveConfigs(conf, hadoopConf)
      val bufferSize = conf.get(BUFFER_SIZE).toString
      hadoopConf.set("io.file.buffer.size", bufferSize)
    }
    
    def appendSparkHadoopConfigs(conf: SparkConf, hadoopConf: Configuration): Uni
    功能: 添加spark对hadoop的配置
    for ((key, value) <- conf.getAll if key.startsWith("spark.hadoop.")) {
      hadoopConf.set(key.substring("spark.hadoop.".length), value)
    }
    
    def appendSparkHiveConfigs(conf: SparkConf, hadoopConf: Configuration): Unit 
    功能: 添加spark对hive的配置
    for ((key, value) <- conf.getAll if key.startsWith("spark.hive.")) {
      hadoopConf.set(key.substring("spark.".length), value)
    }
    
    def createFile(fs: FileSystem, path: Path, allowEC: Boolean): FSDataOutputStream
    功能: 创建文件,关闭EC可以有助于HDFS EC支持hflush(),hsync(),或者append()方法,参考
    <https://hadoop.apache.org/docs/r3.0.0/hadoop-project-dist/hadoop-
    hdfs/HDFSErasureCoding.html#Limitations>
    if (allowEC) {
      fs.create(path)
    } else {
      try {
        val builderMethod = fs.getClass().getMethod("createFile", classOf[Path])
        if (!fs.mkdirs(path.getParent())) {
          throw new IOException(s"Failed to create parents of $path")
        }
        val qualifiedPath = fs.makeQualified(path)
        val builder = builderMethod.invoke(fs, qualifiedPath)
        val builderCls = builder.getClass()
        val replicateMethod = builderCls.getMethod("replicate")
        val buildMethod = builderCls.getMethod("build")
        val b2 = replicateMethod.invoke(builder)
        buildMethod.invoke(b2).asInstanceOf[FSDataOutputStream]
      } catch {
        case  _: NoSuchMethodException =>
          fs.create(path)
      }
    }
}
```

#### SparkSubmit

#### SparkSubmitArguments

#### StandaloneResourceUtils

#### 基础拓展

1.  [抽象语法树 AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
2.  [密钥分配中心 KDC](https://en.wikipedia.org/wiki/Key_distribution_center)
3.  [TGT](https://en.wikipedia.org/wiki/Ticket_Granting_Ticket)
4.   [REST](https://zh.wikipedia.org/wiki/表现层状态转换)

