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

#### history

#### master

#### rest

#### security

#### worker

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

#### RPackageUtils

#### RRunner

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

#### SparkSubmit

#### SparkSubmitArguments

#### StandaloneResourceUtils

#### 基础拓展

1.  [抽象语法树 AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree)

