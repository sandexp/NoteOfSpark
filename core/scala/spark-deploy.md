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

#### SparkSubmit

#### SparkSubmitArguments

#### StandaloneResourceUtils

#### 基础拓展

1.  [抽象语法树 AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
2.  [密钥分配中心 KDC](https://en.wikipedia.org/wiki/Key_distribution_center)
3.  [TGT](https://en.wikipedia.org/wiki/Ticket_Granting_Ticket)
4.   [REST](https://zh.wikipedia.org/wiki/表现层状态转换)
