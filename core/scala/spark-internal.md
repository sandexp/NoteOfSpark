## **spark-internal**

---

1.  [config](# config)
2.  [io](# io)
3.  [plugin](# plugin)
4.  [Logging.scala](# Logging)

---

#### config

1.  [package.scala](# package)
2.  [ConfigBuilder.scala](# ConfigBuilder)
3.  [ConfigEntry.scala](# ConfigEntry)
4.  [ConfigProvider.scala](# ConfigProvider)
5.  [ConfigReader.scala](# ConfigReader)
6.  [Deploy.scala](# Deploy)
7.  [History.scala](# History)
8.  [Kryo.scala](# Kryo)
9.  [Network.scala](# Network)
10.  [Python.scala](# Python)
11.  [R.scala](# R)
12.  [Status.scala](# Status)
13.  [Streaming.scala](# Streaming)
14.  [Tests.scala](# Tests)
15.  [UI.scala](# UI)
16.  [Worker.scala](# Worker)

---

#### package

| 参数名称                        | 参数类型  | 参数值                                                       | 文档注释                                                     |
| ------------------------------- | --------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| SPARK_DRIVER_PREFIX             | String    | spark.driver                                                 | diver前缀                                                    |
| SPARK_EXECUTOR_PREFIX           | String    | spark.executor                                               | excutor前缀                                                  |
| SPARK_TASK_PREFIX               | String    | spark.task                                                   | task前缀                                                     |
| LISTENER_BUS_EVENT_QUEUE_PREFIX | String    | spark.scheduler.listenerbus.eventqueue                       | 监听事件队列前缀                                             |
| SPARK_RESOURCES_COORDINATE      | Boolean   | spark.resources.coordinate.enable=true                       | 是否自动的协调worker和driver之间的资源.如果配置为false,用于需要配置不同的资源给运行在一台机器上的workers和drivers. |
| SPARK_RESOURCES_DIR             | String    | spark.resources.dir = stringConf.createOptional              | 在脱机模式下用于协调资源的目录,默认情况下是SPARK_HOME,需要确保在一台机器上workers和divers使用的是一个目录.在workers和drivers存在时注意不要去删除掉这个目录,很有可能引发资源冲突 |
| DRIVER_RESOURCES_FILE           | String    | spark.driver.resourcesFile=stringConf.createOptional         | 这是一个包含分配给driver的资源的文件.文件需要格式化为json格式的@ResourceAllocation数组,仅仅使用在脱机模式下. |
| DRIVER_CLASS_PATH               | String    | ConfigBuilder(SparkLauncher.DRIVER_EXTRA_CLASSPATH).stringConf.createOptional | Driver类路径(DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath") |
| DRIVER_JAVA_OPTIONS             | String    | ConfigBuilder(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)  .withPrepended(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS)  .stringConf  .createOptional |                                                              |
| DRIVER_LIBRARY_PATH             | String    | ConfigBuilder(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH).stringConf.createOptional | driver 库路径                                                |
| DRIVER_USER_CLASS_PATH_FIRST    | String    | ConfigBuilder("spark.driver.userClassPathFirst").booleanConf.createWithDefault(false) | driver用户首选类路径                                         |
| DRIVER_CORES                    | int       | spark.driver.cores=1                                         | driver进程的核心数量,仅仅使用在集群模式下                    |
| DRIVER_MEMORY                   | bytesConf | ConfigBuilder(SparkLauncher.DRIVER_MEMORY).bytesConf(ByteUnit.MiB)..createWithDefaultString("1g") | 使用在driver进程中的的内存数量,在不指定的模式下单位为MB.     |
| DRIVER_MEMORY_OVERHEAD          | bytesConf | ConfigBuilder("spark.driver.memoryOverhead").bytesConf(ByteUnit.MiB).createOptional | 集群模式下每个驱动器分配的非堆模式内存.除非指定,单位为MB     |
| DRIVER_LOG_DFS_DIR              | String    | ConfigBuilder("spark.driver.log.dfsDir").stringConf.createOptional | 驱动器DFS日志目录                                            |
| DRIVER_LOG_LAYOUT               | String    | ConfigBuilder("spark.driver.log.layout")  .stringConf.createOptional | 驱动器日志布局                                               |
| DRIVER_LOG_PERSISTTODFS         | Boolean   | ConfigBuilder("spark.driver.log.persistToDfs.enabled")  .booleanConf  .createWithDefault(false) | 驱动器日志是否持久化到DFS上                                  |
| DRIVER_LOG_ALLOW_EC             | Boolean   | ConfigBuilder("spark.driver.log.allowErasureCoding")  .booleanConf  .createWithDefault(false) | driver侧是否支持擦除式编程                                   |

#### ConfigBuilder

#### ConfigEntry

#### ConfigProvider

```markdown
private[spark] trait ConfigProvider {
	介绍: 提供配置的查询
	操作集:
	def get(key: String): Option[String]
	功能: 获取配置key对应的value值
}
```

```markdown
private[spark] class EnvProvider{
	关系: father --> ConfigProvider
	介绍: 提供环境的查询
	操作集:
	def get(key: String): Option[String] = sys.env.get(key)
	功能: 获取系统环境参数
}
```

```markdown
private[spark] class SystemProvider{
	关系: father --> ConfigProvider
	介绍: 获取系统属性
	def get(key: String): Option[String] = sys.props.get(key)
	功能: 获取系统属性值
}
```

```markdown
private[spark] class MapProvider(conf: JMap[String, String]) {
	关系: father --> ConfigProvider
	
	def get(key: String): Option[String] = Option(conf.get(key))
	功能: 获取key的value值
}
```

```markdown
private[spark] class SparkConfigProvider(conf: JMap[String, String]) {
	关系: father --> ConfigProvider
	def get(key: String): Option[String]
	功能: 获取spark配置属性值
	val=key.startsWith("spark.") ? 
	Option(conf.get(key)).orElse(SparkConf.getDeprecatedConfig(key, conf)): None
}
```

#### ConfigReader

```markdown
介绍:
	这是一个辅助类,用于读取配置键值对和进行变量的替换.
	如果配置值包含变量,且形如"${prefix:variableName}",参考会根据前缀代替属性值.默认情况下,处理如下属性:
	1. 没有前缀,使用默认配置提供器
	2. 系统: 查找系统属性的值
	3. 环境变量: 查找环境变量的值
	不同前缀受制于#class @ConfigProvider,用于读取数据源中的配置值,系统和环境变量都能够被重写.
	如果参考不能被解决问题,原始字符串会被保留.
```

```markdown
private[spark] class ConfigReader(conf: ConfigProvider) {
	属性:
	#name @bindings=new HashMap[String, ConfigProvider]() 绑定端口
	操作集:
	def bind(prefix: String, provider: ConfigProvider): ConfigReader
	功能: 绑定端口到一个@ConfigProvider，方法是线程不安全的
		bindings(prefix) = provider
	
	def bind(prefix: String, values: JMap[String, String]): ConfigReader 
	功能: 绑定前缀到map中
		bind(prefix, new MapProvider(values))
	
	def bindEnv(provider: ConfigProvider): ConfigReader = bind("env", provider)
	功能: 绑定环境变量参数
	
	def bindSystem(provider: ConfigProvider): ConfigReader = bind("system", provider)
	功能: 绑定系统变量
	
	def get(key: String): Option[String] = conf.get(key).map(substitute)
	功能: 获取配置
	读取默认提供器的key对应的value值,并应用到各种各样的替代物
	
	def substitute(input: String): String = substitute(input, Set())
	功能: 对指定的输入串@input 进行替换@substitute(String,Set<String>)
	0. 判空 if(input==null) val=input else jump 1; 
	1. 替换满足正则表达式的子串
	ConfigReader.REF_RE.replaceAllIn(input, { m =>
        val prefix = m.group(1)
        val name = m.group(2)
        val ref = if (prefix == null) name else s"$prefix:$name"
        require(!usedRefs.contains(ref), s"Circular reference in $input: $ref")

        val replacement = bindings.get(prefix)
          .flatMap(getOrDefault(_, name))
          .map { v => substitute(v, usedRefs + ref) }
          .getOrElse(m.matched)
        Regex.quoteReplacement(replacement)
     })
	
	def getOrDefault(conf: ConfigProvider, key: String): Option[String]
	功能: 获取配置@ConfigProvider key对应的属性值,如果找不到值且@ConfigEntry有默认值,则返回这个默认值
	conf.get(key).orElse {
      ConfigEntry.findEntry(key) match {
        case e: @ConfigEntryWithDefault[_] => Option(e.defaultValueString)
        // 采取默认串
        case e: @ConfigEntryWithDefaultString[_] => Option(e.defaultValueString)
        // 采取默认函数
        case e: @ConfigEntryWithDefaultFunction[_] => Option(e.defaultValueString)
        // 采取回退参数	
        case e: @FallbackConfigEntry[_] => getOrDefault(conf, e.fallback.key)
        // 其他类型 -->None
        case _ => None
      }
    }
    
}
```

#### Deploy

```markdown
介绍:	spark 部署参数
```

| 参数                  | 属性值                                                       | 介绍                                                         |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| RECOVERY_MODE         | ConfigBuilder("spark.deploy.recoveryMode")  .stringConf  .createWithDefault("NONE") | 恢复模式                                                     |
| RECOVERY_MODE_FACTORY | ConfigBuilder("spark.deploy.recoveryMode.factory")  .stringConf  .createWithDefault("") | 恢复模式工厂                                                 |
| RECOVERY_DIRECTORY    | ConfigBuilder("spark.deploy.recoveryDirectory")  .stringConf  .createWithDefault("") | 恢复目录                                                     |
| ZOOKEEPER_URL         | ConfigBuilder("spark.deploy.zookeeper.url").stringConf.createOptional | Zookeeper地址: 当恢复模式设置为zookeeper时,这个属性用于设置zookeeper的连接地址 |
| ZOOKEEPER_DIRECTORY   | ConfigBuilder("spark.deploy.zookeeper.dir").stringConf.createOptional | Zookeeper目录                                                |
| RETAINED_APPLICATIONS | ConfigBuilder("spark.deploy.retainedApplications")  .intConf  .createWithDefault(200) | 保留应用程序数目,默认200                                     |
| RETAINED_DRIVERS      | RETAINED_DRIVERS = ConfigBuilder("spark.deploy.retainedDrivers")  .intConf  .createWithDefault(200) | 保留Driver数量,默认200                                       |
| REAPER_ITERATIONS     | ConfigBuilder("spark.dead.worker.persistence")  .intConf  .createWithDefault(15) | 获取迭代器数量,默认15                                        |
| MAX_EXECUTOR_RETRIES  | ConfigBuilder("spark.deploy.maxExecutorRetries")  .intConf  .createWithDefault(10) | 执行器最大尝试次数,默认10                                    |
| DEFAULT_CORES         | ConfigBuilder("spark.deploy.defaultCores")  .intConf  .createWithDefault(Int.MaxValue) | 默认核心数量                                                 |
| SPREAD_OUT_APPS       | ConfigBuilder("spark.deploy.spreadOut")  .booleanConf  .createWithDefault(true) | 是否溢出属性                                                 |

#### History

| 属性 | 参数值 | 介绍     |
| ---- | ------ | ---------- |
| DEFAULT_LOG_DIR | "file:/tmp/spark-events" | 默认日志目录 |
| SAFEMODE_CHECK_INTERVAL_S | ConfigBuilder("spark.history.fs.safemodeCheck.interval")  .timeConf(TimeUnit.SECONDS)  .createWithDefaultString("5s") | 安全模式检查时间间隔/s,默认5s |
| UPDATE_INTERVAL_S | ConfigBuilder("spark.history.fs.update.interval")  .timeConf(TimeUnit.SECONDS)  .createWithDefaultString("10s") | 更新时间间隔10s |
| CLEANER_ENABLED | ConfigBuilder("spark.history.fs.cleaner.enabled")  .booleanConf  .createWithDefault(false) | 清除标记 |
| CLEANER_INTERVAL_S | ConfigBuilder("spark.history.fs.cleaner.interval")  .timeConf(TimeUnit.SECONDS)  .createWithDefaultString("1d") | 清除事件间隔,默认1d |
| MAX_LOG_AGE_S | ConfigBuilder("spark.history.fs.cleaner.maxAge")  .timeConf(TimeUnit.SECONDS)  .createWithDefaultString("7d") | 最大日志生存时间,默认7d |
| MAX_LOG_NUM | ConfigBuilder("spark.history.fs.cleaner.maxNum").intConf  .createWithDefault(Int.MaxValue) | 最大日志数量,默认为Int最大值 |
| LOCAL_STORE_DIR | ConfigBuilder("spark.history.store.path")  stringConf  .createOptional | 本地存储目录,应用历史信息缓存的地方,默认没有设置,意味着所有的历史信息会存储在内存中 |
| MAX_LOCAL_DISK_USAGE | ConfigBuilder("spark.history.store.maxDiskUsage")  .bytesConf(ByteUnit.BYTE)  .createWithDefaultString("10g") | 本地磁盘最大使用量 |
| HISTORY_SERVER_UI_PORT | ConfigBuilder("spark.history.ui.port") .intConf  .createWithDefault(18080) | spark history服务器的web端口,默认18080 |
| FAST_IN_PROGRESS_PARSING | ConfigBuilder("spark.history.fs.inProgressOptimization.enabled")  .booleanConf  .createWithDefault(true) | 快速转换成in-process的日志.但是会遗留下重命名失败的文件对应的应用 |
| END_EVENT_REPARSE_CHUNK_SIZE | ConfigBuilder("spark.history.fs.endEventReparseChunkSize")  .bytesConf(ByteUnit.BYTE)  .createWithDefaultString("1m") | 在日志文件末尾有多少字节需要转化为结束事件.主要用于加速应用生成,通过跳过不必要的日志文件块.可以将参数设置为0,去关闭这个设置 |
| DRIVER_LOG_CLEANER_ENABLED | ConfigBuilder("spark.history.fs.driverlog.cleaner.enabled")  .fallbackConf(CLEANER_ENABLED) | 驱动器日志生成标志 |
| DRIVER_LOG_CLEANER_INTERVAL | ConfigBuilder("spark.history.fs.driverlog.cleaner.interval")  .fallbackConf(CLEANER_INTERVAL_S) | 驱动器日志清除时间间隔 |
| MAX_DRIVER_LOG_AGE_S | ConfigBuilder("spark.history.fs.driverlog.cleaner.maxAge")  .fallbackConf(MAX_LOG_AGE_S) | driver日志最大生成时间 |
| HISTORY_SERVER_UI_ACLS_ENABLE | ConfigBuilder("spark.history.ui.acls.enable")  .booleanConf  .createWithDefault(false) | history服务器web页面可否请求标志,默认false |
| HISTORY_SERVER_UI_ADMIN_ACLS | ConfigBuilder("spark.history.ui.admin.acls")  .stringConf  .toSequence  .createWithDefault(Nil) | history服务器Web管理请求 |
| HISTORY_SERVER_UI_ADMIN_ACLS | ConfigBuilder("spark.history.ui.admin.acls.groups")  .stringConf  .toSequence  .createWithDefault(Nil) | history服务器Web管理请求组 |
| NUM_REPLAY_THREADS | ConfigBuilder("spark.history.fs.numReplayThreads")  .intConf  .createWithDefaultFunction(() => Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt) | 重新运行线程数量 |
| RETAINED_APPLICATIONS | ConfigBuilder("spark.history.retainedApplications")  .intConf  .createWithDefault(50) | 剩余应用数量,默认50 |
| PROVIDER | ConfigBuilder("spark.history.provider")  .stringConf  .createOptional |  |
| KERBEROS_ENABLED | ConfigBuilder("spark.history.kerberos.enabled")  .booleanConf  .createWithDefault(false) | kerberos安全系统使能标记.默认false |
| KERBEROS_PRINCIPAL | ConfigBuilder("spark.history.kerberos.principal")  .stringConf  .createOptional | Kerberos系统准则 |
| KERBEROS_KEYTAB | ConfigBuilder("spark.history.kerberos.keytab")  .stringConf  .createOptional | Kerberos密钥表 |
| CUSTOM_EXECUTOR_LOG_URL | ConfigBuilder("spark.history.custom.executor.log.url")  .stringConf  .createOptional | 用于执行器日志地址: 指定用户spark执行器日志,支持外部日志服务,而不是使用集群管理器在history服务器中的地址.如果样式在集群中会发生变化的化,spark支持多个路径变量.请检查你的集群模式下样式是否能够被支持.该项配置对于一个已经运行的程序配置无效. |
| APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP | ConfigBuilder("spark.history.custom.executor.log.url.applyIncompleteApplication")  .booleanConf  .createWithDefault(true) | 使用去应用用户执行器上的日志地址,进对于未运行的应用程序设置才有效. |

#### Kryo

kryo注册与序列化参数的设置

| 属性                            | 参数值                                                       | 介绍                                        |
| ------------------------------- | ------------------------------------------------------------ | ------------------------------------------- |
| KRYO_REGISTRATION_REQUIRED      | ConfigBuilder("spark.kryo.registrationRequired")  .booleanConf  .createWithDefault(false) | 是否需要登记Kryo,默认false                  |
| KRYO_USER_REGISTRATORS          | ConfigBuilder("spark.kryo.registrator")  .stringConf  .createOptional | Kryo用户注册                                |
| KRYO_CLASSES_TO_REGISTER        | ConfigBuilder("spark.kryo.classesToRegister")  .stringConf  .toSequence  .createWithDefault(Nil) | 需要Kryo注册列表                            |
| KRYO_USE_UNSAFE                 | ConfigBuilder("spark.kryo.pool")  .booleanConf  .createWithDefault(true) | kryo是否采用不安全的方式,默认不采用,为false |
| KRYO_USE_POOL                   | ConfigBuilder("spark.kryo.pool")  .booleanConf  .createWithDefault(true) | 是否使用kryo池技术,默认使用                 |
| KRYO_REFERENCE_TRACKING         | ConfigBuilder("spark.kryo.referenceTracking")  .booleanConf  .createWithDefault(true) | 是否采用kryo追踪.默认采用,为true            |
| KRYO_SERIALIZER_BUFFER_SIZE     | ConfigBuilder("spark.kryoserializer.buffer")  .bytesConf(ByteUnit.KiB)  .createWithDefaultString("64k") | Kryo序列化缓冲大小.默认64K                  |
| KRYO_SERIALIZER_MAX_BUFFER_SIZE | ConfigBuilder("spark.kryoserializer.buffer.max")  .bytesConf(ByteUnit.MiB)  .createWithDefaultString("64m") | Kryo最大序列化缓冲大小,默认64M              |

#### Network

网络传输相关参数设置

| 属性                                      | 参数值                                                       | 介绍                                |
| ----------------------------------------- | ------------------------------------------------------------ | ----------------------------------- |
| NETWORK_CRYPTO_SASL_FALLBACK              | ConfigBuilder("spark.network.crypto.saslFallback") <br /> .booleanConf <br /> .createWithDefault(true) | 是否留有网络加密传输的备用          |
| NETWORK_CRYPTO_ENABLED                    | ConfigBuilder("spark.network.crypto.enabled") <br /> .booleanConf  .createWithDefault(false) | 是否允许网加密,默认false            |
| NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION | ConfigBuilder("spark.network.remoteReadNioBufferConversion")  .booleanConf  .createWithDefault(false) | 是否允许远端读取NIO缓冲区,默认false |
| NETWORK_TIMEOUT                           | ConfigBuilder("spark.network.timeout") <br /> .timeConf(TimeUnit.SECONDS)<br />  .createWithDefaultString("120s") | 网络传输延时上限                    |
| NETWORK_TIMEOUT_INTERVAL                  | ConfigBuilder("spark.network.timeoutInterval") <br /> .timeConf(TimeUnit.MILLISECONDS)  <br />.createWithDefaultString(<br />STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL.defaultValueString) | 网络超时间隔                        |
| RPC_ASK_TIMEOUT                           | ConfigBuilder("spark.rpc.askTimeout") <br /> .stringConf .createOptional | RPC访问时间阈值                     |
| RPC_CONNECT_THREADS                       | ConfigBuilder("spark.rpc.connect.threads") <br /> .intConf  .createWithDefault(64) | RPC连接线程数量，默认64             |
| RPC_IO_NUM_CONNECTIONS_PER_PEER           | ConfigBuilder("spark.rpc.io.numConnectionsPerPeer") <br /> .intConf  .createWithDefault(1) | RPC单节点io连接数量，默认1个        |
| RPC_IO_THREADS                            | ConfigBuilder("spark.rpc.io.threads") <br /> .intConf  .createOptional | RPC  io线程数量                     |
| RPC_LOOKUP_TIMEOUT                        | ConfigBuilder("spark.rpc.lookupTimeout")<br />  .stringConf  .createOptional | RPC查找时间上限                     |
| RPC_MESSAGE_MAX_SIZE                      | ConfigBuilder("spark.rpc.message.maxSize") <br /> .intConf  .createWithDefault(128) | RPC携带最大信息量                   |
| RPC_NETTY_DISPATCHER_NUM_THREADS          | ConfigBuilder("spark.rpc.netty.dispatcher.numThreads")<br />  .intConf  .createOptional | RPC netty分发线程数量               |
| RPC_NUM_RETRIES                           | ConfigBuilder("spark.rpc.numRetries")  <br />.intConf  .createWithDefault(3) | RPC重试次数，默认3次                |
| RPC_RETRY_WAIT                            | ConfigBuilder("spark.rpc.retry.wait")  <br />.timeConf(TimeUnit.MILLISECONDS) <br /> .createWithDefaultString("3s") | RPC重试等待时间，默认3s             |

#### Python

python使用的一些参数设定

| 属性 | 参数值 | 介绍 |
| ------------------- | ------ | ---- |
| PYTHON_WORKER_REUSE | ConfigBuilder("spark.python.worker.reuse")  .booleanConf  .createWithDefault(true) | Python中worker是否可以重新使用，默认true |
| PYTHON_TASK_KILL_TIMEOUT | ConfigBuilder("spark.python.task.killTimeout")  .timeConf(TimeUnit.MILLISECONDS)  .createWithDefaultString("2s") | python kill任务的时间阈值，默认2s |
| PYTHON_USE_DAEMON | ConfigBuilder("spark.python.use.daemon")  .booleanConf  .createWithDefault(true) | 是否使用守护线程，默认使用 |
| PYTHON_DAEMON_MODULE | ConfigBuilder("spark.python.daemon.module")  .stringConf  .createOptional | python 守护线程模块 |
| PYTHON_WORKER_MODULE | ConfigBuilder("spark.python.worker.module")  .stringConf  .createOptional | python worker模块 |
| PYSPARK_EXECUTOR_MEMORY | ConfigBuilder("spark.executor.pyspark.memory")  .bytesConf(ByteUnit.MiB)  .createOptional | python执行器大小，以MB计数 |



#### R

R参数的设置

| 属性                         | 参数值                                                       | 介绍                           |
| ---------------------------- | ------------------------------------------------------------ | ------------------------------ |
| R_BACKEND_CONNECTION_TIMEOUT | ConfigBuilder("spark.r.backendConnectionTimeout")  .intConf  .createWithDefault(6000) | 后台连接时间上限，默认为6000ms |
| R_NUM_BACKEND_THREADS        | ConfigBuilder("spark.r.numRBackendThreads")  .intConf  .createWithDefault(2) | 后端线程数量,默认为2           |
| R_HEARTBEAT_INTERVAL         | ConfigBuilder("spark.r.heartBeatInterval")  .intConf  .createWithDefault(100) | 心跳间隔时间,默认100ms         |
| SPARKR_COMMAND               | ConfigBuilder("spark.sparkr.r.command")  .stringConf  .createWithDefault("Rscript") | spark指令(R语言)               |
| R_COMMAND                    | ConfigBuilder("spark.r.command")  .stringConf  .createOptional | R指令                          |



#### Status

状态参数

| 属性                                      | 参数值                                                       | 介绍                                    |
| ----------------------------------------- | ------------------------------------------------------------ | --------------------------------------- |
| ASYNC_TRACKING_ENABLED                    | ConfigBuilder("spark.appStateStore.asyncTracking.enable")  .booleanConf  .createWithDefault(true) | 是否允许异步追踪，默认状态下为true      |
| LIVE_ENTITY_UPDATE_PERIOD                 | ConfigBuilder("spark.ui.liveUpdate.period")  .timeConf(TimeUnit.NANOSECONDS)  .createWithDefaultString("100ms") | 存活实例更新周期，默认100ms             |
| LIVE_ENTITY_UPDATE_MIN<br />_FLUSH_PERIOD | ConfigBuilder("spark.ui.liveUpdate.minFlushPeriod")   .timeConf(TimeUnit.NANOSECONDS)  .createWithDefaultString("1s") | 存活实例更新的最小刷新周期，默认1s      |
| MAX_RETAINED_JOBS                         | ConfigBuilder("spark.ui.retainedJobs")  .intConf  .createWithDefault(1000) | 最大存留job数量，默认1000               |
| MAX_RETAINED_STAGES                       | ConfigBuilder("spark.ui.retainedStages")  .intConf  .createWithDefault(1000) | 最大存在stage数量，默认1000             |
| MAX_RETAINED_TASKS<br />_PER_STAGE        | ConfigBuilder("spark.ui.retainedTasks")  .intConf  .createWithDefault(100000) | 每个stage最大保留任务，默认10000        |
| MAX_RETAINED_DEAD_EXECUTORS               | ConfigBuilder("spark.ui.retainedDeadExecutors")  .intConf  .createWithDefault(100) | 处于dead状态的最大保留执行器，默认值100 |
| MAX_RETAINED_ROOT_NODES                   | ConfigBuilder("spark.ui.dagGraph.retainedRootRDDs")  .intConf  .createWithDefault(Int.MaxValue) | 最大保留根节点                          |
| METRICS_APP_STATUS_SOURCE_<br />ENABLED   | ConfigBuilder("spark.metrics.appStatusSource.enabled")  .booleanConf  .createWithDefault(false) | 使能度量状态位                          |

#### Streaming

streaming 参数配置

| 属性                                              | 参数值                                                       | 介绍                                      |
| ------------------------------------------------- | ------------------------------------------------------------ | ----------------------------------------- |
| STREAMING_DYN_<br />ALLOCATION_ENABLED            | ConfigBuilder("spark.streaming.dynamicAllocation.enabled")  .booleanConf.createWithDefault(false) | 运行streaming动态分配标志位，默认false    |
| STREAMING_DYN_<br />ALLOCATION_TESTING            | ConfigBuilder("spark.streaming.dynamicAllocation.testing")  .booleanConf  .createWithDefault(false) | streaming 动态分配是否需要测试，默认false |
| STREAMING_DYN_<br />ALLOCATION_MIN_EXECUTORS      | ConfigBuilder("spark.streaming.dynamicAllocation.minExecutors")  .intConf  .checkValue(_ > 0, "The min executor number of streaming dynamic " +    "allocation must be positive.")  .createOptional | 动态分配最小执行器数量                    |
| STREAMING_DYN_<br />ALLOCATION_MAX_EXECUTORS      | ConfigBuilder("spark.streaming.dynamicAllocation.maxExecutors")  .intConf  .checkValue(_ > 0, "The max executor number of streaming dynamic " +    "allocation must be positive.")  .createWithDefault(Int.MaxValue) | 动态分配最大执行器数量                    |
| STREAMING_DYN_<br />ALLOCATION_SCALING_INTERVAL   | ConfigBuilder("spark.streaming.dynamicAllocation.scalingInterval")  .timeConf(TimeUnit.SECONDS)  .checkValue(_ > 0, "The scaling interval of streaming dynamic " +    "allocation must be positive.")  .createWithDefault(60) | 动态分配扫描时间间隔，默认60s             |
| STREAMING_DYN_<br />ALLOCATION_SCALING_UP_RATIO   | ConfigBuilder("spark.streaming.dynamicAllocation.scalingUpRatio")  .doubleConf  .checkValue(_ > 0, "The scaling up ratio of streaming dynamic " +    "allocation must be positive.")  .createWithDefault(0.9) | 按比例放大比例。默认0.9                   |
| STREAMING_DYN_<br />ALLOCATION_SCALING_DOWN_RATIO | ConfigBuilder("spark.streaming.dynamicAllocation.scalingDownRatio")  .doubleConf  .checkValue(_ > 0, "The scaling down ratio of streaming dynamic " +    "allocation must be positive.")  .createWithDefault(0.3) | 按比例缩小比例。默认0.3                   |
#### Tests

测试相关属性

| 属性                                     | 参数值                                                       | 介绍                       |
| ---------------------------------------- | ------------------------------------------------------------ | -------------------------- |
| TEST_USE_<br />COMPRESSED_<br />OOPS_KEY | "spark.test.useCompressedOops"                               | 测试使用压缩Oops           |
| TEST_MEMORY                              | ConfigBuilder("spark.testing.memory")  .longConf  .createWithDefault(Runtime.getRuntime.maxMemory) | 测试内存量                 |
| TEST_<br />SCHEDULE_INTERVAL             | ConfigBuilder("spark.testing.dynamicAllocation.scheduleInterval")  .longConf  .createWithDefault(100) | 测试调度时间间隔，默认100  |
| IS_TESTING                               | ConfigBuilder("spark.testing")  .booleanConf  .createOptional | 是否测试                   |
| TEST_NO_STAGE_RETRY                      | ConfigBuilder("spark.test.noStageRetry")  .booleanConf  .createWithDefault(false) | 没有stage的重试，默认false |
| TEST_RESERVED_<br />MEMORY               | ConfigBuilder("spark.testing.reservedMemory")  .longConf  .createOptional | 测试保留内存               |
| TEST_N_HOSTS                             | ConfigBuilder("spark.testing.nHosts")  .intConf  .createWithDefault(5) | 测试主机数量，默认5个      |
| TEST_N_<br />EXECUTORS_HOST              | ConfigBuilder("spark.testing.nExecutorsPerHost")  .intConf  .createWithDefault(4) | 测试n个执行器主机，默认4个 |
| TEST_N_<br />CORES_EXECUTOR              | ConfigBuilder("spark.testing.nCoresPerExecutor")  .intConf  .createWithDefault(2) | 测试n个执行器核心，默认2个 |

#### UI

UI组件参数设置

| 属性                                      | 参数值                                                       | 介绍                                                         |
| ----------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| UI_SHOW_<br />CONSOLE_PROGRESS            | ConfigBuilder("spark.ui.showConsoleProgress")<br />.booleanConf  .createWithDefault(false) | 是否在控制台显示进程Bar,默认false                            |
| UI_CONSOLE_PROGRESS_<br />UPDATE_INTERVAL | ConfigBuilder("spark.ui.consoleProgress.update.interval")  .timeConf(TimeUnit.MILLISECONDS)  .createWithDefault(200) | 控制台更新时间间隔，默认200ms                                |
| UI_ENABLED                                | ConfigBuilder("spark.ui.enabled").booleanConf  .createWithDefault(true) | 是否允许WEBUI默认true                                        |
| UI_PORT                                   | ConfigBuilder("spark.ui.port").intConf  .createWithDefault(4040) | WEB端口，默认4040                                            |
| UI_FILTERS                                | ConfigBuilder("spark.ui.filters")<br />.stringConf  .toSequence  .createWithDefault(Nil) | WEB过滤器，列表中的元素会应用到WEB  UI中                     |
| UI_ALLOW_FRAMING_FROM                     | ConfigBuilder("spark.ui.allowFramingFrom")  .stringConf  .createOptional | WEB允许的架构                                                |
| UI_REVERSE_PROXY                          | ConfigBuilder("spark.ui.reverseProxy").booleanConf  .createWithDefault(false) | WEB UI反向代理，默认false                                    |
| UI_REVERSE_PROXY_URL                      | ConfigBuilder("spark.ui.reverseProxyUrl").stringConf  .createOptional | 反向代理地址                                                 |
| UI_KILL_ENABLED                           | ConfigBuilder("spark.ui.killEnabled") .booleanConf  .createWithDefault(true) | 是否允许再WEB UI中删除job或者stage                           |
| UI_THREAD_DUMPS_ENABLED                   | ConfigBuilder("spark.ui.threadDumpsEnabled")  .booleanConf  .createWithDefault(true) | 是否允许线程倾倒，默认true                                   |
| UI_PROMETHEUS_ENABLED                     | ConfigBuilder("spark.ui.prometheus.enabled")  .internal()  .booleanConf  .createWithDefault(false) | 是否允许prometheus，默认tfalse，目的是暴露执行器度量器，当然需要配置conf/metrics.properties |
| UI_X_XSS_PROTECTION                       | ConfigBuilder("spark.ui.xXssProtection").stringConf  .createWithDefaultString("1; mode=block") | XSS攻击防护策略的响应头                                      |
| UI_X_CONTENT_TYPE_OPTIONS                 | ConfigBuilder("spark.ui.xContentTypeOptions.enabled")  .booleanConf  .createWithDefault(true) | 默认为true，设置X-Content-Type-Options HTTP 请求头为 nosniff |
| UI_STRICT_<br />TRANSPORT_SECURITY        | ConfigBuilder("spark.ui.strictTransportSecurity")  .stringConf  .createOptional | HTTP 严格安全传输请求头                                      |
| UI_REQUEST_HEADER_SIZE                    | ConfigBuilder("spark.ui.requestHeaderSize")<br />.bytesConf(ByteUnit.BYTE)  .createWithDefaultString("8k") | HTTP 请求头大小，单位字节                                    |
| UI_TIMELINE_TASKS_MAXIMUM                 | ConfigBuilder("spark.ui.timeline.tasks.maximum")  .intConf.createWithDefault(1000) | 时间轴任务最大值，默认1000                                   |
| ACLS_ENABLE                               | ACLS_ENABLE = ConfigBuilder("spark.acls.enable")  .booleanConf  .createWithDefault(false) | 是否支持访问控制列表，默认false                              |
| UI_VIEW_ACLS                              | ConfigBuilder("spark.ui.view.acls")  .stringConf  .toSequence  .createWithDefault(Nil) | 视图控制列表                                                 |
| UI_VIEW_ACLS_GROUPS                       | ConfigBuilder("spark.ui.view.acls.groups")  .stringConf  .toSequence  .createWithDefault(Nil) | 视图控制列表组                                               |
| ADMIN_ACLS                                | ConfigBuilder("spark.admin.acls")  .stringConf  .toSequence  .createWithDefault(Nil) | 管理控制列表                                                 |
| ADMIN_ACLS_GROUPS                         | ConfigBuilder("spark.admin.acls.groups")  .stringConf  .toSequence  .createWithDefault(Nil) | 管理控制列表组                                               |
| MODIFY_ACLS                               | ConfigBuilder("spark.modify.acls")  .stringConf  .toSequence  .createWithDefault(Nil) | 修改控制列表                                                 |
| MODIFY_ACLS_GROUPS                        | ConfigBuilder("spark.modify.acls.groups")  .stringConf  .toSequence  .createWithDefault(Nil) | 修改控制列表组                                               |
| USER_GROUPS_MAPPING                       | ConfigBuilder("spark.user.groups.mapping")  .stringConf  .createWithDefault("<br />org.apache.spark.security.ShellBasedGroupsMappingProvider") | 指定用户组映射                                               |
| PROXY_REDIRECT_URI                        | ConfigBuilder("spark.ui.proxyRedirectUri").stringConf  .createOptional | 代理重定向资源标识符<br />HTTP重定向时代理地址               |
| CUSTOM_EXECUTOR_LOG_URL                   | ConfigBuilder("spark.ui.custom.executor.log.url")<br />.stringConf.createOptional | 用户执行器日志地址                                           |

#### Worker

worker信息配置

| 属性                                                     | 参数值                                                       | 介绍                                                         |
| -------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| SPARK_WORKER<br />_PREFIX                                | "spark.worker"                                               | spark worker前缀                                             |
| SPARK_WORKER_<br />RESOURCE_FILE                         | ConfigBuilder("spark.worker.resourcesFile")<br />.internal().stringConf.createOptional | spark worker的资源文件<br />包含分配给worker的资源，文件需要格式化为@ResourceAllocation的json数组。仅仅使用在脱机模式下。 |
| WORKER_TIMEOUT                                           | ConfigBuilder("spark.worker.timeout")  .longConf  .createWithDefault(60) | worker的时间上限，默认60                                     |
| WORKER_DRIVER_<br />TERMINATE_TIMEOUT                    | ConfigBuilder("spark.worker.driverTerminateTimeout")  .timeConf(TimeUnit.MILLISECONDS)  .createWithDefaultString("10s") | worker-driver终端时间上限。默认10s                           |
| WORKER_CLEANUP_<br />ENABLED                             | ConfigBuilder("spark.worker.cleanup.enabled")  .booleanConf  .createWithDefault(false) | worker是否能够情况。默认false                                |
| WORKER_CLEANUP_<br />INTERVAL                            | ConfigBuilder("spark.worker.cleanup.interval")  .longConf  .createWithDefault(60 * 30) | worker时间间隔，默认60*30                                    |
| APP_DATA_<br />RETENTION                                 | ConfigBuilder("spark.worker.cleanup.appDataTtl")  .longConf  .createWithDefault(7 * 24 * 3600) | 数据滞留时间。默认7* 24* 60                                  |
| PREFER_CONFIGURED<br />_MASTER_ADDRESS                   | ConfigBuilder("spark.worker.preferConfiguredMasterAddress")  .booleanConf  .createWithDefault(false) | 是否倾向于获得配置的master地址                               |
| WORKER_UI_PORT                                           | ConfigBuilder("spark.worker.ui.port")  .intConf  .createOptional | Worker WEB端口                                               |
| WORKER_UI_<br />RETAINED_EXECUTORS                       | ConfigBuilder("spark.worker.ui.retainedExecutors")  .intConf  .createWithDefault(1000) | worker保留执行器数量，默认1000                               |
| WORKER_UI_<br />RETAINED_DRIVERS                         | ConfigBuilder("spark.worker.ui.retainedDrivers")  .intConf  .createWithDefault(1000) | worker保留驱动器实例，默认1000                               |
| UNCOMPRESSED_LOG<br />_FILE_LENGTH_<br />CACHE_SIZE_CONF | ConfigBuilder("spark.worker.ui.compressedLogFileLengthCacheSize").i<br />ntConf.createWithDefault(100) | 不经压缩日志文件缓存大小配置。默认为100                      |

---

#### io

1.  [FileCommitProtocol.scala](# FileCommitProtocol)
2.  [HadoopMapRedCommitProtocol.scala](# HadoopMapRedCommitProtocol)
3.  [HadoopMapReduceCommitProtocol.scala](# HadoopMapReduceCommitProtocol)
4.  [HadoopWriteConfigUtil.scala](# HadoopWriteConfigUtil)
5.  [SparkHadoopWriter.scala](# SparkHadoopWriter)
6.  [SparkHadoopWriterUtils.scala](# SparkHadoopWriterUtils)

---

#### FileCommitProtocol

#### HadoopMapRedCommitProtocol

#### HadoopMapReduceCommitProtocol

#### HadoopWriteConfigUtil

#### SparkHadoopWriter

#### SparkHadoopWriterUtils

---



#### plugin

1.  [PluginContainer.scala](# PluginContainer)

2. [PluginContextImpl.scala](# PluginContextImpl)

3.  [PluginEndpoint.scala](# PluginEndpoint)

   ---

   #### PluginContainer

   #### PluginContextImpl

   #### PluginEndpoint

#### Logging