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

#### BarrierCoordinator

#### BarrierTaskContext

#### ContextCleaner

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

#### ExecutorAllocationClient

#### ExecutorAllocationManager

#### FutureAction

#### Heartbeater

#### HeartbeatReceiver

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