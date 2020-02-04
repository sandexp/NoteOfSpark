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