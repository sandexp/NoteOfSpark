## **spark-status**

---

1.  [api.v1](# api.v1)
2.  [AppHistoryServerPlugin.scala](# AppHistoryServerPlugin)
3.  [AppStatusListener.scala](# AppStatusListener)
4.  [AppStatusSource.scala](# AppStatusSource)
5.  [AppStatusStore.scala](# AppStatusStore)
6.  [AppStatusUtils.scala](# AppStatusUtils)
7.  [ElementTrackingStore.scala](# ElementTrackingStore)
8.  [KVUtils.scala](# KVUtils)
9.  [LiveEntity.scala](# LiveEntity)
10.  [storeTypes.scala](# storeTypes)

---

#### api.v1

1.  [api.scala](# api)

2. [ApiRootResource.scala](# ApiRootResource)

3. [ApplicationListResource.scala](# ApplicationListResource)

4. [JacksonMessageWriter.scala](# JacksonMessageWriter)

5.  [OneApplicationResource.scala](# OneApplicationResource)

6.  [PrometheusResource.scala](# PrometheusResource)

7.  [SimpleDateParam.scala](# SimpleDateParam)

8.  [StagesResource.scala](# StagesResource) 

   ---

   #### api


   ```scala
    case class ApplicationInfo private[spark] (
       id: String,
    name: String,
       coresGranted: Option[Int],
    maxCores: Option[Int],
       coresPerExecutor: Option[Int],
    memoryPerExecutorMB: Option[Int],
       attempts: Seq[ApplicationAttemptInfo])
    介绍: 应用信息样例类
    参数:
	id	应用id
   	name	应用名称
   	coresGranted	授予核心数量
   	maxCores	最大核心数量
   	coresPerExecutor	每个执行器的核心数量
   	memoryPerExecutorMB	每个执行器的内存(单位MB)
   	attempts	请求列表
   	
   @JsonIgnoreProperties(
     value = Array("startTimeEpoch", "endTimeEpoch", "lastUpdatedEpoch"),
     allowGetters = true)
   case class ApplicationAttemptInfo private[spark] (
       attemptId: Option[String],
       startTime: Date,
       endTime: Date,
       lastUpdated: Date,
       duration: Long,
       sparkUser: String,
       completed: Boolean = false,
       appSparkVersion: String) {
   	介绍: 应用请求信息
   	构造器参数；
   	attemptId	请求ID
   	startTime	开始时间
   	endTime		结束时间
   	lastUpdated	最后一次更新时间
   	duration	持续时间
   	sparkUser	spark使用者
   	completed	是否完成
   	appSparkVersion	应用spark版本
   	
   	操作集:
       def getStartTimeEpoch: Long = startTime.getTime
   	功能: 获取开始时刻

       def getEndTimeEpoch: Long = endTime.getTime
   	功能: 获取结束时间点
   	
       def getLastUpdatedEpoch: Long = lastUpdated.getTime
   	功能: 获取上次更新时间点
   }

   class ExecutorStageSummary private[spark](
       val taskTime : Long,
       val failedTasks : Int,
       val succeededTasks : Int,
       val killedTasks : Int,
       val inputBytes : Long,
       val inputRecords : Long,
       val outputBytes : Long,
       val outputRecords : Long,
       val shuffleRead : Long,
       val shuffleReadRecords : Long,
       val shuffleWrite : Long,
       val shuffleWriteRecords : Long,
       val memoryBytesSpilled : Long,
       val diskBytesSpilled : Long,
       val isBlacklistedForStage: Boolean)
   介绍: 执行器stage统计信息
   参数:
   	taskTime	任务时间
   	failedTasks	失败任务数量
   	succeededTasks 成功任务数量
   	killedTasks	kill任务数量
   	inputBytes	输入字节数量
   	inputRecords 输入记录数量
   	outputBytes	输出字节数量
   	outputRecords 输出字节数量
   	shuffleRead shuffle读取字节数量
   	shuffleReadRecords shuffle读取记录数量
   	shuffleWrite shuffle写出字节数量
   	shuffleWriteRecords	shuffle写出记录数量
   	memoryBytesSpilled	内存溢写字节数量
   	diskBytesSpilled	磁盘溢写字节数量
   	isBlacklistedForStage	是否处于黑名单中
   	
   class ExecutorSummary private[spark](
       val id: String,
       val hostPort: String,
       val isActive: Boolean,
       val rddBlocks: Int,
       val memoryUsed: Long,
       val diskUsed: Long,
       val totalCores: Int,
       val maxTasks: Int,
       val activeTasks: Int,
       val failedTasks: Int,
       val completedTasks: Int,
       val totalTasks: Int,
       val totalDuration: Long,
       val totalGCTime: Long,
       val totalInputBytes: Long,
       val totalShuffleRead: Long,
       val totalShuffleWrite: Long,
       val isBlacklisted: Boolean,
       val maxMemory: Long,
       val addTime: Date,
       val removeTime: Option[Date],
       val removeReason: Option[String],
       val executorLogs: Map[String, String],
       val memoryMetrics: Option[MemoryMetrics],
       val blacklistedInStages: Set[Int],
       @JsonSerialize(using = classOf[ExecutorMetricsJsonSerializer])
       @JsonDeserialize(using = classOf[ExecutorMetricsJsonDeserializer])
       val peakMemoryMetrics: Option[ExecutorMetrics],
       val attributes: Map[String, String],
       val resources: Map[String, ResourceInformation])
   介绍: 执行器参数
   参数:	
   	id	执行器ID
   	hostPort	主机端口
   	isActive	是否处于激活状态
   	rddBlocks	rdd块数量
   	memoryUsed	内存使用量
   	diskUsed	磁盘使用量
   	totalCores	合计核心数量
   	maxTasks	最大任务数量
   	activeTasks	激活任务数量
   	failedTasks	失败任务数量
   	completedTasks	完成的任务数量
   	totalTasks	合计任务数量
   	totalDuration	总计持续时间
   	totalGCTime	合计GC时间
   	totalInputBytes	合计输入字节数量
   	totalShuffleRead	合计shuffle读取字节量
   	totalShuffleWrite	合计shuffle写出字节量
   	isBlacklisted	是否处于黑名单中
   	maxMemory	最大内存量
   	addTime	添加时间
   	removeTime 移除时间
   	removeReason 移除执行器的原因
   	executorLogs	执行器日志映射表
   	memoryMetrics	内存度量器
   	blacklistedInStages 位于黑名单的stage集合
   	peakMemoryMetrics	峰值内存度量器
   	attributes	属性列表
   	resources	资源列表
   	
   class MemoryMetrics private[spark](
       val usedOnHeapStorageMemory: Long,
       val usedOffHeapStorageMemory: Long,
       val totalOnHeapStorageMemory: Long,
       val totalOffHeapStorageMemory: Long)
   介绍: 内存度量器
   参数:
   	usedOnHeapStorageMemory	堆上使用内存量
   	usedOffHeapStorageMemory	非堆模式下内存使用量
   	totalOnHeapStorageMemory	总计堆上内存量
   	totalOffHeapStorageMemory	总计非堆模式下内存量

   class JobData private[spark](
       val jobId: Int,
       val name: String,
       val description: Option[String],
       val submissionTime: Option[Date],
       val completionTime: Option[Date],
       val stageIds: Seq[Int],
       val jobGroup: Option[String],
       val status: JobExecutionStatus,
       val numTasks: Int,
       val numActiveTasks: Int,
       val numCompletedTasks: Int,
       val numSkippedTasks: Int,
       val numFailedTasks: Int,
       val numKilledTasks: Int,
       val numCompletedIndices: Int,
       val numActiveStages: Int,
       val numCompletedStages: Int,
       val numSkippedStages: Int,
       val numFailedStages: Int,
       val killedTasksSummary: Map[String, Int])
   介绍: job数据
   参数:
   	jobId	jobID
   	name 	job名称
   	description	job描述
   	submissionTime	提交时间
   	completionTime	完成时间
   	stageIds	stage列表
   	jobGroup	job组
   	status	job执行状态
   	numTasks	任务数量
   	numActiveTasks	激活任务数量
   	numCompletedTasks	完成任务数量
   	numSkippedTasks	跳过任务数量
   	numFailedTasks	失败任务数量
   	numKilledTasks	kill任务数量
   	numCompletedIndices	完成目录数量
   	numActiveStages	激活stage数量
   	numCompletedStages	完成stage数量
   	numSkippedStages	跳过stage数量
   	numFailedStages	失败stage数量
   	killedTasksSummary 被kill的任务表
   	
   class RDDStorageInfo private[spark](
       val id: Int,
       val name: String,
       val numPartitions: Int,
       val numCachedPartitions: Int,
       val storageLevel: String,
       val memoryUsed: Long,
       val diskUsed: Long,
       val dataDistribution: Option[Seq[RDDDataDistribution]],
       val partitions: Option[Seq[RDDPartitionInfo]])
   介绍: RDD存储信息
   参数:
   	id	RDD编号
   	name	RDD名称
   	numPartitions	分区数量
   	numCachedPartitions	缓存分区数量
   	storageLevel	存储等级
   	memoryUsed	内存使用量
   	diskUsed	磁盘使用量
   	dataDistribution	数据分布列表
   	partitions	分区信息列表
   	
   class RDDDataDistribution private[spark](
       val address: String,
       val memoryUsed: Long,
       val memoryRemaining: Long,
       val diskUsed: Long,
       @JsonDeserialize(contentAs = classOf[JLong])
       val onHeapMemoryUsed: Option[Long],
       @JsonDeserialize(contentAs = classOf[JLong])
       val offHeapMemoryUsed: Option[Long],
       @JsonDeserialize(contentAs = classOf[JLong])
       val onHeapMemoryRemaining: Option[Long],
       @JsonDeserialize(contentAs = classOf[JLong])
       val offHeapMemoryRemaining: Option[Long])
   介绍: RDD数据分布
   	addressaddress	RDD数据分布地址
   	memoryUsed	内存使用量
   	memoryRemaining	内存剩余量
   	diskUsed	磁盘使用量
   	onHeapMemoryUsed 堆上内存使用量
   	offHeapMemoryUsed 非堆模式内存使用量
   	onHeapMemoryRemaining	堆上内存剩余量
   	offHeapMemoryRemaining	非堆模式内存剩余量

   class RDDPartitionInfo private[spark](
       val blockName: String,
       val storageLevel: String,
       val memoryUsed: Long,
       val diskUsed: Long,
       val executors: Seq[String])
   介绍: RDD 分区信息
   参数:
   	blockName	块名称
   	storageLevel	存储等级
   	memoryUsed	内存使用量
   	diskUsed	磁盘使用量
   	executors	执行器列表

   class StageData private[spark](
       val status: StageStatus,
       val stageId: Int,
       val attemptId: Int,
       val numTasks: Int,
       val numActiveTasks: Int,
       val numCompleteTasks: Int,
       val numFailedTasks: Int,
       val numKilledTasks: Int,
       val numCompletedIndices: Int,
       val submissionTime: Option[Date],
       val firstTaskLaunchedTime: Option[Date],
       val completionTime: Option[Date],
       val failureReason: Option[String],
       val executorDeserializeTime: Long,
       val executorDeserializeCpuTime: Long,
       val executorRunTime: Long,
       val executorCpuTime: Long,
       val resultSize: Long,
       val jvmGcTime: Long,
       val resultSerializationTime: Long,
       val memoryBytesSpilled: Long,
       val diskBytesSpilled: Long,
       val peakExecutionMemory: Long,
       val inputBytes: Long,
       val inputRecords: Long,
       val outputBytes: Long,
       val outputRecords: Long,
       val shuffleRemoteBlocksFetched: Long,
       val shuffleLocalBlocksFetched: Long,
       val shuffleFetchWaitTime: Long,
       val shuffleRemoteBytesRead: Long,
       val shuffleRemoteBytesReadToDisk: Long,
       val shuffleLocalBytesRead: Long,
       val shuffleReadBytes: Long,
       val shuffleReadRecords: Long,
       val shuffleWriteBytes: Long,
       val shuffleWriteTime: Long,
       val shuffleWriteRecords: Long,
       val name: String,
       val description: Option[String],
       val details: String,
       val schedulingPool: String,
       val rddIds: Seq[Int],
       val accumulatorUpdates: Seq[AccumulableInfo],
       val tasks: Option[Map[Long, TaskData]],
       val executorSummary: Option[Map[String, ExecutorStageSummary]],
       val killedTasksSummary: Map[String, Int])
   介绍: stage 数据
   参数:
   	status	stage状态
   	stageId	stage ID
   	attemptId	请求ID
   	numTasks	任务数量
   	numActiveTasks	激活任务数量
   	numCompleteTasks	完成任务数量
   	numFailedTasks	失败任务数量
   	numKilledTasks	kill任务数量
   	numCompletedIndices	完成目录数量
   	submissionTime	提交时间
   	firstTaskLaunchedTime	首次运行时间
   	completionTime	完成时间
   	failureReason	失败原因
   	executorDeserializeTime 执行器反序列化时间
   	executorDeserializeCpuTime	执行器反序列化CPU时间
   	executorRunTime	执行器运行时间
   	executorCpuTime	执行器CPU时间
   	resultSize	结果规模大小
   	jvmGcTime	jvm GC时间
   	resultSerializationTime	结果序列化时间
   	memoryBytesSpilled	内存溢写字节量
   	diskBytesSpilled	溢写到磁盘上字节数量
   	peakExecutionMemory	执行器峰值内存使用量
   	inputBytes	输入字节数量
   	inputRecords	输入记录数量
   	outputBytes	输出字节数量
   	outputRecords	输出记录数量
   	shuffleRemoteBlocksFetched	shuffle远端获取块数量
   	shuffleLocalBlocksFetched	shuffle本地获取块数量
   	shuffleFetchWaitTime	shuffle获取等待时间
   	shuffleRemoteBytesRead	shuffle远端读取字节数量
   	shuffleRemoteBytesReadToDisk	shuffle远端读取到磁盘的字节数量
   	shuffleLocalBytesRead	shuffle本地读取字节数量
   	shuffleReadBytes	shuffle读取字节数量
   	shuffleReadRecords	shuffle读取记录数量
   	shuffleWriteBytes	shuffle写出字节数量
   	shuffleWriteTime	shuffle写出时间
   	shuffleWriteRecords	shuffle写出记录数量
   	name stage名称
   	description	stage描述
   	details	stage细节描述
   	schedulingPool	调度池名称
   	rddIds	RDD id列表
   	accumulatorUpdates 更新累加器列表
   	tasks	任务列表
   	executorSummary	执行器描述表
   	killedTasksSummary	kill的任务描述
   	
   class TaskData private[spark](
       val taskId: Long,
       val index: Int,
       val attempt: Int,
       val launchTime: Date,
       val resultFetchStart: Option[Date],
       @JsonDeserialize(contentAs = classOf[JLong])
       val duration: Option[Long],
       val executorId: String,
       val host: String,
       val status: String,
       val taskLocality: String,
       val speculative: Boolean,
       val accumulatorUpdates: Seq[AccumulableInfo],
       val errorMessage: Option[String] = None,
       val taskMetrics: Option[TaskMetrics] = None,
       val executorLogs: Map[String, String],
       val schedulerDelay: Long,
       val gettingResultTime: Long)
    介绍: 任务数据
    参数:
    	taskId	任务id
    	index 	索引
    	attempt	请求编号
    	launchTime	运行时间
    	resultFetchStart	开始获取结果的时间
    	duration	持续时间
    	executorId	执行器ID
    	host	主机名称
    	status	状态名称
    	taskLocality	任务位置
    	speculative		是否推测执行
    	accumulatorUpdates	更新累加器列表
    	errorMessage	错误信息
    	taskMetrics	任务度量器
    	executorLogs	执行器日志表
    	schedulerDelay	调度延时时间
    	gettingResultTime	获取结果事件
    
    class TaskMetrics private[spark](
       val executorDeserializeTime: Long,
       val executorDeserializeCpuTime: Long,
       val executorRunTime: Long,
       val executorCpuTime: Long,
       val resultSize: Long,
       val jvmGcTime: Long,
       val resultSerializationTime: Long,
       val memoryBytesSpilled: Long,
       val diskBytesSpilled: Long,
       val peakExecutionMemory: Long,
       val inputMetrics: InputMetrics,
       val outputMetrics: OutputMetrics,
       val shuffleReadMetrics: ShuffleReadMetrics,
       val shuffleWriteMetrics: ShuffleWriteMetrics)
   介绍: 任务度量
   	executorDeserializeTime	执行器反序列化时间
   	executorDeserializeCpuTime	执行器反序列化
   	executorRunTime	执行器运行时间
   	executorCpuTime 执行器CPU时间
   	resultSize 结果规模大小
   	jvmGcTime JVM GC时间
   	resultSerializationTime	结果序列化时间
   	memoryBytesSpilled	内存溢写字节量	
   	diskBytesSpilled	磁盘溢写字节量
   	peakExecutionMemory	执行器峰值内存量
   	inputMetrics	输入度量器
   	outputMetrics	输出度量器
   	shuffleReadMetrics	shuffle读取度量器
   	shuffleWriteMetrics	shuffle写出度量器

   class InputMetrics private[spark](
       val bytesRead: Long,
       val recordsRead: Long)
    介绍: 输入度量器
    参数:
    	bytesRead	读取字节量
    	recordsRead 读取记录数量
    
    class OutputMetrics private[spark](
       val bytesWritten: Long,
       val recordsWritten: Long)
    介绍: 输出度量器
    	bytesWritten	写出字节量
    	recordsWritten	写出记录数量
    
    class ShuffleReadMetrics private[spark](
       val remoteBlocksFetched: Long,
       val localBlocksFetched: Long,
       val fetchWaitTime: Long,
       val remoteBytesRead: Long,
       val remoteBytesReadToDisk: Long,
       val localBytesRead: Long,
       val recordsRead: Long)
    介绍: shuffle 读取计量器
    参数:
    	remoteBlocksFetched	远端获取块数量
    	localBlocksFetched	本地获取块数量
    	fetchWaitTime 获取等待时间
    	remoteBytesRead 远端读取字节量
    	remoteBytesReadToDisk 远端读到磁盘的字节量
    	localBytesRead	本地读取字节量
    	recordsRead	读取记录数量
    
    class ShuffleWriteMetrics private[spark](
       val bytesWritten: Long,
       val writeTime: Long,
       val recordsWritten: Long)
    介绍: shuffle写度量器
    参数:
    	bytesWritten	字节写出量
    	writeTime 写出时间
    	recordsWritten	记录写出数量
    	
    class InputMetricDistributions private[spark](
       val bytesRead: IndexedSeq[Double],
       val recordsRead: IndexedSeq[Double])
   介绍: 输入度量器分布
   参数:
   	bytesRead	字节读取分布
   	recordsRead	记录读取分布

   class OutputMetricDistributions private[spark] (
       val bytesWritten: IndexedSeq[Double],
       val recordsWritten: IndexedSeq[Double])
   介绍: 输出度量分布
   参数
   	bytesWritten	写出字节量分布
   	recordsWritten	写出记录数量分布

   class ShuffleReadMetricDistributions private[spark] (
       val readBytes: IndexedSeq[Double],
       val readRecords: IndexedSeq[Double],
       val remoteBlocksFetched: IndexedSeq[Double],
       val localBlocksFetched: IndexedSeq[Double],
       val fetchWaitTime: IndexedSeq[Double],
       val remoteBytesRead: IndexedSeq[Double],
       val remoteBytesReadToDisk: IndexedSeq[Double],
       val totalBlocksFetched: IndexedSeq[Double])
   介绍: shuffle读取度量值分布
   参数:
   	readBytes	读取字节量分布
   	readRecords	读取字节量分布
   	remoteBlocksFetched 远程获取块分布
   	localBlocksFetched 本地获取块分布
   	fetchWaitTime 获取等待时间分布
   	remoteBytesRead 远端读取字节量分布
   	remoteBytesReadToDisk	远端读取到磁盘字节量分布
   	totalBlocksFetched 总计获取块文件分布

   class ShuffleWriteMetricDistributions private[spark](
       val writeBytes: IndexedSeq[Double],
       val writeRecords: IndexedSeq[Double],
       val writeTime: IndexedSeq[Double])
   介绍: shuffle写出度量分布
   参数:
   	writeBytes 写出字节分布
   	writeRecords 写出记录分布
   	writeTime 写出时间分布

   class AccumulableInfo private[spark](
       val id: Long,
       val name: String,
       val update: Option[String],
       val value: String)
   功能: 累加器信息
   参数
   	id	累加器编号
   	name	累加器名称
   	update	更新名称
   	value	累加器值

   class VersionInfo private[spark](
     val spark: String)
   参数:
   	spark spark版本信息

   class RuntimeInfo private[spark](
       val javaVersion: String,
       val javaHome: String,
       val scalaVersion: String)
    功能: 运行时信息
    参数
    	javaVersion	java版本
    	javaHome java HOME
    	scalaVersion scala版本
    
    class ApplicationEnvironmentInfo private[spark] (
       val runtime: RuntimeInfo,
       val sparkProperties: Seq[(String, String)],
       val hadoopProperties: Seq[(String, String)],
       val systemProperties: Seq[(String, String)],
       val classpathEntries: Seq[(String, String)])
    介绍: 应用环境信息
    参数
    	runtime	运行时信息
    	sparkProperties		spark属性列表
    	hadoopProperties	hadoop属性列表
    	systemProperties	系统属性列表
    	classpathEntries	类路径属性列表
    	
    class TaskMetricDistributions private[spark] (
       val quantiles: IndexedSeq[Double],
       val executorDeserializeTime: IndexedSeq[Double],
       val executorDeserializeCpuTime: IndexedSeq[Double],
       val executorRunTime: IndexedSeq[Double],
       val executorCpuTime: IndexedSeq[Double],
       val resultSize: IndexedSeq[Double],
       val jvmGcTime: IndexedSeq[Double],
       val resultSerializationTime: IndexedSeq[Double],
       val gettingResultTime: IndexedSeq[Double],
       val schedulerDelay: IndexedSeq[Double],
       val peakExecutionMemory: IndexedSeq[Double],
       val memoryBytesSpilled: IndexedSeq[Double],
       val diskBytesSpilled: IndexedSeq[Double],
       val inputMetrics: InputMetricDistributions,
       val outputMetrics: OutputMetricDistributions,
       val shuffleReadMetrics: ShuffleReadMetricDistributions,
       val shuffleWriteMetrics: ShuffleWriteMetricDistributions)
   介绍: 任务度量器分布
   	quantiles	数量分布
   	executorDeserializeTime	执行器反序列化时间分布
   	executorDeserializeCpuTime	执行器反序列化CPU时间分布
   	executorRunTime 执行器运行时间分布
   	executorCpuTime	执行器CPU时间分布
   	resultSize 获取结果规模大小分布
   	jvmGcTime	GC时间分布
   	resultSerializationTime	结果序列化时间分布
   	gettingResultTime	获取结果时间分布
   	schedulerDelay	调度延时分布
   	peakExecutionMemory	执行器峰值内存使用量分布
   	memoryBytesSpilled	内存溢写量分布
   	diskBytesSpilled	磁盘溢写量分布
   	inputMetrics	输入度量器分布
   	outputMetrics	输出度量器分布
   	shuffleReadMetrics	shuffle读取度量分布
   	shuffleWriteMetrics	shuffle写出度量分布
   case class ThreadStackTrace(
       val threadId: Long,
       val threadName: String,
       val threadState: Thread.State,
       val stackTrace: StackTrace,
       val blockedByThreadId: Option[Long],
       val blockedByLock: String,
       val holdingLocks: Seq[String])
    介绍: 线程栈追踪信息
    参数
    	threadId 线程ID
    	threadName	线程名称
    	threadState	线程名称
    	stackTrace	追踪栈
    	blockedByThreadId	阻塞线程数量
    	blockedByLock	锁阻塞数量
    	holdingLocks	持有锁数量列表	

   ```

   ```markdown
   case class StackTrace(elems: Seq[String]) {
   	介绍: 栈追踪类
   	override def toString: String = elems.mkString
   	功能: 信息显示
     	
     	def html: NodeSeq = {
           val withNewLine = elems.foldLeft(NodeSeq.Empty) { (acc, elem) =>
             if (acc.isEmpty) {
               acc :+ Text(elem)
             } else {
               acc :+ <br /> :+ Text(elem)
             }
       	}
       	withNewLine
     	}
     	功能: 追踪节点信息
   
     	def mkString(start: String, sep: String, end: String): String = {
       	elems.mkString(start, sep, end)
     	}
     	功能: 信息拼接
   }
   ```

   #### ApiRootResource

   ```markdown
   介绍:
   	使用JAX-RS,以json的形式用作服务spark应用程序度量系统
   	每个资源需要返回public的端点，定义在api.scala中。二进制比较(文件一致性校验)检查能够确保不会无意间的变化会使得打断api工作。返回的对象会由jackson使用@JacksonMessageWriter 自动转化为json。除此之外，存在有在@HistoryServerSuite大量测试，比较json和""golden files".任何的改变和添加都会反馈到那儿，请参照@HistoryServerSuite
   ```

   ```markdown
   @Path("/v1")
   private[v1] class ApiRootResource extends ApiRequestContext {
   	操作集:
   	@Path("applications")
       def applicationList(): Class[ApplicationListResource] = classOf[ApplicationListResource]
       功能: 获取应用资源列表
       
       @Path("applications/{appId}")
       def application(): Class[OneApplicationResource] = classOf[OneApplicationResource]
       功能: 获取单个任务资源
       
       @GET
       @Path("version")
       def version(): VersionInfo = new VersionInfo(org.apache.spark.SPARK_VERSION)
   	功能: 获取版本信息
   }
   ```

   ```markdown
   private[spark] object ApiRootResource {
   	操作集:
   	def getServletHandler(uiRoot: UIRoot): ServletContextHandler 
   	功能: 获取服务端处理器
   	val= {
           val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
           jerseyContext.setContextPath("/api")
           val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
           holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES,
           	"org.apache.spark.status.api.v1")
           UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
           jerseyContext.addServlet(holder, "/*")
           jerseyContext
         }
   }
   ```

   ```markdown
   private[spark] trait UIRoot {
   	介绍: 这个特征被所有根目录容器所共享,共享内容为应用UI信息(历史信息服务器@HistoryServer和应用UI)。提供通用接口，且使用json的形式暴露给外面。
   	操作集:
   	def withSparkUI[T](def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T: String, attemptId: Option[String])(fn: SparkUI => T): T
   	功能: 给当前app/请求运行spark UI程序
   	输入参数:
   		appId	应用Id
   		attemptId	请求Id
   		fn	sparkUI程序
   	
   	def getApplicationInfoList: Iterator[ApplicationInfo]
   	功能: 获取任务信息列表
   	
   	def getApplicationInfo(appId: String): Option[ApplicationInfo]
   	功能: 获取任务信息
   	
   	def securityManager: SecurityManager
   	功能: 获取安全管理器
   	
   	def writeEventLogs(appId:String, attemptId:Option[String],zipStream:ZipOutputStream): Unit
   	功能: 写出事件日志	
   		写出事件日志给@ZipOutputStream 实例,如果请求ID为None,所有应用请求的事件日志会被写出。
   	val= Response.serverError()
         .entity("Event logs are only available through the history server.")
         .status(Response.Status.SERVICE_UNAVAILABLE)
         .build()
   }
   ```

   ```markdown
   private[v1] object UIRootFromServletContext {
   	属性:
   	#name @attribute = getClass.getCanonicalName 属性
   	操作集:
   	def setUiRoot(contextHandler: ContextHandler, uiRoot: UIRoot): Unit
   	功能: 设置UIRoot
   		contextHandler.setAttribute(attribute, uiRoot)
   	
   	def getUiRoot(context: ServletContext): UIRoot
   	功能: 获取UIRoot
   	val= context.getAttribute(attribute).asInstanceOf[UIRoot]
   }
   ```

   ```markdown
   private[v1] trait ApiRequestContext {
   	介绍: api请求上下文
   	属性:
   	#name @servletContext: ServletContext = _	servlet上下文
   	#name @httpRequest: HttpServletRequest = _	http请求
   	操作集:
   	def uiRoot: UIRoot = UIRootFromServletContext.getUiRoot(servletContext)
   	功能: 获取UIRoot
   }
   ```

   ```markdown
   private[v1] trait BaseAppResource extends ApiRequestContext {
   	介绍: 资源处理器的基本类，使用app指定的数据，抽离处理应用，处理请求，以及寻找app的UI
   	属性:
   	#name @appId: String = _ @PathParam("appId")	app编号
   	#name @attemptId: String = _ @PathParam("attemptId")	请求编号
   	操作集:
   	def withUI[T](fn: SparkUI => T): T
   	功能: 获取Spark UI程序处理结果
   	val= try {
         uiRoot.withSparkUI(appId, Option(attemptId)) { ui =>
           val user = httpRequest.getRemoteUser()
           if (!ui.securityManager.checkUIViewPermissions(user)) {
             throw new ForbiddenException(raw"""user "$user" is not authorized""")
           }
           fn(ui)
         }
       } catch {
         case _: NoSuchElementException =>
           val appKey = Option(attemptId).map(appId + "/" + _).getOrElse(appId)
           throw new NotFoundException(s"no such app: $appKey")
       }
   }
   ```

   ```markdown
   异常类别:
   
   private[v1] class ForbiddenException(msg: String) extends WebApplicationException(
       UIUtils.buildErrorResponse(Response.Status.FORBIDDEN, msg))
   	介绍: 禁止异常
   	
   private[v1] class NotFoundException(msg: String) extends WebApplicationException(
       UIUtils.buildErrorResponse(Response.Status.NOT_FOUND, msg))
   	介绍: 资源未找到异常
   	
   private[v1] class ServiceUnavailable(msg: String) extends WebApplicationException(
       UIUtils.buildErrorResponse(Response.Status.SERVICE_UNAVAILABLE, msg))
   	介绍: 服务不可用异常
   
   private[v1] class BadParameterException(msg: String) extends WebApplicationException(
       UIUtils.buildErrorResponse(Response.Status.BAD_REQUEST, msg)) {
     def this(param: String, exp: String, actual: String) = {
       this(raw"""Bad value for parameter "$param".  Expected a $exp, got "$actual"""")
     }
   }
   功能: 错误请求异常
   ```

   

   #### ApplicationListResource

   ```markdown
   @Produces(Array(MediaType.APPLICATION_JSON))
   private[v1] class ApplicationListResource extends ApiRequestContext {
   	介绍: 任务资源列表
   	操作集:
   	@GET
       def appList(
             @QueryParam("status") status: JList[ApplicationStatus],
             @DefaultValue("2010-01-01") @QueryParam("minDate") minDate: SimpleDateParam,
             @DefaultValue("3000-01-01") @QueryParam("maxDate") maxDate: SimpleDateParam,
             @DefaultValue("2010-01-01") @QueryParam("minEndDate") minEndDate: SimpleDateParam,
             @DefaultValue("3000-01-01") @QueryParam("maxEndDate") maxEndDate: SimpleDateParam,
             @QueryParam("limit") limit: Integer)
         : Iterator[ApplicationInfo]	
   	功能: 获取应用信息@ApplicationInfo
   	1. 获取应用数量
   	val numApps = Option(limit).map(_.toInt).getOrElse(Integer.MAX_VALUE)
   	2. 是否包含完成的任务
   	val includeCompleted = status.isEmpty || status.contains(ApplicationStatus.COMPLETED)
   	3. 是否包含运行中任务
   	val includeRunning = status.isEmpty || status.contains(ApplicationStatus.RUNNING)
   	4. 获取任务信息
   	uiRoot.getApplicationInfoList.filter { app =>
         val anyRunning = app.attempts.exists(!_.completed)
         ((!anyRunning && includeCompleted) || (anyRunning && includeRunning)) &&
         app.attempts.exists { attempt =>
           isAttemptInRange(attempt, minDate, maxDate, minEndDate, maxEndDate, anyRunning)
         }	 
       }.take(numApps)
       
       def isAttemptInRange(
         attempt: ApplicationAttemptInfo,
         minStartDate: SimpleDateParam,
         maxStartDate: SimpleDateParam,
         minEndDate: SimpleDateParam,
         maxEndDate: SimpleDateParam,
         anyRunning: Boolean): Boolean
       功能: 确定请求@attempt是否在范围内
       1. 确定开始时间是否OK
       val startTimeOk = attempt.startTime.getTime >= minStartDate.timestamp &&
         attempt.startTime.getTime <= maxStartDate.timestamp
       2. 确定结束时间是否OK
       val endTimeOkForRunning = anyRunning && (maxEndDate.timestamp > System.currentTimeMillis())
       val endTimeOkForCompleted = !anyRunning && (attempt.endTime.getTime >= minEndDate.timestamp &&
         attempt.endTime.getTime <= maxEndDate.timestamp)
       val endTimeOk = endTimeOkForRunning || endTimeOkForCompleted
       3. 确定是否在这个范围内
   	val= startTimeOk && endTimeOk
   }
   ```

   #### JacksonMessageWriter

   ```markdown
   介绍:
   	Jackson消息写出器,使用jackson将POJO度量信息转化为json。
   	这里没有遵守标准的jersey-jackson 插件配置，主要是想要兼顾老版本的jersey(可以从yarn中获取)。而且不想从一个新的插件中拉取很多依赖。
   	注意到，jersey自动的发现这个包和它的注解。
   ```

   ```markdown
   @Provider
   @Produces(Array(MediaType.APPLICATION_JSON))
   private[v1] class JacksonMessageWriter extends MessageBodyWriter[Object]{
   	属性:
   	#name @mapper #type @ObjectMapper	参数映射表
           val= new ObjectMapper() {
               override def writeValueAsString(t: Any): String = {
                 super.writeValueAsString(t)
               }
             }
   	初始化操作:
   	// 注册jackson模块
   	mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
       mapper.enable(SerializationFeature.INDENT_OUTPUT) // 运行序列化
       // 设置序列化所包含的范围
     	mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
     	mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat) // 设置消息格式
     	
     	操作集:
     	def isWriteable(aClass: Class[_],`type`: Type,annotations: Array[Annotation],
         mediaType: MediaType): Boolean= true
       功能: 确定是否为可写的
       
       def getSize(t: Object,aClass: Class[_],`type`: Type,
         annotations: Array[Annotation],mediaType: MediaType): Long = -1L
   	功能: 获取写出器的大小
   	
   	def writeTo(t: Object,aClass: Class[_],`type`: Type,annotations: Array[Annotation],
         mediaType: MediaType,multivaluedMap: MultivaluedMap[String, AnyRef],
         outputStream: OutputStream): Unit
       功能: 写出对象@t
       mapper.writeValue(outputStream, t)
   }
   ```

   ```markdown
   private[spark] object JacksonMessageWriter {
   	介绍: jackson消息写出器
   	操作集:
   	def makeISODateFormat: SimpleDateFormat
   	功能: ISO格式化
   	val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'", Locale.US)
       val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
       iso8601.setCalendar(cal)
       val= iso8601
   }
   ```

   #### OneApplicationResource

   ```markdown
   @Produces(Array(MediaType.APPLICATION_JSON)) // 注解指定应用媒体资源列表
   private[v1] class AbstractApplicationResource extends BaseAppResource{
   	操作集:
   	@GET
       @Path("jobs")
       def jobsList(@QueryParam("status") statuses: JList[JobExecutionStatus]): Seq[JobData]
     	功能: 从UI中获取,job数据@JobData 序列
     	val= withUI(_.store.jobsList(statuses))
     	
     	@GET
     	@Path("jobs/{jobId: \\d+}")
     	def oneJob(@PathParam("jobId") jobId: Int): JobData = withUI { ui =>
           try {
             ui.store.job(jobId)
           } catch {
             case _: NoSuchElementException =>
               throw new NotFoundException("unknown job: " + jobId)
           }
     	}
     	
     	@GET
       @Path("executors")
       def executorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(true))
   	功能: 从UI中获取执行器信息@ExecutorSummary
   	
   	@GET
       @Path("allexecutors")
       def allExecutorList(): Seq[ExecutorSummary] = withUI(_.store.executorList(false))
   	功能: 获取所有执行器信息@ExecutorSummary
   	
   	@Path("stages")
       def stages(): Class[StagesResource] = classOf[StagesResource]
   	功能: 获取stage资源信息@StagesResource
   	
   	@GET
       @Path("storage/rdd")
       def rddList(): Seq[RDDStorageInfo] = withUI(_.store.rddList())
   	功能: 获取RDD存储信息@RDDStorageInfo列表
   	
   	@GET
       @Path("storage/rdd/{rddId: \\d+}")
       def rddData(@PathParam("rddId") rddId: Int): RDDStorageInfo = withUI { ui =>
       	try {
             ui.store.rdd(rddId)
           } catch {
             case _: NoSuchElementException =>
               throw new NotFoundException(s"no rdd found w/ id $rddId")
           }
       }
       功能: 获取指定RDD@rddId 数据信息@RDDStorageInfo
   	
   	@GET
       @Path("environment")
       def environmentInfo(): ApplicationEnvironmentInfo = withUI(_.store.environmentInfo())
   	功能: 获取环境信息@ApplicationEnvironmentInfo
   	
       @GET
       @Path("logs")
       @Produces(Array(MediaType.APPLICATION_OCTET_STREAM))
   	def getEventLogs(): Response 
     	功能: 获取时间日志
     	val = {
           try {
             withUI { _ => }
           } catch {
             case _: NotFoundException if attemptId == null =>
               attemptId = "1"
               withUI { _ => }
               attemptId = null
           }
           try {
             val fileName = if (attemptId != null) {
               s"eventLogs-$appId-$attemptId.zip"
             } else {
               s"eventLogs-$appId.zip"
             }
             val stream = new StreamingOutput {
               override def write(output: OutputStream): Unit = {
                 val zipStream = new ZipOutputStream(output)
                 try {
                   uiRoot.writeEventLogs(appId, Option(attemptId), zipStream)
                 } finally {
                   zipStream.close()
                 }
               }
             }
             Response.ok(stream)
               .header("Content-Disposition", s"attachment; filename=$fileName")
               .header("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
               .build()
           } catch {
             case NonFatal(_) =>
               throw new ServiceUnavailable(s"Event logs are not available for app: $appId.")
           }
         }
   	
   	@Path("{attemptId}")
     	def applicationAttempt(): Class[OneApplicationAttemptResource] = {
       	if (attemptId != null) {
         		throw new NotFoundException(httpRequest.getRequestURI())
       	}
       	classOf[OneApplicationAttemptResource]
     	}
     	功能: 获取请求资源信息@OneApplicationAttemptResource，需要最后调用，否则会造成上述方法路径冲突。且	使得JAX-RS 找不到信息。
   }
   ```

   ```markdown
   private[v1] class OneApplicationResource extends AbstractApplicationResource {
   	介绍: 单个应用资源信息
   	操作集:
   	def getApp(): ApplicationInfo
   	功能: 获取应用信息
   	val= { val app = uiRoot.getApplicationInfo(appId)
       	app.getOrElse(throw new NotFoundException("unknown app: " + appId))}
   }
   ```

   ```markdown
   private[v1] class OneApplicationAttemptResource extends AbstractApplicationResource {
   	介绍: 单个应用请求资源
   	操作集:
   	@GET
       def getAttempt(): ApplicationAttemptInfo
       val= {
           uiRoot.getApplicationInfo(appId)
             .flatMap { app =>
               app.attempts.find(_.attemptId.contains(attemptId))
             }
             .getOrElse {
               throw new NotFoundException(s"unknown app $appId, attempt $attemptId")
             }
         }
   }
   ```

   #### PrometheusResource

   ```markdown
   介绍: 
   	这个主要是暴露执行器的度量信息，使其可以使用REST API查找，请参照
   		@https://spark.apache.org/docs/3.0.0/monitoring.html#executor-metrics
   	注意到执行器信息@ExecutorSummary 是不同于 @ExecutorSource 执行器资源的
   ```

   ```markdown
   @Path("/executors")
   private[v1] class PrometheusResource extends ApiRequestContext {
   	操作集:
   	@GET
       @Path("prometheus")
       @Produces(Array(MediaType.TEXT_PLAIN))
       def executors(): String
       功能: 获取执行器的描述
       val= {
           val sb = new StringBuilder
           val store = uiRoot.asInstanceOf[SparkUI].store
           store.executorList(true).foreach { executor =>
             val prefix = "metrics_executor_"
             val labels = Seq(
               "application_id" -> store.applicationInfo.id,
               "application_name" -> store.applicationInfo.name,
               "executor_id" -> executor.id
             ).map { case (k, v) => s"""$k="$v"""" }.mkString("{", ", ", "}")
             sb.append(s"${prefix}rddBlocks_Count$labels ${executor.rddBlocks}\n")
             sb.append(s"${prefix}memoryUsed_Count$labels ${executor.memoryUsed}\n")
             sb.append(s"${prefix}diskUsed_Count$labels ${executor.diskUsed}\n")
             sb.append(s"${prefix}totalCores_Count$labels ${executor.totalCores}\n")
             sb.append(s"${prefix}maxTasks_Count$labels ${executor.maxTasks}\n")
             sb.append(s"${prefix}activeTasks_Count$labels ${executor.activeTasks}\n")
             sb.append(s"${prefix}failedTasks_Count$labels ${executor.failedTasks}\n")
             sb.append(s"${prefix}completedTasks_Count$labels ${executor.completedTasks}\n")
             sb.append(s"${prefix}totalTasks_Count$labels ${executor.totalTasks}\n")
             sb.append(s"${prefix}totalDuration_Value$labels ${executor.totalDuration}\n")
             sb.append(s"${prefix}totalGCTime_Value$labels ${executor.totalGCTime}\n")
             sb.append(s"${prefix}totalInputBytes_Count$labels ${executor.totalInputBytes}\n")
             sb.append(s"${prefix}totalShuffleRead_Count$labels ${executor.totalShuffleRead}\n")
             sb.append(s"${prefix}totalShuffleWrite_Count$labels ${executor.totalShuffleWrite}\n")
             sb.append(s"${prefix}maxMemory_Count$labels ${executor.maxMemory}\n")
             executor.executorLogs.foreach { case (k, v) => }
             executor.memoryMetrics.foreach { m =>
               sb.append(s"${prefix}usedOnHeapStorageMemory_Count$labels
               ${m.usedOnHeapStorageMemory}\n")
               sb.append(s"${prefix}usedOffHeapStorageMemory_Count$labels
               ${m.usedOffHeapStorageMemory}\n")
               sb.append(s"${prefix}totalOnHeapStorageMemory_Count$labels
               ${m.totalOnHeapStorageMemory}\n")
               sb.append(s"${prefix}totalOffHeapStorageMemory_Count$labels " +
                 s"${m.totalOffHeapStorageMemory}\n")
             }
             executor.peakMemoryMetrics.foreach { m =>
               val names = Array(
                 "JVMHeapMemory",
                 "JVMOffHeapMemory",
                 "OnHeapExecutionMemory",
                 "OffHeapExecutionMemory",
                 "OnHeapStorageMemory",
                 "OffHeapStorageMemory",
                 "OnHeapUnifiedMemory",
                 "OffHeapUnifiedMemory",
                 "DirectPoolMemory",
                 "MappedPoolMemory",
                 "ProcessTreeJVMVMemory",
                 "ProcessTreeJVMRSSMemory",
                 "ProcessTreePythonVMemory",
                 "ProcessTreePythonRSSMemory",
                 "ProcessTreeOtherVMemory",
                 "ProcessTreeOtherRSSMemory",
                 "MinorGCCount",
                 "MinorGCTime",
                 "MajorGCCount",
                 "MajorGCTime"
               )
               names.foreach { name =>
                 sb.append(s"$prefix${name}_Count$labels ${m.getMetricValue(name)}\n")
               }
             }
           }
           sb.toString
         }
   }
   ```

   ```markdown
   private[spark] object PrometheusResource {
   	操作集:
   	def getServletHandler(uiRoot: UIRoot): ServletContextHandler 
   	功能: 获取servlet处理器
   	val= {
           val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
           jerseyContext.setContextPath("/metrics")
           val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
           holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES,
           	"org.apache.spark.status.api.v1")
           UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
           jerseyContext.addServlet(holder, "/*")
           jerseyContext
         }
   }
   ```

   #### SimpleDateParam

   ```markdown
   private[v1] class SimpleDateParam(val originalValue: String){
   	属性:
   	#name @timestamp #type @Long 	时间戳
   	val= {
           val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz", Locale.US)
           try {
             format.parse(originalValue).getTime() // 获取转换后的时间
           } catch {
             case _: ParseException => // 转换异常处理
               val gmtDay = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
               gmtDay.setTimeZone(TimeZone.getTimeZone("GMT"))
               try {
                 gmtDay.parse(originalValue).getTime() // 获取并返回默认值
               } catch {
                 case _: ParseException => // 抛出异常，设置状态为BAD_REQUEST
                   throw new WebApplicationException(
                     Response
                       .status(Status.BAD_REQUEST)
                       .entity("Couldn't parse date: " + originalValue)
                       .build()
                   )
               }
           }
         }
   }
   ```

   #### StagesResource

   ```markdown
   @Produces(Array(MediaType.APPLICATION_JSON))
   private[v1] class StagesResource extends BaseAppResource{
   	介绍: stage资源
   	操作集:
   	@GET
       def stageList(@QueryParam("status") statuses: JList[StageStatus]): Seq[StageData] = {
           withUI(_.store.stageList(statuses))}
       功能: 获取stage状态@StageStatus列表
   	
   	@GET
       @Path("{stageId: \\d+}")
       def stageData(@PathParam("stageId") stageId: Int,
         @QueryParam("details") @DefaultValue("true") details: Boolean): Seq[StageData]
   	功能: 获取ui的存储器中指定@stageId 数据@StageData列表
   	val= withUI { ui =>
         val ret = ui.store.stageData(stageId, details = details)
         if (ret.nonEmpty) {
           ret
         } else {
           throw new NotFoundException(s"unknown stage: $stageId")
         }
       }
       
       @GET
       @Path("{stageId: \\d+}/{stageAttemptId: \\d+}")
       def oneAttemptData(@PathParam("stageId") stageId: Int,
         @PathParam("stageAttemptId") stageAttemptId: Int,
         @QueryParam("details") @DefaultValue("true") details: Boolean): StageData
   	功能: 获取指定stage编号@stageId，请求编号@stageAttemptId 的stage数据@StageData
   	val= try {
         ui.store.stageAttempt(stageId, stageAttemptId, details = details)._1
       } catch {
         case _: NoSuchElementException =>
           val all = ui.store.stageData(stageId)
           val msg = if (all.nonEmpty) {
             val ids = all.map(_.attemptId)
             s"unknown attempt for stage $stageId.  Found attempts: [${ids.mkString(",")}]"
           } else {
             s"unknown stage: $stageId"
           }
           throw new NotFoundException(msg)
       }
       
       @GET
       @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskSummary")
       def taskSummary(@PathParam("stageId") stageId: Int,
         @PathParam("stageAttemptId") stageAttemptId: Int,
         @DefaultValue("0.05,0.25,0.5,0.75,0.95") @QueryParam("quantiles") quantileString: String)
     	: TaskMetricDistributions
   	功能: 获取指定stage @stageId @stageAttemptId任务度量属性
   	val= withUI { ui =>
           val quantiles = quantileString.split(",").map { s =>
             try {
               s.toDouble
             } catch {
               case nfe: NumberFormatException =>
                 throw new BadParameterException("quantiles", "double", s)
             }
           }
   
           ui.store.taskSummary(stageId, stageAttemptId, quantiles).getOrElse(
             throw new NotFoundException(s"No tasks reported metrics for $stageId / $stageAttemptId
             yet."))
     	}
     	
     	@GET
       @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskList")
       def taskList(
             @PathParam("stageId") stageId: Int,
             @PathParam("stageAttemptId") stageAttemptId: Int,
             @DefaultValue("0") @QueryParam("offset") offset: Int,
             @DefaultValue("20") @QueryParam("length") length: Int,
             @DefaultValue("ID") @QueryParam("sortBy") sortBy: TaskSorting): Seq[TaskData] = {
           withUI(_.store.taskList(stageId, stageAttemptId, offset, length, sortBy)) }
      	功能: 获取指定stage @stageId @stageAttemptId 的任务数据@TaskData列表
      	
      	@GET
       @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskTable")
       def taskTable(
           @PathParam("stageId") stageId: Int,
           @PathParam("stageAttemptId") stageAttemptId: Int,
           @QueryParam("details") @DefaultValue("true") details: Boolean,
           @Context uriInfo: UriInfo):
       HashMap[String, Object]
       功能: 获取指定stage 的任务列表
       val= withUI { ui =>
         val uriQueryParameters = uriInfo.getQueryParameters(true)
         val totalRecords = uriQueryParameters.getFirst("numTasks")
         var isSearch = false
         var searchValue: String = null
         var filteredRecords = totalRecords
         if (uriQueryParameters.getFirst("search[value]") != null &&
           uriQueryParameters.getFirst("search[value]").length > 0) {
           isSearch = true
           searchValue = uriQueryParameters.getFirst("search[value]")
         }
         val _tasksToShow: Seq[TaskData] = doPagination(uriQueryParameters, stageId, stageAttemptId,
           isSearch, totalRecords.toInt)
         val ret = new HashMap[String, Object]()
         if (_tasksToShow.nonEmpty) {
           if (isSearch) {
             val filteredTaskList = filterTaskList(_tasksToShow, searchValue)
             filteredRecords = filteredTaskList.length.toString
             if (filteredTaskList.length > 0) {
               val pageStartIndex = uriQueryParameters.getFirst("start").toInt
               val pageLength = uriQueryParameters.getFirst("length").toInt
               ret.put("aaData", filteredTaskList.slice(
                 pageStartIndex, pageStartIndex + pageLength))
             } else {
               ret.put("aaData", filteredTaskList)
             }
           } else {
             ret.put("aaData", _tasksToShow)
           }
         } else {
           ret.put("aaData", _tasksToShow)
         }
         ret.put("recordsTotal", totalRecords)
         ret.put("recordsFiltered", filteredRecords)
         ret
       }
       
       def doPagination(queryParameters: MultivaluedMap[String, String], stageId: Int,
       	stageAttemptId: Int, isSearch: Boolean, totalRecords: Int): Seq[TaskData]
       功能: 在服务端侧分页，获取任务数据@TaskData列表
       输入参数: 
       	queryParameters	查询参数表
       	stageId	stageID
       	stageAttemptId	stage请求编号
       	isSearch	是否需要查询
       	totalRecords	总记录数量
       1. 获取需要排序的列名称
       var columnNameToSort = queryParameters.getFirst("columnNameToSort")
       // 将logs列的默认为index列排序
       if (columnNameToSort.equalsIgnoreCase("Logs")) {
         columnNameToSort = "Index"
       }
       // 确定是否为升序
       val isAscendingStr = queryParameters.getFirst("order[0][dir]")
       2. 获取分页参数
       var pageStartIndex = 0
       var pageLength = totalRecords
       3. 查询的特殊处理(查询时需要获取表中所有行信息)
       if (!isSearch) {
         pageStartIndex = queryParameters.getFirst("start").toInt
         pageLength = queryParameters.getFirst("length").toInt
       }
       4. 查询返回数据信息
       val=withUI(_.store.taskList(stageId, stageAttemptId, pageStartIndex, pageLength,
         indexName(columnNameToSort), isAscendingStr.equalsIgnoreCase("asc")))
      	
       def filterTaskList(taskDataList: Seq[TaskData],searchValue: String): Seq[TaskData] 
       功能: 根据指定搜索值@searchValue 过滤任务数据列表@taskDataList，获取满足搜索值要求的数据
       1. 获取默认参数和搜索值(low case)
       val defaultOptionString: String = "d"
       val searchValueLowerCase = searchValue.toLowerCase(Locale.ROOT)
       val containsValue = (taskDataParams: Any) => taskDataParams.toString.toLowerCase(
         Locale.ROOT).contains(searchValueLowerCase)
       2. 确定任务度量信息中是否包含搜索值
       val taskMetricsContainsValue = (task: TaskData) => task.taskMetrics match {
         case None => false
         case Some(metrics) =>
           (containsValue(UIUtils.formatDuration(task.taskMetrics.get.executorDeserializeTime))
           || containsValue(UIUtils.formatDuration(task.taskMetrics.get.executorRunTime))
           || containsValue(UIUtils.formatDuration(task.taskMetrics.get.jvmGcTime))
           || containsValue(UIUtils.formatDuration(task.taskMetrics.get.resultSerializationTime))
           || containsValue(Utils.bytesToString(task.taskMetrics.get.memoryBytesSpilled))
           || containsValue(Utils.bytesToString(task.taskMetrics.get.diskBytesSpilled))
           || containsValue(Utils.bytesToString(task.taskMetrics.get.peakExecutionMemory))
           || containsValue(Utils.bytesToString(task.taskMetrics.get.inputMetrics.bytesRead))
           || containsValue(task.taskMetrics.get.inputMetrics.recordsRead)
           || containsValue(Utils.bytesToString(
             task.taskMetrics.get.outputMetrics.bytesWritten))
           || containsValue(task.taskMetrics.get.outputMetrics.recordsWritten)
           || containsValue(UIUtils.formatDuration(
             task.taskMetrics.get.shuffleReadMetrics.fetchWaitTime))
           || containsValue(Utils.bytesToString(
             task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead))
           || containsValue(Utils.bytesToString(
             task.taskMetrics.get.shuffleReadMetrics.localBytesRead +
             task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead))
           || containsValue(task.taskMetrics.get.shuffleReadMetrics.recordsRead)
           || containsValue(Utils.bytesToString(
             task.taskMetrics.get.shuffleWriteMetrics.bytesWritten))
           || containsValue(task.taskMetrics.get.shuffleWriteMetrics.recordsWritten)
           || containsValue(UIUtils.formatDuration(
             task.taskMetrics.get.shuffleWriteMetrics.writeTime / 1000000)))
       }
       2. 获取过滤数据列表
       val filteredTaskDataSequence: Seq[TaskData] = taskDataList.filter(f =>
         (containsValue(f.taskId) || containsValue(f.index) || containsValue(f.attempt)
           || containsValue(UIUtils.formatDate(f.launchTime))
           || containsValue(f.resultFetchStart.getOrElse(defaultOptionString))
           || containsValue(f.executorId) || containsValue(f.host) || containsValue(f.status)
           || containsValue(f.taskLocality) || containsValue(f.speculative)
           || containsValue(f.errorMessage.getOrElse(defaultOptionString))
           || taskMetricsContainsValue(f)
           || containsValue(UIUtils.formatDuration(f.schedulerDelay))
           || containsValue(UIUtils.formatDuration(f.gettingResultTime))))
       val= filteredTaskDataSequence
       
   }
   ```

#### AppHistoryServerPlugin

```scala
private[spark] trait AppHistoryServerPlugin {
	介绍: 这是一个创建历史监听器(用于重现事件日志)的接口，像SQL一样定义在其他模块中，设置插件的UI和重建历史UI
	操作集:
	def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener]
	功能: 创建监听器列表(用于重演事件日志)
	
	def setupUI(ui: SparkUI): Unit
	功能: 设置这个插件的UI和历史信息的UI
	
	def displayOrder: Int = Integer.MAX_VALUE
	功能: 插件标签位置与其他插件标签位置的差距
}
```

####  AppStatusListener

```markdown
介绍:
	spark应用监听器，将信用信息写到数据存储中，写出到数据存储器的类型请参照基于REST Api的@storeTypes 文
```

```scala
private[spark] class AppStatusListener(kvstore: ElementTrackingStore,conf: SparkConf,live: Boolean,
    appStatusSource: Option[AppStatusSource] = None,lastUpdateTime: Option[Long] = None)
	extends SparkListener with Logging {
	构造器参数:
        	kvstore	KV存储器
        	conf	spark配置集
        	live	是否存活
        	appStatusSource	应用状态来源
        	lastUpdateTime	上传更新时间
     属性:
     	#name @sparkVersion = SPARK_VERSION	spark版本
      	#name @appInfo: v1.ApplicationInfo = null	应用信息
        #name @appSummary = new AppSummary(0, 0)	应用描述
        #name @coresPerTask=1	单个任务核心数量
        #name @liveUpdatePeriodNs=if (live) conf.get(LIVE_ENTITY_UPDATE_PERIOD) else -1L	
        	存活更新周期(ns)
        	决定了更新存活实例的周期，-1代表了永远不会更新
        #name @liveUpdateMinFlushPeriod = conf.get(LIVE_ENTITY_UPDATE_MIN_FLUSH_PERIOD)
        	存活实例更新最小刷新周期
        	旧UI数据最小的刷新时间，主要用于解决到来的任务没有定期清除，而导致的UI数据过时的情况
        #name @maxTasksPerStage = conf.get(MAX_RETAINED_TASKS_PER_STAGE)	单个stage最大任务数量
        #name @maxGraphRootNodes = conf.get(MAX_RETAINED_ROOT_NODES)	图最大根节点数量
        #name @liveStages = new ConcurrentHashMap[(Int, Int), LiveStage]()	存活stage表
        #name @liveJobs = new HashMap[Int, LiveJob]()	存活job 表
        #name @liveExecutors = new HashMap[String, LiveExecutor]()	存活执行器表
        #name @deadExecutors = new HashMap[String, LiveExecutor]()	死亡执行器表
        #name @liveTasks = new HashMap[Long, LiveTask]()	存活任务表
        #name @liveRDDs = new HashMap[Int, LiveRDD]()	存活RDD表
        #name @pools = new HashMap[String, SchedulerPool]()	调度池映射表
        #name @SQL_EXECUTION_ID_KEY = "spark.sql.execution.id"	sql执行ID的key值
        #name @activeExecutorCount = 0 volatile		存活执行器数量
        #name @lastFlushTimeNs = System.nanoTime()	上次刷新时间
        初始化操作:
        操作集:
        def onOtherEvent(event: SparkListenerEvent): Unit
        功能: 对事件日志的元数据@SparkListenerEvent 做处理
        event match {
            case SparkListenerLogStart(version) => sparkVersion = version // 指定事件为事件日志元数据
            case _ =>
        }
        
        def onApplicationStart(event: SparkListenerApplicationStart): Unit
        功能: 应用开始处理方式
        1. 断言事件@event 存在
        assert(event.appId.isDefined, "Application without IDs are not supported.")
        2. 获取应用请求信息
        val attempt = v1.ApplicationAttemptInfo(event.appAttemptId,new Date(event.time),new Date(-1),
          new Date(event.time),-1L,event.sparkUser,false,sparkVersion)
        3. 获取应用信息
        appInfo = v1.ApplicationInfo(event.appId.get,event.appName,None,None,None,None,Seq(attempt))
		4. 将应用信息入库@kvStore
        kvstore.write(new ApplicationInfoWrapper(appInfo))
        kvstore.write(appSummary)
        5. 使用本事件@event 的日志更新driver的块管理器(sparkContext在这之前已经将驱动器注册了)
        event.driverLogs.foreach { logs =>
        val driver = liveExecutors.get(SparkContext.DRIVER_IDENTIFIER) // 获取驱动器
        driver.foreach { d =>
            d.executorLogs = logs.toMap  // 获取当前驱动器的执行器日志
            d.attributes = event.driverAttributes.getOrElse(Map.empty).toMap // 获取驱动器的属性列表
            update(d, System.nanoTime()) // 更新执行器日志信息
          }
        }
    	
        def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit
        功能: 环境更新处理方案
        1. 获取相关信息
        val details = event.environmentDetails // 获取@event指定的新的环境描述信息
        val jvmInfo = Map(details("JVM Information"): _*)	// 获取环境中jvm信息
        val runtime = new v1.RuntimeInfo(
            jvmInfo.get("Java Version").orNull,
            jvmInfo.get("Java Home").orNull,
            jvmInfo.get("Scala Version").orNull) // 获取jvm的运行信息
        val envInfo = new v1.ApplicationEnvironmentInfo(
          runtime,
          details.getOrElse("Spark Properties", Nil),
          details.getOrElse("Hadoop Properties", Nil),
          details.getOrElse("System Properties", Nil),
          details.getOrElse("Classpath Entries", Nil))  // 获取应用环境信息
        coresPerTask = envInfo.sparkProperties.toMap.get(CPUS_PER_TASK.key).map(_.toInt)
        	.getOrElse(coresPerTask)  // 获取单个任务核心数量信息
        2. 将环境信息更新到KV存储中
        kvstore.write(new ApplicationEnvironmentInfoWrapper(envInfo))
        
        def onApplicationEnd(event: SparkListenerApplicationEnd): Unit
        功能: 对应于应用结束事件@event 处理方案
        1. 获取旧的应用请求信息
        val old = appInfo.attempts.head
        2. 获取任务结束后的任务请求信息(任务完成标志置位,更新任务持续时间duration)
        val attempt = v1.ApplicationAttemptInfo(
          old.attemptId,
          old.startTime,
          new Date(event.time),
          new Date(event.time),
          event.time - old.startTime.getTime(),
          old.sparkUser,
          true,
          old.appSparkVersion)
        3. 根据请求信息获取
        appInfo = v1.ApplicationInfo(appInfo.id,appInfo.name,None,None,None,None,Seq(attempt))
	    4. 写出到存储系统中
        kvstore.write(new ApplicationInfoWrapper(appInfo)) 
    	
        def onExecutorAdded(event: SparkListenerExecutorAdded): Unit
        功能: 检测到@event 添加执行器处理方案
    		这个在驱动器dead后，执行器重新注册的情况下会更新
        1. 获取一个存活执行器
        val exec = getOrCreateExecutor(event.executorId, event.time)
        2. 从指定检测事件@event 设置执行器的属性值
    	exec.host = event.executorInfo.executorHost
        exec.isActive = true
        exec.totalCores = event.executorInfo.totalCores
        exec.maxTasks = event.executorInfo.totalCores / coresPerTask
        exec.executorLogs = event.executorInfo.logUrlMap
        exec.resources = event.executorInfo.resourcesInfo
        exec.attributes = event.executorInfo.attributes
    	3. 处于启动状态下更新(热更新)执行器
        liveUpdate(exec, System.nanoTime())
        
        def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit
        功能: 处理执行器移除事件
        1. 从执行器列表中移除事件@event中指定的执行器
    	liveExecutors.remove(event.executorId).foreach { exec =>
          // 获取并更新执行器移除相关信息,并更新执行器信息
          val now = System.nanoTime()
          activeExecutorCount = math.max(0, activeExecutorCount - 1)
          exec.isActive = false
          exec.removeTime = new Date(event.time)
          exec.removeReason = event.reason
          update(exec, now, last = true)
          // 移除与移除执行器相关的RDD分布@RDDDistribution 并更新RDD分布信息
          liveRDDs.values.foreach { rdd =>
            if (rdd.removeDistribution(exec)) {
              update(rdd, now)
            }
          }
          // 移除与移除执行器相关的RDD分区 并更新RDD分区列表
          liveRDDs.values.foreach { rdd =>
            rdd.getPartitions.values
              .filter(_.executors.contains(event.executorId))
              .foreach { partition =>
                if (partition.executors.length == 1) {
                  rdd.removePartition(partition.blockName)
                  rdd.memoryUsed = addDeltaToValue(rdd.memoryUsed, partition.memoryUsed * -1)
                  rdd.diskUsed = addDeltaToValue(rdd.diskUsed, partition.diskUsed * -1)
                } else {
                  rdd.memoryUsed = addDeltaToValue(rdd.memoryUsed,
                    (partition.memoryUsed / partition.executors.length) * -1)
                  rdd.diskUsed = addDeltaToValue(rdd.diskUsed,
                    (partition.diskUsed / partition.executors.length) * -1)
                  partition.update(
                    partition.executors.filter(!_.equals(event.executorId)),
                    addDeltaToValue(partition.memoryUsed,
                      (partition.memoryUsed / partition.executors.length) * -1),
                    addDeltaToValue(partition.diskUsed,
                      (partition.diskUsed / partition.executors.length) * -1))
                }
              }
            update(rdd, now)
          }
            // 检测当前执行器是否对于当前stage处于激活状态,如果处于激活状态,则将其加入已死亡的执行器列表
            // @deadExecutors 完成了执行器状态由运行转化为激活状态
          if (isExecutorActiveForLiveStages(exec)) 
            deadExecutors.put(event.executorId, exec)
          }
        }

	def isExecutorActiveForLiveStages(exec: LiveExecutor): Boolean
	功能: 检测指定执行器@LiveExecutor 是否对于当前stage是处于激活状态(根据提交时间和移除时间判断,移除时间在		后则说明当前执行器处于激活状态)
	val= liveStages.values.asScala.exists { stage =>
      stage.info.submissionTime.getOrElse(0L) < exec.removeTime.getTime
    }
	
	def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit
	功能: 检测到黑名单事件，将指定事件@event 加入到黑名单列表中
	updateBlackListStatus(event.executorId, true)
	
	def onExecutorBlacklistedForStage(event: SparkListenerExecutorBlacklistedForStage): Unit
	功能: 检测到stage处于黑名单中
	1. 将当前stage加入黑名单列表
	val now = System.nanoTime()
    Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      setStageBlackListStatus(stage, now, event.executorId) }
	2. 将指定黑名单stage加入到执行器的stage黑名单列表
	liveExecutors.get(event.executorId).foreach { exec =>
      addBlackListedStageTo(exec, event.stageId, now) }
	
	def onNodeBlacklistedForStage(event: SparkListenerNodeBlacklistedForStage): Unit
	功能: 检测到节点stage处于黑名单处理方案
	1. 获取与节点相关的该stage所处的所有执行器，并设置该stage为黑名单状态
	Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      val executorIds = liveExecutors.values.filter(_.host == event.hostId).map(_.executorId).toSeq
      setStageBlackListStatus(stage, now, executorIds: _*)}
	2. 指定节点中添加该stage到执行器中(有且只是事件指定节点)
	liveExecutors.values.filter(_.hostname == event.hostId).foreach { exec =>
      addBlackListedStageTo(exec, event.stageId, now)}
	
	def addBlackListedStageTo(exec: LiveExecutor, stageId: Int, now: Long): Unit
	功能: 将指定stage添加到指定执行器@exec中, 并更新指定执行器信息
	exec.blacklistedInStages += stageId
    liveUpdate(exec, now)
	
	def setStageBlackListStatus(stage: LiveStage, now: Long, executorIds: String*): Unit 
	功能: 设置stage黑名单状态
	1. 设置执行器中id的黑名单状态，如果有更新就去更新执行器描述@executorStageSummary
	executorIds.foreach { executorId =>
      val executorStageSummary = stage.executorSummary(executorId)
      executorStageSummary.isBlacklisted = true
      maybeUpdate(executorStageSummary, now)
    }
	2. 添加以及加入到黑名单的执行器编号，并更新stage信息
	stage.blackListedExecutors ++= executorIds
	maybeUpdate(stage, now)
	
	def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit
	功能: 执行器解除黑名单
	updateBlackListStatus(event.executorId, false)
	
	def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit
	功能: 将节点加入黑名单
	updateNodeBlackList(event.hostId, true)
	
	def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit
	功能: 节点解除黑名单
	updateNodeBlackList(event.hostId, false)
	
	def updateBlackListStatus(execId: String, blacklisted: Boolean): Unit
	功能: 更新指定执行器黑名单状态为指定@blacklisted
	liveExecutors.get(execId).foreach { exec =>
      exec.isBlacklisted = blacklisted
      if (blacklisted) {
        appStatusSource.foreach(_.BLACKLISTED_EXECUTORS.inc())
      } else {
        appStatusSource.foreach(_.UNBLACKLISTED_EXECUTORS.inc())
      }
      liveUpdate(exec, System.nanoTime())
    }
	
	def updateNodeBlackList(host: String, blacklisted: Boolean): Unit
	功能: 更新节点黑名单状态为指定
	val now = System.nanoTime()
    liveExecutors.values.foreach { exec =>
      if (exec.hostname == host) {
        exec.isBlacklisted = blacklisted
        liveUpdate(exec, now)
      }
    }

	def onJobStart(event: SparkListenerJobStart): Unit
	功能: 监听到job开始事件@event 处理措施
	val now = System.nanoTime()
	1. 计算这个job会运行的任务数量
	val numTasks = {
      val missingStages = event.stageInfos.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }
	2. 获取job相关信息
	val lastStageInfo = event.stageInfos.sortBy(_.stageId).lastOption // 获取最后一条stage信息
    val jobName = lastStageInfo.map(_.name).getOrElse("") // 获取job名称 
    val description = Option(event.properties) 
      .flatMap { p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)) } // 获取job描述
    val jobGroup = Option(event.properties)
      .flatMap { p => Option(p.getProperty(SparkContext.SPARK_JOB_GROUP_ID)) } // 获取job所属组
    val sqlExecutionId = Option(event.properties)
      .flatMap(p => Option(p.getProperty(SQL_EXECUTION_ID_KEY)).map(_.toLong)) // 获取sql执行id
	3. 获取存活job实例,并注册到存活job列表中
	val job = new LiveJob(
      event.jobId,
      jobName,
      description,
      if (event.time > 0) Some(new Date(event.time)) else None,
      event.stageIds,
      jobGroup,
      numTasks,
      sqlExecutionId)
     liveJobs.put(event.jobId, job)
	liveUpdate(job, now) // 更新job时间信息
	4. 将job信息加入到stage列表中,并更新
	event.stageInfos.foreach { stageInfo =>
      val stage = getOrCreateStage(stageInfo)
      stage.jobs :+= job
      stage.jobIds += event.jobIdRDDOperationGraphWrapper
      liveUpdate(stage, now)
    }
	5. 为job所有的stage创建RDD操作图包装器@RDDOperationGraphWrapper
	event.stageInfos.foreach { stage =>
      val graph = RDDOperationGraph.makeOperationGraph(stage, maxGraphRootNodes)
      val uigraph = new RDDOperationGraphWrapper(
        stage.stageId,
        graph.edges,
        graph.outgoingEdges,
        graph.incomingEdges,
        newRDDOperationCluster(graph.rootCluster))
      kvstore.write(uigraph)
    }
	
	def newRDDOperationCluster(cluster: RDDOperationCluster): RDDOperationClusterWrapper
	功能: 包装指定RDD集群操作器
	val= new RDDOperationClusterWrapper(cluster.id,cluster.name,cluster.childNodes,
      	cluster.childClusters.map(newRDDOperationCluster))
	
	def onJobEnd(event: SparkListenerJobEnd): Unit
	功能: 监听到job结束的处理方案
	1. 移除事件指定job的job信息
	liveJobs.remove(event.jobId).foreach { job =>
      val now = System.nanoTime()
      // 检查是否与当前要删除job匹配的待定stage,如果有则标志为skipped 不去执行它
      val it = liveStages.entrySet.iterator()
      while (it.hasNext()) {
        val e = it.next()
        if (job.stageIds.contains(e.getKey()._1)) { // 匹配与结束job的stage处理
          val stage = e.getValue() // 获取stage
          // 处于待定且stage需要运行的任务数量大于0 则设置为skipped
          if (v1.StageStatus.PENDING.equals(stage.status) && stage.info.numTasks > 0) {
              // 修改待定stage状态为skipped
            stage.status = v1.StageStatus.SKIPPED
              // 度量信息设置
            job.skippedStages += stage.info.stageId
            job.skippedTasks += stage.info.numTasks
            job.activeStages -= 1
            pools.get(stage.schedulingPool).foreach { pool =>
                //由于这个stage不会执行,所有需要将其从调度池中移除,之后便不会对其进行调度执行
              pool.stageIds = pool.stageIds - stage.info.stageId
              update(pool, now) // 更新调度池信息
            }
            it.remove() // 移除stage
            update(stage, now, last = true) // 更新stage信息
          }
        }
      }
        // 根据job执行结果,对成功任务和失败任务进行计量
      job.status = event.jobResult match {
        case JobSucceeded =>
          appStatusSource.foreach{_.SUCCEEDED_JOBS.inc()}
          JobExecutionStatus.SUCCEEDED
        case JobFailed(_) =>
          appStatusSource.foreach{_.FAILED_JOBS.inc()}
          JobExecutionStatus.FAILED
      }
        // job完成时间度量
      job.completionTime = if (event.time > 0) Some(new Date(event.time)) else None
        // 计算job持续时间
      for {
        source <- appStatusSource
        submissionTime <- job.submissionTime
        completionTime <- job.completionTime
      } {
        source.JOB_DURATION.value.set(completionTime.getTime() - submissionTime.getTime())
      }
        // 更新stage task度量值
      appStatusSource.foreach { source =>
        source.COMPLETED_STAGES.inc(job.completedStages.size)
        source.FAILED_STAGES.inc(job.failedStages)
        source.COMPLETED_TASKS.inc(job.completedTasks)
        source.FAILED_TASKS.inc(job.failedTasks)
        source.KILLED_TASKS.inc(job.killedTasks)
        source.SKIPPED_TASKS.inc(job.skippedTasks)
        source.SKIPPED_STAGES.inc(job.skippedStages.size)
      }
        // 更新job状态
      update(job, now, last = true)
        // 成功执行则将应用描述入库@kvStore
      if (job.status == JobExecutionStatus.SUCCEEDED) {
        appSummary = new AppSummary(appSummary.numCompletedJobs + 1, appSummary.numCompletedStages)
        kvstore.write(appSummary)
      }
    }

	def onStageSubmitted(event: SparkListenerStageSubmitted): Unit
	功能: stage提交事件处理
	1. 创建stage	
	val now = System.nanoTime()
    val stage = getOrCreateStage(event.stageInfo)
	2. 设置stage参数值
	stage.status = v1.StageStatus.ACTIVE
    stage.schedulingPool = Option(event.properties).flatMap { p =>
      Option(p.getProperty(SparkContext.SPARK_SCHEDULER_POOL))
    }.getOrElse(SparkUI.DEFAULT_POOL_NAME)
    stage.jobs = liveJobs.values
      .filter(_.stageIds.contains(event.stageInfo.stageId))
      .toSeq
    stage.jobIds = stage.jobs.map(_.jobId).toSet
    stage.description = Option(event.properties).flatMap { p =>
      Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }
    stage.jobs.foreach { job =>
      job.completedStages = job.completedStages - event.stageInfo.stageId
      job.activeStages += 1
      liveUpdate(job, now)
    }
	2. 更新调度池信息
	val pool = pools.getOrElseUpdate(stage.schedulingPool, new SchedulerPool(stage.schedulingPool))
    pool.stageIds = pool.stageIds + event.stageInfo.stageId
    update(pool, now)
	3. 更新RDD信息
	event.stageInfo.rddInfos.foreach { info =>
      if (info.storageLevel.isValid) {
        liveUpdate(liveRDDs.getOrElseUpdate(info.id, new LiveRDD(info, info.storageLevel)), now)
      }
    }
	4. 更新stage信息
	liveUpdate(stage, now)

	def onTaskStart(event: SparkListenerTaskStart): Unit
	功能: 任务开始事件处理
	1. 创建存活任务，并注册到存活任务列表中，并更新任务信息
	val now = System.nanoTime()
    val task = new LiveTask(event.taskInfo, event.stageId, event.stageAttemptId, lastUpdateTime)
    liveTasks.put(event.taskInfo.taskId, task)
    liveUpdate(task, now)
	2. 获取事件指定stage编号，并进行处理
	Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      // 设置stage度量信息,并更新
      stage.activeTasks += 1
      stage.firstLaunchTime = math.min(stage.firstLaunchTime, event.taskInfo.launchTime)
      val locality = event.taskInfo.taskLocality.toString()
      val count = stage.localitySummary.getOrElse(locality, 0L) + 1L
      stage.localitySummary = stage.localitySummary ++ Map(locality -> count)
      stage.activeTasksPerExecutor(event.taskInfo.executorId) += 1
      maybeUpdate(stage, now)
      // 更新job度量信息,并更新
      stage.jobs.foreach { job =>
        job.activeTasks += 1
        maybeUpdate(job, now)
      }
	  // 检测是否需要清空stage 如果保存的任务足够多则对其进行清理
      if (stage.savedTasks.incrementAndGet() > maxTasksPerStage && !stage.cleaning) {
        stage.cleaning = true
        kvstore.doAsync {
          cleanupTasks(stage)
        }
      }
    }
	3. 设置事件指定执行器度量值,并更新
	liveExecutors.get(event.taskInfo.executorId).foreach { exec =>
      exec.activeTasks += 1
      exec.totalTasks += 1
      maybeUpdate(exec, now)
    }
	
	def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit
	功能: 获取任务结果事件
	liveTasks.get(event.taskInfo.taskId).foreach { task =>
      maybeUpdate(task, System.nanoTime())
    }
	
	def onTaskEnd(event: SparkListenerTaskEnd): Unit
	功能: 任务结束事件处理
	1. 获取任务度量的更新增量
	val metricsDelta = liveTasks.remove(event.taskInfo.taskId).map { task =>
      task.info = event.taskInfo
      val errorMessage = event.reason match {
        case Success =>
          None
        case k: TaskKilled =>
          Some(k.reason)
        case e: ExceptionFailure => 
          Some(e.toErrorString)
        case e: TaskFailedReason =>
          Some(e.toErrorString)
        case other =>
          logInfo(s"Unhandled task end reason: $other")
          None
      }
      task.errorMessage = errorMessage
      val delta = task.updateMetrics(event.taskMetrics)
      update(task, now, last = true)
      delta
    }.orNull
	2. 获取增量因子三元组
	val (completedDelta, failedDelta, killedDelta) = event.reason match {
      case Success =>
        (1, 0, 0)
      case _: TaskKilled =>
        (0, 0, 1)
      case _: TaskCommitDenied =>
        (0, 0, 1)
      case _ =>
        (0, 1, 0)
    }
	3. 更新stage度量值
	Option(liveStages.get((event.stageId, event.stageAttemptId))).foreach { stage =>
      if (metricsDelta != null) {
        stage.metrics = LiveEntityHelpers.addMetrics(stage.metrics, metricsDelta)
      }
      stage.activeTasks -= 1
      stage.completedTasks += completedDelta
      if (completedDelta > 0) {
        stage.completedIndices.add(event.taskInfo.index)
      }
      stage.failedTasks += failedDelta
      stage.killedTasks += killedDelta
      if (killedDelta > 0) {
        stage.killedSummary = killedTasksSummary(event.reason, stage.killedSummary)
      }
      stage.activeTasksPerExecutor(event.taskInfo.executorId) -= 1
      val removeStage =
        stage.activeTasks == 0 &&
          (v1.StageStatus.COMPLETE.equals(stage.status) ||
            v1.StageStatus.FAILED.equals(stage.status))
      if (removeStage) {
        update(stage, now, last = true)
      } else {
        maybeUpdate(stage, now)
      }
      val taskIndex = (event.stageId.toLong << Integer.SIZE) | event.taskInfo.index
      stage.jobs.foreach { job =>
        job.activeTasks -= 1
        job.completedTasks += completedDelta
        if (completedDelta > 0) {
          job.completedIndices.add(taskIndex)
        }
        job.failedTasks += failedDelta
        job.killedTasks += killedDelta
        if (killedDelta > 0) {
          job.killedSummary = killedTasksSummary(event.reason, job.killedSummary)
        }
        if (removeStage) {
          update(job, now)
        } else {
          maybeUpdate(job, now)
        }
      }
      val esummary = stage.executorSummary(event.taskInfo.executorId)
      esummary.taskTime += event.taskInfo.duration
      esummary.succeededTasks += completedDelta
      esummary.failedTasks += failedDelta
      esummary.killedTasks += killedDelta
      if (metricsDelta != null) {
        esummary.metrics = LiveEntityHelpers.addMetrics(esummary.metrics, metricsDelta)
      }
      val isLastTask = stage.activeTasksPerExecutor(event.taskInfo.executorId) == 0
      if (isLastTask) {
        update(esummary, now)
      } else {
        maybeUpdate(esummary, now)
      }
      if (!stage.cleaning && stage.savedTasks.get() > maxTasksPerStage) {
        stage.cleaning = true
        kvstore.doAsync {
          cleanupTasks(stage)
        }
      }
      if (removeStage) {
        liveStages.remove((event.stageId, event.stageAttemptId))
      }
    }
	4. 更新执行器度量值
	liveExecutors.get(event.taskInfo.executorId).foreach { exec =>
      exec.activeTasks -= 1
      exec.completedTasks += completedDelta
      exec.failedTasks += failedDelta
      exec.totalDuration += event.taskInfo.duration
      if (event.reason != Resubmitted) {
        if (event.taskMetrics != null) {
          val readMetrics = event.taskMetrics.shuffleReadMetrics
          exec.totalGcTime += event.taskMetrics.jvmGCTime
          exec.totalInputBytes += event.taskMetrics.inputMetrics.bytesRead
          exec.totalShuffleRead += readMetrics.localBytesRead + readMetrics.remoteBytesRead
          exec.totalShuffleWrite += event.taskMetrics.shuffleWriteMetrics.bytesWritten
        }
      }
      if (exec.activeTasks == 0) {
        update(exec, now)
      } else {
        maybeUpdate(exec, now)
      }
    }

	def removeBlackListedStageFrom(exec: LiveExecutor, stageId: Int, now: Long): Unit
	功能: 从指定执行器中移除编号为@stageId的黑名单信息
	exec.blacklistedInStages -= stageId
    liveUpdate(exec, now)

	def onStageCompleted(event: SparkListenerStageCompleted): Unit
	功能: stage完成处理
	1. 获取事件指定的stage
	val maybeStage =Option(liveStages.get((event.stageInfo.stageId, event.stageInfo.attemptNumber)))
	2. stage属性设置为事件指定
	maybeStage.foreach { stage =>
      val now = System.nanoTime()
      stage.info = event.stageInfo
      stage.executorSummaries.values.foreach(update(_, now))
      stage.status = event.stageInfo.failureReason match {
        case Some(_) => v1.StageStatus.FAILED
        case _ if event.stageInfo.submissionTime.isDefined => v1.StageStatus.COMPLETE
        case _ => v1.StageStatus.SKIPPED
      }
        // 有stage信息设置job信息
      stage.jobs.foreach { job =>
        stage.status match {
          case v1.StageStatus.COMPLETE =>
            job.completedStages += event.stageInfo.stageId
          case v1.StageStatus.SKIPPED =>
            job.skippedStages += event.stageInfo.stageId
            job.skippedTasks += event.stageInfo.numTasks
          case _ =>
            job.failedStages += 1
        }
        job.activeStages -= 1
        liveUpdate(job, now)
      }
		// 设置调度池信息
      pools.get(stage.schedulingPool).foreach { pool =>
        pool.stageIds = pool.stageIds - event.stageInfo.stageId
        update(pool, now)
      }
		// 获取并移除黑名单执行器stage
      val executorIdsForStage = stage.blackListedExecutors
      executorIdsForStage.foreach { executorId =>
        liveExecutors.get(executorId).foreach { exec =>
          removeBlackListedStageFrom(exec, event.stageInfo.stageId, now)
        }
      }
	// 仅在没有激活任务的情况下移除stage
      val removeStage = stage.activeTasks == 0
      update(stage, now, last = removeStage)
      if (removeStage) {// 移除stage
        liveStages.remove((event.stageInfo.stageId, event.stageInfo.attemptNumber))
      }
      if (stage.status == v1.StageStatus.COMPLETE) { // 将完成的stage写出到KV存储器中
        appSummary = new AppSummary(appSummary.numCompletedJobs, appSummary.numCompletedStages + 1)
        kvstore.write(appSummary)
      }
    }

	def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit
	功能: 处理添加块管理器事件
	1. 获取事件指定的执行器
	val exec = getOrCreateExecutor(event.blockManagerId.executorId, event.time)
	2. 设置执行器参数
	exec.hostPort = event.blockManagerId.hostPort
    event.maxOnHeapMem.foreach { _ =>
      exec.totalOnHeap = event.maxOnHeapMem.get
      exec.totalOffHeap = event.maxOffHeapMem.get
    }
    exec.isActive = true
    exec.maxMemory = event.maxMem
	3. 更新执行器
	liveUpdate(exec, System.nanoTime()
               
    def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit
    功能: 移除块管理器事件
            
    def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit
    功能: 解除RDD的持久化
    1. 从存活RDD列表中移除事件指定RDD
    liveRDDs.remove(event.rddId).foreach { liveRDD =>
      val storageLevel = liveRDD.info.storageLevel
        // 使用RDD分区更新执行器块信息
      liveRDD.getPartitions().foreach { case (_, part) =>
        part.executors.foreach { executorId => // 更新执行器块信息
          liveExecutors.get(executorId).foreach { exec =>
            exec.rddBlocks = exec.rddBlocks - 1
          }
        }
      }
      val now = System.nanoTime()
        // 使用RDD分布来更新内存使用和磁盘使用信息
      liveRDD.getDistributions().foreach { case (executorId, rddDist) =>
        liveExecutors.get(executorId).foreach { exec =>
          if (exec.hasMemoryInfo) {
            if (storageLevel.useOffHeap) {
              exec.usedOffHeap = addDeltaToValue(exec.usedOffHeap, -rddDist.offHeapUsed)
            } else {
              exec.usedOnHeap = addDeltaToValue(exec.usedOnHeap, -rddDist.onHeapUsed)
            }
          }
            // 增量式更新内存/磁盘使用量
          exec.memoryUsed = addDeltaToValue(exec.memoryUsed, -rddDist.memoryUsed)
          exec.diskUsed = addDeltaToValue(exec.diskUsed, -rddDist.diskUsed)
          maybeUpdate(exec, now)
        }
      }
    }      
    2. 删除事件指定的RDD存储信息
    kvstore.delete(classOf[RDDStorageInfoWrapper], event.rddId)         	
    
    def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit
    功能: 处理执行器度量器更新事件
    1. 更新累加器参数@accumUpdates 设置           
    val now = System.nanoTime()
    event.accumUpdates.foreach { case (taskId, sid, sAttempt, accumUpdates) =>
      liveTasks.get(taskId).foreach { task =>
        val metrics = TaskMetrics.fromAccumulatorInfos(accumUpdates)
        val delta = task.updateMetrics(metrics)
        maybeUpdate(task, now)

        Option(liveStages.get((sid, sAttempt))).foreach { stage =>
          stage.metrics = LiveEntityHelpers.addMetrics(stage.metrics, delta)
          maybeUpdate(stage, now)

          val esummary = stage.executorSummary(event.execId)
          esummary.metrics = LiveEntityHelpers.addMetrics(esummary.metrics, delta)
          maybeUpdate(esummary, now)
        }
      }
    }
    2. 更新执行器度量器列表@executorUpdates
       检查当前存活UI是否有执行器等级的内存度量器的峰值
    event.executorUpdates.foreach { case (_, peakUpdates) =>
      liveExecutors.get(event.execId).foreach { exec =>
        if (exec.peakExecutorMetrics.compareAndUpdatePeakValues(peakUpdates)) {
          maybeUpdate(exec, now)
        }
      }
    }
   3. 刷新更新,执行器心跳是一个周期性发生的动作,刷新它们,目的是保证sparkUI的持续时间不会超过
      @max(heartbeat interval, liveUpdateMinFlushPeriod)
   if (now - lastFlushTimeNs > liveUpdateMinFlushPeriod) {
      flush(maybeUpdate(_, now))
      lastFlushTimeNs = System.nanoTime()}
   
   def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit
   功能: stage执行器度量器监听事件处理
   liveExecutors.get(executorMetrics.execId).orElse(
      deadExecutors.get(executorMetrics.execId)).foreach { exec =>
        if (exec.peakExecutorMetrics.compareAndUpdatePeakValues(executorMetrics.executorMetrics)) {
          update(exec, now)
        }
    }
               
   def onBlockUpdated(event: SparkListenerBlockUpdated): Unit
   功能: 监听块更新时间
   event.blockUpdatedInfo.blockId match {
      case block: RDDBlockId => updateRDDBlock(event, block) // 使用RDD更新块策略
      case stream: StreamBlockId => updateStreamBlock(event, stream) // 使用流式块更新策略
      case broadcast: BroadcastBlockId => updateBroadcastBlock(event, broadcast) // 使用广播变量块策略
      case _ =>
    }
               
   def flush(entityFlushFunc: LiveEntity => Unit): Unit
   功能: 遍历所有存活实体，并使用@entityFlushFunc(entity) 去刷新它们
   输入参数: entityFlushFunc	实体刷新函数
    liveStages.values.asScala.foreach { stage =>
      entityFlushFunc(stage)
      stage.executorSummaries.values.foreach(entityFlushFunc)
    }
    liveJobs.values.foreach(entityFlushFunc)
    liveExecutors.values.foreach(entityFlushFunc)
    liveTasks.values.foreach(entityFlushFunc)
    liveRDDs.values.foreach(entityFlushFunc)
    pools.values.foreach(entityFlushFunc)
    
    def activeStages(): Seq[v1.StageData]
    功能: 在一个运行应用中获取激活状态的stage的捷径方法,用于控制台的进度条
    val = liveStages.values.asScala
      .filter(_.info.submissionTime.isDefined)
      .map(_.toApi())
      .toList
      .sortBy(_.stageId)
               
   def addDeltaToValue(old: Long, delta: Long): Long = math.max(0, old + delta)
   功能: 增量式增加delta，但是结果值不能为负数
               
   def updateRDDBlock(event: SparkListenerBlockUpdated, block: RDDBlockId): Unit
   功能: 接受RDD更新事件，更新RDD块信息
   1. 获取执行器度量信息
   val now = System.nanoTime()
   val executorId = event.blockUpdatedInfo.blockManagerId.executorId
   val storageLevel = event.blockUpdatedInfo.storageLevel
   val diskDelta = event.blockUpdatedInfo.diskSize * (if (storageLevel.useDisk) 1 else -1)
   val memoryDelta = event.blockUpdatedInfo.memSize * (if (storageLevel.useMemory) 1 else -1)
   2. 更新执行器内存和磁盘度量值
   val maybeExec = liveExecutors.get(executorId)
   var rddBlocksDelta = 0
   maybeExec.foreach { exec =>
      updateExecutorMemoryDiskInfo(exec, storageLevel, memoryDelta, diskDelta) }
   3. 更新RDD信息中的块信息，保证对增量值的追踪，方便对执行器信息的更新
   liveRDDs.get(block.rddId).foreach { rdd =>
      // 获取分区和执行器信息
      val partition = rdd.partition(block.name)
      val executors = if (storageLevel.isValid) {
        val current = partition.executors
        if (current.contains(executorId)) {
          current
        } else {
          rddBlocksDelta = 1
          current :+ executorId
        }
      } else {
        rddBlocksDelta = -1
        partition.executors.filter(_ != executorId)
      }
      if (executors.nonEmpty) {
        // 分区更新
        partition.update(executors,
          addDeltaToValue(partition.memoryUsed, memoryDelta),
          addDeltaToValue(partition.diskUsed, diskDelta))
      } else {
        // 没有执行器包含该分区,应当避免更新操作
        rdd.removePartition(block.name)
      }
      // 执行器采用增量更新,更新执行器信息
      maybeExec.foreach { exec =>
        if (exec.rddBlocks + rddBlocksDelta > 0) {
          val dist = rdd.distribution(exec)
          dist.memoryUsed = addDeltaToValue(dist.memoryUsed, memoryDelta)
          dist.diskUsed = addDeltaToValue(dist.diskUsed, diskDelta)
          if (exec.hasMemoryInfo) {
            if (storageLevel.useOffHeap) {
              dist.offHeapUsed = addDeltaToValue(dist.offHeapUsed, memoryDelta)
            } else {
              dist.onHeapUsed = addDeltaToValue(dist.onHeapUsed, memoryDelta)
            }
          }
          dist.lastUpdate = null
        } else {
          rdd.removeDistribution(exec)
        }
        liveRDDs.values.foreach { otherRdd =>
          if (otherRdd.info.id != block.rddId) {
            otherRdd.distributionOpt(exec).foreach { dist =>
              dist.lastUpdate = null
              update(otherRdd, now)
            }
          }
        }
      }
      // 计算rdd 内存和磁盘使用量
      rdd.memoryUsed = addDeltaToValue(rdd.memoryUsed, memoryDelta)
      rdd.diskUsed = addDeltaToValue(rdd.diskUsed, diskDelta)
      // 更新rdd信息
      update(rdd, now)
    }
    4. 更新执行器中的rdd数量计量信息,并更新
    maybeExec.foreach { exec =>
      exec.rddBlocks += rddBlocksDelta
      maybeUpdate(exec, now)
    }
               
    def getOrCreateExecutor(executorId: String, addTime: Long): LiveExecutor
    功能: 创建执行器
    val= liveExecutors.getOrElseUpdate(executorId, {
      activeExecutorCount += 1
      new LiveExecutor(executorId, addTime)
    })
               
    def updateStreamBlock(event: SparkListenerBlockUpdated, stream: StreamBlockId): Unit
    功能: 根据事件@event 更新流式数据块@StreamBlockId
    1. 获取存储等级
    val storageLevel = event.blockUpdatedInfo.storageLevel
    2. 获取流式数据块并进行处理
    if (storageLevel.isValid) { // 存储等级可以使用,获取数据并写入到KV存储器中
      val data = new StreamBlockData(
        stream.name,
        event.blockUpdatedInfo.blockManagerId.executorId,
        event.blockUpdatedInfo.blockManagerId.hostPort,
        storageLevel.description,
        storageLevel.useMemory,
        storageLevel.useDisk,
        storageLevel.deserialized,
        event.blockUpdatedInfo.memSize,
        event.blockUpdatedInfo.diskSize)
      kvstore.write(data)
    } else {// 当前存储等级不可以使用,删除KV库中指定类名的条目
      kvstore.delete(classOf[StreamBlockData],
        Array(stream.name, event.blockUpdatedInfo.blockManagerId.executorId))
    }
               
    def updateBroadcastBlock(event: SparkListenerBlockUpdated,broadcast: BroadcastBlockId): Unit
    功能: 更新指定的广播变量数据块@broadcast
    1. 获取执行器信息
     val executorId = event.blockUpdatedInfo.blockManagerId.executorId
    2. 获取指定执行器，更新内存磁盘使用量
    liveExecutors.get(executorId).foreach { exec =>
      val now = System.nanoTime()
      val storageLevel = event.blockUpdatedInfo.storageLevel
      val diskDelta = event.blockUpdatedInfo.diskSize * (if (storageLevel.useDisk) 1 else -1)
      val memoryDelta = event.blockUpdatedInfo.memSize * (if (storageLevel.useMemory) 1 else -1)
      updateExecutorMemoryDiskInfo(exec, storageLevel, memoryDelta, diskDelta)
      maybeUpdate(exec, now)
    }
               
    def updateExecutorMemoryDiskInfo(exec: LiveExecutor,storageLevel: StorageLevel,
      memoryDelta: Long,diskDelta: Long): Unit
    功能: 增量式更新执行器的内存磁盘使用量信息
    if (exec.hasMemoryInfo) {
      if (storageLevel.useOffHeap) {
        exec.usedOffHeap = addDeltaToValue(exec.usedOffHeap, memoryDelta)
      } else {
        exec.usedOnHeap = addDeltaToValue(exec.usedOnHeap, memoryDelta)
      }
    }
    exec.memoryUsed = addDeltaToValue(exec.memoryUsed, memoryDelta)
    exec.diskUsed = addDeltaToValue(exec.diskUsed, diskDelta)

    def getOrCreateStage(info: StageInfo): LiveStage
    功能: 获取/创建一个stage实例
    val stage = liveStages.computeIfAbsent((info.stageId, info.attemptNumber),
      (_: (Int, Int)) => new LiveStage())
    stage.info = info
    val = stage
    
    def killedTasksSummary(reason: TaskEndReason,oldSummary: Map[String, Int]): Map[String, Int]
    功能: 获取kill掉的任务描述
    val= reason match {
      case k: TaskKilled =>
        oldSummary.updated(k.reason, oldSummary.getOrElse(k.reason, 0) + 1)
      case denied: TaskCommitDenied =>
        val reason = denied.toErrorString
        oldSummary.updated(reason, oldSummary.getOrElse(reason, 0) + 1)
      case _ =>
        oldSummary
    }
    
    def update(entity: LiveEntity, now: Long, last: Boolean = false): Unit
    功能: 更新实例到kv存储器中
    输入参数: 
        entity	实例
        now	现在时刻
        last 是否为最后一次更新
    entity.write(kvstore, now, checkTriggers = last)
               
    def maybeUpdate(entity: LiveEntity, now: Long): Unit
    功能: 选择性更新实例
    if (live && liveUpdatePeriodNs >= 0 && now - entity.lastWriteTime > liveUpdatePeriodNs)
        update(entity, now)
               
    def liveUpdate(entity: LiveEntity, now: Long): Unit
    功能: 存活更新,存活的实例才能更新
    if (live) {
      update(entity, now)
    }
               
    def cleanupExecutors(count: Long): Unit
    功能: 清除执行器
    1. 计算死亡的执行器数量
    val threshold = conf.get(MAX_RETAINED_DEAD_EXECUTORS)
    val dead = count - activeExecutorCount
    2. 超出容量则计算需要清理的执行器数量,并执行清除(从KV存储器中)
    if (dead > threshold) {
      val countToDelete = calculateNumberToRemove(dead, threshold)
      val toDelete = kvstore.view(classOf[ExecutorSummaryWrapper]).index("active")
        .max(countToDelete).first(false).last(false).asScala.toSeq
      toDelete.foreach { e => kvstore.delete(e.getClass(), e.info.id) }
    }
        
    def cleanupJobs(count: Long): Unit
    功能: 清除job
    val countToDelete = calculateNumberToRemove(count, conf.get(MAX_RETAINED_JOBS))
    if (countToDelete <= 0L) {
      return
    }
    val view = kvstore.view(classOf[JobDataWrapper]).index("completionTime").first(0L)
    val toDelete = KVUtils.viewToSeq(view, countToDelete.toInt) { j =>
      j.info.status != JobExecutionStatus.RUNNING && j.info.status != JobExecutionStatus.UNKNOWN
    }
    toDelete.foreach { j => kvstore.delete(j.getClass(), j.info.jobId) }
     
    def cleanupCachedQuantiles(stageKey: Array[Int]): Unit
    功能: 清除缓存分割点
    val cachedQuantiles = kvstore.view(classOf[CachedQuantile])
      .index("stage")
      .first(stageKey)
      .last(stageKey)
      .asScala
      .toList
    cachedQuantiles.foreach { q =>
      kvstore.delete(q.getClass(), q.id)
    }
               
    def cleanupTasks(stage: LiveStage): Unit
    功能: 清除任务
    1. 计算需要清除的任务数量
    val countToDelete = calculateNumberToRemove(stage.savedTasks.get(), maxTasksPerStage).toInt
    2. 清除任务
    if (countToDelete > 0) {
      val stageKey = Array(stage.info.stageId, stage.info.attemptNumber)
      val view = kvstore.view(classOf[TaskDataWrapper])// 获取stagekey的任务数据视图
        .index(TaskIndexNames.COMPLETION_TIME)
        .parent(stageKey)
      val toDelete = KVUtils.viewToSeq(view, countToDelete) { t => 
        !live || t.status != TaskState.RUNNING.toString()
      }
      toDelete.foreach { t => kvstore.delete(t.getClass(), t.taskId) }// 删除指定数量的条目
      stage.savedTasks.addAndGet(-toDelete.size)
      val remaining = countToDelete - toDelete.size
      if (remaining > 0) { // 这种情况极度稀少一般情况下 remain <<=0 
        val runningTasksToDelete = view.max(remaining).iterator().asScala.toList
        runningTasksToDelete.foreach { t => kvstore.delete(t.getClass(), t.taskId) }
        stage.savedTasks.addAndGet(-remaining)
      }
      if (live) { // 激活任务中需要清理分割点
        cleanupCachedQuantiles(stageKey)
      }
    }
               
     def calculateNumberToRemove(dataSize: Long, retainedSize: Long): Long
     功能: 计算需要移除数量
     输入数据:
     	dataSize	数据规模大小
        retainedSize	剩余规模大小
     val= if (dataSize > retainedSize) {
      math.max(retainedSize / 10L, dataSize - retainedSize)
    } else {
      0L
    }
}
```

#### AppStatusSource

```scala
private [spark] class JobDuration(val value: AtomicLong) extends Gauge[Long] {
	介绍: job持续时间
	操作集:
	def getValue: Long = value.get()
	功能: 获取job的持续时间
}
```

```markdown
private[spark] class AppStatusSource extends Source {
	功能: APP状态资源
	属性:
	#name @metricRegistry = new MetricRegistry()	度量值注册器
	#name @sourceName = "appStatus"	资源名称
	#name @jobDuration = new JobDuration(new AtomicLong(0L))	job持续时间
	#name @JOB_DURATION = metricRegistry.register(MetricRegistry.name("jobDuration"), jobDuration)
		job持续时间注册信息
	#name @FAILED_STAGES = getCounter("stages", "failedStages")  失败stage数量
	#name @SKIPPED_STAGES= getCounter("stages", "skippedStages") 跳过的stage数量
	#name @COMPLETED_STAGES = getCounter("stages", "completedStages") 完成的stage数量
	#name @SUCCEEDED_JOBS = getCounter("jobs", "succeededJobs") 执行成功的job数量
	#name @FAILED_JOBS = getCounter("jobs", "failedJobs") 执行失败的job数量
	#name @COMPLETED_TASKS = getCounter("tasks", "completedTasks") 已完成的任务数量
	#name @FAILED_TASKS = getCounter("tasks", "failedTasks")	失败任务数量
	#name @KILLED_TASKS = getCounter("tasks", "killedTasks")	kill掉的任务数量
	#name @SKIPPED_TASKS = getCounter("tasks", "skippedTasks") 跳过任务数量
	#name @BLACKLISTED_EXECUTORS = getCounter("tasks", "blackListedExecutors")	黑名单执行器信息
	#name @UNBLACKLISTED_EXECUTORS=getCounter("tasks", "unblackListedExecutors") 未纳入黑名单执行器信息
}
```

```scala
private[spark] object AppStatusSource {
	操作集:
	def getCounter(prefix: String, name: String)(implicit metricRegistry: MetricRegistry): Counter
	功能: 获取指定counter信息
	val= metricRegistry.counter(MetricRegistry.name(prefix, name))
	
	def createSource(conf: SparkConf): Option[AppStatusSource]
	功能: 创建指定配置@conf的应用状态源@AppStatusSource
	val= Option(conf.get(METRICS_APP_STATUS_SOURCE_ENABLED))
      .filter(identity)
      .map { _ => new AppStatusSource() }
}
```

#### AppStatusStore

```markdown
介绍:
	这是对于KV存储器@KVStore 的一层包装，用于提供获取存储的API 数据的方法
```

```scala
private[spark] class AppStatusStore(val store: KVStore,
    val listener: Option[AppStatusListener] = None){
    介绍: 内存存储器
    构造器参数:
    	store	KV存储器
    	listener	应用状态监听器
    操作集:
    def applicationInfo(): v1.ApplicationInfo
    功能: 获取应用信息(从KV存储器视图中获取一个应用信息包装器@ApplicationInfoWrapper)
    val= store.view(classOf[ApplicationInfoWrapper]).max(1).iterator().next().info
	
    def environmentInfo(): v1.ApplicationEnvironmentInfo
    功能: 获取应用环境信息@ApplicationEnvironmentInfo
    1. 获取类对象
    val klass = classOf[ApplicationEnvironmentInfoWrapper]
    2. 读取指定类@klass 的应用环境信息包装类
    val= store.read(klass, klass.getName()).info
    
    def jobsList(statuses: JList[JobExecutionStatus]): Seq[v1.JobData]
    功能: 获取job数据列表
    1. 从job数据视图获取job数据列表
    val it = store.view(classOf[JobDataWrapper]).reverse().asScala.map(_.info)
	2. 获取与指定状态匹配的job加入列表
    val= if (statuses != null && !statuses.isEmpty()) {
      it.filter { job => statuses.contains(job.status) }.toSeq
    } else {
      it.toSeq
    }
    
    def job(jobId: Int): v1.JobData
    功能: 获取指定job@jobId的 job数据
    val= store.read(classOf[JobDataWrapper], jobId).info
    
    def jobWithAssociatedSql(jobId: Int): (v1.JobData, Option[Long]) 
    功能: 获取指定job@jobId job数据，并携带管理sql编号，如果没有相关sql，则第二项为None
	1. 获取job数据
    val data = store.read(classOf[JobDataWrapper], jobId)
    2. 返回job及sql信息
    val= (data.info, data.sqlExecutionId)
    
    def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary]
    功能: 获取执行器列表,如果activeOnly=true，则过滤出激活的执行器
    1. 获取执行器视图
    val base = store.view(classOf[ExecutorSummaryWrapper])
    2. 过滤满足要求的执行器信息
    val filtered = if (activeOnly) {
      base.index("active").reverse().first(true).last(true)
    } else {
      base
    }
    3. 获取执行器列表
    val= filtered.asScala.map(_.info).toSeq
    
    def executorSummary(executorId: String): v1.ExecutorSummary
    功能: 获取API 执行器描述@ExecutorSummary
    val= store.read(classOf[ExecutorSummaryWrapper], executorId).info
    
    def activeStages(): Seq[v1.StageData]
    功能: 获取激活状态中的stage数据@StageData列表
    	用于控制台进度条快速获取并画出进度情况。仅仅当运行应用调用时才会返回有用的数据。
    
    def stageList(statuses: JList[v1.StageStatus]): Seq[v1.StageData]
    功能: 通过指定过滤条件@statuses 过滤并获取stage数据信息@StageData 列表
    1. 获取stage信息的视图
    val it = store.view(classOf[StageDataWrapper]).reverse().asScala.map(_.info)
    2. 过滤出需要的数据
    if (statuses != null && !statuses.isEmpty()) {
      it.filter { s => statuses.contains(s.status) }.toSeq
    } else {
      it.toSeq
    }
    
    def stageData(stageId: Int, details: Boolean = false): Seq[v1.StageData]
    功能: 获取指定stageID的数量信息列表
    val= store.view(classOf[StageDataWrapper]).index("stageId").first(stageId).last(stageId)
      .asScala.map { s =>
        if (details) stageWithDetails(s.info) else s.info
      }.toSeq
    
    def lastStageAttempt(stageId: Int): v1.StageData
    功能: 获取上一个stage请求数据信息@StageData
    1. 从视图中获取指定@stageId 的stage数据集
    val it = store.view(classOf[StageDataWrapper])
      .index("stageId")
      .reverse()
      .first(stageId)
      .last(stageId)
      .closeableIterator()
    2. 返回数据信息
    val= try {
      if (it.hasNext()) {
        it.next().info
      } else {
        throw new NoSuchElementException(s"No stage with id $stageId")
      }
    } finally {
      it.close()
    }
    
    def stageAttempt(stageId: Int, stageAttemptId: Int,
      details: Boolean = false): (v1.StageData, Seq[Int])
	功能: 获取指定@stageId 的stage数据信息(stage数据,所处job列表)
    1. 获取唯一的stage编号
    val stageKey = Array(stageId, stageAttemptId)
    2. 获取stage数据新
    val stage = if (details) stageWithDetails(stageDataWrapper.info) else stageDataWrapper.info
    val= (stage, stageDataWrapper.jobIds.toSeq)
    
    def taskCount(stageId: Int, stageAttemptId: Int): Long
    功能: 统计指定stage key (@stageId,stafeAttemptId)任务计数
    val= store.count(classOf[TaskDataWrapper], "stage", Array(stageId, stageAttemptId))
    
    def localitySummary(stageId: Int, stageAttemptId: Int): Map[String, Long]
    功能: 获取指定stageKey (stageId,stageAttemptId)的定位信息
    val= store.read(classOf[StageDataWrapper], Array(stageId, stageAttemptId)).locality
    
    private def shouldCacheQuantile(q: Double): Boolean = (math.round(q * 100) % 5) == 0
    功能: 确定是否需要缓存分位点信息
    	缓存分位点按照每 0.05 为步长设置。这个会覆盖API和stage页中设置的默认值。
    
    def quantileToString(q: Double): String = math.round(q * 100).toString
    功能: 分位点显示
    
    def taskList(stageId: Int, stageAttemptId: Int, maxTasks: Int): Seq[v1.TaskData]
    功能: 根据指定stage信息获取任务数据列表
    1. 获取stageKey
    val stageKey = Array(stageId, stageAttemptId)
    2. 获取指定@stageKey最大记录数量为@maxTasks 的记录列表
    val taskDataWrapperIter = store.view(classOf[TaskDataWrapper]).index("stage")
      .first(stageKey).last(stageKey).reverse().max(maxTasks).asScala
    3. 将迭代器构建成列表信息
    val= constructTaskDataList(taskDataWrapperIter).reverse
    
    def taskList(stageId: Int,stageAttemptId: Int,offset: Int,length: Int,
      sortBy: v1.TaskSorting): Seq[v1.TaskData]
	功能: 获取指定stage信息的任务数据列表
    1. 获取索引名称和排序方式信息
    val (indexName, ascending) = sortBy match {
      case v1.TaskSorting.ID =>
        (None, true)
      case v1.TaskSorting.INCREASING_RUNTIME =>
        (Some(TaskIndexNames.EXEC_RUN_TIME), true)
      case v1.TaskSorting.DECREASING_RUNTIME =>
        (Some(TaskIndexNames.EXEC_RUN_TIME), false)
    }
    2. 对任务数据列表中的数据排序输出
    val= taskList(stageId, stageAttemptId, offset, length, indexName, ascending)
    
    def taskList(
      stageId: Int,
      stageAttemptId: Int,
      offset: Int,
      length: Int,
      sortBy: Option[String],
      ascending: Boolean): Seq[v1.TaskData]
    功能: 获取指定stage下任务数据@TaskData	列表
    1. 获取stageKey下的记录信息列表
    val stageKey = Array(stageId, stageAttemptId)
    val base = store.view(classOf[TaskDataWrapper])
    val indexed = sortBy match {
      case Some(index) =>
        base.index(index).parent(stageKey)
      case _ => // 仅有一个stageKey的情况
        base.index("stage").first(stageKey).last(stageKey)
    }
    2. 排序
    val ordered = if (ascending) indexed else indexed.reverse()
    3. 获取迭代器
    val taskDataWrapperIter = ordered.skip(offset).max(length).asScala
    4. 通过迭代器构建列表
    val= constructTaskDataList(taskDataWrapperIter)
    
    def executorSummary(stageId: Int, attemptId: Int): Map[String, v1.ExecutorStageSummary]
    功能: 获取执行器描述
    val stageKey = Array(stageId, attemptId)
    store.view(classOf[ExecutorStageSummaryWrapper]).index("stage").first(stageKey).last(stageKey)
      .asScala.map { exec => (exec.executorId -> exec.info) }.toMap
    
    def rddList(cachedOnly: Boolean = true): Seq[v1.RDDStorageInfo]
    功能: 获取RDD存储信息列表
    输入参数: 
    	cachedOnly	过滤条件，是否仅仅只缓存
    
    def asOption[T](fn: => T): Option[T]
    输入:	fn	转换函数
    val= try {
      Some(fn)
    } catch {
      case _: NoSuchElementException => None
    }
    
    def stageWithDetails(stage: v1.StageData): v1.StageData 
    功能: 获取带有详细信息的stage数据@StageData
    1. 获取对应stage的任务列表
    val tasks = taskList(stage.stageId, stage.attemptId, Int.MaxValue)
      .map { t => (t.taskId, t) }
      .toMap
    2. 获取stage数据
    val= new v1.StageData(
      status = stage.status,
      stageId = stage.stageId,
      attemptId = stage.attemptId,
      numTasks = stage.numTasks,
      numActiveTasks = stage.numActiveTasks,
      numCompleteTasks = stage.numCompleteTasks,
      numFailedTasks = stage.numFailedTasks,
      numKilledTasks = stage.numKilledTasks,
      numCompletedIndices = stage.numCompletedIndices,
      submissionTime = stage.submissionTime,
      firstTaskLaunchedTime = stage.firstTaskLaunchedTime,
      completionTime = stage.completionTime,
      failureReason = stage.failureReason,
      executorDeserializeTime = stage.executorDeserializeTime,
      executorDeserializeCpuTime = stage.executorDeserializeCpuTime,
      executorRunTime = stage.executorRunTime,
      executorCpuTime = stage.executorCpuTime,
      resultSize = stage.resultSize,
      jvmGcTime = stage.jvmGcTime,
      resultSerializationTime = stage.resultSerializationTime,
      memoryBytesSpilled = stage.memoryBytesSpilled,
      diskBytesSpilled = stage.diskBytesSpilled,
      peakExecutionMemory = stage.peakExecutionMemory,
      inputBytes = stage.inputBytes,
      inputRecords = stage.inputRecords,
      outputBytes = stage.outputBytes,
      outputRecords = stage.outputRecords,
      shuffleRemoteBlocksFetched = stage.shuffleRemoteBlocksFetched,
      shuffleLocalBlocksFetched = stage.shuffleLocalBlocksFetched,
      shuffleFetchWaitTime = stage.shuffleFetchWaitTime,
      shuffleRemoteBytesRead = stage.shuffleRemoteBytesRead,
      shuffleRemoteBytesReadToDisk = stage.shuffleRemoteBytesReadToDisk,
      shuffleLocalBytesRead = stage.shuffleLocalBytesRead,
      shuffleReadBytes = stage.shuffleReadBytes,
      shuffleReadRecords = stage.shuffleReadRecords,
      shuffleWriteBytes = stage.shuffleWriteBytes,
      shuffleWriteTime = stage.shuffleWriteTime,
      shuffleWriteRecords = stage.shuffleWriteRecords,
      name = stage.name,
      description = stage.description,
      details = stage.details,
      schedulingPool = stage.schedulingPool,
      rddIds = stage.rddIds,
      accumulatorUpdates = stage.accumulatorUpdates,
      tasks = Some(tasks),
      executorSummary = Some(executorSummary(stage.stageId, stage.attemptId)),
      killedTasksSummary = stage.killedTasksSummary)
  	}

	def rdd(rddId: Int): v1.RDDStorageInfo
	功能: 获取RDD存储信息
	val= store.read(classOf[RDDStorageInfoWrapper], rddId).info

	def streamBlocksList(): Seq[StreamBlockData]
	功能: 获取流式块数据列表
	val= store.view(classOf[StreamBlockData]).asScala.toSeq

	def operationGraphForStage(stageId: Int): RDDOperationGraph 
	功能: 获取指定stage的RDD操作图
	val= store.read(classOf[RDDOperationGraphWrapper], stageId).toRDDOperationGraph()

	def operationGraphForJob(jobId: Int): Seq[RDDOperationGraph]
	功能: 获取指定job的RDD操作图@RDDOperationGraph 列表
	1. 获取stage列表
	val job = store.read(classOf[JobDataWrapper], jobId)
    val stages = job.info.stageIds.sorted
	2. 获取RDD操作图列表
	val= stages.map { id =>
      val g = store.read(classOf[RDDOperationGraphWrapper], id).toRDDOperationGraph()
      if (job.skippedStages.contains(id) && !g.rootCluster.name.contains("skipped")) {
        g.rootCluster.setName(g.rootCluster.name + " (skipped)")
      }
      g
    }
	
	def pool(name: String): PoolData
	功能: 获取池数据
	val= store.read(classOf[PoolData], name)

	def appSummary(): AppSummary
	功能: 获取应用描述
	val= store.read(classOf[AppSummary], classOf[AppSummary].getName())

	def close(): Unit = store.close()
	功能: 关闭存储器

	def constructTaskDataList(taskDataWrapperIter: Iterable[TaskDataWrapper]): Seq[v1.TaskData]
	功能: 根据指定迭代器@taskDataWrapperIter构建任务列表
	1. 创建执行器ID列表
	val executorIdToLogs = new HashMap[String, Map[String, String]]()
	2. 映射列表中的任务数据
	taskDataWrapperIter.map { taskDataWrapper =>
      val taskDataOld: v1.TaskData = taskDataWrapper.toApi
      val executorLogs = executorIdToLogs.getOrElseUpdate(taskDataOld.executorId, {
        try {
          executorSummary(taskDataOld.executorId).executorLogs
        } catch {
          case e: NoSuchElementException =>
            Map.empty
        }
      })

      new v1.TaskData(taskDataOld.taskId, taskDataOld.index,
        taskDataOld.attempt, taskDataOld.launchTime, taskDataOld.resultFetchStart,
        taskDataOld.duration, taskDataOld.executorId, taskDataOld.host, taskDataOld.status,
        taskDataOld.taskLocality, taskDataOld.speculative, taskDataOld.accumulatorUpdates,
        taskDataOld.errorMessage, taskDataOld.taskMetrics,
        executorLogs,
        AppStatusUtils.schedulerDelay(taskDataOld),
        AppStatusUtils.gettingResultTime(taskDataOld))
    }.toSeq
}
```

```scala
private[spark] object AppStatusStore {
    属性:
    #name @CURRENT_VERSION = 2L	当前版本信息
    操作集:
    def createLiveStore(conf: SparkConf,
      appStatusSource: Option[AppStatusSource] = None): AppStatusStore
    功能: 为当前应用创建一个内存存储器@AppStatusStore
    1. 获取存储器
    val store = new ElementTrackingStore(new InMemoryStore(), conf)
    2. 获取任务状态监听器
    val listener = new AppStatusListener(store, conf, true, appStatusSource)
    val= new AppStatusStore(store, listener = Some(listener))
}
```

#### AppStatusUtils

```scala
private[spark] object AppStatusUtils {
	属性:
	#name @TASK_FINISHED_STATES = Set("FAILED", "KILLED", "SUCCESS") 任务完成状态列表
	操作集:
	def isTaskFinished(task: TaskData): Boolean
	功能: 获取任务完成状态
	val= TASK_FINISHED_STATES.contains(task.status)
	
	def gettingResultTime(task: TaskData): Long
	功能: 获取结果时间
	val= gettingResultTime(task.launchTime.getTime(), fetchStart(task), task.duration.getOrElse(-1L))
	
	def schedulerDelay(task: TaskData): Long
	功能: 调度器延时
	val= if (isTaskFinished(task) && task.taskMetrics.isDefined && task.duration.isDefined) {
      val m = task.taskMetrics.get
      schedulerDelay(task.launchTime.getTime(), fetchStart(task), task.duration.get,
        m.executorDeserializeTime, m.resultSerializationTime, m.executorRunTime)
    } else {
      0L
    }
    
    def gettingResultTime(launchTime: Long, fetchStart: Long, duration: Long): Long
    功能: 根据运行时间@launchTime,获取起始时刻@fetchStart以及持续时间@duration
    理论上获取结果事件=launchTime + duration - fetchStart 其中@duration>0 且@launchTime
    val=if (fetchStart > 0) {
      if (duration > 0) {
        launchTime + duration - fetchStart
      } else {
        System.currentTimeMillis() - fetchStart
      }
    } else {
      0L
    }
    
    def schedulerDelay(launchTime: Long,fetchStart: Long,duration: Long,
      deserializeTime: Long,serializeTime: Long,runTime: Long): Long
	输出参数:
		launchTime	运行时间
		fetchStart	获取起始时刻
		duration	持续时间
		deserializeTime	反序列化时间
		serializeTime	序列化时间
		runTime	运行时间
		理论上:
		运行持续时间@duration=程序运行时间@runtime + 序列化时间@serializeTime + 
		反序列化时间@serializeTime + 获取执行结果时间
		@gettingResultTime(launchTime, fetchStart, duration)
	功能: 获取调度延时时间
	val= math.max(0,duration - runTime - deserializeTime - serializeTime -
      gettingResultTime(launchTime, fetchStart, duration))
	)
	
	def fetchStart(task: TaskData): Long
	val= if (task.resultFetchStart.isDefined) task.resultFetchStart.get.getTime() else -1
}
```

#### ElementTrackingStore

```markdown
介绍:
	KV存储器@KVStore 包装类指定类型的允许定位元素数量，一旦当元素达到容量上限时就会触发动作。比如说，其允许写出者控制有多少存储数据会被删除和有多少数据会添加进来。
	这个存储器用于事件日志或者运行中的UI中获取的信息。紧接在解除触发到达容量上限的动作后，提供两类小量的功能:
	1. 一般的worker线程,可以异步运行开销大的任务,这些任务在需要更多的运行参数时，可以自己配置运行。
	2. 一般的刷新原理，方便监听器可以获取什么时候需要刷新内部状态到KV存储中(需要SHS完成转换事件目录之后)
	配置完成的触发器默认情况下，运行在不同的线程中。可以强制的通过设置@ASYNC_TRACKING_ENABLED 为false，取消	异步运行
```

```scala
private[spark] class ElementTrackingStore(store: KVStore, conf: SparkConf) extends KVStore{
	构造器参数: 
		store KV存储器
		conf spark配置集
	属性:
	#name @triggers=new HashMap[Class[_], LatchedTriggers]()	触发器列表
	#name @flushTriggers = new ListBuffer[() => Unit]()		刷新触发函数
	#name @executor	#type @ExecutorService	执行器
		val= if (conf.get(ASYNC_TRACKING_ENABLED))
				ThreadUtils.newDaemonSingleThreadExecutor("element-tracking-store-worker")
			else
				MoreExecutors.sameThreadExecutor()
	#name @stop=false 停止标志位
	操作集:
	def addTrigger(klass: Class[_], threshold: Long)(action: Long => Unit): Unit 
	功能: 添加触发器
	输入参数:
		klass	需要监视的类
		threshold	元素触发动作的最大值
		action	触发动作,获取注册到存储@KVStore中的内容
	1. 获取触发器
	val newTrigger = Trigger(threshold, action)
	2. 将获取的触发器注册到KV存储系统中
	triggers.get(klass) match {
      case None => // 没有找到之前的记录，新增一个表目
        triggers(klass) = new LatchedTriggers(Seq(newTrigger))
      case Some(latchedTrigger) => // 添加到之前记录的表目中
        triggers(klass) = latchedTrigger :+ newTrigger
    }
    
    def onFlush(action: => Unit): Unit
    功能: 在存储系统刷新前，添加触发器到存储系统。正常情况下,发生在关闭之前。对于刷写中间状态到存储@KVStore很	有效。比如: 通过SHS重新演绎正在运行中的应用。
    刷新线程可以在一个线程中同步调用。
    输入参数: action	指向函数
    flushTriggers += { () => action }
	
	def doAsync(fn: => Unit): Unit
	功能: 异步执行
		对执行动作排队送给执行器(异步执行)。关闭异步运行的方式: 设置@ASYNC_TRACKING_ENABLED 为false
	输入参数: fn 执行过程
	executor.submit(new Runnable() {
      override def run(): Unit = Utils.tryLog { fn }
    })
    
    def read[T](klass: Class[T], naturalKey: Any): T = store.read(klass, naturalKey)
    功能: 读取数据到KVStore中
    
    def write(value: Any): Unit = store.write(value)
    功能: 存储器写出数据
    
    def write(value: Any, checkTriggers: Boolean): WriteQueueResult
    功能: 写出元素到存储器中，检查是否解除触发器
    1. 写出记录
    write(value)
    2. 检查触发器
    if (checkTriggers && !stopped) { // 运行中且触发器没有被解除
      triggers.get(value.getClass).map { latchedList =>
        latchedList.fireOnce { list => // 解除触发器
          val count = store.count(value.getClass)
          list.foreach { t =>
            if (count > t.threshold) {
              t.action(count)
            }
          }
        }
      }.getOrElse(WriteSkippedQueue)
    } else {
      WriteSkippedQueue
    }
    
    def removeAllByIndexValues[T](klass: Class[T], index: String, indexValues: Iterable[_]): Boolean
    功能: 根据index移除所有value值
    val= removeAllByIndexValues(klass, index, indexValues.asJavaCollection)
    
    def removeAllByIndexValues[T](klass: Class[T],index: String,
      indexValues: Collection[_]): Boolean
    功能: 从存储中移除指定@klass 对应index下的记录
    
    def delete(klass: Class[_], naturalKey: Any): Unit = store.delete(klass, naturalKey)
    功能: 删除一条记录
    
    def getMetadata[T](klass: Class[T]): T = store.getMetadata(klass)
    功能: 获取KVStore的元数据
    
    def setMetadata(value: Any): Unit = store.setMetadata(value)
    功能: 设置存储器元数据信息
	
	def view[T](klass: Class[T]): KVStoreView[T] = store.view(klass)
	功能: 获取KVStore的视图
	
	def count(klass: Class[_]): Long = store.count(klass)
	功能: 获取指定类@klass 的
	
	def close(): Unit = close(true)
	
	def close(closeParent: Boolean): Unit 
	功能: 关闭存储器，可以选择是否保证@parent处于开启状态
	val= synchronized {
        if (stopped) {
          return
        }

        stopped = true
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow()
        }

        flushTriggers.foreach { trigger =>
          Utils.tryLog(trigger())
        }

        if (closeParent) {
          store.close()
        }
  	}
  	
  	样例类:
  	case class Trigger[T](threshold: Long,action: Long => Unit)
	介绍: 触发器
}
```

```scala
private class LatchedTriggers(val triggers: Seq[Trigger[_]]) {
	介绍: 锁定触发器
	构造器属性:
		triggers	触发器列表
	属性:
		#name @pending=new AtomicBoolean(false)	待定标志
	操作集:
	def :+(addlTrigger: Trigger[_]): LatchedTriggers = {
      new LatchedTriggers(triggers :+ addlTrigger)
    }
    功能: 添加触发器@addlTrigger到锁定触发器列表中
	
	def fireOnce(f: Seq[Trigger[_]] => Unit): WriteQueueResult
	功能: 获取写出结果队列，并解除锁定
	val= if (pending.compareAndSet(false, true)) {
        doAsync {
          pending.set(false)
          f(triggers)
        }
        WriteQueued
      } else {
        WriteSkippedQueue
      }
}
```


```scala
private[spark] object ElementTrackingStore {
	sealed trait WriteQueueResult	// 需要单独的支持执行器的正确性，否则写方法无效
	object WriteQueued extends WriteQueueResult			写队列
	object WriteSkippedQueue extends WriteQueueResult	跳写队列
}
```

#### KVUtils

```scala
private[spark] object KVUtils extends Logging {
	定义类型:
	type KVIndexParam = KVIndex @getter
	介绍: 用于注释构造器参数作为KV存储器@KVStore目录
	操作集:
	def open[M: ClassTag](path: File, metadata: M): LevelDB
	功能: 打开或是创建一个LevelDB数据库存储系统
	输入参数: 
		path 数据库存储的位置
		metadata 元数据值，用于和数据库中元数据作比较。如果数据库中不包含任何元数据，那么这个值会被写入到数据			库元数据信息中。
	操作条件: 输入元数据存在
	0. 断言元数据合法性
	require(metadata != null, "Metadata is required.")
	1. 获取指定位置@path的LevelDB,并获取其元数据
    val db = new LevelDB(path, new KVStoreScalaSerializer())
    val dbMeta = db.getMetadata(classTag[M].runtimeClass)
    2. 获取/创建元数据信息
    if (dbMeta == null) { // 元数据不存在,创建元数据
      db.setMetadata(metadata)
    } else if (dbMeta != metadata) { // 元数据匹配失败，抛出异常
      db.close()
      throw new MetadataMismatchException()
    }
    val= db // 返回匹配元数据对应的levelDB实例
    
    def viewToSeq[T](view: KVStoreView[T],max: Int)(filter: T => Boolean): Seq[T] 
	功能: 视图转列表
	输入参数: 
		view	KV存储的视图
		max		获取最大元素数量
		filter	过滤函数
	val iter = view.closeableIterator()
    try {
      iter.asScala.filter(filter).take(max).toList
    } finally {
      iter.close()
    }
    
    内部类:
    private[spark] class MetadataMismatchException extends Exception
    介绍: 元数据匹配失败异常
    
    private[spark] class KVStoreScalaSerializer extends KVStoreSerializer
    功能: KV存储的scala序列化器
    内部设置:
    mapper.registerModule(DefaultScalaModule) // 注册Scala模块
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT) // 设置序列化包含的内容
}
```

#### LiveEntity

```scala
/*
	spark中存活实例(job,stage,任务 等等)的可变表达方式.每个存活的实例使用一个实例去追踪进化的状态,且周期性的刷新实例不可变部分到应用状态存储器中.
*/
private[spark] abstract class LiveEntity {
    属性:
    #name @lastWriteTime = -1L	上次写出时间
    操作集:
    def write(store: ElementTrackingStore, now: Long, checkTriggers: Boolean = false): Unit
    功能: 写出更新部分到KV存储器中
    	首次写的时候总是会检查触发器，因为增加元素到存储器中可能导致元素类型的最大数目超出限制。
    store.write(doUpdate(), checkTriggers || lastWriteTime == -1L)
    lastWriteTime = now
    
    def doUpdate(): Any
    功能: 返回实例数据更新的部分，这些数据需要存储到状态存储器中，反映了监听器收集的最新消息。
}
```

```scala
private class LiveJob(
    val jobId: Int,
    name: String,
    description: Option[String],
    val submissionTime: Option[Date],
    val stageIds: Seq[Int],
    jobGroup: Option[String],
    numTasks: Int,
    sqlExecutionId: Option[Long]){
    介绍: 存活的job
    构造器属性:
    	jobId	jobID
    	name	job名称
    	description	描述
    	submissionTime	提交时间
    	stageIds	stageID列表
    	jobGroup	job所属组
    	numTasks	任务数量
    	sqlExecutionId	sql执行ID
    关系: father --> LiveEntity
    属性:
    #name @activeTasks = 0	激活任务
    #name @completedTasks = 0	完成任务
    #name @failedTasks = 0	失败任务数量
    #name @completedIndices = new OpenHashSet[Long]() 已完成的索引信息
    #name @killedTasks = 0	被kill掉的任务数量
    #name @killedSummary: Map[String, Int] = Map()	kill掉任务信息表
    #name @skippedTasks=0	跳过任务数量
    #name @skippedStages=Set[Int]()	跳过的stage信息
    #name @status = JobExecutionStatus.RUNNING	job执行状态
    #name @completionTime: Option[Date] = None	完成时间
    #name @completedStages: Set[Int] = Set()	完成的stage列表
    #name @activeStages=0	激活stage数量
    #name @failedStages=0	失败stage数量
    操作集:
    def doUpdate(): Any
    功能: 更新并获得最新的消息
    1. 获取需要更新的新值
    val info = new v1.JobData(
      jobId,
      name,
      description,
      submissionTime,
      completionTime,
      stageIds,
      jobGroup,
      status,
      numTasks,
      activeTasks,
      completedTasks,
      skippedTasks,
      failedTasks,
      killedTasks,
      completedIndices.size,
      activeStages,
      completedStages.size,
      skippedStages.size,
      failedStages,
      killedSummary)
    2. 获取新的job数据
    val= new JobDataWrapper(info, skippedStages, sqlExecutionId)
}
```

```scala
private class LiveTask(var info: TaskInfo,stageId: Int,stageAttemptId: Int,
    lastUpdateTime: Option[Long]){
	关系: father --> LiveEntity
    属性:
    #name @metrics: v1.TaskMetrics = createMetrics(default = -1L)	任务度量器
    #name @errorMessage: Option[String] = None 错误信息
    操作集:
    def updateMetrics(metrics: TaskMetrics): v1.TaskMetrics
    功能: 更新度量值
    if (metrics != null) {
      val old = this.metrics
      val newMetrics = createMetrics(
        metrics.executorDeserializeTime,
        metrics.executorDeserializeCpuTime,
        metrics.executorRunTime,
        metrics.executorCpuTime,
        metrics.resultSize,
        metrics.jvmGCTime,
        metrics.resultSerializationTime,
        metrics.memoryBytesSpilled,
        metrics.diskBytesSpilled,
        metrics.peakExecutionMemory,
        metrics.inputMetrics.bytesRead,
        metrics.inputMetrics.recordsRead,
        metrics.outputMetrics.bytesWritten,
        metrics.outputMetrics.recordsWritten,
        metrics.shuffleReadMetrics.remoteBlocksFetched,
        metrics.shuffleReadMetrics.localBlocksFetched,
        metrics.shuffleReadMetrics.fetchWaitTime,
        metrics.shuffleReadMetrics.remoteBytesRead,
        metrics.shuffleReadMetrics.remoteBytesReadToDisk,
        metrics.shuffleReadMetrics.localBytesRead,
        metrics.shuffleReadMetrics.recordsRead,
        metrics.shuffleWriteMetrics.bytesWritten,
        metrics.shuffleWriteMetrics.writeTime,
        metrics.shuffleWriteMetrics.recordsWritten)
      this.metrics = newMetrics
      if (old.executorDeserializeTime >= 0L) {
        subtractMetrics(newMetrics, old)
      } else {
        newMetrics
      }
    } else {
      null
    }
    
    def doUpdate(): Any
    功能: 更新并获取最新值
    1. 求任务持续周期
    val duration = if (info.finished) {
      info.duration
    } else {
      info.timeRunning(lastUpdateTime.getOrElse(System.currentTimeMillis()))
    }
    2. 确定是否有度量信息
    val hasMetrics = metrics.executorDeserializeTime >= 0
    3. 获取任务度量信息(对于没有成功的任务,存储度量值为负值以避免计算任务描述,@toApi方法会是它变为正常值)
    val taskMetrics: v1.TaskMetrics = if (hasMetrics && !info.successful) {
      makeNegative(metrics)
    } else {
      metrics
    }
	4. 返回任务数量包装器
    val= new TaskDataWrapper(
      info.taskId,
      info.index,
      info.attemptNumber,
      info.launchTime,
      if (info.gettingResult) info.gettingResultTime else -1L,
      duration,
      weakIntern(info.executorId),
      weakIntern(info.host),
      weakIntern(info.status),
      weakIntern(info.taskLocality.toString()),
      info.speculative,
      newAccumulatorInfos(info.accumulables),
      errorMessage,
      hasMetrics,
      taskMetrics.executorDeserializeTime,
      taskMetrics.executorDeserializeCpuTime,
      taskMetrics.executorRunTime,
      taskMetrics.executorCpuTime,
      taskMetrics.resultSize,
      taskMetrics.jvmGcTime,
      taskMetrics.resultSerializationTime,
      taskMetrics.memoryBytesSpilled,
      taskMetrics.diskBytesSpilled,
      taskMetrics.peakExecutionMemory,
      taskMetrics.inputMetrics.bytesRead,
      taskMetrics.inputMetrics.recordsRead,
      taskMetrics.outputMetrics.bytesWritten,
      taskMetrics.outputMetrics.recordsWritten,
      taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      taskMetrics.shuffleReadMetrics.localBlocksFetched,
      taskMetrics.shuffleReadMetrics.fetchWaitTime,
      taskMetrics.shuffleReadMetrics.remoteBytesRead,
      taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      taskMetrics.shuffleReadMetrics.localBytesRead,
      taskMetrics.shuffleReadMetrics.recordsRead,
      taskMetrics.shuffleWriteMetrics.bytesWritten,
      taskMetrics.shuffleWriteMetrics.writeTime,
      taskMetrics.shuffleWriteMetrics.recordsWritten,
      stageId,
      stageAttemptId)
}
```

```scala
private class LiveExecutor(val executorId: String, _addTime: Long){
    关系: father --> LiveEntity
    介绍: 存活执行器
    属性:
    #name @hostPort: String = null	主机端口
    #name @host: String = null	主机名
    #name @isActive = true	是否处于激活状态
    #name @totalCores = 0	总计核心数量
    #name @addTime = new Date(_addTime)	添加时间
    #name @removeTime: Date = null	移除时间
    #name @removeReason: String = null	移除原因
    #name @rddBlocks = 0	rdd块数量
    #name @memoryUsed = 0L	内存使用量
    #name @diskUsed = 0L	磁盘使用量
    #name @maxTasks = 0		最大任务数量
    #name @maxMemory = 0L	最大内存数量
    #name @totalTasks = 0	总计任务数量
    #name @activeTasks = 0	激活任务数量
    #name @completedTasks = 0	完成任务数量
    #name @failedTasks = 0 	失败任务数量
    #name @totalDuration = 0L 总计持续时间
    #name @totalGcTime = 0L	总计GC时间
    #name @totalInputBytes = 0L	总计输入字节数量
    #name @totalShuffleRead = 0L 总计shuffle读取数量
    #name @totalShuffleWrite = 0L	shuffle写出字节数量
    #name @isBlacklisted = false	是否处于黑名单中
    #name @blacklistedInStages: Set[Int] = TreeSet()	黑名单stage列表
    #name @executorLogs= Map[String, String]()	执行器日志
    #name @attributes = Map[String, String]()	属性表
    #name @resources = Map[String, ResourceInformation]()	资源列表
    #name @totalOnHeap = -1L	合计堆上内存
    #name @totalOffHeap = 0L	总计非堆模式下内存
    #name @usedOnHeap = 0L	已使用堆内存
    #name @usedOffHeap = 0L	已使用非堆模式下内存
 	#name @peakExecutorMetrics = new ExecutorMetrics()	执行器峰值内存度量器
    操作集:
    def hasMemoryInfo: Boolean = totalOnHeap >= 0L
    功能: 确认是否有堆上内存
    
    def hostname: String = if (host != null) host else hostPort.split(":")(0)
    功能: 获取主机名称
    
    def doUpdate(): Any 
    功能: 更新并获取最新的值
    1. 获取内存度量器
    val memoryMetrics = if (totalOnHeap >= 0) {
      Some(new v1.MemoryMetrics(usedOnHeap, usedOffHeap, totalOnHeap, totalOffHeap))
    } else {
      None
    }
    2. 获取新的执行器描述值
    val info = new v1.ExecutorSummary(
      executorId,
      if (hostPort != null) hostPort else host,
      isActive,
      rddBlocks,
      memoryUsed,
      diskUsed,
      totalCores,
      maxTasks,
      activeTasks,
      failedTasks,
      completedTasks,
      totalTasks,
      totalDuration,
      totalGcTime,
      totalInputBytes,
      totalShuffleRead,
      totalShuffleWrite,
      isBlacklisted,
      maxMemory,
      addTime,
      Option(removeTime),
      Option(removeReason),
      executorLogs,
      memoryMetrics,
      blacklistedInStages,
      Some(peakExecutorMetrics).filter(_.isSet),
      attributes,
      resources)
    3. 返回最新值
    val= new ExecutorSummaryWrapper(info)
}
```

```scala
private class LiveExecutorStageSummary(stageId: Int,attemptId: Int,executorId: String){
    关系: father --> LiveEntity
    属性:
    #name @taskTime = 0L	任务时间
    #name @succeededTasks = 0	成功任务数量
    #name @failedTasks = 0	失败任务数量
    #name @killedTasks = 0	kill任务数量
    #name @isBlacklisted = false	是否处于黑名单中
    #name @metrics = createMetrics(default = 0L)	任务度量器
    操作集:
    def doUpdate(): Any
    功能: 更新并获取最新值
    1. 获取最新信息
    val info = new v1.ExecutorStageSummary(
      taskTime,
      failedTasks,
      succeededTasks,
      killedTasks,
      metrics.inputMetrics.bytesRead,
      metrics.inputMetrics.recordsRead,
      metrics.outputMetrics.bytesWritten,
      metrics.outputMetrics.recordsWritten,
      metrics.shuffleReadMetrics.remoteBytesRead + metrics.shuffleReadMetrics.localBytesRead,
      metrics.shuffleReadMetrics.recordsRead,
      metrics.shuffleWriteMetrics.bytesWritten,
      metrics.shuffleWriteMetrics.recordsWritten,
      metrics.memoryBytesSpilled,
      metrics.diskBytesSpilled,
      isBlacklisted)
    2. 返回执行器stage描述包装器
    val= new ExecutorStageSummaryWrapper(stageId, attemptId, executorId, info)
}
```

```scala
private class LiveStage extends LiveEntity {
    介绍: 存活的stage
    属性:
    #name @jobs = Seq[LiveJob]()	存活job列表
    #name @jobIds = Set[Int]()	job列表
    #name @info: StageInfo = null	应用信息
    #name @status = v1.StageStatus.PENDING	stage状态标记
    #name @description: Option[String] = None	描述
    #name @schedulingPool: String = SparkUI.DEFAULT_POOL_NAME	调度池
    #name @activeTasks = 0	激活任务
    #name @completedTasks = 0	完成任务数量
    #name @failedTasks = 0	失败任务数量
    #name @completedIndices = new OpenHashSet[Int]()	完成索引集合
    #name @killedTasks = 0	kill的任务数量
    #name @killedSummary: Map[String, Int] = Map()	被kill任务描述表
    #name @firstLaunchTime = Long.MaxValue	首次运行时间
    #name @localitySummary: Map[String, Long] = Map()	位置描述表
    #name @metrics = createMetrics(default = 0L)	任务度量器
    #name @executorSummaries = new HashMap[String, LiveExecutorStageSummary]()	执行器描述表
    #name @activeTasksPerExecutor = new HashMap[String, Int]().withDefaultValue(0) 每个执行器激活任务表
	#name @blackListedExecutors = new HashSet[String]()	黑名单执行器集合
    #name @savedTasks = new AtomicInteger(0)	是否保存task
    #name @cleaning = false volatile	清除标志位(是否要def doUpdate(): Any在达到配置上限时清除任务而不是写到KV存储器中)
    操作集:
    def executorSummary(executorId: String): LiveExecutorStageSummary
    功能: 获取指定驱动器@executorId 的存活stage描述
    val= executorSummaries.getOrElseUpdate(executorId,
      new LiveExecutorStageSummary(info.stageId, info.attemptNumber, executorId))
    
    def doUpdate(): Any
	功能: 更新获取最新的存储数据信息
    val= new StageDataWrapper(toApi(), jobIds, localitySummary)
    
    def toApi(): v1.StageData
    val= new v1.StageData(
      status = status,
      stageId = info.stageId,
      attemptId = info.attemptNumber,
      numTasks = info.numTasks,
      numActiveTasks = activeTasks,
      numCompleteTasks = completedTasks,
      numFailedTasks = failedTasks,
      numKilledTasks = killedTasks,
      numCompletedIndices = completedIndices.size,
      submissionTime = info.submissionTime.map(new Date(_)),
      firstTaskLaunchedTime =
        if (firstLaunchTime < Long.MaxValue) Some(new Date(firstLaunchTime)) else None,
      completionTime = info.completionTime.map(new Date(_)),
      failureReason = info.failureReason,
      executorDeserializeTime = metrics.executorDeserializeTime,
      executorDeserializeCpuTime = metrics.executorDeserializeCpuTime,
      executorRunTime = metrics.executorRunTime,
      executorCpuTime = metrics.executorCpuTime,
      resultSize = metrics.resultSize,
      jvmGcTime = metrics.jvmGcTime,
      resultSerializationTime = metrics.resultSerializationTime,
      memoryBytesSpilled = metrics.memoryBytesSpilled,
      diskBytesSpilled = metrics.diskBytesSpilled,
      peakExecutionMemory = metrics.peakExecutionMemory,
      inputBytes = metrics.inputMetrics.bytesRead,
      inputRecords = metrics.inputMetrics.recordsRead,
      outputBytes = metrics.outputMetrics.bytesWritten,
      outputRecords = metrics.outputMetrics.recordsWritten,
      shuffleRemoteBlocksFetched = metrics.shuffleReadMetrics.remoteBlocksFetched,
      shuffleLocalBlocksFetched = metrics.shuffleReadMetrics.localBlocksFetched,
      shuffleFetchWaitTime = metrics.shuffleReadMetrics.fetchWaitTime,
      shuffleRemoteBytesRead = metrics.shuffleReadMetrics.remoteBytesRead,
      shuffleRemoteBytesReadToDisk = metrics.shuffleReadMetrics.remoteBytesReadToDisk,
      shuffleLocalBytesRead = metrics.shuffleReadMetrics.localBytesRead,
      shuffleReadBytes =
        metrics.shuffleReadMetrics.localBytesRead + metrics.shuffleReadMetrics.remoteBytesRead,
      shuffleReadRecords = metrics.shuffleReadMetrics.recordsRead,
      shuffleWriteBytes = metrics.shuffleWriteMetrics.bytesWritten,
      shuffleWriteTime = metrics.shuffleWriteMetrics.writeTime,
      shuffleWriteRecords = metrics.shuffleWriteMetrics.recordsWritten,
      name = info.name,
      description = description,
      details = info.details,
      schedulingPool = schedulingPool,
      rddIds = info.rddInfos.map(_.id),
      accumulatorUpdates = newAccumulatorInfos(info.accumulables.values),
      tasks = None,
      executorSummary = None,
      killedTasksSummary = killedSummary)
}
```

```scala
private class LiveRDDPartition(val blockName: String, rddLevel: StorageLevel){
    构造器属性:
    	blockName	块名称
    	rddLevel	RDD存储等级
    介绍:
    一个缓存RDD的单分区数据，RDD存储等级用于计算分区的有效存储等级，这里需要考虑到在执行器中分区实际使用到的存储内容。因此可以不同于应用程序发起的存储等级请求。
	属性:
    #name @prev: LiveRDDPartition = null	volatile	上一个存活RDD分区指针
    #name @next: LiveRDDPartition = null	volatile	下一个存活RDD分区指针
    #name @var value: v1.RDDPartitionInfo = null	RDD分区信息
    操作集:
    def executors: Seq[String] = value.executors
    功能: 获取分区的执行器
    
    def memoryUsed: Long = value.memoryUsed
    功能: 获取分区内存使用量
    
    def diskUsed: Long = value.diskUsed
    功能: 获取分区磁盘使用量
    
    def update(executors: Seq[String],memoryUsed: Long,diskUsed: Long): Unit
	功能: 更新分区内部属性值
    1. 获取存储等级
    val level = StorageLevel(diskUsed > 0, memoryUsed > 0, rddLevel.useOffHeap,
      if (memoryUsed > 0) rddLevel.deserialized else false, executors.size)
    2. 更新分区值
    value = new v1.RDDPartitionInfo(
      blockName,
      weakIntern(level.description),
      memoryUsed,
      diskUsed,
      executors)
}
```

```scala
private class LiveRDDDistribution(exec: LiveExecutor){
    介绍: 存活的RDD分布
    属性:
    #name @executorId = exec.executorId	执行器编号
    #name @memoryUsed = 0L	内存使用量
    #name @diskUsed = 0L	磁盘使用量
    #name @onHeapUsed = 0L	堆上内存使用量
    #name @offHeapUsed = 0L	非堆模式下内存使用量
    #name @lastUpdate: v1.RDDDataDistribution = null	上次更新时间
    操作集:
    def toApi(): v1.RDDDataDistribution
    功能: 转化为API --> RDD数据分布 @RDDDataDistribution
	val= lastUpdate!=null ? lastUpdate :{
        lastUpdate = new v1.RDDDataDistribution(
        weakIntern(exec.hostPort),
        memoryUsed,
        exec.maxMemory - exec.memoryUsed,
        diskUsed,
        if (exec.hasMemoryInfo) Some(onHeapUsed) else None,
        if (exec.hasMemoryInfo) Some(offHeapUsed) else None,
        if (exec.hasMemoryInfo) Some(exec.totalOnHeap - exec.usedOnHeap) else None,
        if (exec.hasMemoryInfo) Some(exec.totalOffHeap - exec.usedOffHeap) else None)
    }
}
```

```scala
private class LiveRDD(val info: RDDInfo, storageLevel: StorageLevel) extends LiveEntity {
	介绍: 存活的RDD
    构造器属性:
    	info	RDD信息
    	storageLevel	存储等级
    属性:
    #name @memoryUsed = 0L	内存使用量
    #name @diskUsed = 0L	磁盘使用量
    #name @levelDescription = weakIntern(storageLevel.description)	存储等级描述
    #name @partitions = new HashMap[String, LiveRDDPartition]()	存活分区映射表
    #name @partitionSeq = new RDDPartitionSeq()	RDD分区序列
    #name @distributions = new HashMap[String, LiveRDDDistribution]()	RDD分布映射表
    操作集:
    def partition(blockName: String): LiveRDDPartition
    功能: 获取指定块名称的存活RDD分区
    val= partitions.getOrElseUpdate(blockName, {
      val part = new LiveRDDPartition(blockName, storageLevel)
      part.update(Nil, 0L, 0L)
      partitionSeq.addPartition(part)
      part
    })
    
    def removePartition(blockName: String): Unit
    功能: 移除指定分区(分区映射表和RDD分区序列都需要移除)
    partitions.remove(blockName).foreach(partitionSeq.removePartition)
	
    def distribution(exec: LiveExecutor): LiveRDDDistribution
    功能: 获取指定执行器@LiveExecutor 的RDD分布
    
    def removeDistribution(exec: LiveExecutor): Boolean
    功能: 确定指定执行器@LiveExecutor 是否被移除
    
    def distributionOpt(exec: LiveExecutor): Option[LiveRDDDistribution]
    功能: 获取分布属性
    val= distributions.get(exec.executorId)
    
    def getPartitions(): scala.collection.Map[String, LiveRDDPartition] = partitions
    功能: 获取分区映射表
    
    def getDistributions(): scala.collection.Map[String, LiveRDDDistribution] = distributions
    功能: 获取RDD映射关系
    
    def doUpdate(): Any
    功能: 更新并获取最新值
    1. 获取RDD分布列表
    val dists = if (distributions.nonEmpty) {
      Some(distributions.values.map(_.toApi()).toSeq)
    } else {
      None
    }
    2. 获取RDD新值
    val rdd = new v1.RDDStorageInfo(
      info.id,
      info.name,
      info.numPartitions,
      partitions.size,
      levelDescription,
      memoryUsed,
      diskUsed,
      dists,
      Some(partitionSeq))
    3. 返回新RDD存储信息
    val= new RDDStorageInfoWrapper(rdd)
}
```

```scala
private class SchedulerPool(name: String) extends LiveEntity {
    介绍: 调度池
    属性:
    #name @stageIds = Set[Int]()	Stage ID列表
    操作集:
    def doUpdate(): Any
    功能: 更新并获取最新值
    val= new PoolData(name, stageIds)
}
```

```scala
private object LiveEntityHelpers {
    属性:
    #name @stringInterner = Interners.newWeakInterner[String]() 	远程串列表
    操作集:
    def newAccumulatorInfos(accums: Iterable[AccumulableInfo]): Seq[v1.AccumulableInfo]
    功能: 新建累加器信息，并返回累加器列表 accums为过滤条件
    val= accums
      .filter { acc =>
		!acc.internal && acc.metadata != Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER)
      }.map { acc =>
        new v1.AccumulableInfo(
          acc.id,
          acc.name.map(weakIntern).orNull,
          acc.update.map(_.toString()),
          acc.value.map(_.toString()).orNull)
      }.toSeq
    
    def weakIntern(s: String): String
    功能: 串拘留用于减少内存使用
    val= stringInterner.intern(s)
    
    def createMetrics(
      executorDeserializeTime: Long,
      executorDeserializeCpuTime: Long,
      executorRunTime: Long,
      executorCpuTime: Long,
      resultSize: Long,
      jvmGcTime: Long,
      resultSerializationTime: Long,
      memoryBytesSpilled: Long,
      diskBytesSpilled: Long,
      peakExecutionMemory: Long,
      inputBytesRead: Long,
      inputRecordsRead: Long,
      outputBytesWritten: Long,
      outputRecordsWritten: Long,
      shuffleRemoteBlocksFetched: Long,
      shuffleLocalBlocksFetched: Long,
      shuffleFetchWaitTime: Long,
      shuffleRemoteBytesRead: Long,
      shuffleRemoteBytesReadToDisk: Long,
      shuffleLocalBytesRead: Long,
      shuffleRecordsRead: Long,
      shuffleBytesWritten: Long,
      shuffleWriteTime: Long,
      shuffleRecordsWritten: Long): v1.TaskMetrics
    功能: 创建任务度量器
    val= new v1.TaskMetrics(
      executorDeserializeTime,
      executorDeserializeCpuTime,
      executorRunTime,
      executorCpuTime,
      resultSize,
      jvmGcTime,
      resultSerializationTime,
      memoryBytesSpilled,
      diskBytesSpilled,
      peakExecutionMemory,
      new v1.InputMetrics(
        inputBytesRead,
        inputRecordsRead),
      new v1.OutputMetrics(
        outputBytesWritten,
        outputRecordsWritten),
      new v1.ShuffleReadMetrics(
        shuffleRemoteBlocksFetched,
        shuffleLocalBlocksFetched,
        shuffleFetchWaitTime,
        shuffleRemoteBytesRead,
        shuffleRemoteBytesReadToDisk,
        shuffleLocalBytesRead,
        shuffleRecordsRead),
      new v1.ShuffleWriteMetrics(
        shuffleBytesWritten,
        shuffleWriteTime,
        shuffleRecordsWritten))
    
    def createMetrics(default: Long): v1.TaskMetrics
    功能: 创建默认值为@default的任务度量器
    val= createMetrics(default, default, default, default, default, default, default, default,
      default, default, default, default, default, default, default, default,
      default, default, default, default, default, default, default, default)
    
    def addMetrics(m1: v1.TaskMetrics, m2: v1.TaskMetrics): v1.TaskMetrics = addMetrics(m1, m2, 1)
    功能: 度量器合并 m2合并到m1中
    
    def subtractMetrics(m1: v1.TaskMetrics, m2: v1.TaskMetrics): v1.TaskMetrics
    功能: 度量器相减 m1-m2
    
    def addMetrics(m1: v1.TaskMetrics, m2: v1.TaskMetrics, mult: Int): v1.TaskMetrics
    功能: 计算度量器相加m1+m2
	val= createMetrics(
      m1.executorDeserializeTime + m2.executorDeserializeTime * mult,
      m1.executorDeserializeCpuTime + m2.executorDeserializeCpuTime * mult,
      m1.executorRunTime + m2.executorRunTime * mult,
      m1.executorCpuTime + m2.executorCpuTime * mult,
      m1.resultSize + m2.resultSize * mult,
      m1.jvmGcTime + m2.jvmGcTime * mult,
      m1.resultSerializationTime + m2.resultSerializationTime * mult,
      m1.memoryBytesSpilled + m2.memoryBytesSpilled * mult,
      m1.diskBytesSpilled + m2.diskBytesSpilled * mult,
      m1.peakExecutionMemory + m2.peakExecutionMemory * mult,
      m1.inputMetrics.bytesRead + m2.inputMetrics.bytesRead * mult,
      m1.inputMetrics.recordsRead + m2.inputMetrics.recordsRead * mult,
      m1.outputMetrics.bytesWritten + m2.outputMetrics.bytesWritten * mult,
      m1.outputMetrics.recordsWritten + m2.outputMetrics.recordsWritten * mult,
      m1.shuffleReadMetrics.remoteBlocksFetched + m2.shuffleReadMetrics.remoteBlocksFetched * mult,
      m1.shuffleReadMetrics.localBlocksFetched + m2.shuffleReadMetrics.localBlocksFetched * mult,
      m1.shuffleReadMetrics.fetchWaitTime + m2.shuffleReadMetrics.fetchWaitTime * mult,
      m1.shuffleReadMetrics.remoteBytesRead + m2.shuffleReadMetrics.remoteBytesRead * mult,
      m1.shuffleReadMetrics.remoteBytesReadToDisk +
        m2.shuffleReadMetrics.remoteBytesReadToDisk * mult,
      m1.shuffleReadMetrics.localBytesRead + m2.shuffleReadMetrics.localBytesRead * mult,
      m1.shuffleReadMetrics.recordsRead + m2.shuffleReadMetrics.recordsRead * mult,
      m1.shuffleWriteMetrics.bytesWritten + m2.shuffleWriteMetrics.bytesWritten * mult,
      m1.shuffleWriteMetrics.writeTime + m2.shuffleWriteMetrics.writeTime * mult,
      m1.shuffleWriteMetrics.recordsWritten + m2.shuffleWriteMetrics.recordsWritten * mult)
    
    def makeNegative(m: v1.TaskMetrics): v1.TaskMetrics
    功能: 将度量值转化为负值，同时也会处理0值。这个方法假定所有度量值非负
    1. 处理度量值映射(需要保证特殊值0映射后也是负数)
    def updateMetricValue(metric: Long): Long = {
      metric * -1L - 1L
    }
    2. 创建负值度量器
    val= createMetrics(
      updateMetricValue(m.executorDeserializeTime),
      updateMetricValue(m.executorDeserializeCpuTime),
      updateMetricValue(m.executorRunTime),
      updateMetricValue(m.executorCpuTime),
      updateMetricValue(m.resultSize),
      updateMetricValue(m.jvmGcTime),
      updateMetricValue(m.resultSerializationTime),
      updateMetricValue(m.memoryBytesSpilled),
      updateMetricValue(m.diskBytesSpilled),
      updateMetricValue(m.peakExecutionMemory),
      updateMetricValue(m.inputMetrics.bytesRead),
      updateMetricValue(m.inputMetrics.recordsRead),
      updateMetricValue(m.outputMetrics.bytesWritten),
      updateMetricValue(m.outputMetrics.recordsWritten),
      updateMetricValue(m.shuffleReadMetrics.remoteBlocksFetched),
      updateMetricValue(m.shuffleReadMetrics.localBlocksFetched),
      updateMetricValue(m.shuffleReadMetrics.fetchWaitTime),
      updateMetricValue(m.shuffleReadMetrics.remoteBytesRead),
      updateMetricValue(m.shuffleReadMetrics.remoteBytesReadToDisk),
      updateMetricValue(m.shuffleReadMetrics.localBytesRead),
      updateMetricValue(m.shuffleReadMetrics.recordsRead),
      updateMetricValue(m.shuffleWriteMetrics.bytesWritten),
      updateMetricValue(m.shuffleWriteMetrics.writeTime),
      updateMetricValue(m.shuffleWriteMetrics.recordsWritten))
}
```

```scala
private class RDDPartitionSeq extends Seq[v1.RDDPartitionInfo] {
	介绍: RDD分区队列
    基于可变链表的分区序列，外部接口是一个不可变的序列(遍历时线程安全)。尽管如此也不能保证数据的一致性，因为迭代器可能会返回已经被移除或是遗漏添加的元素。内部来说，序列时可变的，可以修改其向外部暴露的数据，添加和移除的时间复杂度都是O(1)。对于并发写入时不安全的。
    属性:
    #name @_head: LiveRDDPartition = null volatile	首个RDD分区指针
    #name @_tail: LiveRDDPartition = null volatile	末尾RDD分区指针
    #name @count = 0  volatile	列表长度
    操作集:
    def length: Int = count
    功能: 获取列表长度
    
    def apply(idx: Int): v1.RDDPartitionInfo
    功能: 获取位置为idx的RDD分区信息@RDDPartitionInfo
    var curr = 0
    var e = _head
    while (curr < idx && e != null) {
      curr += 1
      e = e.next
    }
    if (e != null) e.value else throw new IndexOutOfBoundsException(idx.toString)
    
    def iterator: Iterator[v1.RDDPartitionInfo]
    功能: 获取RDD迭代器信息
    val= new Iterator[v1.RDDPartitionInfo] {
      var current = _head // 链表指针初始化在头结点
      override def hasNext: Boolean = current != null
      override def next(): v1.RDDPartitionInfo = {
        if (current != null) {
          val tmp = current
          current = tmp.next
          tmp.value
        } else {
          throw new NoSuchElementException()
        }
      }
    }
    
    def addPartition(part: LiveRDDPartition): Unit
    功能: 添加分区信息
    part.prev = _tail
    if (_tail != null) {
      _tail.next = part
    }
    if (_head == null) {
      _head = part
    }
    _tail = part
    count += 1
    
    def removePartition(part: LiveRDDPartition): Unit
    功能: 移除指定分区
    count -= 1
    if (part.prev != null) {
      part.prev.next = part.next
    }
    if (part eq _head) {
      _head = part.next
    }
    if (part.next != null) {
      part.next.prev = part.prev
    }
    if (part eq _tail) {
      _tail = part.prev
    }
}
```

#### storeTypes

```scala
private[spark] case class AppStatusStoreMetadata(version: Long)
介绍: 应用状态存储的元数据

private[spark] class ApplicationInfoWrapper(val info: ApplicationInfo){
    介绍: 应用信息包装器
    构造器参数:
    info	应用信息@ApplicationInfo
    操作集:
    @JsonIgnore @KVIndex
    def id: String = info.id
    功能: 获取应用ID信息
}

private[spark] class ApplicationEnvironmentInfoWrapper(val info: ApplicationEnvironmentInfo){
    介绍: 应用环境信息包装类
    构造器属性:
    	info 应用环境信息
    操作集:
    @JsonIgnore @KVIndex
    def id: String = classOf[ApplicationEnvironmentInfoWrapper].getName()
    功能: 获取应用环境信息名称
}

private[spark] class ExecutorSummaryWrapper(val info: ExecutorSummary){
    介绍: 执行器信息包装器
    构造器参数:
    info	执行器信息
    操作集:
    @JsonIgnore @KVIndex
    private def id: String = info.id
    功能: 获取执行器id
    
    @JsonIgnore @KVIndex("active")
    private def active: Boolean = info.isActive
    功能: 获取执行器当前状态是否是激活状态
    
    @JsonIgnore @KVIndex("host")
    val host: String = info.hostPort.split(":")(0)
    功能: 获取执行器主机名
}

private[spark] class JobDataWrapper(val info: JobData,val skippedStages: Set[Int],
    val sqlExecutionId: Option[Long]){
    介绍: job数据包装器
    	当job提交时，保持追踪当前存在的stage,在job执行中完成的stage.运行精确统计有多少个任务中途skip过去
    构造器参数:
    	info	job数据
    	skippedStages	跳过的stage集合
    	sqlExecutionId	sql执行编号
   	操作集:
    @JsonIgnore @KVIndex
    private def id: Int = info.jobId
    功能: 获取jobId
    
    @JsonIgnore @KVIndex("completionTime")
    private def completionTime: Long = info.completionTime.map(_.getTime).getOrElse(-1L)
    功能: 获取完成时间，缺省值为-1
}

private[spark] class StageDataWrapper(val info: StageData,val jobIds: Set[Int],
    @JsonDeserialize(contentAs = classOf[JLong]) val locality: Map[String, Long]){
	介绍: stage数据包装器
    构造器参数:
    	info	stage数据信息
    	jobIds	jobID列表
    	locality	位置映射表
    操作集:
   	@JsonIgnore @KVIndex
    private[this] val id: Array[Int] = Array(info.stageId, info.attemptId)
	功能: 获取stageid列表(stage的id和请求编号)
    
    @JsonIgnore @KVIndex("stageId")
    private def stageId: Int = info.stageId
    功能: 获取stage id
    
    @JsonIgnore @KVIndex("active")
    private def active: Boolean = info.status == StageStatus.ACTIVE
    功能: 获取stage是否处于激活状态
    
    @JsonIgnore @KVIndex("completionTime")
    private def completionTime: Long = info.completionTime.map(_.getTime).getOrElse(-1L)
    功能: 获取stage完成时间
}

private[spark] object TaskIndexNames {
  介绍: 任务索引名称，任务有许多目录项，可以使用在不同的地方。这个对象保存了这些目录项的逻辑名称，当使用磁盘存储数据时映射到短名称。
  final val ACCUMULATORS = "acc"	// 累加器
  final val ATTEMPT = "att"	// 请求
  final val DESER_CPU_TIME = "dct"	// 反序列化CPU时间
  final val DESER_TIME = "des" //反序列化时间
  final val DISK_SPILL = "dbs" // 磁盘溢写量
  final val DURATION = "dur" // 持续时间(周期)
  final val ERROR = "err" // 错误
  final val EXECUTOR = "exe" // 执行器
  final val HOST = "hst" // 主机名
  final val EXEC_CPU_TIME = "ect" // 执行器CPU运行时间
  final val EXEC_RUN_TIME = "ert" // 执行器运行时间
  final val GC_TIME = "gc" // gc时间
  final val GETTING_RESULT_TIME = "grt" // 获取结果的时间
  final val INPUT_RECORDS = "ir" // 输入记录数量
  final val INPUT_SIZE = "is" // 输入规模大小
  final val LAUNCH_TIME = "lt" // 运行时间
  final val LOCALITY = "loc" // 位置
  final val MEM_SPILL = "mbs" // 内存溢写量
  final val OUTPUT_RECORDS = "or" // 输出记录数量
  final val OUTPUT_SIZE = "os" // 输出规模大小
  final val PEAK_MEM = "pem" // 峰值内存大小
  final val RESULT_SIZE = "rs" // 结果规模大小
  final val SCHEDULER_DELAY = "dly" // 调度延时
  final val SER_TIME = "rst" // 序列化时间
  final val SHUFFLE_LOCAL_BLOCKS = "slbl" //shuffle本地获取文件块数量
  final val SHUFFLE_READ_RECORDS = "srr" //shuffle读取记录数量
  final val SHUFFLE_READ_TIME = "srt" // shuffle读取时间
  final val SHUFFLE_REMOTE_BLOCKS = "srbl" // shuffle远端获取文件块数量
  final val SHUFFLE_REMOTE_READS = "srby" // shuffle远端读取字节数量
  final val SHUFFLE_REMOTE_READS_TO_DISK = "srbd" // shuffle远端读取到磁盘上字节数量
  final val SHUFFLE_TOTAL_READS = "stby" //shuffle总计读取字节数量
  final val SHUFFLE_TOTAL_BLOCKS = "stbl" // shuffle总计获取文件块数量
  final val SHUFFLE_WRITE_RECORDS = "swr" // shuffle写出记录数量
  final val SHUFFLE_WRITE_SIZE = "sws" // shuffle写出规模大小
  final val SHUFFLE_WRITE_TIME = "swt" // shuffle写出时间
  final val STAGE = "stage" // 任务所属stage
  final val STATUS = "sta" // 任务状态
  final val TASK_INDEX = "idx" // 任务索引
  final val COMPLETION_TIME = "ct" // 任务完成时间
}

private[spark] class RDDStorageInfoWrapper(val info: RDDStorageInfo) {
    介绍: RDD存储信息包装器
    构造器参数:
    	info	RDD存储信息
    操作集:
    @JsonIgnore @KVIndex
    def id: Int = info.id
    功能: 获取RDD编号
    
    @JsonIgnore @KVIndex("cached")
    def cached: Boolean = info.numCachedPartitions > 0
    功能: 确定是否存在缓存
}

private[spark] class ExecutorStageSummaryWrapper(val stageId: Int,val stageAttemptId: Int,
    val executorId: String,val info: ExecutorStageSummary){
	介绍: 执行器stage信息包装器
    构造器参数:
    	stageId	stageID
    	stageAttemptId	stage请求ID
    	executorId	执行器ID
    	info 	执行器Stage信息
    属性:
    @JsonIgnore @KVIndex
    private val _id: Array[Any] = Array(stageId, stageAttemptId, executorId)
    	stageID,stage请求ID,执行器ID三元组信息
    操作集:
    @JsonIgnore @KVIndex("stage")
    private def stage: Array[Int] = Array(stageId, stageAttemptId)
    功能: 获取stage ID和stage请求信息
    
    @JsonIgnore
    def id: Array[Any] = _id
    功能: 获取stage三元组信息
}

private[spark] class StreamBlockData(val name: String,val executorId: String,val hostPort: String,
  val storageLevel: String,val useMemory: Boolean,val useDisk: Boolean,val deserialized: Boolean,
  val memSize: Long,val diskSize: Long){
    介绍: 流式块数据信息
    构造器:
    	name	块名称
    	executorId	执行器ID
    	hostPort	主机端口
    	storageLevel	存储等级
    	useMemory	是否使用内存
    	useDisk	是否使用磁盘
    	deserialized	是否反序列化
    	memSize	内存大小
    	diskSize	磁盘大小
    操作集:
    @JsonIgnore @KVIndex
    def key: Array[String] = Array(name, executorId)
    功能: 获取信息二元组(名称 和 执行器ID)
}

private[spark] class RDDOperationClusterWrapper(val id: String,val name: String,
    val childNodes: Seq[RDDOperationNode],val childClusters: Seq[RDDOperationClusterWrapper]){
    介绍: RDD集群操作包装器
    构造器参数:
    	id	RDD编号
    	name	RDD名称
    	childNodes	RDD操作子结点列表(RDD操作图中的一个节点)
    	childClusters	RDD操作子节点集群列表
    操作集:
    def toRDDOperationCluster(): RDDOperationCluster 
    功能: 获取RDD操作集群
    1. 获取RDD操作集群名称
    val isBarrier = childNodes.exists(_.barrier)
    val name = if (isBarrier) this.name + "\n(barrier mode)" else this.name
 	2. 获取RDD操作集群
    val cluster = new RDDOperationCluster(id, isBarrier, name)
    3. 将子节点链接到集群中
    childNodes.foreach(cluster.attachChildNode)
    childClusters.foreach { child =>
      cluster.attachChildCluster(child.toRDDOperationCluster())
    }
    val= cluster
}

private[spark] class RDDOperationGraphWrapper(
    @KVIndexParam val stageId: Int,
    val edges: Seq[RDDOperationEdge],
    val outgoingEdges: Seq[RDDOperationEdge],
    val incomingEdges: Seq[RDDOperationEdge],
    val rootCluster: RDDOperationClusterWrapper) {
    介绍: RDD图操作包装器
    构造器参数:
    	stageId		stageID
    	edges		RDD操作边集
    	outgoingEdges	出向边列表
    	incomingEdges	入向边列表
    	rootCluster	集群根节点
    操作集:
    def toRDDOperationGraph(): RDDOperationGraph
    功能: 获取RDD操作图@RDDOperationGraph
    val= new RDDOperationGraph(edges, outgoingEdges, incomingEdges,
                               rootCluster.toRDDOperationCluster())
}

private[spark] class PoolData(@KVIndexParam val name: String,val stageIds: Set[Int]){
    介绍: 池数据
    构造器参数:
    	name	池名称
    	stageIds	stageID列表
}

private[spark] class AppSummary(val numCompletedJobs: Int,val numCompletedStages: Int){
    介绍: 应用信息
    构造器参数:
    	numCompletedJobs	完成job数量
    	numCompletedStages	完成stage数量
    操作集:
    @KVIndex
    def id: String = classOf[AppSummary].getName()
    功能: 获取应用名(KV存储器存储的应用名称就是它的类名)
}

private[spark] class CachedQuantile(
    val stageId: Int, // stage ID
    val stageAttemptId: Int,	// stage 请求ID
    val quantile: String,  // 分位点
    val taskCount: Long, // 任务数量
	// 下面的属性是@TaskMetricDistributions的分解视图属性
    val executorDeserializeTime: Double, // 执行器反序列化时间
    val executorDeserializeCpuTime: Double, // 执行器反序列化CPU时间
    val executorRunTime: Double, // 执行器运行时间
    val executorCpuTime: Double, // 执行器CPU时间
    val resultSize: Double, // 结果规模大小
    val jvmGcTime: Double, // jvm gc时间
    val resultSerializationTime: Double, // 结果序列化时间
    val gettingResultTime: Double, // 获取结果时间
    val schedulerDelay: Double, // 调度延时
    val peakExecutionMemory: Double, // 执行器峰值内存使用量
    val memoryBytesSpilled: Double, // 溢写内存字节量
    val diskBytesSpilled: Double, // 溢写磁盘字节量
    val bytesRead: Double, // 读取字节量
    val recordsRead: Double, // 读取记录数量
    val bytesWritten: Double, // 写出字节量
    val recordsWritten: Double, // 写出记录数量
    val shuffleReadBytes: Double, // shuffle读取字节数量
    val shuffleRecordsRead: Double, // shuffle读取记录数量
    val shuffleRemoteBlocksFetched: Double, // shuffle远端获取文件块数量
    val shuffleLocalBlocksFetched: Double, // shufle本地获取文件块数量
    val shuffleFetchWaitTime: Double, // shuffle获取等待时间
    val shuffleRemoteBytesRead: Double, // shuffle远端读取字节数量
    val shuffleRemoteBytesReadToDisk: Double, // shuffle远端读取到磁盘的字节数量
    val shuffleTotalBlocksFetched: Double, // shuffle总计获取文件块数量
    val shuffleWriteBytes: Double, // shuffle写字节数量
    val shuffleWriteRecords: Double, // shuffle写出记录数量
    val shuffleWriteTime: Double //  shuffle写出时间)
{
    操作集:
    @KVIndex @JsonIgnore
    def id: Array[Any] = Array(stageId, stageAttemptId, quantile)
    功能: 获取stage信息三元组
    
    @KVIndex("stage") @JsonIgnore
    def stage: Array[Int] = Array(stageId, stageAttemptId)
    功能: 获取stage信息
}
    
private[spark] class TaskDataWrapper(
    @KVIndexParam
    val taskId: JLong, // 任务编号
    @KVIndexParam(value = TaskIndexNames.TASK_INDEX, parent = TaskIndexNames.STAGE)
    val index: Int, // 任务索引
    @KVIndexParam(value = TaskIndexNames.ATTEMPT, parent = TaskIndexNames.STAGE)
    val attempt: Int, // 请求编号
    @KVIndexParam(value = TaskIndexNames.LAUNCH_TIME, parent = TaskIndexNames.STAGE)
    val launchTime: Long, // 运行时间
    val resultFetchStart: Long, // 开始获取结果时间
    @KVIndexParam(value = TaskIndexNames.DURATION, parent = TaskIndexNames.STAGE)
    val duration: Long, // 持续周期
    @KVIndexParam(value = TaskIndexNames.EXECUTOR, parent = TaskIndexNames.STAGE)
    val executorId: String, // 执行器编号
    @KVIndexParam(value = TaskIndexNames.HOST, parent = TaskIndexNames.STAGE)
    val host: String, // 主机名
    @KVIndexParam(value = TaskIndexNames.STATUS, parent = TaskIndexNames.STAGE)
    val status: String, // 任务状态
    @KVIndexParam(value = TaskIndexNames.LOCALITY, parent = TaskIndexNames.STAGE)
    val taskLocality: String, // 任务位置名称
    val speculative: Boolean, // 是否推测执行
    val accumulatorUpdates: Seq[AccumulableInfo], // 更新累加器列表
    val errorMessage: Option[String],  // 错误信息
    val hasMetrics: Boolean, // 是否有度量信息
    @KVIndexParam(value = TaskIndexNames.DESER_TIME, parent = TaskIndexNames.STAGE)
    val executorDeserializeTime: Long,// 执行器反序列化时间
    @KVIndexParam(value = TaskIndexNames.DESER_CPU_TIME, parent = TaskIndexNames.STAGE)
    val executorDeserializeCpuTime: Long, // 执行器反序列化CPU时间
    @KVIndexParam(value = TaskIndexNames.EXEC_RUN_TIME, parent = TaskIndexNames.STAGE)
    val executorRunTime: Long, // 执行器运行时间
    @KVIndexParam(value = TaskIndexNames.EXEC_CPU_TIME, parent = TaskIndexNames.STAGE)
    val executorCpuTime: Long, // 执行器CPU运行时间
    @KVIndexParam(value = TaskIndexNames.RESULT_SIZE, parent = TaskIndexNames.STAGE)
    val resultSize: Long, //结果规模大小
    @KVIndexParam(value = TaskIndexNames.GC_TIME, parent = TaskIndexNames.STAGE)
    val jvmGcTime: Long, // gc时间
    @KVIndexParam(value = TaskIndexNames.SER_TIME, parent = TaskIndexNames.STAGE)
    val resultSerializationTime: Long, //结果序列化时间
    @KVIndexParam(value = TaskIndexNames.MEM_SPILL, parent = TaskIndexNames.STAGE)
    val memoryBytesSpilled: Long, // 内存溢写字节量
    @KVIndexParam(value = TaskIndexNames.DISK_SPILL, parent = TaskIndexNames.STAGE)
    val diskBytesSpilled: Long, // 磁盘溢写字节量
    @KVIndexParam(value = TaskIndexNames.PEAK_MEM, parent = TaskIndexNames.STAGE)
    val peakExecutionMemory: Long, // 执行器峰值内存量
    @KVIndexParam(value = TaskIndexNames.INPUT_SIZE, parent = TaskIndexNames.STAGE)
    val inputBytesRead: Long, // 读取输入字节量
    @KVIndexParam(value = TaskIndexNames.INPUT_RECORDS, parent = TaskIndexNames.STAGE)
    val inputRecordsRead: Long, // 读取输入记录数量
    @KVIndexParam(value = TaskIndexNames.OUTPUT_SIZE, parent = TaskIndexNames.STAGE)
    val outputBytesWritten: Long, // 输出字节量
    @KVIndexParam(value = TaskIndexNames.OUTPUT_RECORDS, parent = TaskIndexNames.STAGE)
    val outputRecordsWritten: Long,// 输出记录数量
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_BLOCKS, parent = TaskIndexNames.STAGE)
    val shuffleRemoteBlocksFetched: Long, // shuffle远端获取文件块数据
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_LOCAL_BLOCKS, parent = TaskIndexNames.STAGE)
    val shuffleLocalBlocksFetched: Long, // shuffle本地获取文件块数量
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_READ_TIME, parent = TaskIndexNames.STAGE)
    val shuffleFetchWaitTime: Long, // shuffle获取等待时间
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_READS, parent = TaskIndexNames.STAGE)
    val shuffleRemoteBytesRead: Long, // shuffle远端读取字节量
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_READS_TO_DISK,
      parent = TaskIndexNames.STAGE)
    val shuffleRemoteBytesReadToDisk: Long, // shuffle远端读取到磁盘上的字节量
    val shuffleLocalBytesRead: Long, //shuffle本地读取字节量
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_READ_RECORDS, parent = TaskIndexNames.STAGE)
    val shuffleRecordsRead: Long, // shuffle读取记录数量
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_SIZE, parent = TaskIndexNames.STAGE)
    val shuffleBytesWritten: Long, // shuffle写出字节量
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_TIME, parent = TaskIndexNames.STAGE)
    val shuffleWriteTime: Long, // shuffle写出时间
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_RECORDS, parent = TaskIndexNames.STAGE)
    val shuffleRecordsWritten: Long, // shujffle写出记录数量
    val stageId: Int, //stageID
    val stageAttemptId: Int // stage请求编号
){
 	介绍: 任务数据包装器，不像其他数据类型，这个并没有获取@TaskData 的引用。主要是节省内存，因为大量应用就会导		致大量的这种属性(默认情况下，可以达到每个stage 100,000)，每个浪费内存累加起来的得到的位数。
    这里也包含二级索引，用于高效的对数据进行排序。
    操作集:
    def toApi: TaskData 
    功能: 获取任务数据信息@TaskData
    val metrics = if (hasMetrics) {
      Some(new TaskMetrics(
        getMetricValue(executorDeserializeTime),
        getMetricValue(executorDeserializeCpuTime),
        getMetricValue(executorRunTime),
        getMetricValue(executorCpuTime),
        getMetricValue(resultSize),
        getMetricValue(jvmGcTime),
        getMetricValue(resultSerializationTime),
        getMetricValue(memoryBytesSpilled),
        getMetricValue(diskBytesSpilled),
        getMetricValue(peakExecutionMemory),
        new InputMetrics(
          getMetricValue(inputBytesRead),
          getMetricValue(inputRecordsRead)),
        new OutputMetrics(
          getMetricValue(outputBytesWritten),
          getMetricValue(outputRecordsWritten)),
        new ShuffleReadMetrics(
          getMetricValue(shuffleRemoteBlocksFetched),
          getMetricValue(shuffleLocalBlocksFetched),
          getMetricValue(shuffleFetchWaitTime),
          getMetricValue(shuffleRemoteBytesRead),
          getMetricValue(shuffleRemoteBytesReadToDisk),
          getMetricValue(shuffleLocalBytesRead),
          getMetricValue(shuffleRecordsRead)),
        new ShuffleWriteMetrics(
          getMetricValue(shuffleBytesWritten),
          getMetricValue(shuffleWriteTime),
          getMetricValue(shuffleRecordsWritten))))
    } else {
      None
    }
    val= new TaskData(
      taskId,
      index,
      attempt,
      new Date(launchTime),
      if (resultFetchStart > 0L) Some(new Date(resultFetchStart)) else None,
      if (duration > 0L) Some(duration) else None,
      executorId,
      host,
      status,
      taskLocality,
      speculative,
      accumulatorUpdates,
      errorMessage,
      metrics,
      executorLogs = null,
      schedulerDelay = 0L,
      gettingResultTime = 0L)
    
    @JsonIgnore @KVIndex(TaskIndexNames.STAGE)
    private def stage: Array[Int] = Array(stageId, stageAttemptId)
    功能: 获取stageID信息
    
    @JsonIgnore @KVIndex(value = TaskIndexNames.COMPLETION_TIME, parent = TaskIndexNames.STAGE)
    private def completionTime: Long = launchTime + duration
    功能: 获取完成时刻
    
    @JsonIgnore @KVIndex(value = TaskIndexNames.ERROR, parent = TaskIndexNames.STAGE)
    private def error: String = if (errorMessage.isDefined) errorMessage.get else ""
    功能: 获取错误信息
    
    @JsonIgnore @KVIndex(value = TaskIndexNames.SHUFFLE_TOTAL_BLOCKS, parent = TaskIndexNames.STAGE)
    private def shuffleTotalBlocks: Long 
    功能: 获取shuffle总计获取的文件块数量
    val= !hasMetrics ? -1L :
    	getMetricValue(shuffleLocalBlocksFetched) + getMetricValue(shuffleRemoteBlocksFetched)
    
    @JsonIgnore @KVIndex(value = TaskIndexNames.SHUFFLE_TOTAL_READS, parent = TaskIndexNames.STAGE)
    private def shuffleTotalReads: Long
    功能: 获取shuffle读取字节量
    val= !hasMetrics ? -1L :
    	getMetricValue(shuffleLocalBytesRead) + getMetricValue(shuffleRemoteBytesRead)
	
    @JsonIgnore @KVIndex(value = TaskIndexNames.GETTING_RESULT_TIME, parent = TaskIndexNames.STAGE)
    def gettingResultTime: Long
    功能: 获取结果的时间
    val= !hasMetrics? -1L :
    	AppStatusUtils.gettingResultTime(launchTime, resultFetchStart, duration)
   	
    @JsonIgnore @KVIndex(value = TaskIndexNames.SCHEDULER_DELAY, parent = TaskIndexNames.STAGE)
    def schedulerDelay: Long
    功能: 获取调度延时时间
    val= !hasMetrics? -1L :
    	AppStatusUtils.schedulerDelay(launchTime, resultFetchStart, duration,
        	getMetricValue(executorDeserializeTime),
        	getMetricValue(resultSerializationTime),
        	getMetricValue(executorRunTime))
   	
    @JsonIgnore @KVIndex(value = TaskIndexNames.ACCUMULATORS, parent = TaskIndexNames.STAGE)
    private def accumulators: String
    功能: 获取更新累加器@accumulatorUpdates 列表中累加器的名称/数值信息(调用一次头指针移动一次)
    val= if (accumulatorUpdates.nonEmpty) {
      val acc = accumulatorUpdates.head
      s"${acc.name}:${acc.value}"
    } else {
      ""
    }
} 
```

#### 基础拓展

1.  scala type关键字

2.  LevelDB

   参考链接: https://zh.wikipedia.org/wiki/LevelDB
   
3.   注解