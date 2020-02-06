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

   ```markdown
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
   
   class OutputMetricDistributions private[spark](
       val bytesWritten: IndexedSeq[Double],
       val recordsWritten: IndexedSeq[Double])
   介绍: 输出度量分布
   参数
   	bytesWritten	写出字节量分布
   	recordsWritten	写出记录数量分布
   
   class ShuffleReadMetricDistributions private[spark](
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
    	
    class TaskMetricDistributions private[spark](
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

```markdown
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

#### AppStatusSource

```markdown
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

```markdown
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

#### AppStatusUtils

```markdown
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

```markdown
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

```markdown
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


```markdown
private[spark] object ElementTrackingStore {
	sealed trait WriteQueueResult	需要单独的支持执行器的正确性，否则写方法无效
	object WriteQueued extends WriteQueueResult			写队列
	object WriteSkippedQueue extends WriteQueueResult	跳写队列
}
```

#### KVUtils

```markdown
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

#### storeTypes

#### 基础拓展

1.  scala type关键字

2.  LevelDB

   参考链接: https://zh.wikipedia.org/wiki/LevelDB