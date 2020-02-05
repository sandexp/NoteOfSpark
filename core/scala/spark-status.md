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

   #### ApiRootResource

   #### ApplicationListResource

   #### JacksonMessageWriter

   #### OneApplicationResource

   #### PrometheusResource

   #### SimpleDateParam

   #### StagesResource

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
    
}
```



#### ElementTrackingStore

#### KVUtils

#### LiveEntity

#### storeTypes

