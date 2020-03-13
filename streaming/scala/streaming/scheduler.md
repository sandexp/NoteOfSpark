1. [rate](# rate)
2. [BatchInfo.scala](# BatchInfo)
3. [ExecutorAllocationManager.scala](# ExecutorAllocationManager)
4. [InputInfoTracker.scala](# InputInfoTracker)
5. [Job.scala](# Job)
6. [JobGenerator.scala](# JobGenerator)
7. [JobScheduler.scala](# JobScheduler)
8. [JobSet.scala](# JobSet)
9. [OutputOperationInfo.scala](# OutputOperationInfo)
10. [RateController.scala](# RateController)
11. [ReceivedBlockInfo.scala](# ReceivedBlockInfo)
12. [ReceivedBlockTracker.scala](# ReceivedBlockTracker)
13. [ReceiverInfo.scala](# ReceiverInfo)
14. [ReceiverSchedulingPolicy.scala](# ReceiverSchedulingPolicy)
15. [ReceiverTracker.scala](# ReceiverTracker)
16. [ReceiverTrackingInfo.scala](# ReceiverTrackingInfo)
17. [StreamingListener.scala](# StreamingListener)
18. [StreamingListenerBus.scala](# StreamingListenerBus)

---

#### rate

##### PIDRateEstimator

```markdown
介绍:
PID控制器(比例积分微分控制器),这个会加速spark streaming的元素吞吐速度.PID控制器通过计算输出和期望值之间的误差.在spark-streaming中误差就是度量进程比例(val= 元素/进程延时)和前一个值的差.
可以参考PID控制器
<https://en.wikipedia.org/wiki/PID_controller> 	
构造器参数:
	batchIntervalMillis	批次持续时间
	proportional	比例参数,默认值为1.可以为正数和0,但是设置过大则会使控制器超出预设的上限值,但是过小则会导致控制器不敏感
	integral	积分参数,描述对过去误差的积分参数,可以为正数或者0,这个值可以加速逼近期望值的速度.但是较大的值会导致超出限制,默认值为0.2
	derivative	微分参数,影响下一个误差的预测值,基于当前比例的改变速度,这个值可以为正数或者0.这个值不常用,且会影响系统的稳定性.默认值为0.
	minRate	估测的最小比例,必须大于0,这样系统总是可以接受数据,使得比例估测可以进行
```

```scala
private[streaming] class PIDRateEstimator(
    batchIntervalMillis: Long,
    proportional: Double,
    integral: Double,
    derivative: Double,
    minRate: Double
) extends RateEstimator with Logging {
    属性:
    #name @firstRun: Boolean = true	是否首次运行
    #name @latestTime: Long = -1L	最新运行时间
    #name @latestRate: Double = -1D	最新计算比例
    #name @latestError: Double = -1L	最新误差
    初始化操作:
    require(
        batchIntervalMillis > 0,
        s"Specified batch interval $batchIntervalMillis in PIDRateEstimator is invalid.")
    require(
        proportional >= 0,
        s"Proportional term $proportional in PIDRateEstimator should be >= 0.")
    require(
        integral >= 0,
        s"Integral term $integral in PIDRateEstimator should be >= 0.")
    require(
        derivative >= 0,
        s"Derivative term $derivative in PIDRateEstimator should be >= 0.")
    require(
        minRate > 0,
        s"Minimum rate in PIDRateEstimator should be > 0")
    功能: 参数校验
    
    logInfo(s"Created PIDRateEstimator with proportional = $proportional, 
    integral = $integral, " +
    s"derivative = $derivative, min rate = $minRate")
    功能: 显示PID信息
    
    def compute(
      time: Long, // in milliseconds
      numElements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long // in milliseconds
    ): Option[Double] 
    功能: 计算误差值
    输入参数:
    	time	时间
    	numElements	元素数量
    	processingDelay	进程延时
    	schedulingDelay	调度延时
    0. 信息显示
    logTrace(s"\ntime = $time, # records = $numElements, " +
      s"processing time = $processingDelay, scheduling delay = $schedulingDelay")
    1. 计算误差
    this.synchronized {
      if (time > latestTime && numElements > 0 && processingDelay > 0) {
          // 计算自上次更新锁经过的时间(s)
        val delaySinceUpdate = (time - latestTime).toDouble / 1000
          // 计算每秒传输的元素数量(double类型)
        val processingRate = numElements.toDouble / processingDelay * 1000
          // 计算期望值和测量值的误差,将期望值看做最新比例,上一个值就是@processingRate,求得误差如下
        val error = latestRate - processingRate
          // 计算历史误差
          /*
          	基于调度延时@schedulingDelay 作为指示参数,用于累积误差,从而计算误差积分.
          	调度延时s对应于s * processingRate个溢出元素,这些元素不能在上一个批次中处理,从而导致了这个
          	延时的存在.做出如下假设,假定@processingRate 不会发生太大的变化.可以从溢出的元素数量中,可
          	以计算这个比例(通过批次间隔使之分开).这个误差叫做历史误差或者积分误差.如果从之前的比例中减去			这个比例,那么调度延时就会变成0.
          	单位: 元素个数/s
          */
        val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis
          // 计算误差变化率
        val dError = (error - latestError) / delaySinceUpdate
          // 根据PID计算公式,计算新的比例
        val newRate = (latestRate - proportional * error -
                                    integral * historicalError -
                                    derivative * dError).max(minRate)
        logTrace(s"""
            | latestRate = $latestRate, error = $error
            | latestError = $latestError, historicalError = $historicalError
            | delaySinceUpdate = $delaySinceUpdate, dError = $dError
            """.stripMargin)
          // 设置更新时间
        latestTime = time
        if (firstRun) { // 首次运行,初始化比例/误差阐述
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          logTrace("First run, rate estimation skipped")
          None
        } else { // 更新比例/误差参数
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some(newRate)
        }
      } else {
        logTrace("Rate estimation skipped")
        None
      }
    }
}
```

##### RateEstimator

```scala
private[streaming] trait RateEstimator extends Serializable {
    介绍: 比例估算器
    这个可以估算@InputDStream 的比例,@InputDStream需要接受记录(基于每个批次完成的更新)
    可以参考@org.apache.spark.streaming.scheduler.RateController,获取详细的信息
    
    def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Double]
    功能: 计算stream连接到@RateEstimator的记录数量,返回每秒接受的记录数量.给定更新的大小和最新批次完成的次数.
    输入参数:
    time	当前刚刚完成的批次时间间隔的时间戳
    elements	当前批次处理的元素数量
    processingDelay	job进行的时间
    schedulingDelay	调度需要的时间
}
```

```scala
object RateEstimator {
    def create(conf: SparkConf, batchInterval: Duration): RateEstimator 
    功能: 创建比例估测器
    conf.get("spark.streaming.backpressure.rateEstimator", "pid") match {
      case "pid" =>
        val proportional =
        	conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
        val integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
        val derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
        val minRate = conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
        new PIDRateEstimator(
            batchInterval.milliseconds, proportional, integral, derived, minRate)
      case estimator =>
        throw new IllegalArgumentException(s"Unknown rate estimator: $estimator")
    }
}
```

#### BatchInfo

```scala
@DeveloperApi
case class BatchInfo(
    batchTime: Time,
    streamIdToInputInfo: Map[Int, StreamInputInfo],
    submissionTime: Long,
    processingStartTime: Option[Long],
    processingEndTime: Option[Long],
    outputOperationInfos: Map[Int, OutputOperationInfo]
  ) {
    介绍: 批次信息
    构造器参数:
        batchTime	批次数据
        streamIdToInputInfo	streamID->输入stream信息映射表
        submissionTime	提交时间
        processingStartTime	处理开始时间
        processingEndTime	处理结束时间
        outputOperationInfos	输出操作信息映射表
    操作集:
    def schedulingDelay: Option[Long] = processingStartTime.map(_ - submissionTime)
    功能: 获取调度时间
    
    def processingDelay: Option[Long]
    功能: 获取任务处理时间
    val= processingEndTime.zip(processingStartTime)
    .map(x => x._1 - x._2).headOption
    
    def totalDelay: Option[Long]
    功能: 获取这个批次任务的总延时,包括运行延时和调度延时
    val= schedulingDelay.zip(processingDelay)
    .map(x => x._1 + x._2).headOption
    
    def numRecords: Long = streamIdToInputInfo.values.map(_.numRecords).sum
    功能: 获取当前批次接收器接受的记录数量
}
```

#### ExecutorAllocationManager

```markdown
介绍:
 这个类管理分配到@StreamingContext中的执行器,动态的请求或者kill执行器(基于流式计算).这个不同于核心动态分配策略,核心策略短时间内依赖于空载执行器,但是流的微批次模型最迟了执行器的长期空载行为.相反,空载测量需要基于每个批次的进行时间.
 在高版本中,这个策略由这个类实现
 - 使用流式监听器接口@StreamingListener 获取完成的批次的批次运行时间
 - 周期性的获取批次完成时间的平均值,并与批次间隔比较
 - 如果 平均运行时间@avg.proc.time/@batch interval<= scaling down ratio,即单位时间处理的时间小于指定比例,则会kill运行接收器的执行器.
 这个特征需要和反压力一起运行,因为反压力(backpressure)保证了系统的稳定性,在这种系统中执行器是只读的.
 注意执行器的初始化状态在spark上下文创建的时候就被分配了,这个类在streaming上下文启动之后可以扩大/缩小.
```

```scala
private[streaming] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    receiverTracker: ReceiverTracker,
    conf: SparkConf,
    batchDurationMs: Long,
    clock: Clock) extends StreamingListener with Logging {
    构造器参数:
    client	执行器分配上下文
    receiverTracker	接收器定位器
    conf	spark配置
    batchDurationMs	批次持续时间
    clock	时钟
    属性:
    #name @scalingIntervalSecs = conf.get(STREAMING_DYN_ALLOCATION_SCALING_INTERVAL)
    	streaming动态分配扩展的时间间隔
    #name @scalingUpRatio = conf.get(STREAMING_DYN_ALLOCATION_SCALING_UP_RATIO)
    	streaming 动态分配扩展比例(上限,默认0.9)
    #name @scalingUpRatio = conf.get(STREAMING_DYN_ALLOCATION_SCALING_UP_RATIO)
    	streaming 动态分配扩展比例(下限,默认0.3)
    #name @minNumExecutors	最小执行器数量(极限最小为1)
    val= conf.get(STREAMING_DYN_ALLOCATION_MIN_EXECUTORS)
    .getOrElse(math.max(1, receiverTracker.numReceivers()))
    #name @maxNumExecutors = conf.get(STREAMING_DYN_ALLOCATION_MAX_EXECUTORS)
    	最大执行器数量
    #name @timer	重现计时器
    val= new RecurringTimer(clock, scalingIntervalSecs * 1000,
    	_ => manageAllocation(), "streaming-executor-allocation-manager")
    #name @batchProcTimeSum = 0L	volatile	批次处理总时间
    #name @batchProcTimeCount = 0	volatile	批次计数器
    初始化操作:
    validateSettings()
    功能: 参数校验
    
    操作集:
    def start(): Unit
    功能: 启动执行器分配管理器
    timer.start()
    logInfo(s"ExecutorAllocationManager started with " +
      s"ratios = [$scalingUpRatio, $scalingDownRatio] and interval 
      = $scalingIntervalSecs sec")
    
    def stop(): Unit
    功能: 停止执行器分配管理器
    timer.stop(interruptTimer = true)
    logInfo("ExecutorAllocationManager stopped")
    
    def manageAllocation(): Unit
    功能: 基于收集的批次统计请求/kill执行器,从而管理执行器分配
    1. 申请/释放执行器
    logInfo(s"Managing executor allocation with ratios = 
    	[$scalingUpRatio, $scalingDownRatio]")
    if (batchProcTimeCount > 0) {
        // 计算批次中单位时间内平均批次的处理时间(处理比例),如果大于上限则需要申请执行器
        // 小于下限则需要kill执行器,保持比例处于指定的范围内部
      val averageBatchProcTime = batchProcTimeSum / batchProcTimeCount
      val ratio = averageBatchProcTime.toDouble / batchDurationMs
      logInfo(s"Average: $averageBatchProcTime, ratio = $ratio" )
      if (ratio >= scalingUpRatio) {
        logDebug("Requesting executors")
          // 计算需要申请的执行器数量
        val numNewExecutors = math.max(math.round(ratio).toInt, 1)
        requestExecutors(numNewExecutors)
      } else if (ratio <= scalingDownRatio) {
        logDebug("Killing executors")
        killExecutor()
      }
    }
    2. 重置批次统计值
    batchProcTimeSum = 0
    batchProcTimeCount = 0
    
    def requestExecutors(numNewExecutors: Int): Unit
    功能: 请求指定数量的执行器
    0. 参数断言
    require(numNewExecutors >= 1)
    1. 获取执行器列表
    val allExecIds = client.getExecutorIds()
    logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")
    2. 获取需要申请的执行器数量
    val targetTotalExecutors =
      math.max(
          math.min(maxNumExecutors, allExecIds.size + numNewExecutors), minNumExecutors)
    3. 申请执行器
    client.requestTotalExecutors(targetTotalExecutors, 0, Map.empty)
    logInfo(s"Requested total $targetTotalExecutors executors")
    
    def killExecutor(): Unit
    功能: kill没有运行接收器的执行器
    1. 获取执行器列表
    val allExecIds = client.getExecutorIds()
    logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")
    2. 清除执行器
    if (allExecIds.nonEmpty && allExecIds.size > minNumExecutors) {
        // 确定带有接收器的执行器
      val execIdsWithReceivers = receiverTracker.allocatedExecutors.values.flatten.toSeq
      logInfo(s"Executors with receivers (${execIdsWithReceivers.size}):
      ${execIdsWithReceivers}")
        // 确定需要移除的执行器
      val removableExecIds = allExecIds.diff(execIdsWithReceivers)
      logDebug(s"Removable executors (${removableExecIds.size}): ${removableExecIds}")
        // 移除执行器
      if (removableExecIds.nonEmpty) {
        val execIdToRemove = removableExecIds(Random.nextInt(removableExecIds.size))
        client.killExecutor(execIdToRemove)
        logInfo(s"Requested to kill executor $execIdToRemove")
      } else {
        logInfo(s"No non-receiver executors to kill")
      }
    } else {
      logInfo("No available executor to kill")
    }
    
    def addBatchProcTime(timeMs: Long): Unit =
    功能: 批次处理时间增加@timeMs
    batchProcTimeSum += timeMs
    batchProcTimeCount += 1
    logDebug(
      s"Added batch processing time $timeMs, sum = $batchProcTimeSum, 
      count = $batchProcTimeCount")
    
    def validateSettings(): Unit
    功能: 参数校验
    1. 校验扩展比的关系
    require(
      scalingUpRatio > scalingDownRatio,
      s"Config ${STREAMING_DYN_ALLOCATION_SCALING_UP_RATIO.key} 
      must be more than config " +
        s"${STREAMING_DYN_ALLOCATION_SCALING_DOWN_RATIO.key}")
    2. 校验执行器数量的关系
    if (conf.contains(STREAMING_DYN_ALLOCATION_MIN_EXECUTORS.key) &&
      conf.contains(STREAMING_DYN_ALLOCATION_MAX_EXECUTORS.key)) {
      require(
        maxNumExecutors >= minNumExecutors,
        s"Config ${STREAMING_DYN_ALLOCATION_MAX_EXECUTORS.key} must 
        be more than config " +
          s"${STREAMING_DYN_ALLOCATION_MIN_EXECUTORS.key}")
    }
    
    def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit
    功能: 批次完成处理(累加批次处理时间)
    if (!batchCompleted.batchInfo.outputOperationInfos.
        values.exists(_.failureReason.nonEmpty)) {
      batchCompleted.batchInfo.processingDelay.foreach(addBatchProcTime)
    }
}
```

```scala
private[streaming] object ExecutorAllocationManager extends Logging {
    def isDynamicAllocationEnabled(conf: SparkConf): Boolean
    功能: 确定是否允许动态分配
    1. 确定是否开启动态分配
    val streamingDynamicAllocationEnabled = 
    	Utils.isStreamingDynamicAllocationEnabled(conf)
    2. 不允许同时开启streaming和core的动态分配
    if (Utils.isDynamicAllocationEnabled(conf) && streamingDynamicAllocationEnabled) {
      throw new IllegalArgumentException(
        """
          |Dynamic Allocation cannot be enabled for both streaming and core 
          at the same time.
          |Please disable core Dynamic Allocation by setting
          spark.dynamicAllocation.enabled to
          |false to use Dynamic Allocation in streaming.
        """.stripMargin)
    }
    val= streamingDynamicAllocationEnabled
    
    def createIfEnabled(
      client: ExecutorAllocationClient,
      receiverTracker: ReceiverTracker,
      conf: SparkConf,
      batchDurationMs: Long,
      clock: Clock): Option[ExecutorAllocationManager]
    功能: 创建执行器分配管理器
    val= if (isDynamicAllocationEnabled(conf) && client != null) {
      Some(new ExecutorAllocationManager(
          client, receiverTracker, conf, batchDurationMs, clock))
    } else None
}
```

#### InputInfoTracker

```scala
@DeveloperApi
case class StreamInputInfo(
    inputStreamId: Int, numRecords: Long, metadata: Map[String, Any] = Map.empty) {
    介绍: 定位指定批次的输入流
    构造器参数:
    inputStreamId	输入流编号
    numRecords	批次记录数量
    metadata	当前批次的元数据表(需要至少包含一个@Description属性,这个的内容会映射到sparkUI上)
    操作集:
    def metadataDescription: Option[String]
    功能: 获取元数据描述
    val= metadata.get(StreamInputInfo.METADATA_KEY_DESCRIPTION).map(_.toString)
}
```

```scala
@DeveloperApi
object StreamInputInfo {
    #name @METADATA_KEY_DESCRIPTION: String = "Description"	元数据描述(key)
}
```

```scala
private[streaming] class InputInfoTracker(ssc: StreamingContext) extends Logging {
    介绍: 输入信息定位器
    这个类管理了所有的输入流,同时管理数据的统计信息,信息会通过@StreamingListener 暴露出来,用于监视.
    属性:
    #name @batchTimeToInputInfos	批次时间->输入信息映射关系
    val= new mutable.HashMap[Time, mutable.HashMap[Int, StreamInputInfo]]
    操作集:
    def reportInfo(batchTime: Time, inputInfo: StreamInputInfo): Unit
    功能: 汇报输入信息和批次时间给定位器
    synchronized {
        val inputInfos = batchTimeToInputInfos.getOrElseUpdate(
            batchTime,new mutable.HashMap[Int, StreamInputInfo]())
        if (inputInfos.contains(inputInfo.inputStreamId)) {
            throw new IllegalStateException(s"Input stream ${inputInfo.inputStreamId} 
            for batch " +s"$batchTime is already added into InputInfoTracker, 
            this is an illegal state")
        }
        inputInfos += ((inputInfo.inputStreamId, inputInfo))
    }
    
    def getInfo(batchTime: Time): Map[Int, StreamInputInfo]
    功能: 获取指定批次时间的输入流信息
    synchronized {
        val inputInfos = batchTimeToInputInfos.get(batchTime)
        inputInfos.map(_.toMap).getOrElse(Map[Int, StreamInputInfo]())
    }
    
    def cleanup(batchThreshTime: Time): Unit 
    功能: 清理早于指定时间@batchThreshTime的输出定位信息
    val timesToCleanup = batchTimeToInputInfos.keys.filter(_ < batchThreshTime)
    logInfo(s"remove old batch metadata: ${timesToCleanup.mkString(" ")}")
    batchTimeToInputInfos --= timesToCleanup
}
```

#### Job

```scala
private[streaming]
class Job(val time: Time, func: () => _) {
    构造器参数:
    time 	计时器
    func	job处理函数
    属性:
    #name @_id: String = _	jobID
    #name @_outputOpId: Int = _	输出流操作编号
    #name @isSet = false	job是否设置
    #name @_result: Try[_] = null	job执行结果
    #name @_callSite: CallSite = null	调用位置
    #name @_startTime: Option[Long] = None	job起始时间
    #name @_endTime: Option[Long] = None	job结束时间
    操作集:
    def run(): Unit
    功能: 运行job,获取执行结果
    _result = Try(func())
    
    def result: Try[_]
    功能: 获取执行结果
    if (_result == null) {
      throw new IllegalStateException("Cannot access result before job finishes")
    }
    val= _result
    
    def id: String 
    功能: 获取job全局id
    if (!isSet) {
      throw new IllegalStateException("Cannot access id before calling setId")
    }
    val= _id
    
    def outputOpId: Int
    功能: 获取输出流操作编号
    if (!isSet) {
      throw new IllegalStateException("Cannot access number before calling setId")
    }
    val= _outputOpId
    
    def setOutputOpId(outputOpId: Int): Unit
    功能: 设置输出流操作编号
    if (isSet) {
      throw new IllegalStateException("Cannot call setOutputOpId more than once")
    }
    isSet = true
    _id = s"streaming job $time.$outputOpId"
    _outputOpId = outputOpId
    
    def setCallSite(callSite: CallSite): Unit 
    功能: 设置调用位置
    _callSite = callSite
    
    def callSite: CallSite = _callSite
    功能: 获取调用位置
    
    def setStartTime(startTime: Long): Unit
    功能: 设置job起始时间
    _startTime = Some(startTime)
    
    def setEndTime(endTime: Long): Unit 
    功能: 设置job结束时间
    _endTime = Some(endTime)
    
    def toOutputOperationInfo: OutputOperationInfo
    功能: 转换为输出操作信息
    val failureReason = if (_result != null && _result.isFailure) {
      Some(Utils.exceptionString(_result.asInstanceOf[Failure[_]].exception))
    } else {
      None
    }
    val= OutputOperationInfo(
      time, outputOpId, callSite.shortForm, 
        callSite.longForm, _startTime, _endTime, failureReason)
    
    def toString: String = id
    功能: 信息显示
}
```

#### JobGenerator

```scala
=======================
	job生成器的事件类
=======================
private[scheduler] sealed trait JobGeneratorEvent
介绍: job生成器事件

private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
介绍: 生成指定时间的job事件

private[scheduler] case class ClearMetadata(time: Time) extends JobGeneratorEvent
介绍: 指定时间清除元数据事件

private[scheduler] case class DoCheckpoint(
    time: Time, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
介绍: 指定时间进行检查点设置

private[scheduler] case class ClearCheckpointData(time: Time) extends JobGeneratorEvent
介绍: 指定时间清除检查点数据事件
```

```scala
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {
    介绍: 由@DStreams生成job,同时驱动检查点和清除@DStream元数据
    #name @ssc = jobScheduler.ssc	streaming上下文
    #name @conf = ssc.conf	spark配置
    #name @graph = ssc.graph	DStreamGraph
    #name @clock	时钟
    val= {
        val clockClass = ssc.sc.conf.get(
          "spark.streaming.clock", "org.apache.spark.util.SystemClock")
        try {
          Utils.classForName[Clock](clockClass).getConstructor().newInstance()
        } catch {
          case e: ClassNotFoundException if
            clockClass.startsWith("org.apache.spark.streaming") =>
            val newClockClass = clockClass.replace("org.apache.spark.streaming",
                                                   "org.apache.spark")
            Utils.classForName[Clock](newClockClass).getConstructor().newInstance()
        }
      }
    #name @timer	重现计时器(回调中使用@EventLoop发送产生job的消息)
    val= new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
    #name @shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null
    确定是否需要设置检查点(lazy)
    #name @checkpointWriter	lazy	检查点写出器
    val= if (shouldCheckpoint) {
        new CheckpointWriter(
            this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
      } else {
        null
      }
    #name @eventLoop: EventLoop[JobGeneratorEvent] = null	事件环
    #name @lastProcessedBatch: Time = null	上个批次进行时间(完成且检查点和元数据都清理完毕)
    操作集:
    def start(): Unit
    功能: 启动job的生成
    synchronized{
        0. 事件环校验
        if (eventLoop != null) return 
        1. 调用检查点写出器用于初始化,如果先设置事件环则会导致死锁.参考SPARK-10125
        checkpointWriter
        2. 设置事件环
        eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
            override protected def onReceive(event: JobGeneratorEvent): Unit =
            processEvent(event)
            override protected def onError(e: Throwable): Unit = {
                jobScheduler.reportError("Error in job generator", e)
            }
        }
        3. 启动事件环
        eventLoop.start()
        4. 启动生成器
        if (ssc.isCheckpointPresent) {
            restart()
        } else {
            startFirstTime()
        }
    }
    
    def stop(processReceivedData: Boolean): Unit
    功能: 停止生成器
    输入参数:
    	processReceivedData	是否处理接受的数据
    synchronized{
        1. 事件环校验
        if (eventLoop == null) return
        2. 处理接收到的数据
        if (processReceivedData) {
            logInfo("Stopping JobGenerator gracefully")
            val timeWhenStopStarted = System.nanoTime()
            val stopTimeoutMs = conf.getTimeAsMs(
                "spark.streaming.gracefulStopTimeout", 
                s"${10 * ssc.graph.batchDuration.milliseconds}ms")
            val pollTime = 100
            def hasTimedOut: Boolean = {
                val diff = TimeUnit.NANOSECONDS.toMillis((
                    System.nanoTime() - timeWhenStopStarted))
                val timedOut = diff > stopTimeoutMs
                if (timedOut) {
                    logWarning("Timed out while stopping the 
                    job generator (timeout = " + stopTimeoutMs + ")")
                }
                timedOut
            }
            // 等待到所有网络中接受的数据块的输入定位被网络输入的@DStream消费完毕才能操作.
            logInfo("Waiting for all received blocks to be consumed for job generation")
            while(!hasTimedOut && jobScheduler.receiverTracker.hasUnallocatedBlocks) {
                Thread.sleep(pollTime)
            }
            logInfo("Waited for all received blocks to be consumed for job generation")
            // 停止生成job
            val stopTime = timer.stop(interruptTimer = false)
            logInfo("Stopped generation timer")
            // 等待job完成和检查点写入完成
            def haveAllBatchesBeenProcessed: Boolean = {
                lastProcessedBatch != null && lastProcessedBatch.milliseconds == stopTime
            }
            logInfo("Waiting for jobs to be processed and checkpoints to be written")
            while (!hasTimedOut && !haveAllBatchesBeenProcessed) {
                Thread.sleep(pollTime)
            }
            logInfo("Waited for jobs to be processed and checkpoints to be written")
            graph.stop()
        } else {
            logInfo("Stopping JobGenerator immediately")
            timer.stop(true)
            graph.stop()
        }
        3. 先停止事件环,再停止检查点写出器,参考SPARK-14701
        eventLoop.stop()
        if (shouldCheckpoint) checkpointWriter.stop()
        logInfo("Stopped JobGenerator")
    }
    
    def onBatchCompletion(time: Time): Unit 
    功能: 批次完成处理(发送清理元数据信息)
    eventLoop.post(ClearMetadata(time))
    
    def onCheckpointCompletion(time: Time, clearCheckpointDataLater: Boolean): Unit 
    功能: 检查点写出成功处理
    if (clearCheckpointDataLater) {
      eventLoop.post(ClearCheckpointData(time))
    }
    
    def processEvent(event: JobGeneratorEvent): Unit
    功能: 处理指定时间@event
    logDebug("Got event " + event)
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
    
    def startFirstTime(): Unit
    功能: 启动首次job的生成
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
    logInfo("Started JobGenerator at " + startTime)
    
    def restart(): Unit
    功能: 基于检查点信息重启生成器
    1. 如果操作的时钟是用于测试的,然后设置时钟到最后一个检查点时间,且如果时间定义了则设置这个时间
    if (clock.isInstanceOf[ManualClock]) {
      val lastTime = ssc.initialCheckpoint.checkpointTime.milliseconds
      val jumpTime = ssc.sc.conf.getLong("spark.streaming.manualClock.jump", 0)
      clock.asInstanceOf[ManualClock].setTime(lastTime + jumpTime)
    }
    2. 设置重启时间和检查点时间
    val checkpointTime = ssc.initialCheckpoint.checkpointTime
    val restartTime = new Time(timer.getRestartTime(graph.zeroTime.milliseconds))
    val downTimes = checkpointTime.until(restartTime, batchDuration)
    logInfo("Batches during down time (" + downTimes.size + " batches): "
      + downTimes.mkString(", "))
    3. 处理失败前未处理的批次
    val pendingTimes = ssc.initialCheckpoint.pendingTimes.sorted(Time.ordering)
    logInfo("Batches pending processing (" + pendingTimes.length + " batches): " +
      pendingTimes.mkString(", "))
    4. 重新调度job
    // 确定重新调度的次数
    val timesToReschedule = (pendingTimes ++ downTimes).filter { _ < restartTime }
      .distinct.sorted(Time.ordering)
    logInfo("Batches to reschedule (" + timesToReschedule.length + " batches): " +
      timesToReschedule.mkString(", "))
    timesToReschedule.foreach { time =>
        // 重新调度,从失败中恢复的时候分配相应的数据块,由于数据块被添加但是没有分配,所以恢复之后悬挂在队列中,不需要在下一个批次中分配这些数据块.
      jobScheduler.receiverTracker.allocateBlocksToBatch(time)
      jobScheduler.submitJobSet(JobSet(time, graph.generateJobs(time)))
    }
    5. 重启计时器
    timer.start(restartTime.milliseconds)
    logInfo("Restarted JobGenerator at " + restartTime)
    
    def generateJobs(time: Time): Unit
    功能: 产生job,并在给定时间@time 设置检查点
    1. 检查点设置所有RDD,从而确保它们的血统可以周期性的删除.否则可能导致栈内存移除(SPARK-6847)
    ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
    Try {
        // 分配接受的数据块到批次中
      jobScheduler.receiverTracker.allocateBlocksToBatch(time)
        // 使用分配的数据块产生job
      graph.generateJobs(time) 
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
        PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
    }
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
    
    def clearMetadata(time: Time): Unit
    功能: 清除指定时间@time的元数据信息
    1. 清理@DStreamGraph 的元数据
    ssc.graph.clearMetadata(time)
    2. 如果运行检查点则设置检查点,否则标记批次完成
    if (shouldCheckpoint) {
      eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = true))
    } else {
        // 如果不允许进行检查点设置,那么删除接受数据块相关的元数据信息.否则,等待批次检查点的完成
      val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
      jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
      jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
      markBatchFullyProcessed(time) // 标记批次完成
    }
    
    def clearCheckpointData(time: Time): Unit
    功能: 清除指定时间@time的检查点数据
    ssc.graph.clearCheckpointData(time)
    // 批次中所有数据必须要保存到检查点中,这样才可以安全的删除数据块信息和数据wal文件.
    val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
    jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
    jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
    markBatchFullyProcessed(time)
    
    def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean): Unit 
    功能: 指定时间@time 进行检查点设置
    if (shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration))
    {
      logInfo("Checkpointing graph for time " + time)
      ssc.graph.updateCheckpointData(time)
      checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
    } else if (clearCheckpointDataLater) {
      markBatchFullyProcessed(time)
    }
    
    def markBatchFullyProcessed(time: Time): Unit 
    功能: 标记批次处理完成
    lastProcessedBatch = time
}
```

#### JobScheduler

```scala
=======================
	job调度事件类
=======================

private[scheduler] sealed trait JobSchedulerEvent
介绍: job调度事件

private[scheduler] case class JobStarted(job: Job, startTime: Long) extends JobSchedulerEvent
介绍: job启动事件

private[scheduler] case class JobCompleted(job: Job, completedTime: Long) extends JobSchedulerEvent
介绍: job完成事件

private[scheduler] case class ErrorReported(msg: String, e: Throwable) extends JobSchedulerEvent
介绍: 错误汇报事件
```

```scala
private[streaming]
class JobScheduler(val ssc: StreamingContext) extends Logging {
    介绍: 这个类调度运行在spark上的job,使用job生成器@JobGenerator去产生job,并使用线程池去运行.
    #name @jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]
    	job集合
    #name @numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
    	并发的job数量
    #name @jobExecutor	job执行线程(线程池)
    val= ThreadUtils.newDaemonFixedThreadPool(
        numConcurrentJobs, "streaming-job-executor")
    #name @jobGenerator = new JobGenerator(this)	job生成器
    #name @clock = jobGenerator.clock	时钟
    #name @listenerBus = new StreamingListenerBus(ssc.sparkContext.listenerBus)	监听总线
    #name @receiverTracker: ReceiverTracker = null	接收器定位器
    #name @inputInfoTracker: InputInfoTracker = null	输入定位器(输入流信息和进行的记录数量)
    #name @executorAllocationManager: Option[ExecutorAllocationManager] = None	
    执行器分配管理器
    #name @eventLoop: EventLoop[JobSchedulerEvent] = null	事件环
    操作集:
    def start(): Unit
    功能: 启动调度器
    1. 事件环校验
    if (eventLoop != null) return 
    2. 设置事件环并启动
    logDebug("Starting JobScheduler")
    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      override protected def onReceive(event: JobSchedulerEvent): Unit 
        = processEvent(event)
      override protected def onError(e: Throwable): Unit 
        = reportError("Error in job scheduler", e)
    }
    eventLoop.start()
    3. 连接输入流比例控制器,用于接收批量完成的更新
    for {
      inputDStream <- ssc.graph.getInputStreams
      rateController <- inputDStream.rateController
    } ssc.addStreamingListener(rateController)
    4. 启动监听总线,定位器
    listenerBus.start()
    receiverTracker = new ReceiverTracker(ssc)
    inputInfoTracker = new InputInfoTracker(ssc)
    5. 设置执行器分配管理器
    val executorAllocClient: ExecutorAllocationClient = ssc.sparkContext.
    schedulerBackend match {
      case b: ExecutorAllocationClient => b.asInstanceOf[ExecutorAllocationClient]
      case _ => null
    }
    executorAllocationManager = ExecutorAllocationManager.createIfEnabled(
      executorAllocClient,
      receiverTracker,
      ssc.conf,
      ssc.graph.batchDuration.milliseconds,
      clock)
    executorAllocationManager.foreach(ssc.addStreamingListener)
    6. 启动接收器定位器,job生成器和执行器分配管理器
    receiverTracker.start()
    jobGenerator.start()
    executorAllocationManager.foreach(_.start())
    logInfo("Started JobScheduler")
    
    def stop(processAllReceivedData: Boolean): Unit
    功能: 停止调度器,可以选择是否将所有接收的数据处理完毕
    synchronized {
        if (eventLoop == null) return 
        logDebug("Stopping JobScheduler")
        1. 停止数据的接受
        if (receiverTracker != null) {
          receiverTracker.stop(processAllReceivedData)
        }
        if (executorAllocationManager != null) {
          executorAllocationManager.foreach(_.stop())
        }
        2. 停止job生成器，如果需要处理完所有的接受数据，则等待其完成
        jobGenerator.stop(processAllReceivedData)
        logDebug("Stopping job executor")
        3. 停止驱动器接受job
        jobExecutor.shutdown()
        4. 等待队列中job的完成
        val terminated = if (processAllReceivedData) {
          jobExecutor.awaitTermination(1, TimeUnit.HOURS) 
        } else {
          jobExecutor.awaitTermination(2, TimeUnit.SECONDS)
        }
        if (!terminated) {
          jobExecutor.shutdownNow()
        }
        5、 停止所有组件
        logDebug("Stopped job executor")
        listenerBus.stop()
        eventLoop.stop()
        eventLoop = null
        logInfo("Stopped JobScheduler")
    }
    
    def submitJobSet(jobSet: JobSet): Unit
    功能: 提交任务集合
    if (jobSet.jobs.isEmpty) {
      logInfo("No jobs added for time " + jobSet.time)
    } else {
      listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))
      jobSets.put(jobSet.time, jobSet)
      jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
      logInfo("Added jobs for time " + jobSet.time)
    }
    
    def getPendingTimes(): Seq[Time]
    功能: 获取待定计时器列表
    val= jobSets.asScala.keys.toSeq
    
    def reportError(msg: String, e: Throwable): Unit
    功能: 报告错误信息
    eventLoop.post(ErrorReported(msg, e))
    
    def isStarted(): Boolean
    功能: 确定调度器是否开启
    val= synchronized { eventLoop != null}
    
    def processEvent(event: JobSchedulerEvent): Unit 
    功能: 处理指定事件@event
    try {
      event match {
        case JobStarted(job, startTime) => handleJobStart(job, startTime)
        case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
        case ErrorReported(m, e) => handleError(m, e)
      }
    } catch {
      case e: Throwable =>
        reportError("Error in job scheduler", e)
    }
    
    def handleJobStart(job: Job, startTime: Long): Unit
    功能: 处理job在指定时间开始
    1. 获取当前时间的job
    val jobSet = jobSets.get(job.time)
    2. 确定是否为第一个job
    val isFirstJobOfJobSet = !jobSet.hasStarted
    jobSet.handleJobStart(job)
    3. 如果是第一个job,则监听总线需要发送批次开始消息
    if (isFirstJobOfJobSet) {
      listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
    }
    job.setStartTime(startTime)
    4. 监听总线发送输出操作开始的消息
    listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
    logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
    
    def handleJobCompletion(job: Job, completedTime: Long): Unit
    功能: 处理job的完成
    1. 发送输出操作完成的消息
    val jobSet = jobSets.get(job.time)
    jobSet.handleJobCompletion(job)
    job.setEndTime(completedTime)
    listenerBus.post(
       StreamingListenerOutputOperationCompleted(job.toOutputOperationInfo))
    logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
    2. 如果批次结束需要发送批次结束的消息
    if (jobSet.hasCompleted) {
      listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo))
    }
    3. 结果处理
    job.result match {
      case Failure(e) =>
        reportError("Error running job " + job, e)
      case _ =>
        if (jobSet.hasCompleted) {
          jobSets.remove(jobSet.time)
          jobGenerator.onBatchCompletion(jobSet.time)
          logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
            jobSet.totalDelay / 1000.0, jobSet.time.toString,
            jobSet.processingDelay / 1000.0
          ))
        }
    }
    
    def handleError(msg: String, e: Throwable): Unit
    功能: 处理错误信息
    logError(msg, e)
    ssc.waiter.notifyError(e)
    PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
    
    内部类:
    private class JobHandler(job: Job) extends Runnable with Logging {
        操作集:
        def run(): Unit
        功能: 运行job处理
        1. 获取本地参数
        val oldProps = ssc.sparkContext.getLocalProperties
        2. 处理job
        try {       
       	ssc.sparkContext.setLocalProperties(
            Utils.cloneProperties(ssc.savedProperties.get()))
        val formattedTime = UIUtils.formatBatchTime(
          job.time.milliseconds, ssc.graph.batchDuration.milliseconds,
          showYYYYMMSS = false)
        val batchUrl = s"/streaming/batch/?id=${job.time.milliseconds}"
        val batchLinkText = s"[output operation ${job.outputOpId}, 
        batch time ${formattedTime}]"
        ssc.sc.setJobDescription(
          s"""Streaming job from <a href="$batchUrl">$batchLinkText</a>""")
        ssc.sc.setLocalProperty(BATCH_TIME_PROPERTY_KEY, job.time.milliseconds.toString)
        ssc.sc.setLocalProperty(OUTPUT_OP_ID_PROPERTY_KEY, job.outputOpId.toString)
        ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
        var _eventLoop = eventLoop
        if (_eventLoop != null) {
          _eventLoop.post(JobStarted(job, clock.getTimeMillis()))
          SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
            job.run()
          }
          _eventLoop = eventLoop
          if (_eventLoop != null) {
            _eventLoop.post(JobCompleted(job, clock.getTimeMillis()))
          }
        } else {
        }
      } finally {
        ssc.sparkContext.setLocalProperties(oldProps)
      }
    }
}
```

```scala
private[streaming] object JobScheduler {
  val BATCH_TIME_PROPERTY_KEY = "spark.streaming.internal.batchTime" 批次时间属性(key)
  val OUTPUT_OP_ID_PROPERTY_KEY = "spark.streaming.internal.outputOpId" 输出操作ID属性(key)
}
```

#### JobSet

```scala
private[streaming]
case class JobSet(
    time: Time,
    jobs: Seq[Job],
    streamIdToInputInfo: Map[Int, StreamInputInfo] = Map.empty) {
    介绍: 代表属于一个批次的job集合
    构造器参数:
    time	计时器
    jobs	job集合
    streamIdToInputInfo	steam编号--> stream输入信息映射
    #name @incompleteJobs = new HashSet[Job]()	不完备的job集合(进行中job集合)
    #name @submissionTime = System.currentTimeMillis() 	任务集提交时间
    #name @processingStartTime = -1L	处理开始时间
    #name @processingEndTime = -1L	处理结束时间
    初始化操作:
    jobs.zipWithIndex.foreach { case (job, i) => job.setOutputOpId(i) }
    功能: 设置输出操作编号
    
    incompleteJobs ++= jobs
    功能: 将完成的job添加到@incompleteJobs
    
    操作集:
    def handleJobStart(job: Job): Unit
    功能: 处理job的开始
    if (processingStartTime < 0) processingStartTime = System.currentTimeMillis()
    
    def handleJobCompletion(job: Job): Unit 
    功能: 处理job的完成
    incompleteJobs -= job
    if (hasCompleted) processingEndTime = System.currentTimeMillis()
    
    def hasStarted: Boolean = processingStartTime > 0
    功能: 确定任务是否开始
    
    def hasCompleted: Boolean = incompleteJobs.isEmpty
    功能: 确定任务是否结束
    
    def processingDelay: Long = processingEndTime - processingStartTime
    功能: 获取处理延时
    
    def totalDelay: Long = processingEndTime - time.milliseconds
    功能: 获取总计延时
    
    def toBatchInfo: BatchInfo
    功能: 转换为批次信息
    val= BatchInfo(
      time,
      streamIdToInputInfo,
      submissionTime,
      if (hasStarted) Some(processingStartTime) else None,
      if (hasCompleted) Some(processingEndTime) else None,
      jobs.map { job => (job.outputOpId, job.toOutputOperationInfo) }.toMap
    )
}
```

#### OutputOperationInfo

```scala
@DeveloperApi
case class OutputOperationInfo(
    batchTime: Time,
    id: Int,
    name: String,
    description: String,
    startTime: Option[Long],
    endTime: Option[Long],
    failureReason: Option[String]) {
    介绍: 输出操作信息
    构造器参数:
    batchTime	批次时间
    id	输出操作的编号
    name	输出操作的名称
    description	输出的描述
    startTime	输出起始时间
    endTime	输出结束时间
    failureReason	失败原因
    
    def duration: Option[Long] = for (s <- startTime; e <- endTime) yield e - s
    功能: 获取输出操作的持续时间
}
```

#### RateController

```scala
private[streaming] abstract class RateController(
    val streamUID: Int, rateEstimator: RateEstimator)
extends StreamingListener with Serializable {
    介绍: 接受批次完成更新的监听器@StreamingListener,按照流式接受消息的速度维持估算.
    输入参数:
    streamUID	streamUID
    rateEstimator	比例估算器
    初始化操作:
    init()
    功能: 初始化控制器
    属性:
    #name @executionContext: ExecutionContext = _	执行器上下文
    #name @rateLimit: AtomicLong = _	比例界值
    操作集:
    def publish(rate: Long): Unit
    功能: 发布比例信息
    
    def init(): Unit
    功能: 初始化
    executionContext = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonSingleThreadExecutor("stream-rate-update"))
    rateLimit = new AtomicLong(-1L)
    
    def readObject(ois: ObjectInputStream): Unit
    功能: 读取数据(反序列化)
    Utils.tryOrIOException {
        ois.defaultReadObject()
        init()
    }
    
    def computeAndPublish(
        time: Long, elems: Long, workDelay: Long, waitDelay: Long): Unit
    功能: 计算新的比例界限值,并异步发布这个值
    Future[Unit] {
        // 这个异步处理任务,主要设置界限值,并发布
      val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay)
      newRate.foreach { s =>
        rateLimit.set(s.toLong)
        publish(getLatestRate())
      }
    }
    
    def getLatestRate(): Long = rateLimit.get()
    功能: 获取最新的界限值
    
    def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit
    功能: 批次完成处理
    val elements = batchCompleted.batchInfo.streamIdToInputInfo
    for {
      processingEnd <- batchCompleted.batchInfo.processingEndTime
      workDelay <- batchCompleted.batchInfo.processingDelay
      waitDelay <- batchCompleted.batchInfo.schedulingDelay
      elems <- elements.get(streamUID).map(_.numRecords)
    } computeAndPublish(processingEnd, elems, workDelay, waitDelay)
}
```

```scala
object RateController {
    def isBackPressureEnabled(conf: SparkConf): Boolean
    功能: 确定是否可以允许反压设置
    val= conf.getBoolean("spark.streaming.backpressure.enabled", false)
}
```

#### ReceivedBlockInfo

```scala
private[streaming] case class ReceivedBlockInfo(
    streamId: Int,
    numRecords: Option[Long],
    metadataOption: Option[Any],
    blockStoreResult: ReceivedBlockStoreResult
  ) {
    介绍: 接收器接受的数据块信息
    构造器信息:
    streamId	stream编号
    numRecords	记录数量
    metadataOption	元数据配置
    blockStoreResult	数据块存储结果
    初始化操作:
    require(numRecords.isEmpty || numRecords.get >= 0, "numRecords must not be negative")
    功能: 记录数量校验
    属性:
    #name @_isBlockIdValid = true	volatile	数据块是否可用
    操作集:
    def blockId: StreamBlockId = blockStoreResult.blockId
    功能: 获取流式数据块编号
    
    def walRecordHandleOption: Option[WriteAheadLogRecordHandle] 
    功能: 获取WAL记录处理器
    val= blockStoreResult match {
      case walStoreResult: WriteAheadLogBasedStoreResult => 
      	Some(walStoreResult.walRecordHandle)
      case _ => None
    }
    
    def isBlockIdValid(): Boolean = _isBlockIdValid
    功能: 确定指定数据块是否可用
    
    def setBlockIdInvalid(): Unit
    功能: 设置数据块不可用
    _isBlockIdValid = false
}
```

#### ReceivedBlockTracker

```scala
==========================
	接受数据块定位日志事件
==========================
private[streaming] sealed trait ReceivedBlockTrackerLogEvent
介绍: 接收数据块定位器日志事件

private[streaming] case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceivedBlockTrackerLogEvent
介绍: 数据块添加事件

private[streaming] case class BatchAllocationEvent(time: Time, allocatedBlocks: AllocatedBlocks) extends ReceivedBlockTrackerLogEvent
介绍: 批次分配事件

private[streaming] case class BatchCleanupEvent(times: Seq[Time])
  extends ReceivedBlockTrackerLogEvent
介绍: 批次清理事件
```

```scala
private[streaming]
case class AllocatedBlocks(streamIdToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {
    介绍: 这个类代表所有流的数据块(连接到一个批次上的)
    构造器参数:
    streamIdToAllocatedBlocks	stream编号--> 分配数据块映射
    操作集:
    def getBlocksOfStream(streamId: Int): Seq[ReceivedBlockInfo]
    功能: 获取指定流@streamId 的数据块信息
    val= streamIdToAllocatedBlocks.getOrElse(streamId, Seq.empty)
}
```

```scala
private[streaming] class ReceivedBlockTracker(
    conf: SparkConf,
    hadoopConf: Configuration,
    streamIds: Seq[Int],
    clock: Clock,
    recoverFromWriteAheadLog: Boolean,
    checkpointDirOption: Option[String])
extends Logging {
    介绍: 接受数据块定位器
    这个类保证追踪了所有的接受数据块,且将其分配到需要的批次中去.这个类的所有动作会被保存在WAL中(如果提供了检查点目录).以便于状态的追踪(接受数据块并处理数据块到批次的分配过程)可能从驱动器失败中恢复.
    注意到这个类的任何实例可以使用检查点目录创建,会尝试从日志中读取事件.
    构造器参数:
    streamIds	stream编号列表
    recoverFromWriteAheadLog	是否可从WAL中恢复数据
    checkpointDirOption: Option[String]	检查点目录
    #name @streamIdToUnallocatedBlockQueues	streamID-->未分配的数据块
    val= new mutable.HashMap[Int, ReceivedBlockQueue]
    #name @timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]
    	计时器--> 分配数据块映射表
    #name @writeAheadLogOption = createWriteAheadLog()	WAL
    #name @lastAllocatedBatchTime: Time = null	最新分配批次的计时器
    初始化操作:
    if (recoverFromWriteAheadLog) {
        recoverPastEvents()
    }
    功能: 从过去的事件中恢复WAL
    
    操作集:
    def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean
    功能: 添加接收到的数据块,这个事件会写到WAL中(如果可能)
    try {
      1. 写入WAL日志,并获取WAL结果
      val writeResult = writeToLog(BlockAdditionEvent(receivedBlockInfo))
      2. 将接收到的数据块信息添加到接受数据块队列中
      if (writeResult) {
        synchronized {
          getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
        }
        logDebug(s"Stream ${receivedBlockInfo.streamId} received " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId}")
      } else {
        logDebug(s"Failed to acknowledge stream ${receivedBlockInfo.streamId} receiving " +
          s"block ${receivedBlockInfo.blockStoreResult.blockId} in the Write Ahead Log.")
      }
      writeResult
    } catch {
      case NonFatal(e) =>
        logError(s"Error adding block $receivedBlockInfo", e)
        false
    }
    
    def allocateBlocksToBatch(batchTime: Time): Unit 
    功能: 分配数据块到批次中去,这个事件可能会被写入到WAL中
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {
      val streamIdToBlocks = streamIds.map { streamId =>
        (streamId, mutable.ArrayBuffer(getReceivedBlockQueue(streamId).clone(): _*))
      }.toMap
      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
      if (writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks))) {
        streamIds.foreach(getReceivedBlockQueue(_).clear())
        timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
        lastAllocatedBatchTime = batchTime
      } else {
        logInfo(s"Possibly processed batch $batchTime needs 
        to be processed again in WAL recovery")
      }
    } else {
      logInfo(s"Possibly processed batch $batchTime needs to 
      be processed again in WAL recovery")
    }
    
    def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] 
    功能: 获取指定批次的数据块数量
    val= timeToAllocatedBlocks.get(batchTime).map { 
        _.streamIdToAllocatedBlocks }.getOrElse(Map.empty)
    
    def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo]
    功能: 获取分配到指定批次下指定流的数据块列表
    val= synchronized {
      timeToAllocatedBlocks.get(batchTime).map {
        _.getBlocksOfStream(streamId)
      }.getOrElse(Seq.empty)
    }
    
    def hasUnallocatedReceivedBlocks: Boolean
    功能: 确定数据块是否不需要分配到批次中
    val=!streamIdToUnallocatedBlockQueues.values.forall(_.isEmpty)
    
    def getUnallocatedBlocks(streamId: Int): Seq[ReceivedBlockInfo]
    功能: 获取所有添加但是没有分配的数据块信息,这个方法用于测试
    val= synchronized { getReceivedBlockQueue(streamId).toSeq }
    
    def cleanupOldBatches(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit
    功能: 清理旧的批次信息中的数据块信息,如果@waitForCompletion为true,方法只有在文件清除完毕之后才会返回(同步)
    require(cleanupThreshTime.milliseconds < clock.getTimeMillis())
    val timesToCleanup = timeToAllocatedBlocks.keys.filter {
        _ < cleanupThreshTime }.toSeq
    logInfo(s"Deleting batches: ${timesToCleanup.mkString(" ")}")
    if (writeToLog(BatchCleanupEvent(timesToCleanup))) {
      timeToAllocatedBlocks --= timesToCleanup
      writeAheadLogOption.foreach(_.clean(
          cleanupThreshTime.milliseconds, waitForCompletion))
    } else {
      logWarning("Failed to acknowledge batch clean up in the Write Ahead Log.")
    }
    
    def stop(): Unit
    功能: 停止块追踪器(关闭所有wal)
    writeAheadLogOption.foreach { _.close() }
    
    def recoverPastEvents(): Unit
    功能: 恢复从wal中的所有定位信息,用于恢复失败之前的状态(分配的和未分配的都会分配)
    1. 定义插入新数据块方法
    def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo): Unit = {
      logTrace(s"Recovery: Inserting added block $receivedBlockInfo")
      receivedBlockInfo.setBlockIdInvalid()
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    }
    2. 定义方法,插入恢复的数据块->批次分配关系,并从接受的数据块中移除这些数据
    def insertAllocatedBatch(batchTime: Time, allocatedBlocks: AllocatedBlocks): Unit = {
      logTrace(s"Recovery: Inserting allocated batch for time $batchTime to " +
        s"${allocatedBlocks.streamIdToAllocatedBlocks}")
      allocatedBlocks.streamIdToAllocatedBlocks.foreach {
        case (streamId, allocatedBlocksInStream) =>
          getReceivedBlockQueue(streamId).dequeueAll(allocatedBlocksInStream.toSet)
      }
      timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
      lastAllocatedBatchTime = batchTime
    }
    3. 定义批次分配的方法
    def cleanupBatches(batchTimes: Seq[Time]): Unit = {
      logTrace(s"Recovery: Cleaning up batches $batchTimes")
      timeToAllocatedBlocks --= batchTimes
    }
    4. 处理wal日志
    writeAheadLogOption.foreach { writeAheadLog =>
      logInfo(s"Recovering from write ahead logs in ${checkpointDirOption.get}")
      writeAheadLog.readAll().asScala.foreach { byteBuffer =>
        logInfo("Recovering record " + byteBuffer)
        Utils.deserialize[ReceivedBlockTrackerLogEvent](
          JavaUtils.bufferToArray(byteBuffer),
            Thread.currentThread().getContextClassLoader) match {
          case BlockAdditionEvent(receivedBlockInfo) =>
            insertAddedBlock(receivedBlockInfo)
          case BatchAllocationEvent(time, allocatedBlocks) =>
            insertAllocatedBatch(time, allocatedBlocks)
          case BatchCleanupEvent(batchTimes) =>
            cleanupBatches(batchTimes)
        }
      }
    }
    
    def writeToLog(record: ReceivedBlockTrackerLogEvent): Boolean
    功能: 写出定位器的更新到WAL中
    if (isWriteAheadLogEnabled) {
      logTrace(s"Writing record: $record")
      try {
        writeAheadLogOption.get.write(ByteBuffer.wrap(Utils.serialize(record)),
          clock.getTimeMillis())
        true
      } catch {
        case NonFatal(e) =>
          logWarning(s"Exception thrown while writing record: $record 
          to the WriteAheadLog.", e)
          false
      }
    } else {
      true
    }
    
    def getReceivedBlockQueue(streamId: Int): ReceivedBlockQueue
    功能: 获取指定流@streamId 的接受数据块队列
    val= streamIdToUnallocatedBlockQueues.getOrElseUpdate(
        streamId, new ReceivedBlockQueue)
    
    def createWriteAheadLog(): Option[WriteAheadLog]
    功能: 由检查点创建WAL
    val= checkpointDirOption.map { checkpointDir =>
      val logDir = ReceivedBlockTracker.checkpointDirToLogDir(checkpointDirOption.get)
      WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
    }
    
    def isWriteAheadLogEnabled: Boolean = writeAheadLogOption.nonEmpty
    功能: 确认是否支持WAL
}
```

```scala
private[streaming] object ReceivedBlockTracker {
    def checkpointDirToLogDir(checkpointDir: String): String
    功能: 获取检查点目录对应的wal目录
    val= new Path(checkpointDir, "receivedBlockMetadata").toString
}
```

#### ReceivedInfo

```scala
@DeveloperApi
case class ReceiverInfo(
    streamId: Int,
    name: String,
    active: Boolean,
    location: String,
    executorId: String,
    lastErrorMessage: String = "",
    lastError: String = "",
    lastErrorTime: Long = -1L
   ) {
}
介绍: 接收器信息
    streamId	stream编号
    name	名称
    active 接收器是否处于激活状态
    location	接收器位置
    executorId	执行器编号
    lastErrorMessage	上一个错误信息
    lastError	上一个错误
    lastErrorTime	上一次发生错误时间
```

#### ReceiverSchedulingPolicy

```scala
介绍:
	接收器掉的策略
这个类尝试使用分布式的调度接收器.接收器有两种类型.

- 首个类型是全局调度.发生在@ReceiverTracker启动的时候,需要同时调度接收器.@ReceiverTracker需要调用@scheduleReceivers方法,尝试调度接收器,以便于接收器均衡分布.@ReceiverTracker 更加是否需要调度接受@scheduleReceivers的结果,使用@ReceiverTracker的@receiverTrackingInfoMap更新.对于每个接收器,使用@ReceiverTrackingInfo.scheduledLocations 设置包含调度位置的位置列表.当接收器启动的时候,会发送注册请求,且会调用@ReceiverTracker.registerReceiver方法.如果设置了调度,将会检查首接收器的位置是调度的位置.如果不是则会被拒绝.

- 第二个类型是本地调度,发生在接收器重启的时候,有两种重启的情况
1. 由于实际位置和调度位置的不匹配,接收器重启.换句话说,@scheduleReceivers中导致了启动失败.
2. 没有调度位置列表,导致接收器的重置.或者列表中的执行器死亡也会导致重启.@ReceiverTracker会调用@rescheduleReceiver,如果如此,@ReceiverTracker需要给接收器设置@scheduledLocations.且应当清除.当接收器注册的时候,需要的得知这是一个本地调度,且@ReceiverTrackingInfo需要调用@rescheduleReceiver,去检查运行位置是否匹配.
总之，需要一个全局调度，尽可能获取全局调度，否则获取本地调度.
```

```scala
private[streaming] class ReceiverSchedulingPolicy {
    def scheduleReceivers(
      receivers: Seq[Receiver[_]],
      executors: Seq[ExecutorCacheTaskLocation]): Map[Int, Seq[TaskLocation]] 
    功能: 尽可能的调度接收器,使得其满足均衡分布.但是如果接收器的最佳位置不是均衡的,由于没有遵循这个原理,所以不能均衡调度.下面是一个调度执行器的方法.
    1. 首先使用最佳位置调度所有接收器,在运行的主机之间进行均衡调度
    2. 均衡调度所有其他的接收器以至于接收器的全局分布是均衡的
    这个方法在首次启动运行的时候调用.
    返回调度器和调度位置的映射表
    0. 接收器和执行线程参数校验
    if (receivers.isEmpty) {
      return Map.empty
    }
    if (executors.isEmpty) {
      return receivers.map(_.streamId -> Seq.empty).toMap
    }
    1. 初始化执行任务位置@executors 上的接收器数量
    val hostToExecutors = executors.groupBy(_.host)
    val scheduledLocations = Array.fill(receivers.length)(
        new mutable.ArrayBuffer[TaskLocation])
    val numReceiversOnExecutor = mutable.HashMap[ExecutorCacheTaskLocation, Int]()
    executors.foreach(e => numReceiversOnExecutor(e) = 0)
    2. 首先按照最佳位置调度,如果接收器有最佳位置,需要保证最佳位置在执行器列表中
    for (i <- 0 until receivers.length) {
      // Note: preferredLocation is host but executors are host_executorId
      receivers(i).preferredLocation.foreach { host =>
        hostToExecutors.get(host) match {
          case Some(executorsOnHost) =>
            // 有最佳位置的处理,选择最佳位置中接收器最少的,并分配给它
            val leastScheduledExecutor =
              executorsOnHost.minBy(executor => numReceiversOnExecutor(executor))
            scheduledLocations(i) += leastScheduledExecutor
            numReceiversOnExecutor(leastScheduledExecutor) =
              numReceiversOnExecutor(leastScheduledExecutor) + 1
          case None =>
            // 最佳位置位置,可能包含两种情况
            /*
            	1. 执行器没有开启,但是之后可以开启
            	2. 执行器死亡,或者集群中没有这个节点
            	当前,简单的将主机号添加到调度执行中.
            	注意: host可能是@HDFSCacheTaskLocation,这种情况使用@TaskLocation.apply处理
            */
            scheduledLocations(i) += TaskLocation(host)
        }
      }
    }
    3. 保证没有最佳位置的接收器，确保至少要分配一个执行器
    for (scheduledLocationsForOneReceiver <- scheduledLocations.filter(_.isEmpty)) {
      // 选择最少接收器的执行器，并分配
      val (leastScheduledExecutor, numReceivers) = numReceiversOnExecutor.minBy(_._2)
      scheduledLocationsForOneReceiver += leastScheduledExecutor
      numReceiversOnExecutor(leastScheduledExecutor) = numReceivers + 1
    }
    4. 分配空载执行器给含有较少执行器的接收器
    val idleExecutors = numReceiversOnExecutor.filter(_._2 == 0).map(_._1)
    for (executor <- idleExecutors) {
        // 分配空载执行器给最少执行器的接受器
      val leastScheduledExecutors = scheduledLocations.minBy(_.size)
      leastScheduledExecutors += executor
    }
    5.返回接收器信息表
    val=receivers.map(_.streamId).zip(scheduledLocations).toMap
    
    def rescheduleReceiver(
      receiverId: Int,
      preferredLocation: Option[String],
      receiverTrackingInfoMap: Map[Int, ReceiverTrackingInfo],
      executors: Seq[ExecutorCacheTaskLocation]): Seq[TaskLocation]
    功能: 重新调度接收器
    返回运行接收器的候补位置列表,如果列表为空,调用者可以运行任意执行器中的接收器.
    这个方法用于平衡执行器的负载,下面是调度接收器的执行器的方法.
    1. 如果设置了最佳位置,最佳位置应该是候补位置
    2. 根据接收器的运行和调度,执行器应当分配一个权重.
    	=> 如果接收器运行在执行器上,权重为1.0
    	=> 如果调度器中有接收器但是还没有运行,权值为1.0/@当前接收器候补执行器数量
    最后,如果有空载执行器,权值则为0,返回所有空载驱动器,否则返回最小权值的执行器.
    这个方法在接收器使用@ReceiverTracker注册的时候重启.
    1. 尝试先获取最佳位置
    val scheduledLocations = mutable.Set[TaskLocation]()
    2. 使用@TaskLocation.apply 处理HDFS的情况
    scheduledLocations ++= preferredLocation.map(TaskLocation(_))
    3. 获取执行器权重
    val executorWeights: Map[ExecutorCacheTaskLocation, Double] = {
     receiverTrackingInfoMap.values.flatMap(convertReceiverTrackingInfoToExecutorWeights)
        .groupBy(_._1).mapValues(_.map(_._2).sum) // Sum weights for each executor
    }
    4. 获取空载执行器
    val idleExecutors = executors.toSet -- executorWeights.keys
    if (idleExecutors.nonEmpty) {
      scheduledLocations ++= idleExecutors
    } else {
      val sortedExecutors = executorWeights.toSeq.sortBy(_._2)
      if (sortedExecutors.nonEmpty) {
        val minWeight = sortedExecutors(0)._2
        scheduledLocations ++= sortedExecutors.takeWhile(_._2 == minWeight).map(_._1)
      } else {
        // This should not happen since "executors" is not empty
      }
    }
    val= scheduledLocations.toSeq
    
    def convertReceiverTrackingInfoToExecutorWeights(
      receiverTrackingInfo: ReceiverTrackingInfo): 
    Seq[(ExecutorCacheTaskLocation, Double)] 
    功能: 将接收器定位信息@ReceiverTrackingInfo 转换为执行器信息
    每个执行器会根据运行的接收器分配权限:
    - 如果运行在执行器上,权重为1.0
    - 运行在执行器上但是并没有开始,权重为1.0/候补执行器数量
    val= receiverTrackingInfo.state match {
      case ReceiverState.INACTIVE => Nil
      case ReceiverState.SCHEDULED =>
        val scheduledLocations = receiverTrackingInfo.scheduledLocations.get
        scheduledLocations.filter(_.isInstanceOf[ExecutorCacheTaskLocation]).map {
            location =>
            location.asInstanceOf[ExecutorCacheTaskLocation] -> 
            (1.0 / scheduledLocations.size)
        }
      case ReceiverState.ACTIVE => Seq(receiverTrackingInfo.runningExecutor.get -> 1.0)
    }
}
```

#### ReceiverTracker

#### ReceiverTrackingInfo

```scala
private[streaming] case class ReceiverErrorInfo(
    lastErrorMessage: String = "", lastError: String = "", lastErrorTime: Long = -1L)
介绍: 接收器错误信息
构造器参数:
lastErrorMessage	上次错误信息
lastError	上一个错误
lastErrorTime	上一次错误时间
```

```scala
private[streaming] case class ReceiverTrackingInfo(
    receiverId: Int,
    state: ReceiverState,
    scheduledLocations: Option[Seq[TaskLocation]],
    runningExecutor: Option[ExecutorCacheTaskLocation],
    name: Option[String] = None,
    endpoint: Option[RpcEndpointRef] = None,
    errorInfo: Option[ReceiverErrorInfo] = None) {
    介绍: 接收器定位信息
    构造器参数:
        receiverId	接收器编号
        state 接收器状态
        scheduledLocations	调度位置
        runningExecutor	执行器位置
        name 接收器名称
        endpoint	RPC端点引用
        errorInfo	错误信息
    操作集:
    def toReceiverInfo: ReceiverInfo
    功能: 获取接收器信息
    val= ReceiverInfo(
        receiverId,
        name.getOrElse(""),
        state == ReceiverState.ACTIVE,
        location = runningExecutor.map(_.host).getOrElse(""),
        executorId = runningExecutor.map(_.executorId).getOrElse(""),
        lastErrorMessage = errorInfo.map(_.lastErrorMessage).getOrElse(""),
        lastError = errorInfo.map(_.lastError).getOrElse(""),
        lastErrorTime = errorInfo.map(_.lastErrorTime).getOrElse(-1L)
      )
}
```

#### StreamingListener

```scala
@DeveloperApi
sealed trait StreamingListenerEvent
介绍: 流式监听事件

@DeveloperApi
case class StreamingListenerStreamingStarted(time: Long) extends StreamingListenerEvent
介绍: 流式监听器流启动事件

@DeveloperApi
case class StreamingListenerBatchCompleted(batchInfo: BatchInfo) 
extends StreamingListenerEvent
介绍: 流式监听器流完成事件

@DeveloperApi
case class StreamingListenerBatchStarted(batchInfo: BatchInfo) 
extends StreamingListenerEvent
介绍: 流式监听批量开始事件

@DeveloperApi
case class StreamingListenerOutputOperationStarted(
    outputOperationInfo: OutputOperationInfo)
extends StreamingListenerEvent
介绍: 流式监听输出操作开始事件

@DeveloperApi
case class StreamingListenerOutputOperationCompleted(
    outputOperationInfo: OutputOperationInfo)
extends StreamingListenerEvent
介绍: 流式监听器输出操作完成

@DeveloperApi
case class StreamingListenerReceiverStarted(receiverInfo: ReceiverInfo)
extends StreamingListenerEvent
介绍: 流式监听器-接收器启动事件

@DeveloperApi
case class StreamingListenerReceiverError(receiverInfo: ReceiverInfo)
extends StreamingListenerEvent
介绍: 流式监听器-接收器错误事件

@DeveloperApi
case class StreamingListenerReceiverStopped(receiverInfo: ReceiverInfo)
extends StreamingListenerEvent
介绍: 流式监听器-接收器停止
```

```scala
@DeveloperApi
trait StreamingListener {
    介绍: 流式监听器接口
    def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted):Unit= { }
    功能: 流开始
    
    def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = { }
    功能: 接收器开始
    
    def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = { }
    功能: 接收器结束
    
    def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = { }
    功能: 接收器停止
    
    def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = { }
    功能: 批量提交job
    
    def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = { }
    功能: 批量job开始
    
    def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = { }
    功能: 批量job完成
    
    def onOutputOperationStarted(
      outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = { }
    功能: 输出操作启动
    
    def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = { }
    功能: 输出操作完成
}
```

```scala
@DeveloperApi
class StatsReportListener(numBatchInfos: Int = 10) extends StreamingListener {
    功能: 状态汇报监听器
    属性:
    #name @batchInfos = new Queue[BatchInfo]()	批量信息队列
    操作集:
    def onBatchCompleted(batchStarted: StreamingListenerBatchCompleted): Unit
    功能: 批量完成动作
    batchInfos.enqueue(batchStarted.batchInfo)
    if (batchInfos.size > numBatchInfos) batchInfos.dequeue()
    printStats()
    
    def printStats(): Unit
    功能: 打印状态信息
    showMillisDistribution("Total delay: ", _.totalDelay)
    showMillisDistribution("Processing time: ", _.processingDelay)
    
    def showMillisDistribution(
        heading: String, getMetric: BatchInfo => Option[Long]): Unit
    功能: 显示时间分布情况
    org.apache.spark.scheduler.StatsReportListener.showMillisDistribution(
      heading, extractDistribution(getMetric))
    
    def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution]
    功能: 抓取分布情况
    val= Distribution(batchInfos.flatMap(getMetric(_)).map(_.toDouble))
}
```

#### StreamingListenerBus

```scala
private[streaming] class StreamingListenerBus(sparkListenerBus: LiveListenerBus)
extends SparkListener with ListenerBus[StreamingListener, StreamingListenerEvent] {
    介绍: streaming监听总线
    监听总线发送事件到监听器上,这个会包装接受的流式事件@WrappedStreamingListenerEvent且将其发送到spark监听器总线上,自己也会使用spark监听总线注册,以便于可以接受@WrappedStreamingListenerEvent的事件,解除@StreamingListenerEvent的包装,并将其分发到监听器上@StreamingListener.
    操作集:
    def post(event: StreamingListenerEvent): Unit
    功能: 异步发送事件到到spark监听总线上.事件会被发送到所有的streaming监听器@StreamingListener上.(使用spark监听总线)
    sparkListenerBus.post(new WrappedStreamingListenerEvent(event))
    
    def onOtherEvent(event: SparkListenerEvent): Unit
    功能: 事件处理
    event match {
      case WrappedStreamingListenerEvent(e) =>
        postToAll(e)
      case _ =>
    }
    
    def doPostEvent(
      listener: StreamingListener,
      event: StreamingListenerEvent): Unit
    功能: 处理事件的发送
    event match {
      case receiverStarted: StreamingListenerReceiverStarted =>
        listener.onReceiverStarted(receiverStarted)
      case receiverError: StreamingListenerReceiverError =>
        listener.onReceiverError(receiverError)
      case receiverStopped: StreamingListenerReceiverStopped =>
        listener.onReceiverStopped(receiverStopped)
      case batchSubmitted: StreamingListenerBatchSubmitted =>
        listener.onBatchSubmitted(batchSubmitted)
      case batchStarted: StreamingListenerBatchStarted =>
        listener.onBatchStarted(batchStarted)
      case batchCompleted: StreamingListenerBatchCompleted =>
        listener.onBatchCompleted(batchCompleted)
      case outputOperationStarted: StreamingListenerOutputOperationStarted =>
        listener.onOutputOperationStarted(outputOperationStarted)
      case outputOperationCompleted: StreamingListenerOutputOperationCompleted =>
        listener.onOutputOperationCompleted(outputOperationCompleted)
      case streamingStarted: StreamingListenerStreamingStarted =>
        listener.onStreamingStarted(streamingStarted)
      case _ =>
    }
    
    def start(): Unit
    功能: 启动监听总线
    sparkListenerBus.addToStatusQueue(this)
    
    def stop(): Unit
    功能: 停止监听总线
    sparkListenerBus.removeListener(this)
    
    样例类:
    case class WrappedStreamingListenerEvent(
        streamingListenerEvent: StreamingListenerEvent)
    extends SparkListenerEvent {
        介绍: 包装的流式监听事件,以便于可以发送到spark监听总线上
        def logEvent: Boolean = false
        功能: 是否记录事件
        不会记录流式事件到事件日志中,因为历史服务器不支持流式事件
    }
}
```



#### 基础拓展

1.  [PID控制器](https://en.wikipedia.org/wiki/PID_controller)