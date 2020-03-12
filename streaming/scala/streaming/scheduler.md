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

#### InputInfoTracker

#### Job

#### JobGenerator

#### JobScheduler

#### JobSet

#### OutputOperationInfo

#### RateController

#### ReceivedBlockInfo

#### ReceivedBlockTracker

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

#### ReceiverTracker

#### ReceiverTrackingInfo

#### StreamingListener

#### StreamingListenerBus

#### 基础拓展

1.  [PID控制器](https://en.wikipedia.org/wiki/PID_controller)