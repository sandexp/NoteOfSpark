#### **status**

---

#### api

```scala
class StreamingStatistics private[spark](
    val startTime: Date,	// 启动时间
    val batchDuration: Long, // 批次持续时间
    val numReceivers: Int, //receiver数量
    val numActiveReceivers: Int, // 激活的receiver数量
    val numInactiveReceivers: Int, // 非激活的receiver数量
    val numTotalCompletedBatches: Long, //总计完成的批次数量
    val numRetainedCompletedBatches: Long, // 剩余完成的批次数量
    val numActiveBatches: Long, //激活的批次数量
    val numProcessedRecords: Long, // 进行中的记录数量
    val numReceivedRecords: Long, //接受的记录数量
    val avgInputRate: Option[Double], //平均输出比率
    val avgSchedulingDelay: Option[Long], // 平均调度延时
    val avgProcessingTime: Option[Long], // 平均进行时间
    val avgTotalDelay: Option[Long]) // 平均总计延时
介绍: streaming统计类

class ReceiverInfo private[spark](
    val streamId: Int, // stream编号
    val streamName: String, // stream名称
    val isActive: Option[Boolean], // 是否处于激活状态
    val executorId: Option[String], //执行器ID
    val executorHost: Option[String], // 执行器主机
    val lastErrorTime: Option[Date], // 最后一个错误时间
    val lastErrorMessage: Option[String], // 最后一个错误的信息
    val lastError: Option[String], // 最后一个错误
    val avgEventRate: Option[Double], // 平均事件比率
    val eventRates: Seq[(Long, Double)]) // 事件比率表
介绍: receiver信息

class BatchInfo private[spark](
    val batchId: Long, //批次编号
    val batchTime: Date, //批次时间
    val status: String, //状态
    val batchDuration: Long, //批次持续时间
    val inputSize: Long, //输入大小
    val schedulingDelay: Option[Long], // 调度延时
    val processingTime: Option[Long], // 进行时间
    val totalDelay: Option[Long], //总计延时
    val numActiveOutputOps: Int, //激活输出操作的数量
    val numCompletedOutputOps: Int, //完成输出操作的数量
    val numFailedOutputOps: Int, // 失败输出操作的数量
    val numTotalOutputOps: Int, // 总计输出操作的数量
    val firstFailureReason: Option[String]) //首次失败原因
介绍: 批次信息

class OutputOperationInfo private[spark](
    val outputOpId: OutputOpId, // 输出操作编号
    val name: String, // 名称
    val description: String, //描述
    val startTime: Option[Date], //开始时间
    val endTime: Option[Date], //结束时间
    val duration: Option[Long], // 持续时间
    val failureReason: Option[String], // 失败原因
    val jobIds: Seq[SparkJobId]) // job编号
介绍: 输出操作信息

```

##### ApiStreamingApp

```scala
@Path("/v1")
private[v1] class ApiStreamingApp extends ApiRequestContext {
    @Path("applications/{appId}/streaming")
    def getStreamingRoot(@PathParam("appId") appId: String)
    : Class[ApiStreamingRootResource] = {
        classOf[ApiStreamingRootResource]
    }
    功能: 获取streaming的root
    
    @Path("applications/{appId}/{attemptId}/streaming")
    def getStreamingRoot(
        @PathParam("appId") appId: String,
        @PathParam("attemptId") attemptId: String): Class[ApiStreamingRootResource] = {
        classOf[ApiStreamingRootResource]
    }
    功能: 获取streaming的root
}
```

##### ApiStreamingRootResource

```scala
private[v1] trait BaseStreamingAppResource extends BaseAppResource {
    介绍: streaming API处理器的基础类,提供获取streaming监听器,监听器可以容纳应用的信息
    def withListener[T](fn: StreamingJobProgressListener => T): T
    功能: 从监听器中获取信息,使用转换函数@fn
    val listener = ui.getStreamingJobProgressListener match {
      case Some(listener) => listener.asInstanceOf[StreamingJobProgressListener]
      case None => throw new NotFoundException("no streaming listener attached to 
      " + ui.getAppName)
    }
    listener.synchronized {
      fn(listener)
    }
}
```

