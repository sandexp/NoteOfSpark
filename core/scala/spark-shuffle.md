## **spark-shuffle**

---

1.  [sort](# sort)
2.  [BaseShuffleHandle.scala](# BaseShuffleHandle)
3.  [BlockStoreShuffleReader.scala](# BlockStoreShuffleReader)
4.  [FetchFailedException.scala](# FetchFailedException)
5.  [IndexShuffleBlockResolver.scala](# IndexShuffleBlockResolver)
6.  [Metrics.scala](# Metrics)
7.  [ShuffleBlockResolver.scala](# ShuffleBlockResolver)
8.  [ShuffleDataIOUtils.scala](# ShuffleDataIOUtils)
9.  [ShuffleHandle.scala](# ShuffleHandle)
10.  [ShuffleManager.scala](# ShuffleManager)
11.  [ShufflePartitionPairsWriter.scala](# ShufflePartitionPairsWriter)
12.  [ShuffleReader.scala](# ShuffleReader)
13.  [ShuffleWriteProcessor.scala](# ShuffleWriteProcessor)
14.  [ShuffleWriter.scala](# ShuffleWriter)

---

#### sort

#### BaseShuffleHandle

```markdown
介绍:
	基本的shuffle处理器，仅仅处理登记shuffle的属性
```

```markdown
private[spark] class BaseShuffleHandle[K, V, C] (shuffleId: Int,
    val dependency: ShuffleDependency[K, V, C]) {
	关系: father -->  ShuffleHandle(shuffleId) 
    构造器属性:
    	shuffleId	构造器Id
    	dependency	shuffle依赖
}
```

#### BlockStoreShuffleReader

```markdown
private[spark] class BlockStoreShuffleReader[K, C] (
    handle: BaseShuffleHandle[K, _, C],
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    context: TaskContext,readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    shouldBatchFetch: Boolean = false){
	介绍: 通过请求其他节点的块获取和读取一个shuffle
    关系: father --> ShuffleReader[K, C]
    	sibling --> Logging
    构造器属性:
    	handle	shuffle处理器
    	blocksByAddress	块地址列表
    	context	任务上下文
    	readMetrics	shuffle读取度量器
    	serializerManager	序列化管理器
    	blockManager	块管理器
    	mapOutputTracker	map输出追踪器
    	shouldBatchFetch=false	比例获取标志
	属性:
		#name @dep=handle.dependency	依赖
	操作集:
	def fetchContinuousBlocksInBatch: Boolean 
	功能: 批量获取连续块
	1. 获取序列化,压缩，编码信息
        val conf = SparkEnv.get.conf
        val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
        val compressed = conf.get(config.SHUFFLE_COMPRESS)
		// 串联编码状态位
		val codecConcatenation= compressed ?
			CompressionCodec.supportsConcatenationOfSerializedStreams : true
		// 是否获取旧的获取协议
		val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)
	2. 确定是否可以批量获取
    	val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
    		(!compressed || codecConcatenation) && !useOldFetchProtocol
    	val= doBatchFetch
    
    def read(): Iterator[Product2[K, C]]
    功能: 读取reduce 任务的kv值
    
}
```



#### FetchFailedException

```markdown
介绍:
	获取shuffle块失败，执行器捕捉到异常并传递给DAGScheduler(通过 @TaskEndReason),所以需要重新提交上一个stage。注意到块管理器编号@BlockManagerId 可以为空。
	为了阻止用户代码隐藏获取失败，在构造器中调用@TaskContext.setFetchFailed()。这意味着在创建出它之后必须要立马抛出异常。
```

```markdown
private[spark] class FetchFailedException(bmAddress: BlockManagerId,shuffleId: Int,
    mapId: Long,mapIndex: Int,reduceId: Int,message: String,cause: Throwable = null){
	关系: father --> Exception(message, cause)
    构造器属性:
    	bmAddress	块管理器Id
    	shuffleId	shuffleId
    	mapId		mapId
    	mapIndex	map序号
    	reduceId	reduceId
    	message		消息内容
    	cause		异常/错误原因
	构造器:
	def this(bmAddress: BlockManagerId,shuffleId: Int,mapTaskId: Long,mapIndex: Int,
      	reduceId: Int,cause: Throwable)
	val= this(bmAddress, shuffleId, mapTaskId, mapIndex, reduceId, cause.getMessage, cause)
	
	初始化操作:
	Option(TaskContext.get()).map(_.setFetchFailed(this))
	功能: 使得即使在用户代码中隐藏了异常，执行器依旧可以分辨出异常，并将异常汇报给驱动器。[是需要创建这个对象		即可抛出]
	
	操作集:
	def toTaskFailedReason: TaskFailedReason = 
		FetchFailed(bmAddress, shuffleId, mapId, mapIndex, reduceId, Utils.exceptionString(this))
	功能: 获取任务失败原因
}
```

```markdown
private[spark] class MetadataFetchFailedException(shuffleId: Int,reduceId: Int,message: String){
	介绍: 获取元数据失败
	关系: father --> FetchFailedException(null, shuffleId, -1L, -1, reduceId, message)
	构造器属性:
		shuffleId	shuffleId
		reduceId	reduceId
		message		消息内容
}
```

#### IndexShuffleBlockResolver

#### Metrics

```markdown
private[spark] trait ShuffleReadMetricsReporter {
	介绍: 用于汇报shuffle读取的度量数据，对于每个shuffle，这个接口假定所有方法都是单线程调用。
		所有方法都有对spark的额外可见性@
	操作集:
	  private[spark] def incRemoteBlocksFetched(v: Long): Unit
	  功能: 获取远端数据块+=v 
	  
      private[spark] def incLocalBlocksFetched(v: Long): Unit
      功能: 获取本地数据块+=v
      
      private[spark] def incRemoteBytesRead(v: Long): Unit
      功能: 远端字节读取数量+=v
      
      private[spark] def incRemoteBytesReadToDisk(v: Long): Unit
      功能: 远端字节落盘数量+=v
      
      private[spark] def incLocalBytesRead(v: Long): Unit
      功能: 本地读取字节量+=v
      
      private[spark] def incFetchWaitTime(v: Long): Unit
      功能: 获取等待时间+=v
      
      private[spark] def incRecordsRead(v: Long): Unit
	 功能: 读取记录数量+=v
}
```

```markdown
private[spark] trait ShuffleWriteMetricsReporter {
	  
	 介绍: shuffle的写度量器，接口假定所有操作都是单线程的。
	  
	  private[spark] def incBytesWritten(v: Long): Unit
	  功能: 写出字节数量+=v
	  
      private[spark] def incRecordsWritten(v: Long): Unit
      功能: 写出记录数量+=v
      
      private[spark] def incWriteTime(v: Long): Unit
      功能: 写出时间+=v
      
      private[spark] def decBytesWritten(v: Long): Unit
      功能: 写出字节数量-=v
      
      private[spark] def decRecordsWritten(v: Long): Unit
	  功能: 写出记录数量-=v
}
```

#### ShuffleBlockResolver

```markdown
trait ShuffleBlockResolver {
	介绍:
	实现这个特征需要理解如何去定位一个逻辑数据块标志对应的数据块。可以使用文件或者文件片段去定位文件块。用于
	@BlockStore去抽象各类shuffle。
	属性:
	#name @shuffleId #type @int 	shuffleId
	def getBlockData(blockId: BlockId, dirs: Option[Array[String]] = None): ManagedBuffer
	功能: 获取指定块的数据,当目录变量不存在@dirs不存在时，使用磁盘管理器本地目录，其他情况读取指定目录数据。
		如果数据块无法到达，会抛出IO异常
	
	def stop(): Unit
	功能: 停止
}
```

#### ShuffleDataIOUtils

```markdown
private[spark] object ShuffleDataIOUtils {
	属性：
	#name @SHUFFLE_SPARK_CONF_PREFIX = "spark.shuffle.plugin.__config__."
		spark配置前缀
	操作集:
	def loadShuffleDataIO(conf: SparkConf): ShuffleDataIO
	功能: 加载shuffle IO数据
	1. 获取配置信息
	val configuredPluginClass = conf.get(SHUFFLE_IO_PLUGIN_CLASS)
	2. 获取备选的shuffle IO数据
	val maybeIO = Utils.loadExtensions(classOf[ShuffleDataIO], Seq(configuredPluginClass), conf)
	3. 断言输出
	val= maybeIO.nonEmpty ? maybeIO.head : Nop
	
}
```

#### ShuffleHandle

```markdown
@DeveloperApi
abstract class ShuffleHandle(val shuffleId: Int){
	关系: father --> Serializable
	介绍: 对应一个shuffle来说时不透明的，使用shuffle管理器@ShuffleManager将信息传递给任务
}
```

#### ShuffleManager

```markdown
介绍:
	对于shuffle系统来说是一个可插入的接口。shuffle管理器@ShuffleManager在驱动器和每个执行器的@sparkEnv上创建,基于@spark.shuffle.manager 配置。驱动器注册shuffle，执行器(或者跑在驱动器上的任务)请求读写数据。
	注意: 这个会被sparkEnv实例化，构造器能够接受@SparkConf类型数据和一个参数@isDriver(是否为驱动器标志)
```

```markdown
private[spark] trait ShuffleManager {
	操作集:
	def registerShuffle[K, V, C](shuffleId: Int,
		dependency: ShuffleDependency[K, V, C]): ShuffleHandle
	功能: 使用管理器去注册shuffle，获取一个shuffle任务的处理器@ShuffleHandle
	
	def getWriter[K, V](handle: ShuffleHandle,mapId: Long,context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]
     功能: 获取指定分区的写出器@ShuffleWriter[K, V] ,在map任务的执行器上调用
     
     def getReader[K, C](handle: ShuffleHandle,startPartition: Int,endPartition: Int,
      	context: TaskContext,metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]
	功能: 获取获取一个返回分区的聚合结果，在执行器的reduce阶段调用
	
	def getReaderForOneMapper[K, C](handle: ShuffleHandle,mapIndex: Int,startPartition: Int,
      	endPartition: Int,context: TaskContext,metrics: ShuffleReadMetricsReporter)
      	: ShuffleReader[K, C]
	功能: 获取指定map在reduce阶段指定区域的聚合结果，在执行器reduce阶段调用
	
	def unregisterShuffle(shuffleId: Int): Boolean
	功能: 从shuffle管理器@ShuffleManager中移除shuffle元数据(返回true表示移除成功)
	
	def shuffleBlockResolver: ShuffleBlockResolver
	功能: 返回一个可以根据块坐标找到shuffle块数据的处理方案@ShuffleBlockResolver
	
	def stop(): Unit
	功能: 关闭shuffle管理器@ShuffleManager
}
```

#### ShufflePartitionPairsWriter

```markdown
private[spark] class ShufflePartitionPairsWriter(
    partitionWriter: ShufflePartitionWriter,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    blockId: BlockId,writeMetrics: ShuffleWriteMetricsReporter){
	关系: father --> PairsWriter
    	sibling --> Closable
    属性:
    #name @isClosed = false	shuffle分区pair写出器状态
    #name @partitionStream=_ #type @OutputStream 分区输出流
    #name @timeTrackingStream #type @OutputStream	时间追踪输出流
    #name @wrappedStream=_	包装输出流
    #name @objOut #type @SerializationStream	序列化对象输出流
    #name @numRecordsWritten=0	已写记录数量
    #name @curNumBytesWritten=0	当前已写字节数量
    
    操作集:
    def write(key: Any, value: Any): Unit 
    功能: 记录写入
    操作条件: 写出器未关闭isClose=false
        if (objOut == null) {open()}
        objOut.writeKey(key)
        objOut.writeValue(value)
        recordWritten()
	
	def open(): Unit
	功能: 打开流(partitionStream,timeTrackingStream,wrappedStream,objOut)
	```scala
	try {
      partitionStream = partitionWriter.openStream
      timeTrackingStream = new TimeTrackingOutputStream(writeMetrics, partitionStream)
      wrappedStream = serializerManager.wrapStream(blockId, timeTrackingStream)
      objOut = serializerInstance.serializeStream(wrappedStream)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          close()
        }
        throw e
    }
	```
	
	def closeIfNonNull[T <: Closeable](closeable: T): T
	功能: 非空情况下关闭
	if (closeable != null) { closeable.close()}
    null.asInstanceOf[T]
    
    def recordWritten(): Unit
    功能: 提示写出器有多少字节的记录已经使用输出流写入
        numRecordsWritten += 1 // 以写记录+1
        writeMetrics.incRecordsWritten(1) // 汇报器属性+=1
        if (numRecordsWritten % 16384 == 0) {updateBytesWritten()}
	
	def updateBytesWritten(): Unit
	功能: 更新已经写出的字节数量
	```scala
	    val numBytesWritten = partitionWriter.getNumBytesWritten
        val bytesWrittenDiff = numBytesWritten - curNumBytesWritten
        writeMetrics.incBytesWritten(bytesWrittenDiff)
        curNumBytesWritten = numBytesWritten
	```
	
	def close(): Unit 
	功能: 关闭写出器
	操作条件: 写出器没有关闭@isClosed=false
	操作逻辑:
	1. 内部所有流置空
	```scala
		Utils.tryWithSafeFinally {
            Utils.tryWithSafeFinally {
              objOut = closeIfNonNull(objOut)
              wrappedStream = null
              timeTrackingStream = null
              partitionStream = null
        } {
          Utils.tryWithSafeFinally {
            wrappedStream = closeIfNonNull(wrappedStream)\
            timeTrackingStream = null
            partitionStream = null
          } {
            Utils.tryWithSafeFinally {
              timeTrackingStream = closeIfNonNull(timeTrackingStream)
              partitionStream = null
            } {
              partitionStream = closeIfNonNull(partitionStream)
            }
          }
        }
        updateBytesWritten()
      }
	```
	2. 更新标志位
	isClosed = true
}
```

#### ShuffleReader

```markdown
private[spark] trait ShuffleReader[K, C] {
	介绍: 从mapper中读取记录放入reduce中
	操作集:
	def read(): Iterator[Product2[K, C]]
	功能: 读取kv对置入reduce中
	
	def stop(): Unit
	功能: 关闭阅读器@this #class @ShuffleReader
}
```

#### ShuffleWriteProcessor

```markdown
private[spark] class ShuffleWriteProcessor{
	关系: father --> Serializable
	sibling --> Logging
	操作集:
	def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter
	功能: 给当前任务上下文@context创建度量报告器@ShuffleWriteMetricsReporter
		由于报告器是每行操作的，所以这里需要好好考虑操作
	val= context.taskMetrics().shuffleWriteMetrics
	
	def write(rdd: RDD[_],dep: ShuffleDependency[_, _, _],mapId: Long,
      	context: TaskContext,partition: Partition): MapStatus
     功能: 对于指定分区进行写的过程，通过@ShuffleWriter控制生命周期，从shuffle管理器@ShuffleManager获取并触		发RDD计算。最后返回当前任务的@MapStatus
     1. 获取shuffle写出器(作为生命周期开始，但是实际的值需要指定)
     var writer: ShuffleWriter[Any, Any] = null
     2. 获取shuffle管理器
     val manager = SparkEnv.get.shuffleManager
     3. shuffle管理器指定shuffle写出器
      writer = manager.getWriter[Any, Any](dep.shuffleHandle,mapId,context,
      	createMetricsReporter(context))
	4. 写出RDD中的数据
	writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
	5. 停止写出
	writer.stop(success = true).get
	
}
```

#### ShuffleWriter

```markdown
private[spark] abstract class ShuffleWriter[K, V] {
	介绍: 写入记录到shuffle系统(在map任务中)
	操作集:
	def write(records: Iterator[Product2[K, V]]): Unit
	功能: 写入记录到map中
	
	def stop(success: Boolean): Option[MapStatus]
	功能: 关闭写出器@this #class @ShuffleWriter
}
```

