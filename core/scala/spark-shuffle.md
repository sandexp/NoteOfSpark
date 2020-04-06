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

1.  [SortShuffleManager.scala](# SortShuffleManager)

2.  [SortShuffleWriter.scala](# SortShuffleWriter)

   ---

   #### SortShuffleManager

   ```scala
   private[spark] class SortShuffleManager(conf: SparkConf){
   	关系: father --> ShuffleManager
   		sibling --> Logging
   	属性:
   	#name @taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()
   		shuffle任务id
   	#name @shuffleExecutorComponents = loadShuffleExecutorComponents(conf)
   		shuffle执行组件
   	#name @shuffleBlockResolver = new IndexShuffleBlockResolver(conf)
   		shuffle块处理器
   	操作集:
   	def registerShuffle[K, V, C](shuffleId: Int,
         dependency: ShuffleDependency[K, V, C]): ShuffleHandle
   	功能: 注册shuffle，并返回shuffle处理器
   	val= SortShuffleWriter.shouldBypassMergeSort(conf, dependency) ?
   		BypassMergeSortShuffleHandle[K, V](shuffleId,
           	dependency.asInstanceOf[ShuffleDependency[K, V, V]]) :
           SortShuffleManager.canUseSerializedShuffle(dependency) ?
           SerializedShuffleHandle[K, V](shuffleId, 
           	dependency.asInstanceOf[ShuffleDependency[K, V, V]]) :
           new BaseShuffleHandle(shuffleId, dependency)
           
      	def stop(): Unit = {shuffleBlockResolver.stop()}
   	功能: 关闭shuffle处理器@shuffleBlockResolver
   	
   	def unregisterShuffle(shuffleId: Int): Boolean
   	功能: 从shuffle管理器中移除指定shuffle的元数据
           Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
             mapTaskIds.iterator.foreach { mapTaskId =>
               shuffleBlockResolver.removeDataByMap(shuffleId, mapTaskId)
             }
           }
   	
   	def getReader[K, C](handle: ShuffleHandle,startPartition: Int,endPartition: Int,
         context: TaskContext,metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]
   	功能: 获取指定区域reduce分区的阅读器,有执行器上的reduce任务调用
   	输入参数:
   		handle	shuffle处理器
   		startPartition	起始分区
   		endPartition	截止分区
   		context	任务上下文
   		metrics	shuffle读取度量器
   	操作逻辑:
   	1. 获取块信息
   	val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
         	handle.shuffleId, startPartition, endPartition)
        #name @blocksByAddress #type @Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] 
   	2. 返回可以存储块信息的shuffle阅读器@BlockStoreShuffleReader
   	val= BlockStoreShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
       	blocksByAddress, context, metrics,shouldBatchFetch = canUseBatchFetch(startPartition,
           endPartition, context))
   	
   	def getWriter[K, V](handle: ShuffleHandle,mapId: Long,
         context: TaskContext,metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]
   	功能: 获取指定分区的写出器
   	1. 获取map任务id列表
       val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
       	handle.shuffleId, _ => new OpenHashSet[Long](16))
       mapTaskIds.synchronized { mapTaskIds.add(context.taskAttemptId()) }
   	2. 根据不同类型的shuffle处理器@handle返回不同的shuffle写出器
   	val= 
   	handle match {
         case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
           new UnsafeShuffleWriter(
             env.blockManager,
             context.taskMemoryManager(),
             unsafeShuffleHandle,
             mapId,
             context,
             env.conf,
             metrics,
             shuffleExecutorComponents)
         case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
           new BypassMergeSortShuffleWriter(
             env.blockManager,
             bypassMergeSortHandle,
             mapId,
             env.conf,
             metrics,
             shuffleExecutorComponents)
         case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
           new SortShuffleWriter(
             shuffleBlockResolver, other, mapId, context, shuffleExecutorComponents)
   	
   	
   	def getReaderForOneMapper[K, C](handle: ShuffleHandle,mapIndex: Int,startPartition: Int,
         endPartition: Int,context: TaskContext,
         metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] 
   	功能: 获取指定map@mapIndex的shuffle阅读器
   	1. 获取map任务id列表
   	val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByMapIndex(
         handle.shuffleId, mapIndex, startPartition, endPartition)
        2. 获取包含块存储信息的shuffle阅读器@BlockStoreShuffleReader
        val= BlockStoreShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        	blocksByAddress, context, metrics,
         shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
   }
   ```

   ```scala
   private[spark] object SortShuffleManager{
   	关系: father --> Logging
   	属性:
   	#name @MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
       	PackedRecordPointer.MAXIMUM_PARTITION_ID + 1
   	在序列化模式下最大shuffle输出分区数量
   	#name @FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY =
       	"__fetch_continuous_blocks_in_batch_enabled"
   	允许连续获取shuffle块
   	
   	操作集:
   	def canUseBatchFetch(startPartition: Int, endPartition: Int, context: TaskContext): Boolean
   	功能: 是否可以批量获取
   	val= endPartition - startPartition > 1 &&
         context.getLocalProperty(FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY) == "true"
        
       def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean 
       功能: 是否可以使用序列化shuffle
       1. 获取shuffleId和分区数量
       val shufId = dependency.shuffleId
       val numPartitions = dependency.partitioner.numPartitions
   	2. 确定是否能够使用序列化shuffle
   	val= !dependency.serializer.supportsRelocationOfSerializedObjects ? false :
   		dependency.mapSideCombine ? false : 
   		numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE ? false : true
   	
   	def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents
   	功能: 加载shuffle执行器组件
   	1. 获取执行器组件
   	val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
   	2. 获取配置列表
   	val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap
   	3. 初始化组件信息，并返回
   	executorComponents.initializeExecutor(conf.getAppId,SparkEnv.get.executorId,
         	extraConfigs.asJava)
   	val= executorComponents
   }
   ```

   ```scala
   private[spark] class SerializedShuffleHandle[K, V] (
     shuffleId: Int,dependency: ShuffleDependency[K, V, V]){
   	关系: father --> BaseShuffleHandle(shuffleId, dependency)
       介绍: BaseShuffleHandle子类，用于标识序列化shuffle
   }
   
   private[spark] class BypassMergeSortShuffleHandle[K, V] (
     shuffleId: Int,dependency: ShuffleDependency[K, V, V]){
    	关系: father --> BaseShuffleHandle(shuffleId, dependency)
       介绍: BaseShuffleHandle子类，用于标记可以绕开归并排序的shuffle路径
    }
   
   ```

   

   #### SortShuffleWriter

   ```scala
   private[spark] class SortShuffleWriter[K, V, C] (
       shuffleBlockResolver: IndexShuffleBlockResolver,
       handle: BaseShuffleHandle[K, V, C],mapId: Long,
       context: TaskContext,shuffleExecutorComponents: ShuffleExecutorComponents){
   	关系: father --> ShuffleWriter[K, V]
       sibling --> Logging
       构造器属性:
       	shuffleBlockResolver	shuffle块处理器
       	handle	基本处理器
       	context	任务上下文
       	mapId	mapId
       	shuffleExecutorComponents	shuffle执行器组件
       属性:
       	#name @dep = handle.dependency	依赖
       	#name @blockManager = SparkEnv.get.blockManager	块管理器
       	#name @sorter: ExternalSorter[K, V, _] = null	外部排序器
       	#name @stopping = false	停止状态位
   		#name @mapStatus: MapStatus = null	Map侧状态
           #name @writeMetrics= context.taskMetrics().shuffleWriteMetrics	写度量器
       操作集:
       def write(records: Iterator[Product2[K, V]]): Unit
       功能: 写出记录
       1. 获取排序器
       sorter=dep.mapSideCombine?new ExternalSorter[K, V, C](context, dep.aggregator,
       		Some(dep.partitioner), dep.keyOrdering, dep.serializer):
       		new ExternalSorter[K, V, V](context, aggregator = None, Some(dep.partitioner),
               ordering = None, dep.serializer)
   	2. 插入所有记录
   	sorter.insertAll(records)
   	3. 获取map输出写出器
   	val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
         	dep.shuffleId, mapId, dep.partitioner.numPartitions)
   	4. 写出分区内容并提交
       sorter.writePartitionedMapOutput(dep.shuffleId, mapId, mapOutputWriter)
       val partitionLengths = mapOutputWriter.commitAllPartitions()
       5. 更新map侧状态
       mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
       
       def stop(success: Boolean): Option[MapStatus]
       功能: 关闭写出器
       1. 关闭写出器
           if (stopping) {
               return None
             }
             stopping = true
             if (success) {
               return Option(mapStatus)
             } else {
               return None
             }
       2. 情况排序器
           if (sorter != null) {
               val startTime = System.nanoTime()
               sorter.stop()
               writeMetrics.incWriteTime(System.nanoTime - startTime)
               sorter = null
             }
   }
   ```

   ```scala
   private[spark] object SortShuffleWriter{
   	def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean
   	功能: 确定是否需要合并排序
   	val= if (dep.mapSideCombine) { // map侧合并就不会引起合并排序
         false
       } else {
         val bypassMergeThreshold: Int = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
         dep.partitioner.numPartitions <= bypassMergeThreshold
       }
   }
   ```

   

#### BaseShuffleHandle

```markdown
介绍:
	基本的shuffle处理器，仅仅处理登记shuffle的属性
```

```scala
private[spark] class BaseShuffleHandle[K, V, C] (shuffleId: Int,
    val dependency: ShuffleDependency[K, V, C]) {
	关系: father -->  ShuffleHandle(shuffleId) 
    构造器属性:
    	shuffleId	构造器Id
    	dependency	shuffle依赖
}
```

#### BlockStoreShuffleReader

```scala
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

```scala
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

```scala
private[spark] class MetadataFetchFailedException(shuffleId: Int,reduceId: Int,message: 
){
	介绍: 获取元数据失败
	关系: father --> FetchFailedException(null, shuffleId, -1L, -1, reduceId, message)
	构造器属性:
		shuffleId	shuffleId
		reduceId	reduceId
		message		消息内容
}
```

#### IndexShuffleBlockResolver

```markdown
介绍:
	创建和维护shuffle块逻辑块与物理文件位置的映射。同一个map任务的shuffle块数据存储在单个统一的数据文件中。数据块在数据文件中的偏移量存储在一个分散的索引文件中。
	我们使用带有reduceID的shuffle数据的shuffle块编号@shuffleBlockId，将其设置为0，且添加.data作为数据文件的后缀。.index为索引文件后缀。
	注意: 这个文件的格式改变需要使用#class@org.apache.spark.network.shuffle.ExternalShuffleBlockResolver
	#method @getSortBasedShuffleBlockData()同步保存。
```

```scala
private[spark] class IndexShuffleBlockResolver(conf: SparkConf,_blockManager: BlockManager = null){
	关系: father --> ShuffleBlockResolver
		sibling --> Logging
	构造器属性:
		conf 	应用程序配置集
		_blockManager	块管理器
	属性:
	#name @blockManager= Option(_blockManager).getOrElse(SparkEnv.get.blockManager) lazy 块管理器
	#name @transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")	传输配置
    操作集:
     def getDataFile(shuffleId: Int, mapId: Long): File=getDataFile(shuffleId, mapId, None)
     功能: 根据shuffleId,mapId获取数据文件
     
     def getDataFile(shuffleId: Int, mapId: Long, dirs: Option[Array[String]]): File
     功能: 获取shuffle的数据文件,当目录为None时，使用磁盘管理器的本地目录。
     1. 获取块编号#class @BlockId #class @ShuffleDataBlockId
     val blockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
     2. 获取数据文件
     dirs.map(ExecutorDiskUtils.getFile(_, blockManager.subDirsPerLocalDir, blockId.name))
      	.getOrElse(blockManager.diskBlockManager.getFile(blockId))
	
	def getIndexFile(shuffleId: Int,mapId: Long,dirs: Option[Array[String]] = None): File 
	功能: 获取索引文件，当输入目录为空则读入磁盘管理器的本地目录
	1. 获取块编号
	val blockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
	2. 获取索引文件
	dirs.map(ExecutorDiskUtils.getFile(_, blockManager.subDirsPerLocalDir, blockId.name))
      	.getOrElse(blockManager.diskBlockManager.getFile(blockId))
	
	def removeDataByMap(shuffleId: Int, mapId: Long): Unit
	功能: 移除一个map中包含输出数据的数据文件和索引文件
	1. 获取数据文件
	var file = getDataFile(shuffleId, mapId)
	2. 删除数据文件
	if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }
    3. 获取索引文件
    file = getIndexFile(shuffleId, mapId)
    4. 删除索引文件
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
	
	def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] 
	功能: 检查索引文件和数据文件是否匹配，如果匹配则返回数据文件中的分区数量，否则返回null
	1. 必须要有block+1个长度，否则返回null
	if (index.length() != (blocks + 1) * 8L) { return null }
	2. 指定长度列表信息
	val lengths = new Array[Long](blocks)
	3. 获取每个数据块的长度
        val in = try {
          new DataInputStream(new NioBufferedFileInputStream(index))
        } catch {
          case e: IOException =>
            return null
        }
        try {
          var offset = in.readLong()
          if (offset != 0L) {
            return null
          }
          var i = 0
          while (i < blocks) {
            val off = in.readLong()
            lengths(i) = off - offset
            offset = off
            i += 1
          }
        } catch {
          case e: IOException =>
            return null
        } finally {
          in.close()
        }
	4. 检查数据文件和索引文件是否匹配
        if (data.length() == lengths.sum) {
          lengths
        } else {
          null
        }
   
    def writeIndexFileAndCommit(shuffleId: Int,mapId: Long,
      lengths: Array[Long],dataTmp: File): Unit
    功能：写出索引文件并提交
    1. 获取索引文件
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = Utils.tempFileWith(indexFile)
    2. 获取数据文件
    val dataFile = getDataFile(shuffleId, mapId)
    3. 确保检查索引文件与数据文件的匹配性
    	synchronized {
        	val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
            if (existingLengths != null) {
              System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
              if (dataTmp != null && dataTmp.exists()) {
                dataTmp.delete()
              }
        } else {
          val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
          Utils.tryWithSafeFinally {
            var offset = 0L
            out.writeLong(offset)
            for (length <- lengths) {
              offset += length
              out.writeLong(offset)
            }
          } {
            out.close()
          }
   	4. 删除索引文件和数据文件
   	if (indexFile.exists()) { indexFile.delete() }
    if (dataFile.exists()) { dataFile.delete()}
	5. 临时文件重命名工作
	if(!indexTmp.renameTo(indexFile)) {
        throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
     }
     if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
        throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
     }
	6. 删除临时文件
	if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    
    def stop(): Unit = {}
    功能: 关闭
    
    def getBlockData(blockId: BlockId,dirs: Option[Array[String]]): ManagedBuffer
	功能: 获取数据块
	1. 获取shuffleId,mapId,起始reduceId，结束reduceId
	val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
    }
    2. 获取索引文件
    val indexFile = getIndexFile(shuffleId, mapId, dirs)
    3. 设置字节通道初始位置
    val channel = Files.newByteChannel(indexFile.toPath)
    channel.position(startReduceId * 8L)
    4. 获取起始与结束偏移量
    val in = new DataInputStream(Channels.newInputStream(channel))
    val startOffset = in.readLong()
    channel.position(endReduceId * 8L)
    val endOffset = in.readLong()
    val actualPosition = channel.position()
    val expectedPosition = endReduceId * 8L + 8
    if (actualPosition != expectedPosition) {
    	throw new Exception(s"SPARK-22982: Incorrect channel position after index file reads: " +
    	s"expected $expectedPosition but actual position was $actualPosition.")
    }
    5. 返回一个缓冲区
    val= FileSegmentManagedBuffer(transportConf,getDataFile(shuffleId, mapId, dirs),
        startOffset,endOffset - startOffset)
}
```

```scala
private[spark] object IndexShuffleBlockResolver {
	属性: 
	#name @NOOP_REDUCE_ID = 0	与磁盘操作中没有操作的reduce ID
}
```



#### Metrics

```scala
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

```scala
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

```scala
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

```scala
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

```scala
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

```scala
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
	
	def getReaderForOneMapper[K, C](handle: ShuffleHandle,mapIndex: Int,startPartition: Int,endPartition: Int,context: TaskContext,metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]
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

```scala
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
	    val numBytesWritten = partitionWriter.getNumBytesWritten
	    val bytesWrittenDiff = numBytesWritten - curNumBytesWritten
	    writeMetrics.incBytesWritten(bytesWrittenDiff)
	    curNumBytesWritten = numBytesWritten


	def close(): Unit 
	功能: 关闭写出器
	操作条件: 写出器没有关闭@isClosed=false
	操作逻辑:
	1. 内部所有流置空
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
	2. 更新标志位
	isClosed = true
}

```

#### ShuffleReader

```scala
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

```scala
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

```scala
private[spark] abstract class ShuffleWriter[K, V] {
	介绍: 写入记录到shuffle系统(在map任务中)
	操作集:
	def write(records: Iterator[Product2[K, V]]): Unit
	功能: 写入记录到map中
	
	def stop(success: Boolean): Option[MapStatus]
	功能: 关闭写出器@this #class @ShuffleWriter
}
```

