#### **rdd**

---

##### MapWithStateRDD

```scala
private[streaming] case class MapWithStateRDDRecord[K, S, E](
    var stateMap: StateMap[K, S], var mappedData: Seq[E])
介绍: 记录,存储着带有key状态的@MapWithStateRDD,每个记录包含@StateMap 和一系列的记录
构造器参数:
	stateMap 状态列表
	mappedData	记录列表
```

```scala
private[streaming] object MapWithStateRDDRecord {
    操作集:
    def updateRecordWithData[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
        prevRecord: Option[MapWithStateRDDRecord[K, S, E]],
        dataIterator: Iterator[(K, V)],
        mappingFunction: (Time, K, Option[V], State[S]) => Option[E],
        batchTime: Time,
        timeoutThresholdTime: Option[Long],
        removeTimedoutData: Boolean
      ): MapWithStateRDDRecord[K, S, E]
    功能: 使用数据更新记录(旧的记录)
    输入参数:
    	prevRecord	上一条记录
    	dataIterator	数据信息(迭代器)
    	mappingFunction	映射函数
    	batchTime	批次时间
    	timeoutThresholdTime	超时上限时间
    	removeTimedoutData	是否移除超时数据
    1. 创建状态map(对旧map进行映射)
    val newStateMap = prevRecord.map { _.stateMap.copy() }
    . getOrElse { new EmptyStateMap[K, S]() }
    val mappedData = new ArrayBuffer[E]
    val wrappedState = new StateImpl[S]()
    2. 调用映射函数,用于数据迭代器的每条记录上,通过更新状态,收集映射函数返回的数据 
    dataIterator.foreach { case (key, value) =>
      wrappedState.wrap(newStateMap.get(key))
      val returned = mappingFunction(batchTime, key, Some(value), wrappedState)
      if (wrappedState.isRemoved) {
        newStateMap.remove(key)
      } else if (wrappedState.isUpdated
          || (wrappedState.exists && timeoutThresholdTime.isDefined)) {
        newStateMap.put(key, wrappedState.get(), batchTime.milliseconds)
      }
      mappedData ++= returned
    }
    3. 获取超时状态记录,调用映射函数,用于每个返回的数据
    if (removeTimedoutData && timeoutThresholdTime.isDefined) {
      newStateMap.getByTime(timeoutThresholdTime.get).foreach { case (key, state, _) =>
        wrappedState.wrapTimingOutState(state)
        val returned = mappingFunction(batchTime, key, None, wrappedState)
        mappedData ++= returned
        newStateMap.remove(key)
      }
    }
    val= MapWithStateRDDRecord(newStateMap, mappedData)
}
```

```scala
private[streaming] class MapWithStateRDDPartition(
    override val index: Int,
    @transient private var prevStateRDD: RDD[_],
    @transient private var partitionedDataRDD: RDD[_]) extends Partition {
    介绍: @MapWithStateRDD的分区,依靠于前一个状态RDD的对应分区,和分区的keyed-data RDD.
    构造器参数:
    	index	分区编号
    	prevStateRDD	上一个状态RDD
    	partitionedDataRDD	分区数据RDD
    属性:
    #name @previousSessionRDDPartition: Partition = null	前一个会话RDD分区
    #name @partitionedDataRDDPartition: Partition = null	分区数据RDD分区
    操作集:
    def hashCode(): Int = index
    功能: 计算散列值
    
    def equals(other: Any): Boolean
    功能: 判断两个值是否相等
    val= other match {
        case that: MapWithStateRDDPartition => index == that.index
        case _ => false
      }
    
    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit 
    功能: 写出属性值(序列化)
    Utils.tryOrIOException {
        previousSessionRDDPartition = prevStateRDD.partitions(index)
        partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
        oos.defaultWriteObject()
    }
}
```

```scala
private[streaming] class MapWithStateRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
    private var prevStateRDD: RDD[MapWithStateRDDRecord[K, S, E]],
    private var partitionedDataRDD: RDD[(K, V)],
    mappingFunction: (Time, K, Option[V], State[S]) => Option[E],
    batchTime: Time,
    timeoutThresholdTime: Option[Long]
  ) extends RDD[MapWithStateRDDRecord[K, S, E]](
    partitionedDataRDD.sparkContext,
    List(
      new OneToOneDependency[MapWithStateRDDRecord[K, S, E]](prevStateRDD),
      new OneToOneDependency(partitionedDataRDD))
  ) {
    介绍: 
    RDD存储@mapWithState 操作的key状态和项羽的数据,这个RDD的每个分区都有单一的记录类型@MapWithStateRDDRecord.包含@StateMap,且记录列表右映射函数返回.
    构造器参数:
        prevStateRDD	之前的@MapWithStateRDD,其中@StateMap数据的RDD已经被创建
        partitionedDataRDD	分区数据RDD,,用于在@prevStateRDD更新之前的@StateMaps,创建这个RDD
        mappingFunction	映射函数
        batchTime	当前RDD的批次时间,用于更新
        timeoutThresholdTime	超时时间上限
    属性:
    #name @doFullScan = false volatile	是否进行全扫描
    #name @partitioner = prevStateRDD.partitioner	分区器
    操作集:
    def checkpoint(): Unit
    功能: 设置检查点
    super.checkpoint()
    doFullScan = true
    
    def compute(
      partition: Partition, context: TaskContext): 
    Iterator[MapWithStateRDDRecord[K, S, E]]
    功能: 计算指定的分区内容
    val stateRDDPartition = partition.asInstanceOf[MapWithStateRDDPartition]
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)
    val prevRecord = if (prevStateRDDIterator.hasNext) Some(prevStateRDDIterator.next())
    	else None
    val newRecord = MapWithStateRDDRecord.updateRecordWithData(
      prevRecord,
      dataIterator,
      mappingFunction,
      batchTime,
      timeoutThresholdTime,
      removeTimedoutData = doFullScan
    )
    val= Iterator(newRecord)
    
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    val= Array.tabulate(prevStateRDD.partitions.length) { i =>
      new MapWithStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
    
    def clearDependencies(): Unit
    功能: 清除依赖
    super.clearDependencies()
    prevStateRDD = null
    partitionedDataRDD = null
    
    def setFullScan(): Unit
    功能: 设置全扫描
    doFullScan = true
}
```

```scala
private[streaming] object MapWithStateRDD {
    操作集:
    def createFromPairRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
      pairRDD: RDD[(K, S)],
      partitioner: Partitioner,
      updateTime: Time): MapWithStateRDD[K, V, S, E]
    功能: 由pairRDD创建状态RDD
    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions ({ iterator =>
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { 
          case (key, state) => stateMap.put(key, state, updateTime.milliseconds) }
      Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[E]))
    }, preservesPartitioning = true)
    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)
    val noOpFunc = (time: Time, key: K, value: Option[V], state: State[S]) => None
    val= new MapWithStateRDD[K, V, S, E](
      stateRDD, emptyDataRDD, noOpFunc, updateTime, None)
    
    def createFromRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
      rdd: RDD[(K, S, Long)],
      partitioner: Partitioner,
      updateTime: Time): MapWithStateRDD[K, V, S, E]
    功能: 由RDD创建状态RDD
    1. 获取pairRDD
    val pairRDD = rdd.map { x => (x._1, (x._2, x._3)) }
    2. 在pairRDD的基础上创建状态RDD
    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({ iterator =>
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { case (key, (state, updateTime)) =>
        stateMap.put(key, state, updateTime)
      }
      Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[E]))
    }, preservesPartitioning = true)
    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)
    val noOpFunc = (time: Time, key: K, value: Option[V], state: State[S]) => None
    val= new MapWithStateRDD[K, V, S, E](
      stateRDD, emptyDataRDD, noOpFunc, updateTime, None)
}
```

##### WriteAheadLogBackedBlockRDD

```scala
private[streaming]
class WriteAheadLogBackedBlockRDDPartition(
    val index: Int,
    val blockId: BlockId,
    val isBlockIdValid: Boolean,
    val walRecordHandle: WriteAheadLogRecordHandle
  ) extends Partition
介绍: 这个是@WriteAheadLogBackedBlockRDD 的分区类,包含关于数据块标识符的信息，含有分区数据且响应的记录处理在预先写日志中，这个可以返回分区。
构造器参数:
	index	分区索引
	blockId	数据块标识符
	isBlockIdValid	数据块标识符代表的数据块是否可用
	walRecordHandle	在预先写日志中处理记录，这个日志中含有分区数据
```

```scala
private[streaming]
class WriteAheadLogBackedBlockRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val _blockIds: Array[BlockId],
    @transient val walRecordHandles: Array[WriteAheadLogRecordHandle],
    @transient private val isBlockIdValid: Array[Boolean] = Array.empty,
    storeInBlockManager: Boolean = false,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER)
extends BlockRDD[T](sc, _blockIds) {
    介绍: 返回预先写日志数据块的RDD
   这个类代表数据块RDD@BlockRDD 的一种特殊情况,块管理器中的数据块也会在预先写日志中返回.对于读取的数据,这个RDD首先按照数据块标识符@BlockId查找数据块.如果没有找到,使用相应的记录处理器查找WAL.查找到的数据块会通过设置相应的元素(isBlockIdValid = false)来跳过，这个是一种性能的优化，但是不会影响结果的正确性，且在得知这个数据块的时候使用。
    构造器参数:
    	sc	spark上下文
    	_blockIds	数据块标识符列表
    	walRecordHandles	在包含RDD数据的预先写日志中进行记录处理
    	isBlockIdValid	数据块是否可用
    	storeInBlockManager	从WAL读取之后是否需要将数据块存储到数据块管理器中
    	storageLevel	存储等级
    属性:
    #name @hadoopConfig = sc.hadoopConfiguration transient	hadoop配置
    #name @broadcastedHadoopConf = new SerializableConfiguration(hadoopConfig)
    	广播变量的hadoop配置(hadoop配置比较大,所以选择广播变量)
    操作集:
    def isValid(): Boolean = true
    功能: RDD是否可用
    
    def getPartitions: Array[Partition]
    功能: 获取分区列表
    assertValid()
    Array.tabulate(_blockIds.length) { i =>
      val isValid = if (isBlockIdValid.length == 0) true else isBlockIdValid(i)
      new WriteAheadLogBackedBlockRDDPartition(
          i, _blockIds(i), isValid, walRecordHandles(i))
    }
    
    def getBlockFromBlockManager(): Option[Iterator[T]]
    功能: 从数据块管理器中获取数据块
    val= blockManager.get[T](blockId).map(_.data.asInstanceOf[Iterator[T]])
    
    def getBlockFromWriteAheadLog(): Iterator[T] 
    功能: 从预先写日志中获取数据块
    1. 初始化读取数据和预先写日志
    var dataRead: ByteBuffer = null
    var writeAheadLog: WriteAheadLog = null
    2. 设置读取数据和预先写日志
    try {
        // WriteAheadLogUtils.createLog***方法需要目录创建一个预先写对象作为默认值,因为基于预先写日
        // 志的文件@FileBasedWriteAheadLog需要一个目录去写日志数据,但是如果数据需要被读取的话是不需
        // 要的.假设伪路径用于满足方法参数的需求,@FileBasedWriteAheadLog不会在指定路径上创建任何文件
        // 和目录.通用为目录不需要,否则WAL会从目录中恢复过去的事件并抛出错误.
        val nonExistentDirectory = new File(
          	System.getProperty("java.io.tmpdir"),
            UUID.randomUUID().toString).toURI.toString
        writeAheadLog = WriteAheadLogUtils.createLogForReceiver(
          SparkEnv.get.conf, nonExistentDirectory, hadoopConf)
        dataRead = writeAheadLog.read(partition.walRecordHandle)
      } catch {
        case NonFatal(e) =>
          throw new SparkException(
            s"Could not read data 
            from write ahead log record ${partition.walRecordHandle}", e)
      } finally {
        if (writeAheadLog != null) {
          writeAheadLog.close()
          writeAheadLog = null
        }
      }
    3. 参数校验
    if (dataRead == null) {
        throw new SparkException(
          s"Could not read data from write ahead log record 
          ${partition.walRecordHandle}, " +
            s"read returned null")
      }
      logInfo(s"Read partition data of $this from write ahead log, record handle " +
        partition.walRecordHandle)
    4. 存储数据
    if (storeInBlockManager) {
        blockManager.putBytes(blockId, new ChunkedByteBuffer(
            dataRead.duplicate()), storageLevel)
        logDebug(s"Stored partition data of $this into 
        block manager with level $storageLevel")
        dataRead.rewind()
      }
    val= serializerManager
        .dataDeserializeStream(
          blockId,
          new ChunkedByteBuffer(dataRead).toInputStream())(elementClassTag)
        .asInstanceOf[Iterator[T]]
    
    def compute(split: Partition, context: TaskContext): Iterator[T]
    功能: 计算分区数据
    输入参数:
    	split	分区
    	context	任务上下文
    1. 参数校验
    assertValid()
    2. 获取相关参数
    val hadoopConf = broadcastedHadoopConf.value
    val blockManager = SparkEnv.get.blockManager
    val serializerManager = SparkEnv.get.serializerManager
    val partition = split.asInstanceOf[WriteAheadLogBackedBlockRDDPartition]
    val blockId = partition.blockId
    val= if (partition.isBlockIdValid) {
      getBlockFromBlockManager().getOrElse { getBlockFromWriteAheadLog() }
    } else {
      getBlockFromWriteAheadLog()
    }
    
    def getPreferredLocations(split: Partition): Seq[String] 
    功能: 获取最佳位置列表
    获取分区的最佳位置,如果在数据块管理器中存在,则返回数据块的位置,负责就使用@FileBasedWriteAheadLogSegment,返回HDFS相对的文件段的位置.
    1. 获取数据块的位置
    val partition = split.asInstanceOf[WriteAheadLogBackedBlockRDDPartition]
    val blockLocations = if (partition.isBlockIdValid) {
      getBlockIdLocations().get(partition.blockId)
    } else {
      None
    }
    val= blockLocations.getOrElse {
      partition.walRecordHandle match {
        case fileSegment: FileBasedWriteAheadLogSegment =>
          try {
            HdfsUtils.getFileSegmentLocations(
              fileSegment.path, fileSegment.offset, fileSegment.length, hadoopConfig)
          } catch {
            case NonFatal(e) =>
              logError("Error getting WAL file segment locations", e)
              Seq.empty
          }
        case _ =>
          Seq.empty
      }
    }
}
```