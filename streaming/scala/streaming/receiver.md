1. [BlockGeneratorListener.scala](# BlockGeneratorListener)
2. [RateLimiter.scala](# RateLimiter)
3. [ReceivedBlock.scala](# ReceivedBlock)
4. [ReceivedBlockHandler.scala](# ReceivedBlockHandler)
5. [Receiver.scala](# Receiver)
6. [ReceiverMessage.scala](# ReceiverMessage)
7. [ReceiverSupervisor.scala](# ReceiverSupervisor)
8. [ReceiverSupervisorImpl.scala](# ReceiverSupervisorImpl)

---

#### BlockGeneratorListener

```scala
private[streaming] trait BlockGeneratorListener {
    介绍: 数据块监听器,用于兼容数据块生成器@BlockGenerator
    操作集:
    def onAddData(data: Any, metadata: Any): Unit
    功能: 添加数据(数据和元数据)
    在数据添加到数据块生成器@BlockGenerator之后调用,数据的添加和调用使用数据块生成器同步,数据块生成器等待激活数据的添加和回调完成,用于更新数据项成功缓存的元数据很有效,当数据块生成之后元数据就变得有效了.任何长的数据块操作会影响通量.
    
    def onGenerateBlock(blockId: StreamBlockId): Unit
    功能: 由数据块生成生成新的数据块，数据块生成器和这个调用使用数据同步和相关回调同步，数据添加的时候等待数据块生成和回调的完成。对于更新元数据是有效的，指定的元数据会在数据块存储成功之后有效。任何长数据块操作都会伤害通量。
    
    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit
    功能: 新数据块准备push的时候调用,调用者在这个方法中需要将数据块存入spark中.内部使用单线程调用,没有和其他调用同步,假设这里使用长数据块操作是合法的.
    
    def onError(message: String, throwable: Throwable): Unit
    功能: 错误处理,当数据块生成器发生错误的时候调用.
}
```

```scala
private[streaming] class BlockGenerator(
    listener: BlockGeneratorListener,
    receiverId: Int,
    conf: SparkConf,
    clock: Clock = new SystemClock()
  ) extends RateLimiter(conf) with Logging {
    介绍: 数据块生成器,产生批量对象,这些对象由@Receiver接受,按照指定时间间隔将对象放到合适的命名数据块中.这个类开启了两个线程,一个周期性的启动一个新的批次,且将之前批次的数据块.另一个线程将数据块放置到数据块管理器中.
    注意: 不要在接收器中之间实例化@BlockGenerator,使用@ReceiverSupervisor.createBlockGenerator实例化.
    构造器参数:
    	listener	数据块生成监听器
    	receiverId	接收器编号
    	clock	系统时钟
    内部类:
    private case class Block(id: StreamBlockId, buffer: ArrayBuffer[Any])
    介绍: 数据块
    构造器参数:
    id	流式数据块编号
    buffer	数据缓冲区
    
    private object GeneratorState extends Enumeration {
        介绍: 生成状态
        type GeneratorState = Value
        val Initialized, Active, 
        	StoppedAddingData, 
        	StoppedGeneratingBlocks, 
        	StoppedAll = Value
        生成状态列表:
        Initialized	初始化状态
        Active	激活状态
        StoppedAddingData	停止添加状态
        StoppedGeneratingBlocks	停止生成数据块状态
        StoppedAll	停止所有状态
    }
    属性:
    #name @blockIntervalMs = conf.getTimeAsMs("spark.streaming.blockInterval", "200ms")
    	数据块间隔时间
    #name @blockIntervalTimer	数据块时间间隔管理器
    val= new RecurringTimer(
        clock, blockIntervalMs, updateCurrentBuffer, "BlockGenerator")
    #name @blockQueueSize = conf.getInt("spark.streaming.blockQueueSize", 10)
    	数据块队列大小
    #name @blocksForPushing = new ArrayBlockingQueue[Block](blockQueueSize)
    	需要添加的数据块队列
    #name @blockPushingThread	数据块添加线程
    val= new Thread() { override def run(): Unit = keepPushingBlocks() }
    #name @currentBuffer = new ArrayBuffer[Any]	volatile	当前缓冲区
    #name @state = Initialized	volatile	生成器状态	
    操作集:
    def start(): Unit
    功能: 启动数据块生成和添加线程
    synchronized {
        if (state == Initialized) {
          state = Active
          blockIntervalTimer.start()
          blockPushingThread.start()
          logInfo("Started BlockGenerator")
        } else {
          throw new SparkException(
            s"Cannot start BlockGenerator as its not in 
            the Initialized state [state = $state]")
        }
      }
    
    def stop(): Unit 
    功能: 安装正确的顺序停止所有
    1. 停止添加数据到当前缓冲区
    synchronized {
      if (state == Active) {
        state = StoppedAddingData
      } else {
        logWarning(s"Cannot stop BlockGenerator as its not in the 
        Active state [state = $state]")
        return
      }
    }
    2. 停止生成数据块
    logInfo("Stopping BlockGenerator")
    blockIntervalTimer.stop(interruptTimer = false)
    synchronized { state = StoppedGeneratingBlocks }
    3. 等待队列中的需要添加的数据块排除成功
    logInfo("Waiting for block pushing thread to terminate")
    blockPushingThread.join()
    synchronized { state = StoppedAll }
    logInfo("Stopped BlockGenerator")
    
    def addData(data: Any): Unit 
    功能: 添加单个数据到缓冲区中
    if (state == Active) {
      waitToPush()
      synchronized {
        if (state == Active) {
          currentBuffer += data
        } else {
          throw new SparkException(
            "Cannot add data as BlockGenerator has not been started or has been stopped")
        }
      }
    } else {
      throw new SparkException(
        "Cannot add data as BlockGenerator has not been started or has been stopped")
    }
    
    def addDataWithCallback(data: Any, metadata: Any): Unit 
    功能: 添加数据到缓冲区中,缓冲数据之后,调用@BlockGeneratorListener.onAddData处理数据
    if (state == Active) {
      waitToPush()
      synchronized {
        if (state == Active) {
          currentBuffer += data
          listener.onAddData(data, metadata)
        } else {
          throw new SparkException(
            "Cannot add data as BlockGenerator has not been started or has been stopped")
        }
      }
    } else {
      throw new SparkException(
        "Cannot add data as BlockGenerator has not been started or has been stopped")
    }
    
    def addMultipleDataWithCallback(dataIterator: Iterator[Any], metadata: Any): Unit
    功能: 将多个数据添加到缓冲器中,处理完毕之后调用@BlockGeneratorListener.onAddData处理数据,注意到所有数据都会自动添加到缓冲区,且保证在一个数据块中.
    if (state == Active) {
      val tempBuffer = new ArrayBuffer[Any]
      dataIterator.foreach { data =>
        waitToPush()
        tempBuffer += data
      }
      synchronized {
        if (state == Active) {
          currentBuffer ++= tempBuffer
          listener.onAddData(tempBuffer, metadata)
        } else {
          throw new SparkException(
            "Cannot add data as BlockGenerator has not been started or has been stopped")
        }
      }
    } else {
      throw new SparkException(
        "Cannot add data as BlockGenerator has not been started or has been stopped")
    }
    
    def isActive(): Boolean = state == Active
    def isStopped(): Boolean = state == StoppedAll
    功能: 确定当前是否处于激活/停止状态
    
    def updateCurrentBuffer(time: Long): Unit
    功能: 更新记录添加的缓冲区
    try {
      var newBlock: Block = null
      synchronized {
        if (currentBuffer.nonEmpty) {
          val newBlockBuffer = currentBuffer
          currentBuffer = new ArrayBuffer[Any]
          val blockId = StreamBlockId(receiverId, time - blockIntervalMs)
          listener.onGenerateBlock(blockId)
          newBlock = new Block(blockId, newBlockBuffer)
        }
      }
      if (newBlock != null) {
        blocksForPushing.put(newBlock)  // put is blocking when queue is full
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block updating timer thread was interrupted")
      case e: Exception =>
        reportError("Error in block updating thread", e)
    }
    
    def keepPushingBlocks(): Unit
    功能: 保持将数据块送入数据块管理器中@BlockManager
    logInfo("Started block pushing thread")
    def areBlocksBeingGenerated: Boolean = synchronized {
      state != StoppedGeneratingBlocks
    }
    try {
      while (areBlocksBeingGenerated) {
        Option(blocksForPushing.poll(10, TimeUnit.MILLISECONDS)) match {
          case Some(block) => pushBlock(block)
          case None =>
        }
      }
      logInfo("Pushing out the last " + blocksForPushing.size() + " blocks")
      while (!blocksForPushing.isEmpty) {
        val block = blocksForPushing.take()
        logDebug(s"Pushing block $block")
        pushBlock(block)
        logInfo("Blocks left to push " + blocksForPushing.size())
      }
      logInfo("Stopped block pushing thread")
    } catch {
      case ie: InterruptedException =>
        logInfo("Block pushing thread was interrupted")
      case e: Exception =>
        reportError("Error in block pushing thread", e)
    }
    
    def reportError(message: String, t: Throwable): Unit 
    功能: 汇报错误信息
    logError(message, t)
    listener.onError(message, t)
    
    def pushBlock(block: Block): Unit 
    功能: 添加数据块
    listener.onPushBlock(block.id, block.buffer)
    logInfo("Pushed block " + block.id)
}
```

#### RateLimiter

```scala
private[receiver] abstract class RateLimiter(conf: SparkConf) extends Logging {
    介绍: 提供@waitToPush()方法限制接收器消费数据的比例(速率)
    等待到push的方法如果太多的数据快速填充的时候会阻塞线程,且仅在新消息添加的时候才会返回.这里假定同一个时刻只有一条信息被消费.
    spark配置@spark.streaming.receiver.maxRate给定了每秒每个接收器最大消息消费量.
    属性:
    #name @maxRateLimit = conf.getLong("spark.streaming.receiver.maxRate", Long.MaxValue)
    	最大比例限制值
    #name @rateLimiter = GuavaRateLimiter.create(getInitialRateLimit().toDouble)	
    	比例限制器
    操作集:
    def waitToPush(): Unit
    功能: 等待数据的添加
    rateLimiter.acquire()
    
    def getCurrentLimit: Long = rateLimiter.getRate.toLong
    功能: 获取当前界限值
    
    def updateRate(newRate: Long): Unit
    功能: 更新比例值
    if (newRate > 0) {
      if (maxRateLimit > 0) {
        rateLimiter.setRate(newRate.min(maxRateLimit))
      } else {
        rateLimiter.setRate(newRate)
      }
    }
    
    def getInitialRateLimit(): Long
    功能: 获取初始比例值
    val= math.min(conf.getLong(
        "spark.streaming.backpressure.initialRate", maxRateLimit), maxRateLimit)
}
```

#### ReceivedBlock

```scala
private[streaming] sealed trait ReceivedBlock
介绍: 接受的数据块

private[streaming] case class ArrayBufferBlock(arrayBuffer: ArrayBuffer[_]) 
extends ReceivedBlock
介绍: 数组形式的接收数据块

private[streaming] case class IteratorBlock(iterator: Iterator[_]) extends ReceivedBlock
介绍: 迭代器形式的接收数据块

private[streaming] case class ByteBufferBlock(byteBuffer: ByteBuffer) 
extends ReceivedBlock
介绍: 字节缓冲区形式的接受数据块
```

#### ReceivedBlockHandler

```scala
private[streaming] trait ReceivedBlockStoreResult {
    介绍: 接收数据块的存储结果
    def blockId: StreamBlockId
    功能: 获取流式数据块编号
    def numRecords: Option[Long]
    功能: 获取数据块中记录数量
}
```

```scala
private[streaming] trait ReceivedBlockHandler {
    介绍: 接受数据块处理器
    def storeBlock(
        blockId: StreamBlockId, receivedBlock: ReceivedBlock): ReceivedBlockStoreResult
    功能: 使用给定的数据块id存储接收的数据块,并返回相关元数据
    
    def cleanupOldBlocks(threshTime: Long): Unit
    功能: 清除指定时间@threshTime之前的老数据块
}

private[streaming] case class BlockManagerBasedStoreResult(
      blockId: StreamBlockId, numRecords: Option[Long])
extends ReceivedBlockStoreResult
介绍: 基于数据块管理器的存储结果
	存储与数据块相关的元数据信息
```

```scala
private[streaming] class BlockManagerBasedBlockHandler(
    blockManager: BlockManager, storageLevel: StorageLevel)
extends ReceivedBlockHandler with Logging {
    介绍: 基于数据块管理器的数据块处理器
    构造器参数:
    	blockManager	数据块管理器
    	storageLevel	存储等级
    操作集:
    def storeBlock(blockId: StreamBlockId, block: ReceivedBlock):
    	ReceivedBlockStoreResult 
    功能: 存储指定数据块
    var numRecords: Option[Long] = None
    val putSucceeded: Boolean = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        numRecords = Some(arrayBuffer.size.toLong)
        blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel,
          tellMaster = true)
      case IteratorBlock(iterator) =>
        val countIterator = new CountingIterator(iterator)
        val putResult = blockManager.putIterator(blockId, countIterator, storageLevel,
          tellMaster = true)
        numRecords = countIterator.count
        putResult
      case ByteBufferBlock(byteBuffer) =>
        blockManager.putBytes(
          blockId, new ChunkedByteBuffer(
              byteBuffer.duplicate()), storageLevel, tellMaster = true)
      case o =>
        throw new SparkException(
          s"Could not store $blockId to block manager, unexpected block 
          type ${o.getClass.getName}")
    }
    if (!putSucceeded) {
      throw new SparkException(
        s"Could not store $blockId to block manager with storage level $storageLevel")
    }
    val= BlockManagerBasedStoreResult(blockId, numRecords)
    
    def cleanupOldBlocks(threshTime: Long): Unit={}
    功能: 清除旧的数据块,这里不做处理,因为数据块管理器中的数据块被@DStream清理了
}
```

```scala
private[streaming] case class WriteAheadLogBasedStoreResult(
    blockId: StreamBlockId,
    numRecords: Option[Long],
    walRecordHandle: WriteAheadLogRecordHandle
  ) extends ReceivedBlockStoreResult
介绍: 基于WAL的存储结果
构造器参数:
	blockId	数据块编号
	numRecords	记录数量
	walRecordHandle	wal记录处理器
```

```scala
private[streaming] class WriteAheadLogBasedBlockHandler(
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    streamId: Int,
    storageLevel: StorageLevel,
    conf: SparkConf,
    hadoopConf: Configuration,
    checkpointDir: String,
    clock: Clock = new SystemClock
) extends ReceivedBlockHandler with Logging {
    介绍: 基于WAL的数据块处理器
    构造器参数:
        blockManager	数据块管理器
        serializerManager	序列化管理器
        streamId	stream编号
        storageLevel	存储等级
        conf	spark配置
        hadoopConf	hadoop配置
        checkpointDir	检查点目录
        clock	系统时钟
    属性:
    #name @blockStoreTimeout	数据块存储超时时间
    val= conf.getInt("spark.streaming.receiver.blockStoreTimeout", 30).seconds
    #name @effectiveStorageLevel	有效的存储等级
    val= {
        if (storageLevel.deserialized) {
          logWarning(s"Storage level serialization ${storageLevel.deserialized} 
          is not supported when" +
            s" write ahead log is enabled, change to serialization false")
        }
        if (storageLevel.replication > 1) {
          logWarning(s"Storage level replication ${storageLevel.replication} 
          is unnecessary when " +
            s"write ahead log is enabled, change to replication 1")
        }
        StorageLevel(
            storageLevel.useDisk, storageLevel.useMemory, 
            storageLevel.useOffHeap, false, 1)
      }
    #name @writeAheadLog	wal
    val= WriteAheadLogUtils.createLogForReceiver(
        conf, checkpointDirToLogDir(checkpointDir, streamId), hadoopConf)
    #name @executionContext	执行线程池
    val= ExecutionContext.fromExecutorService(
        ThreadUtils.newDaemonFixedThreadPool(2, this.getClass.getSimpleName))
    初始化操作:
    if (storageLevel != effectiveStorageLevel) {
        logWarning(s"User defined storage level $storageLevel is 
        changed to effective storage level " +
          s"$effectiveStorageLevel when write ahead log is enabled")
      }
    功能: 校验存储等级
    
    操作集:
    def storeBlock(blockId: StreamBlockId, block: ReceivedBlock):
    	ReceivedBlockStoreResult
    功能: 存储指定数据块
    1. 序列化数据块以便可以插入
    var numRecords = Option.empty[Long]
    val serializedBlock = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        numRecords = Some(arrayBuffer.size.toLong)
        serializerManager.dataSerialize(blockId, arrayBuffer.iterator)
      case IteratorBlock(iterator) =>
        val countIterator = new CountingIterator(iterator)
        val serializedBlock = serializerManager.dataSerialize(blockId, countIterator)
        numRecords = countIterator.count
        serializedBlock
      case ByteBufferBlock(byteBuffer) =>
        new ChunkedByteBuffer(byteBuffer.duplicate())
      case _ =>
        throw new Exception(s"Could not push $blockId to block manager, 
        unexpected block type")
    }
    2. 将数据块存储到数据块管理器中
    val storeInBlockManagerFuture = Future {
      val putSucceeded = blockManager.putBytes(
        blockId,
        serializedBlock,
        effectiveStorageLevel,
        tellMaster = true)
      if (!putSucceeded) {
        throw new SparkException(
          s"Could not store $blockId to block manager with storage level $storageLevel")
      }
    }
    3. 存储数据块到wal
    val storeInWriteAheadLogFuture = Future {
      writeAheadLog.write(serializedBlock.toByteBuffer, clock.getTimeMillis())
    }
    4. 合并线程任务,等待两者的完成,返回wal记录处理器
    val combinedFuture =
    	storeInBlockManagerFuture.zip(storeInWriteAheadLogFuture).map(_._2)
    val walRecordHandle = ThreadUtils.awaitResult(combinedFuture, blockStoreTimeout)
    val= WriteAheadLogBasedStoreResult(blockId, numRecords, walRecordHandle)
    
    def cleanupOldBlocks(threshTime: Long): Unit
    功能: 清除早于指定时间的数据块
    writeAheadLog.clean(threshTime, false)
    
    def stop(): Unit
    功能: 停止处理器
    writeAheadLog.close()
    executionContext.shutdown()
}
```

```scala
private[streaming] object WriteAheadLogBasedBlockHandler {
  	介绍: 基于wal的数据块处理器
  def checkpointDirToLogDir(checkpointDir: String, streamId: Int): String = {
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString
  }
  功能: 获取检查点目录的日志目录
}
```

```scala
private[streaming] class CountingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
    介绍: 计数迭代器
    属性:
    #name @_count = 0	计数值
    操作集:
    def isFullyConsumed: Boolean = !iterator.hasNext
    功能: 确定数据是否全部消费
    
    def hasNext(): Boolean = iterator.hasNext
    功能: 确定是否还有未消费数据
    
    def count(): Option[Long]
    功能: 获取计数值
    val= if (isFullyConsumed) Some(_count) else None
    
    def next(): T 
    功能: 获取当前迭代器的值
    _count += 1
    val= iterator.next()
}
```

#### Receiver

```markdown
介绍:
 	这个抽象类代表可以运行在worker节点上,用于接收外部数据的类.消费接收器可以使用@onStart()或者@onStop进行启动和停止，@onStart 顶一个创建必要的步骤，使之开始接收数据。@onStop方法定位停止接收数据的方法。当重启接收器或者完全停止接收的时候可能引发异常。
 	一个scala中的消费者接收器可以如下定义
 	{{{
    class MyReceiver(storageLevel: StorageLevel) 
    	extends NetworkReceiver[String](storageLevel){
        	def onStart() {
        		//  启动元素用于接收数据(开启线程,启动socket端口)
        		// 必须启动新的先出,因为这个方法是非阻塞的
        		// 调用store(),将线程接收到的数据存储到spark内存中.
        		// 调用stop(...), restart(...) or reportError(...)进行错误处理
        		// 参考相应的参考文档
        	}
            def onStop() {
            	// 清除元素,用于停止接收器(停止线程,关闭socket等等)
            }
    	} 
 	}}}
 	java的思路类似
```

```scala
@DeveloperApi
abstract class Receiver[T](val storageLevel: StorageLevel) extends Serializable {
    介绍: 接收器
    构造器参数:
    storageLevel	存储器等级
    属性:
    #name @id: Int = -1	stream唯一标识符
    #name @_supervisor: ReceiverSupervisor = null	接收器管理器
    操作集:
    def onStart(): Unit
    功能: 接收器启动
    这个方法在接收器启动之后被系统调用,函数必须要初始化所有用于接收数据的资源(线程,缓冲区).函数必须是非阻塞的,所以接收数据必须在额外的线程中发生.否则会导致阻塞.接收的数据可以使用spark调用@store(data)存储起来.
    如果这里启动的先出有错误的话,可以使用下述配置
    1. `reportError(...)`  调用这个去汇报错误给驱动器,接收数据会不中断的执行
    2. `stop(...)` 调用这个停止接收数据,可以调用@onStop清理所有分配的资源
    3. `restart(...)`可以重启接收器,这个会立刻调用@onStop,然后延时调用@onStart
    
    def onStop(): Unit
    功能: 停止接收器
    系统调用使得接收器停止,所有资源都会被清理
    
    def preferredLocation: Option[String] = None
    功能: 获取接收器的最佳位置
    
    def store(dataItem: T): Unit
    功能: 存储单个接收数据到spark内存中.单个数据在放到spark内存之前需要进行聚合.
    supervisor.pushSingle(dataItem)
    
    def store(dataBuffer: ArrayBuffer[T]): Unit
    功能: 存储一组数据到spark内存中
    supervisor.pushArrayBuffer(dataBuffer, None, None)
    
    def store(dataBuffer: ArrayBuffer[T], metadata: Any): Unit
    功能: 存储接受数据的数组,使之以数据块的形式存储.与这个数据块相关元数据会被存储在相应@InputDStream中
    supervisor.pushArrayBuffer(dataBuffer, Some(metadata), None)
    
    def store(dataIterator: java.util.Iterator[T], metadata: Any): Unit
    功能: 同上,java使用的版本
    
    def store(dataIterator: Iterator[T]): Unit
    功能: 以迭代器的形式存储接收的数据
    supervisor.pushIterator(dataIterator, None, None)
    
    def store(dataIterator: java.util.Iterator[T]): Unit
    功能: 同上,java使用的版本
    
    def store(dataIterator: Iterator[T], metadata: Any): Unit
    功能: 存储接受数据的数组,使之以数据块的形式存储.与这个数据块相关元数据会被存储在相应@InputDStream中
    supervisor.pushIterator(dataIterator, Some(metadata), None)
    
    def store(bytes: ByteBuffer): Unit
    功能: 以字节缓冲的形式将数据存储到spark内存中
    supervisor.pushBytes(bytes, None, None)
    
    def store(bytes: ByteBuffer, metadata: Any): Unit 
    功能:  以字节缓冲的形式将数据存储到spark内存中,与这个数据块相关元数据会被存储在相应@InputDStream中
    supervisor.pushBytes(bytes, Some(metadata), None)
    
    def reportError(message: String, throwable: Throwable): Unit
    功能: 汇报错误信息
    supervisor.reportError(message, throwable)
    
    def restart(message: String): Unit
    功能: 重启接收器
    这个方法会调用@onStop和@onStart,操作是异步的.且处于后台线程中.停止和再次启动的延时可以在@spark.streaming.receiverRestartDelay中配置,@message会被汇报到驱动器上.
    supervisor.restartReceiver(message)
    
    def restart(message: String, error: Throwable): Unit
    功能: 同上,error信息也会报给驱动器
    supervisor.restartReceiver(message, Some(error))
    
    def restart(message: String, error: Throwable, millisecond: Int): Unit
    功能: 同上,但是这里指定了起停延时@millisecond
    supervisor.restartReceiver(message, Some(error), millisecond)
    
    def stop(message: String): Unit
    功能: 完全停止接收器
    supervisor.stop(message, None)
    
    def stop(message: String, error: Throwable): Unit
    功能: 由于某个错误完全停止接收器
    supervisor.stop(message, Some(error))
    
    def isStarted(): Boolean
    功能: 检查接收器是否已经开始
    supervisor.isReceiverStarted()
    
    def isStopped(): Boolean
    功能: 检查接收器是否停止了
    val= supervisor.isReceiverStopped()
    
    def streamId: Int = id
    功能: 获取stream唯一标识符
    
    def setReceiverId(_id: Int): Unit = id = _id
    功能: 设置接收器唯一标识符
    
    def attachSupervisor(exec: ReceiverSupervisor): Unit 
    功能: 连接这个接收器的网络接收器执行器
    assert(_supervisor == null)
    _supervisor = exec
    
    def supervisor: ReceiverSupervisor
    功能: 获取连接的管理器
    assert(_supervisor != null,
      "A ReceiverSupervisor has not been attached to the receiver yet. 
      Maybe you are starting " +
        "some computation in the receiver before the Receiver.
        onStart() has been called.")
    val= _supervisor
}
```

#### ReceiverMessage

```scala
private[streaming] sealed trait ReceiverMessage extends Serializable
介绍: 接收器消息

private[streaming] object StopReceiver extends ReceiverMessage
介绍: 停止接收器的消息

private[streaming] case class CleanupOldBlocks(threshTime: Time) extends ReceiverMessage
介绍: 清除指定时间之前的数据块消息

private[streaming] case class UpdateRateLimit(elementsPerSecond: Long)
extends ReceiverMessage
介绍: 更新比例限制值消息
```

#### ReceiverSupervisor

```scala
private[streaming] abstract class ReceiverSupervisor(
    receiver: Receiver[_],
    conf: SparkConf
) extends Logging {
    介绍: 接收器管理器,这个抽象类用于管理worker中的接收器,提供必要的接口,用于处理接收器接收到的数据.
    构造器参数:
    receiver	接收器
    conf	spark配置
    属性:
    #name @streamId = receiver.streamId	stream唯一标识符
    #name @futureExecutionContext	执行线程池
    val= ExecutionContext.fromExecutorService(
        ThreadUtils.newDaemonCachedThreadPool("receiver-supervisor-future", 128))
    #name @stopLatch = new CountDownLatch(1)	是否管理器停止(只有一个信号量)
    #name @defaultRestartDelay	默认重启延时
    val= conf.getInt("spark.streaming.receiverRestartDelay", 2000)
    #name @stoppingError: Throwable = null	volatile	接收器停止时发生的异常
    #name @receiverState = Initialized	volatile	接收器状态
    初始化操作:
    receiver.attachSupervisor(this)
    功能: 连接管理器到接收器
    操作集:
    def pushSingle(data: Any): Unit
    功能: 将单个数据元素送入后台存储中
    
    def pushBytes(
      bytes: ByteBuffer,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): Unit
    功能: 以数据块的形式存储接收字节到spark内存中
    
    def pushIterator(
      iterator: Iterator[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): Unit
    功能: 存储迭代器中的数据到spark内存中
    
    def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    ): Unit
    功能: 以数组的形式将数据存储到spark内存中(数据块的形式)
    
    def createBlockGenerator(blockGeneratorListener: BlockGeneratorListener):
    	BlockGenerator
    功能: 创建数据块生成器
    
    def reportError(message: String, throwable: Throwable): Unit
    功能: 汇报错误给驱动器
    
    def onStart(): Unit = { }
    功能: 启动接收器管理器
    注意这个必须要在接收器启动@receiver.onStart() 之前调用,从而确保数据块生成器在发送数据之前启动
    
    def onStop(message: String, error: Option[Throwable]): Unit = { }
    功能: 关闭接收器管理器,必须在接收器关闭之后关闭,确保数据块生成器可以被清除
    
    def onReceiverStart(): Boolean
    功能: 确定接收器是否启动
    
    def onReceiverStop(message: String, error: Option[Throwable]): Unit = { }
    功能: 确定接收器是否关闭
    
    def start(): Unit
    功能: 启动管理器
    onStart()
    startReceiver()
    
    def stop(message: String, error: Option[Throwable]): Unit
    功能: 关闭接收器管理器
    stoppingError = error.orNull
    stopReceiver(message, error)
    onStop(message, error)
    futureExecutionContext.shutdownNow()
    stopLatch.countDown()
    
    def startReceiver(): Unit
    功能: 启动接收器
    synchronized {
        try {
          if (onReceiverStart()) {
            logInfo(s"Starting receiver $streamId")
            receiverState = Started
            receiver.onStart()
            logInfo(s"Called receiver $streamId onStart")
          } else {
            stop("Registered unsuccessfully because Driver refused 
            to start receiver " + streamId, None)
          }
        } catch {
          case NonFatal(t) =>
            stop("Error starting receiver " + streamId, Some(t))
        }
    }
    
    def stopReceiver(message: String, error: Option[Throwable]): Unit
    功能: 停止接收器
    synchronized {
        try {
            logInfo("Stopping receiver with message: " + message + ": 
            " + error.getOrElse(""))
            receiverState match {
                case Initialized =>
                logWarning("Skip stopping receiver because it has not yet stared")
                case Started =>
                receiverState = Stopped
                receiver.onStop()
                logInfo("Called receiver onStop")
                onReceiverStop(message, error)
                case Stopped =>
                logWarning("Receiver has been stopped")
            }
        } catch {
            case NonFatal(t) =>
            logError(s"Error stopping receiver $streamId ${Utils.exceptionString(t)}")
        }
    }
    
    def restartReceiver(message: String, error: Option[Throwable] = None): Unit
    功能: 重启接收器
    restartReceiver(message, error, defaultRestartDelay)
    
    def restartReceiver(message: String, error: Option[Throwable], delay: Int): Unit
    功能: 延时重启接收器
    Future {
      logWarning("Restarting receiver with delay " + delay + " ms: " + message,
        error.getOrElse(null))
      stopReceiver("Restarting receiver with delay " + delay + "ms: " + message, error)
      logDebug("Sleeping for " + delay)
      Thread.sleep(delay)
      logInfo("Starting receiver again")
      startReceiver()
      logInfo("Receiver started again")
    }(futureExecutionContext)
    
    def isReceiverStarted(): Boolean
    功能: 确定接收器是否开启
    logDebug("state = " + receiverState)
    receiverState == Started
    
    def isReceiverStopped(): Boolean
    功能: 确定接收器是否关闭
    logDebug("state = " + receiverState)
    receiverState == Stopped
    
    def awaitTermination(): Unit
    功能: 等待管理器停止
    logInfo("Waiting for receiver to be stopped")
    stopLatch.await()
    if (stoppingError != null) {
      logError("Stopped receiver with error: " + stoppingError)
      throw stoppingError
    } else {
      logInfo("Stopped receiver without error")
    }
}
```

#### ReceiverSupervisorImpl

```scala
private[streaming] class ReceiverSupervisorImpl(
    receiver: Receiver[_],
    env: SparkEnv,
    hadoopConf: Configuration,
    checkpointDirOption: Option[String]
  ) extends ReceiverSupervisor(receiver, env.conf) with Logging {
    介绍: 这里提供了所有必须的功能,用于处理接收器数据的接受.特别地,数据块生成器用于将接收的数据流分割成数据块.
    属性:
    #name @host = SparkEnv.get.blockManager.blockManagerId.host	主机号
    #name @executorId = SparkEnv.get.blockManager.blockManagerId.executorId	执行器编号
    #name @receivedBlockHandler: ReceivedBlockHandler 	接受数据块处理器
    val= {
        if (WriteAheadLogUtils.enableReceiverLog(env.conf)) {
          if (checkpointDirOption.isEmpty) {
            throw new SparkException(
              "Cannot enable receiver write-ahead log without checkpoint 
              directory set. " +
                "Please use streamingContext.checkpoint() to set the checkpoint
                directory. " +
                "See documentation for more details.")
          }
          new WriteAheadLogBasedBlockHandler(
              env.blockManager, env.serializerManager, receiver.streamId,
              receiver.storageLevel, env.conf, hadoopConf, checkpointDirOption.get)
        } else {
          new BlockManagerBasedBlockHandler(env.blockManager, receiver.storageLevel)
        }
      }
    #name @trackerEndpoint	远端rpc端点，用于接收器定位
    val= RpcUtils.makeDriverRef("ReceiverTracker", env.conf, env.rpcEnv)
    #name @endpoint	RPC端点，用于接受来自驱动器@ReceiverTracker的消息
    val= env.rpcEnv.setupEndpoint(
    "Receiver-" + streamId + "-" + System.currentTimeMillis(), 
        new ThreadSafeRpcEndpoint {
      override val rpcEnv: RpcEnv = env.rpcEnv
      override def receive: PartialFunction[Any, Unit] = {
        case StopReceiver =>
          logInfo("Received stop signal")
          ReceiverSupervisorImpl.this.stop("Stopped by driver", None)
        case CleanupOldBlocks(threshTime) =>
          logDebug("Received delete old batch signal")
          cleanupOldBlocks(threshTime)
        case UpdateRateLimit(eps) =>
          logInfo(s"Received a new rate limit: $eps.")
          registeredBlockGenerators.asScala.foreach { bg =>
            bg.updateRate(eps)
          }
      }
    })
    #name @newBlockId = new AtomicLong(System.currentTimeMillis())	新的数据块编号
    #name @registeredBlockGenerators = new ConcurrentLinkedQueue[BlockGenerator]()
    	注册的数据块生成器队列
    #name @defaultBlockGenerator = createBlockGenerator(defaultBlockGeneratorListener)
    	默认数据块生成器
    #name @defaultBlockGeneratorListener 默认数据块生成器监听器
    val= new BlockGeneratorListener {
        def onAddData(data: Any, metadata: Any): Unit = { }
        def onGenerateBlock(blockId: StreamBlockId): Unit = { }
        def onError(message: String, throwable: Throwable): Unit = {
          reportError(message, throwable)
        }
        def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]): Unit = {
          pushArrayBuffer(arrayBuffer, None, Some(blockId))
        }
    }
    操作集:
    def getCurrentRateLimit: Long = defaultBlockGenerator.getCurrentLimit
    功能: 获取当前比例界限值
    
    def pushSingle(data: Any): Unit
    功能: 存放单个接受数据的记录
    defaultBlockGenerator.addData(data)
    
    def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ): Unit
    功能: 添加接受的数组元素,以数据块的形式存储到spark内存中
    pushAndReportBlock(ArrayBufferBlock(arrayBuffer), metadataOption, blockIdOption)
    
    def pushIterator(
      iterator: Iterator[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ): Unit
    功能: 以迭代器的形式将数据存储到内存中
    pushAndReportBlock(IteratorBlock(iterator), metadataOption, blockIdOption)
    
    def pushBytes(
      bytes: ByteBuffer,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ): Unit
    功能: 以数据块的形式将字节存储到spark内存中
    pushAndReportBlock(ByteBufferBlock(bytes), metadataOption, blockIdOption)
    
    def pushAndReportBlock(
      receivedBlock: ReceivedBlock,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ): Unit
    功能: 存储数据块并汇报给驱动器
    val blockId = blockIdOption.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)
    logDebug(s"Pushed block $blockId in ${(System.currentTimeMillis - time)} ms")
    val numRecords = blockStoreResult.numRecords
    val blockInfo = ReceivedBlockInfo(
        streamId, numRecords, metadataOption, blockStoreResult)
    if (!trackerEndpoint.askSync[Boolean](AddBlock(blockInfo))) {
      throw new SparkException("Failed to add block to receiver tracker.")
    }
    logDebug(s"Reported block $blockId")
    
    def reportError(message: String, error: Throwable): Unit
    功能: 汇报错误给接收器定位器
    val errorString = Option(error).map(Throwables.getStackTraceAsString).getOrElse("")
    trackerEndpoint.send(ReportError(streamId, message, errorString))
    logWarning("Reported error " + message + " - " + error)
    
    def onStart(): Unit
    功能: 管理器启动
    registeredBlockGenerators.asScala.foreach { _.start() }
    
    def onStop(message: String, error: Option[Throwable]): Unit 
    功能: 管理器停止处理
    receivedBlockHandler match {
      case handler: WriteAheadLogBasedBlockHandler =>
        // Write ahead log should be closed.
        handler.stop()
      case _ =>
    }
    registeredBlockGenerators.asScala.foreach { _.stop() }
    env.rpcEnv.stop(endpoint)
    
    def onReceiverStart(): Boolean
    功能: 启动接收器
    val msg = RegisterReceiver(
      streamId, receiver.getClass.getSimpleName, host, executorId, endpoint)
    trackerEndpoint.askSync[Boolean](msg)
    
    def onReceiverStop(message: String, error: Option[Throwable]): Unit
    功能: 接收器停止
    logInfo("Deregistering receiver " + streamId)
    val errorString = error.map(Throwables.getStackTraceAsString).getOrElse("")
    trackerEndpoint.askSync[Boolean](DeregisterReceiver(streamId, message, errorString))
    logInfo("Stopped receiver " + streamId)
    
    def createBlockGenerator(
      blockGeneratorListener: BlockGeneratorListener): BlockGenerator
    功能: 创建数据块生成器
    val stoppedGenerators = registeredBlockGenerators.asScala.filter{ _.isStopped() }
    stoppedGenerators.foreach(registeredBlockGenerators.remove(_))
    val newBlockGenerator = new BlockGenerator(
        blockGeneratorListener, streamId, env.conf)
    registeredBlockGenerators.add(newBlockGenerator)
    val= newBlockGenerator
    
    def nextBlockId = StreamBlockId(streamId, newBlockId.getAndIncrement)
    功能: 获取流式数据块编号
    
    def cleanupOldBlocks(cleanupThreshTime: Time): Unit
    功能: 清理在指定时间之前的数据块
    logDebug(s"Cleaning up blocks older then $cleanupThreshTime")
    receivedBlockHandler.cleanupOldBlocks(cleanupThreshTime.milliseconds)
}
```

