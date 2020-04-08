## **spark-storage**

---

1.  [MemoryStore.scala](# MemoryStore)
2.  [BlockException.scala](# BlockException)
3.  [BlockId.scala](# BlockId)
4.  [BlockInfoManager.scala](# BlockInfoManager)
5.  [BlockManager.scala](# BlockManager)
6.  [BlockManagerId.scala](# BlockManagerId)
7.  [BlockManagerManagedBuffer.scala](# BlockManagerManagedBuffer)
8.  [BlockManagerMaster.scala](# BlockManagerMaster)
9.  [BlockManagerMasterEndpoint.scala](# BlockManagerMasterEndpoint)
10.  [BlockManagerMasterHeartbeatEndpoint.scala](# BlockManagerMasterHeartbeatEndpoint)
11.  [BlockManagerMessages.scala](# BlockManagerMessages)
12.  [BlockManagerSlaveEndpoint.scala](# BlockManagerSlaveEndpoint)
13.  [BlockManagerSource.scala](# BlockManagerSource)
14.  [BlockNotFoundException.scala](# BlockNotFoundException)
15.  [BlockReplicationPolicy.scala](# BlockReplicationPolicy)
16.  [BlockUpdatedInfo.scala](# BlockUpdatedInfo)
17.  [DiskBlockManager.scala](# DiskBlockManager)
18.  [DiskBlockObjectWriter.scala](# DiskBlockObjectWriter)
19.  [DiskStore.scala](# DiskStore)
20.  [FileSegment.scala](# FileSegment)
21.  [RDDInfo.scala](# RDDInfo)
22.  [ShuffleBlockFetcherIterator.scala](# ShuffleBlockFetcherIterator)
23.  [StorageLevel.scala](# StorageLevel)
24.  [StorageStatus.scala](# StorageStatus)
25.   [StorageUtils.scala](# StorageUtils)
26.  [TopologyMapper.scala](# TopologyMapper)
27.  [基础拓展](# 基础拓展)

---

#### MemoryStore

```scala
private sealed trait MemoryEntry[T]{
    操作集:
    def size: Long
    功能: 获取内存条目的数量
    def memoryMode: MemoryMode
    功能: 获取内存模式
    def classTag: ClassTag[T]
    功能: 获取类标签
}

private case class DeserializedMemoryEntry[T](
    value: Array[T],
    size: Long,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
    介绍: 反序列化内存信息组合
    构造器参数:
    	value	参数列表
    	size	条目数量
    	classTag	类标签
    属性:
    #name @memoryMode: MemoryMode = MemoryMode.ON_HEAP	内存模式
}

private case class SerializedMemoryEntry[T](
    buffer: ChunkedByteBuffer,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
    介绍: 序列化内存信息
    构造器属性:
        buffer	块状字节缓冲区
        memoryMode	内存模式
        classTag	类标签
    操作集:
    def size: Long = buffer.size
    功能: 获取缓冲区大小
}

private[storage] trait BlockEvictionHandler {
    介绍: 数据块回收处理器
    操作集:
    def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
    功能: 对指定数据块@blockId 进行内存回收，返回高效的存储等级
    将数据块的内存回收,可能将其存入磁盘.当内存存储到达上限时,需要释放空间.如果数据处理函数@data 没有放在磁盘,那么就不会被创建.方法调用者必须拥有数据块的写出锁,且这个方法不会释放这个写出锁.
}

private trait MemoryEntryBuilder[T] {
  介绍: 内存信息构建器
  def preciseSize: Long
  功能: 获取信息长度
  def build(): MemoryEntry[T]
  功能: 构建内存信息
}

private trait ValuesHolder[T] {
    介绍: 值容纳器
    操作集:
    def storeValue(value: T): Unit
    功能: 存储指定值@value
    
    def estimatedSize(): Long
    功能: 估计值的大小
    
    def getBuilder(): MemoryEntryBuilder[T]
    功能: 获取构建器
    这个方法调用之后,值容纳器@ValuesHolder 会失去效用.不能够再存储数据,和估量长度了.
}
```

```scala
private class DeserializedValuesHolder[T] (classTag: ClassTag[T]) extends ValuesHolder[T] {
    介绍: 反序列化值的持有者
    构造器参数: classTag	类标签
    属性:
    #name @vector = new SizeTrackingVector[T]()(classTag)	预估值向量表
    #name @arrayValues: Array[T] = null	数组值
    操作集:
    def storeValue(value: T): Unit
    功能: 存储@value 值
    vector += value
    
    def estimatedSize(): Long 
    功能: 预估值
    val= vector.estimateSize()
    
    def getBuilder(): MemoryEntryBuilder[T]
    功能: 获取内存信息构建器
    val= new MemoryEntryBuilder[T] {
        arrayValues = vector.toArray
        vector = null
        override val preciseSize: Long = SizeEstimator.estimate(arrayValues)
        override def build(): MemoryEntry[T] =
          DeserializedMemoryEntry[T](arrayValues, preciseSize, classTag)
  	}
}
```

```scala
private class SerializedValuesHolder[T](
    blockId: BlockId,
    chunkSize: Int,
    classTag: ClassTag[T],
    memoryMode: MemoryMode,
    serializerManager: SerializerManager) extends ValuesHolder[T] {
    介绍: 存储序列化值的持有者
    #name @allocator #Type @ByteBuffer	内存分配器
    val= memoryMode match {
        case MemoryMode.ON_HEAP => ByteBuffer.allocate _
        case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
      }
    #name @redirectableStream = new RedirectableOutputStream	重定向输出流
    #name @bbos = new ChunkedByteBufferOutputStream(chunkSize, allocator)	块状字节缓冲输出流
    #name @serializationStream #Type @SerializationStream	反序列化流
        val autoPick = !blockId.isInstanceOf[StreamBlockId]
        val ser = serializerManager.getSerializer(classTag, autoPick).newInstance()
        val= ser.serializeStream(serializerManager.wrapForCompression(blockId, redirectableStream))
	初始化操作:
    redirectableStream.setOutputStream(bbos)
    功能: 重定向输出流
    操作集:
    def storeValue(value: T): Unit
    功能: 存储值@value
    serializationStream.writeObject(value)(classTag)
    
    def estimatedSize(): Long
    功能: 预估长度
    val= bbos.size
    
    def getBuilder(): MemoryEntryBuilder[T]
    功能: 获取内存信息构建器
    val= new MemoryEntryBuilder[T] {
    serializationStream.close()
    override def preciseSize(): Long = bbos.size
    override def build(): MemoryEntry[T] =
      SerializedMemoryEntry[T](bbos.toChunkedByteBuffer, memoryMode, classTag)
  	}
}
```

```scala
private[storage] class PartiallyUnrolledIterator[T](
    memoryStore: MemoryStore,
    memoryMode: MemoryMode,
    unrollMemory: Long,
    private[this] var unrolled: Iterator[T],
    rest: Iterator[T])
extends Iterator[T] {
    介绍: @MemoryStore.putIteratorAsValues() 失败调用的结果 
    	部分展开迭代器
    构造器参数:
    	memoryStore	内存存储器
    	memoryMode	内存模式
    	unrollMemory	部分展开迭代器的内存占有量
    	unrolled	部分展开迭代器
    	rest	剩余迭代器
    操作集:
    def close(): Unit
    功能: 迭代结束之后的动作,用于释放内存
    if (unrolled != null) {
      releaseUnrollMemory()
    }
    
    def releaseUnrollMemory(): Unit 
    功能: 释放部分展开迭代器的内存
    1. 释放内存
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    2. 重置部分展开迭代器
    unrolled = null
    
    def next(): T 
    功能: 获取下一个元素
    val= if (unrolled == null || !unrolled.hasNext) {
      rest.next()
    } else {
      unrolled.next()
    }
    
    def hasNext: Boolean
    功能: 确认是否含有下一个元素
    val= if (unrolled == null) {
      rest.hasNext
    } else if (!unrolled.hasNext) {
      releaseUnrollMemory()
      rest.hasNext
    } else {
      true
    }
}
```

```scala
private[storage] class RedirectableOutputStream extends OutputStream {
    介绍: 重定向输出流,对普通输出流@OutputStream 的包装,可以重定向到不同的目标地点(sink)
    属性:
    #name @os: OutputStream = _	输出流
    操作集:
    def setOutputStream(s: OutputStream): Unit = { os = s }	
    功能: 设置输出流(负责流的重定向)
    
    def write(b: Int): Unit = os.write(b)
    功能: 写出一个字节
    
    def write(b: Array[Byte]): Unit = os.write(b)
    功能: 写出一个缓冲区
    
    def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    功能: 写出缓冲区的一个范围
    
    def flush(): Unit = os.flush()
    功能: 刷写
    
    def close(): Unit = os.close()
    功能: 关流
}
```

```scala
private[storage] class PartiallySerializedBlock[T](
    memoryStore: MemoryStore,
    serializerManager: SerializerManager,
    blockId: BlockId,
    private val serializationStream: SerializationStream,
    private val redirectableOutputStream: RedirectableOutputStream,
    val unrollMemory: Long,
    memoryMode: MemoryMode,
    bbos: ChunkedByteBufferOutputStream,
    rest: Iterator[T],
    classTag: ClassTag[T]) {
    介绍: @MemoryStore.putIteratorAsBytes() 调用失败时返回
    构造器参数:
        memoryStore	内存存储器
        serializerManager	序列化管理器
        blockId	数据块标识符
        serializationStream	序列化流
        redirectableOutputStream	重定向输出流
        unrollMemory	展开内存大小
        memoryMode	内存模式
        bbos	块状字节缓冲输出流
        rest	剩余迭代器
        classTag	类标签
    属性:
    #name @unrolledBuffer #Type @ChunkedByteBuffer lazy	块状字节缓冲区
    val= {bbos.close()
         bbos.toChunkedByteBuffer
         }
    #name @discarded = false	是否抛弃
    #name @consumed = false	是否消费
    初始化操作:
    Option(TaskContext.get()).foreach { taskContext =>
        taskContext.addTaskCompletionListener[Unit] { _ =>
            unrolledBuffer.dispose()
        }
    }
    功能: 监听任务,当任务完成时,进行收尾工作
    
    操作集:
    def getUnrolledChunkedByteBuffer: ChunkedByteBuffer = unrolledBuffer
    功能: 获取展开的块状字节缓冲区(暴露给测试使用)
    
    def verifyNotConsumedAndNotDiscarded(): Unit
    功能: 校验是否被消费,或者弃用
    if (consumed) {
      throw new IllegalStateException(
        "Can only call one of finishWritingToStream() or valuesIterator() and can only call once.")
    }
    if (discarded) {
      throw new IllegalStateException("Cannot call methods on a discarded PartiallySerializedBlock")
    }
    
    def discard(): Unit
    功能: 弃用数据块,是否内存
    if (!discarded) {
      try {
          // 直接关闭会导致数据写出,所以重定向输出流即可
        redirectableOutputStream.setOutputStream(ByteStreams.nullOutputStream())
        serializationStream.close()
      } finally {
        discarded = true
        unrolledBuffer.dispose()
          // 最后再释放内存
        memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
      }
    }
    
    def finishWritingToStream(os: OutputStream): Unit
    功能: 停止指定输出流@os写出,后边的数据使用序列化写出
    0. 校验当前状态
    verifyNotConsumedAndNotDiscarded()
    1. 修改消费标记位
    consumed = true
    2. 拷贝缓冲数据,并截止写出
    ByteStreams.copy(unrolledBuffer.toInputStream(dispose = true), os)
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)  // 释放展开缓冲区内存
    redirectableOutputStream.setOutputStream(os) // 重定向输出流,停止输出
    3. 将剩余数据写出
    while (rest.hasNext) {
      serializationStream.writeObject(rest.next())(classTag)
    }
    serializationStream.close()
    
    def valuesIterator: PartiallyUnrolledIterator[T]
    功能： 获取部分展开的值的迭代器
    0. 校验参数合法性
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    1. 关闭序列化流
    serializationStream.close()
    2. 获取展开迭代器
    val unrolledIter = serializerManager.dataDeserializeStream(
      blockId, unrolledBuffer.toInputStream(dispose = true))(classTag)
    val= new PartiallyUnrolledIterator(
      memoryStore,
      memoryMode,
      unrollMemory,
      unrolled = unrolledIter,
      rest = rest)
}
```

```scala
private[spark] class MemoryStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager,
    serializerManager: SerializerManager,
    memoryManager: MemoryManager,
    blockEvictionHandler: BlockEvictionHandler)
extends Logging {
    介绍: 内存存储器
    构造器属性:
    	conf spark配置
    	blockInfoManager	数据块信息管理器
    	serializerManager	序列化管理器
    	memoryManager	内存管理器
    	blockEvictionHandler	数据块换出处理器
    属性:
    #name @entries #Type @LinkedHashMap	数据块内存映射表
    #name @onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()	堆上展开内存映射表
    	任务编号和内存量之间的映射关系
    #name @offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()	非堆模式下展开内存映射表
    #name @unrollMemoryThreshold: Long = conf.get(STORAGE_UNROLL_MEMORY_THRESHOLD) 展开内存容量初始情况下内存量
    操作集:
    def maxMemory: Long= memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
    功能: 获取最大内存量
    
    def memoryUsed: Long = memoryManager.storageMemoryUsed
    功能: 获取最大内存使用量
    
    def blocksMemoryUsed: Long
    功能: 获取数据块内存使用量(不包括以用于展现unroll的内存量)
    val= memoryManager.synchronized {  memoryUsed - currentUnrollMemory }
    
    def getSize(blockId: BlockId): Long
    功能: 获取指定数据块@blockId 的占用内存大小
    val= entries.synchronized {
      entries.get(blockId).size
    }
    
    def putBytes[T: ClassTag](blockId: BlockId,size: Long,memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean
    功能: 使用size测试是否有足够的内存容纳指定数据块@blockId ,如果可以,则创建一个字节缓冲区,将其置于内存存储器中,否则不创建.
    调用者需要保证size合法性,put成功则返回true
    0. 数据块存在性断言
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    1. 尝试添加数据块
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
        // 内存充足,创建字节缓冲区,置于内存存储器中
      val bytes = _bytes()
      assert(bytes.size == size)
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized { // 置于内存存储器中
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else {
      false
    }
    
    def putIterator[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode,
      valuesHolder: ValuesHolder[T]): Either[Long, Long]
    功能: 将指定迭代器@values 存储到内存管理器中
    有可能迭代器太大,不能放进内存,为了避免OOM,这个方法会周期性的检查是否有足够内存区分配,如果数据块成功的实体化,那么临时的展示内存会被转化为存储内存.所以,不需要获取超过实际需要的内存.
    0. 数据存在性校验
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    1. 获取内存分配相关参数
    var elementsUnrolled = 0 // 需要展示的元素数量
    var keepUnrolling = true	// 是否需要保持展示状态
    val initialMemoryThreshold = unrollMemoryThreshold // 获取每个任务初始化内存量
    val memoryCheckPeriod = conf.get(UNROLL_MEMORY_CHECK_PERIOD) // 获取内存检查周期
    var memoryThreshold = initialMemoryThreshold //当前内存容量
    val memoryGrowthFactor = conf.get(UNROLL_MEMORY_GROWTH_FACTOR) // 内存增长因子
    var unrollMemoryUsedByThisBlock = 0L // 数据块使用展示内存
    2. 确定是否留有足够的内存容量用于展示
    keepUnrolling =
      reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, memoryMode)
    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }
    3. 展示数据块,并周期性的检查是否超出了容量上限
    while (values.hasNext && keepUnrolling) {//检查是否具有下一个迭代器元素,和是否满足展示条件(内存是否够)
      valuesHolder.storeValue(values.next()) // 存储value 到内存存储器上
      if (elementsUnrolled % memoryCheckPeriod == 0) { // 到达内存检查周期
        val currentSize = valuesHolder.estimatedSize()
        // 检查容量是否超出限制
        if (currentSize >= memoryThreshold) {
            // 向系统申请指定大小的内存
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          keepUnrolling = // 重置展示状态
            reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // 重置内存容量
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }
    4. 将展示内存转化为数据块占用的内存
    if (keepUnrolling) {
      val entryBuilder = valuesHolder.getBuilder()
      val size = entryBuilder.preciseSize
      if (size > unrollMemoryUsedByThisBlock) { // 数据块内存超过展示内存,需要申请额外内存区容纳数据块
        val amountToRequest = size - unrollMemoryUsedByThisBlock
        keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) { // 重置展示内存
          unrollMemoryUsedByThisBlock += amountToRequest
        }
      }
      if (keepUnrolling) { // 数据块内存分配完毕,释放展示内存
        val entry = entryBuilder.build()
        memoryManager.synchronized {
          releaseUnrollMemoryForThisTask(memoryMode, unrollMemoryUsedByThisBlock) // 释放展示内存
          val success = memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
          assert(success, "transferring unroll memory to storage memory failed")
        }
        entries.synchronized { // 将数据存储到内存存储器中
          entries.put(blockId, entry)
        }
        logInfo("Block %s stored as values in memory (estimated size %s, free %s)".format(blockId,
          Utils.bytesToString(entry.size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
        Right(entry.size)
      } else {
        logUnrollFailureMessage(blockId, entryBuilder.preciseSize)
        Left(unrollMemoryUsedByThisBlock)
      }
    } else {
      logUnrollFailureMessage(blockId, valuesHolder.estimatedSize()) // 展示失败处理
      Left(unrollMemoryUsedByThisBlock)
    }
    
    def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long]
    功能: 将迭代器按照value的形式放入内存存储器中
    1. 获取指定类型@classTag 的反序列化持有者
    val valuesHolder = new DeserializedValuesHolder[T](classTag)
    2. 将迭代器,按堆模式置入内存
    putIterator(blockId, values, classTag, MemoryMode.ON_HEAP, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        val unrolledIterator = if (valuesHolder.vector != null) {
          valuesHolder.vector.iterator
        } else {
          valuesHolder.arrayValues.toIterator
        }

        Left(new PartiallyUnrolledIterator(
          this,
          MemoryMode.ON_HEAP,
          unrollMemoryUsedByThisBlock,
          unrolled = unrolledIterator,
          rest = values))
    }
    
    def putIteratorAsBytes[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long]
    功能: 按照字节形式将指定迭代器@values 置入内存
    0. 数据块合法性校验
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    1. 获取每个任务的初始化内存用于申请展示数据块
    val initialMemoryThreshold = unrollMemoryThreshold
    2. 获取数据块大小
    val chunkSize = if (initialMemoryThreshold > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      logWarning(s"Initial memory threshold of ${Utils.bytesToString(initialMemoryThreshold)} " +
        s"is too large to be set as chunk size. Chunk size has been capped to " +
        s"${Utils.bytesToString(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)}")
      ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
    } else {
      initialMemoryThreshold.toInt
    }
    3. 获取序列化值持有者
    val valuesHolder = new SerializedValuesHolder[T](blockId, chunkSize, classTag,
      memoryMode, serializerManager)
    4. 将数据块置入内存
    val= putIterator(blockId, values, classTag, memoryMode, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        Left(new PartiallySerializedBlock(
          this,
          serializerManager,
          blockId,
          valuesHolder.serializationStream,
          valuesHolder.redirectableStream,
          unrollMemoryUsedByThisBlock,
          memoryMode,
          valuesHolder.bbos,
          values,
          classTag))
    }
    
    def getBytes(blockId: BlockId): Option[ChunkedByteBuffer]
    功能: 获取指定数据块的数据内容
    1. 获取内存存储器中该数据块的存储信息
    val entry = entries.synchronized { entries.get(blockId) }
    2. 获取数据块数据
    val= entry match {
      case null => None
      case e: DeserializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getBytes on serialized blocks")
      case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
    }
    
    def getValues(blockId: BlockId): Option[Iterator[_]]
    功能: 获取指定数据块的迭代器信息
    val entry = entries.synchronized { entries.get(blockId) }
    val= entry match {
      case null => None
      case e: SerializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getValues on deserialized blocks")
      case DeserializedMemoryEntry(values, _, _) =>
        val x = Some(values)
        x.map(_.iterator)
    }
    
    def remove(blockId: BlockId): Boolean 
    功能: 移除指定数据块,移除成功返回true
    val= memoryManager.synchronized {
        val entry = entries.synchronized { // 移除内存存储器中的记录
          entries.remove(blockId)
        }
        // 释放内存数据的内存
        if (entry != null) {
          entry match {
            case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
            case _ =>
          }
          memoryManager.releaseStorageMemory(entry.size, entry.memoryMode)
          logDebug(s"Block $blockId of size ${entry.size} dropped " +
            s"from memory (free ${maxMemory - blocksMemoryUsed})")
          true
        } else {
          false
        }
      }
    
    def clear(): Unit 
    功能: 清理所有内存存储器中的所有内存数据
    memoryManager.synchronized {
        entries.synchronized {
          entries.clear()
        }
        onHeapUnrollMemoryMap.clear()
        offHeapUnrollMemoryMap.clear()
        memoryManager.releaseAllStorageMemory()
        logInfo("MemoryStore cleared")
      }
    
    def getRddId(blockId: BlockId): Option[Int]
    功能: 获取RDD编号
    val= blockId.asRDDId.map(_.rddId)
    
    def afterDropAction(blockId: BlockId): Unit = {}
    功能: 扔掉指定数据块的处理,测试的点,用于模拟竞争
    
    def contains(blockId: BlockId): Boolean
    功能: 确认是否包含某个数据块
    val= entries.synchronized { entries.containsKey(blockId) }
    
    def currentTaskAttemptId(): Long
    功能: 获取当前任务请求号(driver端才可以获取)
    val= Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
    
    def currentUnrollMemory: Long 
    功能: 获取当前展示内存量
    val= onHeapUnrollMemoryMap.values.sum + offHeapUnrollMemoryMap.values.sum
    
    def currentUnrollMemoryForThisTask: Long
    功能: 获取当前任务展示内存
    val= memoryManager.synchronized {
        onHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L) +
          offHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
      }
    
    def numTasksUnrolling: Int
    功能: 参与展示的任务数量
    val= memoryManager.synchronized {
    	(onHeapUnrollMemoryMap.keys ++ offHeapUnrollMemoryMap.keys).toSet.size
  	}
    
    def logMemoryUsage(): Unit
    功能: 显示当前内存使用信息
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
    
    def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit
    功能: 展示失败处理信息
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
    
    def reserveUnrollMemoryForThisTask(
      blockId: BlockId,
      memory: Long,
      memoryMode: MemoryMode): Boolean
    功能: 展示这个任务中指定数据块而保留内存
    输入参数:
    	blockId	数据块标识符
    	memory	需要保持的内存数据量
    	memoryMode	内存容量
    val= 
    memoryManager.synchronized {
        // 向系统申请@memory 内存量
      val success = memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
      if (success) {
        val taskAttemptId = currentTaskAttemptId()
        val unrollMemoryMap = memoryMode match {
          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
        }
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
    
    def releaseUnrollMemoryForThisTask(memoryMode: MemoryMode, memory: Long = Long.MaxValue): Unit
    功能: 释放本任务的展示内存
    1. 获取任务请求编号
    val taskAttemptId = currentTaskAttemptId()
    2. 释放给任务请求编号对于的展示内存
    memoryManager.synchronized {
      val unrollMemoryMap = memoryMode match {
        case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
        case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
      }
      if (unrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          memoryManager.releaseUnrollMemory(memoryToRelease, memoryMode)
        }
        if (unrollMemoryMap(taskAttemptId) == 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
    
    def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }
    功能: 确定指定数据块内存是否被回收
    
    def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit 
    功能: 丢掉指定数据块
    val data = entry match {
        case DeserializedMemoryEntry(values, _, _) => Left(values)
        case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
    }
    val newEffectiveStorageLevel =
    blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
    if (newEffectiveStorageLevel.isValid) {
        blockInfoManager.unlock(blockId)
    } else {
        blockInfoManager.removeBlock(blockId)
    }
    
    def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit
    功能: 扔掉指定数据块@blockId
    1. 获取指定数据块@blockId 的数据块
    val data = entry match {
          case DeserializedMemoryEntry(values, _, _) => Left(values)
          case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
        }
    2. 将指定数据块从内存中移出
    val newEffectiveStorageLevel =
          blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
    3. 移出数据块
    if (newEffectiveStorageLevel.isValid) {
        // 数据块仍旧存储于至少一个存储中,则释放锁,但是并不需要删除数据块
        blockInfoManager.unlock(blockId)
    } else {
        // 如果数据块不存在于任何存储中,删除数据块信息,以便于数据块可以再次被存储
        blockInfoManager.removeBlock(blockId)
    }
    
    def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      space: Long,
      memoryMode: MemoryMode): Long 
    功能: 回收数据块已达到释放空间的功能
    尝试释放数据块内存，用户存储特点的数据块，如果数据块大于内存或者需要替代同一个RDD的其他数据块时会引发失败。(会导致RDD循环替代格式的浪费,以及内存容量不足的问题),需要块管理器对操作进行临界资源管理.
    输入参数:
    	blockId	数据块标识符
    	space	数据块大小
    	memoryMode	内存模式
	返回释放的内存量
    0. 存储空间大小断言
    assert(space > 0)
    1. 移除指定数据块内存
    memoryManager.synchronized {
      var freedMemory = 0L
      val rddToAdd = blockId.flatMap(getRddId)
      val selectedBlocks = new ArrayBuffer[BlockId]
      def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        while (freedMemory < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          val entry = pair.getValue
          if (blockIsEvictable(blockId, entry)) {
              if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              selectedBlocks += blockId
              freedMemory += pair.getValue.size
            }
          } 
       if (freedMemory >= space) { // 执行对内存的释放
        var lastSuccessfulBlock = -1
        try {
          logInfo(s"${selectedBlocks.size} blocks selected for dropping " +
            s"(${Utils.bytesToString(freedMemory)} bytes)")
          (0 until selectedBlocks.size).foreach { idx =>
              // 释放所有的数据块
            val blockId = selectedBlocks(idx)
            val entry = entries.synchronized {
              entries.get(blockId)
            }
            if (entry != null) {
              dropBlock(blockId, entry)
              afterDropAction(blockId)
            }
            lastSuccessfulBlock = idx
          }
          logInfo(s"After dropping ${selectedBlocks.size} blocks, " +
            s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
          freedMemory //返回释放内存大小
        } finally {
          if (lastSuccessfulBlock != selectedBlocks.size - 1) {
            (lastSuccessfulBlock + 1 until selectedBlocks.size).foreach { idx =>
              val blockId = selectedBlocks(idx)
              blockInfoManager.unlock(blockId)
            }
          }
        }
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id")
        }
        selectedBlocks.foreach { id =>
          blockInfoManager.unlock(id)
        }
        0L
      }
    }
}
```

#### BlockException

```scala
private[spark]
case class BlockException(blockId: BlockId, message: String) extends Exception(message)
介绍: 存储块异常信息
```

#### BlockId

```markdown
介绍:
	特点数据块的唯一标识符,经常与单个文件相联系,一个数据块可以被这个文件名称唯一标识,但是每种数据块含有不同类型的key的集合,这些key决定了独特的名称.
	如果你的块编号@BlockId 可以被序列化,请确定添加到@`BlockId.apply()`中
```

```scala
@DeveloperApi
sealed abstract class BlockId {
  操作集:
  def name: String
  功能: 获取数据块的唯一标识符(数据块的全局唯一标识符,可以用于序列化和反序列化)
    
  def asRDDId: Option[RDDBlockId] = if (isRDD) Some(asInstanceOf[RDDBlockId]) else None
  功能: 获取数据块RDD编号  
    
  def isRDD: Boolean = isInstanceOf[RDDBlockId]
  功能: 确定是否存在当前数据块的RDD
    
  def isShuffle: Boolean = isInstanceOf[ShuffleBlockId] || isInstanceOf[ShuffleBlockBatchId]
  功能: 确定当前数据块是否参加了shuffle  
    
  def isBroadcast: Boolean = isInstanceOf[BroadcastBlockId]
  功能: 确定当前数据块数据是否为广播变量
    
  def toString: String = name
  功能: 信息显示
}
```

```scala
@DeveloperApi
case class ShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  介绍: shuffle块描述
  构造器参数:
    	shuffleId	shuffleID
    	mapId	map任务id
    	reduceId	reduce任务ID
  操作集:
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
  功能: 信息显示
}

@DeveloperApi
case class ShuffleDataBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  介绍: shuffle数据块描述
  构造器描述:
    	shuffleId	shuffleID
    	mapId	mapID
    	reduceId	reduce任务编号
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
  功能: 信息显示
}

@DeveloperApi
case class ShuffleIndexBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  介绍: shuffle索引数据块(使用索引查找外存数据)
  构造器属性:
    	shuffleId	shuffle编号
    	mapId	map编号
    	reduceId	reduce编号
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index"
  功能: 信息显示
}

@DeveloperApi
case class BroadcastBlockId(broadcastId: Long, field: String = "") extends BlockId {
  介绍: 广播变量块编号
  构造器属性:
    	broadcastId	广播变量编号
    	field	属性
  override def name: String = "broadcast_" + broadcastId + (if (field == "") "" else "_" + field)
  功能: 信息显示
}

@DeveloperApi
case class TaskResultBlockId(taskId: Long) extends BlockId {
  介绍: 任务结构数据块标识
  构造器属性:
    taskId	任务编号
  override def name: String = "taskresult_" + taskId
  功能: 信息显示
}

@DeveloperApi
case class StreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  介绍: 流式数据块编号
  构造器: 
    	streamId	流编号
    	uniqueId	唯一标识符
  override def name: String = "input-" + streamId + "-" + uniqueId
  功能: 信息显示
}

private[spark] case class TempLocalBlockId(id: UUID) extends BlockId {
  介绍: 本地临时数据块(没有进过序列化)
  构造器属性:
    	id	本地临时数据块唯一标识符
  override def name: String = "temp_local_" + id
  功能: 信息显示
}

private[spark] case class TempShuffleBlockId(id: UUID) extends BlockId {
  介绍: 临时shuffle数据块唯一标识符
  构造器:
    	id	本地shuffle数据块唯一标识符
  override def name: String = "temp_shuffle_" + id
  功能: 信息显示
}

private[spark] case class TestBlockId(id: String) extends BlockId {
  介绍: 测试数据块标识符(仅仅用于测试)
  构造器参数:
    id	数据块标识符
  override def name: String = "test_" + id
  功能: 显示新
}

@DeveloperApi
class UnrecognizedBlockId(name: String)
    extends SparkException(s"Failed to parse $name into a block ID")
介绍: 未识别的数据块类型
```

```scala
@DeveloperApi
object BlockId {
    属性:
    #name @RDD = "rdd_([0-9]+)_([0-9]+)".r	RDD格式
    #name @SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r	shuffle格式
    #name @SHUFFLE_BATCH = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)".r	批量shuffle格式
    #name @SHUFFLE_DATA = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).data".r	shuffle数据格式
    #name @SHUFFLE_INDEX = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).index".r	shuffle索引
    #name @BROADCAST = "broadcast_([0-9]+)([_A-Za-z0-9]*)".r 广播变量格式
    #name @TASKRESULT = "taskresult_([0-9]+)".r	任务结果格式
    #name @STREAM = "input-([0-9]+)-([0-9]+)".r	流式数据格式
    #name @TEMP_LOCAL = "temp_local_([-A-Fa-f0-9]+)".r	本地临时文件格式
    #name @TEMP_SHUFFLE = "temp_shuffle_([-A-Fa-f0-9]+)".r	临时shuffle格式
    #name @TEST = "test_(.*)".r	测试格式
    操作集：
    def apply(name: String): BlockId
    功能: 获取块标识实例
    val= 
        name match {
        case RDD(rddId, splitIndex) =>
          RDDBlockId(rddId.toInt, splitIndex.toInt)
        case SHUFFLE(shuffleId, mapId, reduceId) =>
          ShuffleBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
        case SHUFFLE_BATCH(shuffleId, mapId, startReduceId, endReduceId) =>
          ShuffleBlockBatchId(shuffleId.toInt, mapId.toLong, startReduceId.toInt, endReduceId.toInt)
        case SHUFFLE_DATA(shuffleId, mapId, reduceId) =>
          ShuffleDataBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
        case SHUFFLE_INDEX(shuffleId, mapId, reduceId) =>
          ShuffleIndexBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
        case BROADCAST(broadcastId, field) =>
          BroadcastBlockId(broadcastId.toLong, field.stripPrefix("_"))
        case TASKRESULT(taskId) =>
          TaskResultBlockId(taskId.toLong)
        case STREAM(streamId, uniqueId) =>
          StreamBlockId(streamId.toInt, uniqueId.toLong)
        case TEMP_LOCAL(uuid) =>
          TempLocalBlockId(UUID.fromString(uuid))
        case TEMP_SHUFFLE(uuid) =>
          TempShuffleBlockId(UUID.fromString(uuid))
        case TEST(value) =>
          TestBlockId(value)
        case _ =>
          throw new UnrecognizedBlockId(name)
      	}
}
```

#### BlockInfoManager

```scala
private[storage] class BlockInfo(
    val level: StorageLevel,
    val classTag: ClassTag[_],
    val tellMaster: Boolean) {
    介绍: 定位一个数据块的元数据
    这个类的实例是线程不安全的需要从@BlockInfoManager 获取锁才能进行操作
    构造器参数:
    level	存储等级
    classTag	类标签
    tellMaster	块的状态变化是否汇报给master.对于大多数块都选择汇报,对于广播变量数据块选择false
    属性:
    #name @_readerCount: Int = 0	读取字节量
    #name @_size: Long = 0	数据块大小(字节)
    #name @_writerTask: Long = BlockInfo.NO_WRITER	持有当前写所的任务编号(初始化为-1表示无任务进行)
    初始化操作:
    checkInvariants()
    功能: 参数断言
    
    操作集:
    def size: Long = _size
    功能: 获取数据块的大小(字节)
    
    def size_=(s: Long): Unit
    功能: 设置字节量
    _writerTask = t
    checkInvariants() // 检查参数合法性
    
    def readerCount: Int = _readerCount
    功能: 获取读取字节量
    
    def readerCount_=(c: Int): Unit
    功能: 设置字节读取量
    _readerCount = c
    checkInvariants()
    
    def writerTask: Long = _writerTask
    功能: 获取当前写的任务请求编号
    
    def writerTask_=(t: Long): Unit = {
        _writerTask = t
        checkInvariants()
      }
    功能: 设置当前正在写的任务编号
    
    def checkInvariants(): Unit
    功能: 参数合法性断言(主要是读取字节量,和正在写的任务)
    assert(_readerCount >= 0) 
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER) // 初始化状态断言
}
```

```scala
private[storage] object BlockInfo {
    属性:
    #name @NO_WRITER: Long = -1	当前无任务进行写操作的任务编号(表示当前写所处于释放状态)
    #name @NON_TASK_WRITER: Long = -1024	无任务线程任务编号(比如说是单元测试代码)
}
```

```scala
private[storage] class BlockInfoManager extends Logging {
    介绍: 块管理器@BlockManager 的组件,用于定位块的元数据,和管理数据块的锁
    这个类暴露出来的锁接口类型时读写锁,每一个锁请求都是自动与相关运行任务联系起来的.在任务执行完毕或者执行失败时都会自动释放.这个类是线程安全的.
    属性:
    type TaskAttemptId = Long	任务请求ID
    #name @infos = new mutable.HashMap[BlockId, BlockInfo]	GuardedBy("this")	数据块元数据映射表
    	添加信息通过原子操作来进行增删
    #name @ writeLocksByTask = new mutable.HashMap[TaskAttemptId, mutable.Set[BlockId]]
      						with mutable.MultiMap[TaskAttemptId, BlockId]
    任务写锁映射表
    #name @readLocksByTask = new mutable.HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]
    读取锁映射表(可重入)
    初始化操作:
    registerTask(BlockInfo.NON_TASK_WRITER)
    功能: 注册特殊类型任务到读取锁映射表(例如,单元测试任务)
    
    操作集:
    def registerTask(taskAttemptId: TaskAttemptId): Unit 
    功能: 注册指定任务@taskAttemptId, 必须在该任务其他操作之前继续调用
    0. 读取锁列表校验是否已经包含该锁
    require(!readLocksByTask.contains(taskAttemptId),
      s"Task attempt $taskAttemptId is already registered")
    1. 注册到读取锁映射表中@readLocksByTask
    readLocksByTask(taskAttemptId) = ConcurrentHashMultiset.create()
    
    def currentTaskAttemptId: TaskAttemptId 
    功能: 获取当前任务请求编号(唯一标识符)
    val= Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
    
    def lockForReading(blockId: BlockId,blocking: Boolean = true): Option[BlockInfo]
    功能: 锁定读取,并且返回元数据信息,如果其他任务已经锁定当前数据块@blockId的读取,那么读取锁就会立刻移交给调用任务,锁计数值会增加(可重入).如果另一个任务锁定了数据块的写入,这个调用会一直阻塞,直到写出锁释放.且立刻返回block=false.单个任务可以多次锁定一个数据块的读取,每个锁需要分开释放.
    输入数据:
    	blockId	需要上锁的数据块唯一标识符
    	blocking	是否阻塞执行,为true(默认),则会阻塞到获取锁为止.如果为false,获取锁失败会直接返回.
    返回数据:
    	如果指定数据块不存在或者被移除,则返回None,否则返回Some(BlockInfo)
    val= synchronized {
        do {
          infos.get(blockId) match { // 获取块的元数据信息
            case None => return None
            case Some(info) =>
              if (info.writerTask == BlockInfo.NO_WRITER) {
                info.readerCount += 1
                readLocksByTask(currentTaskAttemptId).add(blockId)
                logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
                return Some(info)
              }
          }
          if (blocking) { // 阻塞式获取执行结果
            wait()
          }
        } while (blocking)
        None        
    }
    
    def lockForWriting(blockId: BlockId,blocking: Boolean = true): Option[BlockInfo] 
    功能: 锁定写出
    锁定数据块的写出,并返回元数据信息.
    如果其他任务对当前数据块同时锁定读写,这个调用会阻塞到锁的释放,如果block=false则会直接返回.
    val= synchronized {
        do {
          infos.get(blockId) match { // 获取写出锁
            case None => return None
            case Some(info) =>
              if (info.writerTask == BlockInfo.NO_WRITER && info.readerCount == 0) {
                info.writerTask = currentTaskAttemptId
                writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
                logTrace(s"Task $currentTaskAttemptId acquired write lock for $blockId")
                return Some(info)
              }
          }
          if (blocking) { // 阻塞式获取执行结果
            wait()
          }
        } while (blocking)
        None
    }
    
    def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo 
    功能: 断言指定数据块@blockId 存在写出锁(没有则抛出异常)
    val= synchronized {
        infos.get(blockId) match {
          case Some(info) =>
            if (info.writerTask != currentTaskAttemptId) { // 当前任务没有占用写锁,抛出异常
              throw new SparkException(
                s"Task $currentTaskAttemptId has not locked block $blockId for writing")
            } else {
              info
            }
          case None =>
            throw new SparkException(s"Block $blockId does not exist")
        }
    }
    
    def get(blockId: BlockId): Option[BlockInfo]
    功能: 获取指定数据块@blockId 的元数据信息,本类之外的代码无法调用
    val= synchronized { infos.get(blockId) }
    
    def downgradeLock(blockId: BlockId): Unit
    功能: 将一个排他写出锁降级为共享读取锁
    synchronized {
        1. 获取元数据信息
        val info=get(blockId).get // 获取元数据信息
        2. 排它锁断言
        require(info.writerTask == currentTaskAttemptId,
      s"Task $currentTaskAttemptId tried to downgrade a write lock that it does not hold on" +
        s" block $blockId")
        3. 是否当前数据块的锁
        unlock(blockId)
        4. 将当前锁修改为共享读取锁
        val lockOutcome = lockForReading(blockId, blocking = false)
        5. 共享锁断言
        assert(lockOutcome.isDefined)
    }
    
    def unlock(blockId: BlockId, taskAttemptId: Option[TaskAttemptId] = None): Unit
    功能: 释放指定数据块@blockId 的锁
    在@TaskContext 没有将所有信息传递给各个子线程中，获取不到@TaskContext 的线程编号，所以必须显式地将TID值传递过去，用于释放锁。详情参考SPARK-18406。
    输入参数:
    	blockId	块编号
    	taskAttemptId	任务请求编号
    synchronized {
        1. 获取任务编号TID
        val taskId = taskAttemptId.getOrElse(currentTaskAttemptId)
        2. 获取元数据信息
        val info = get(blockId).getOrElse {
          throw new IllegalStateException(s"Block $blockId not found")
        }
        3. 释放写出锁(排它锁)
        if (info.writerTask != BlockInfo.NO_WRITER) { // 释放写出锁
          info.writerTask = BlockInfo.NO_WRITER
          writeLocksByTask.removeBinding(taskId, blockId) // 移除任务与数据块之间的绑定关系(任务撤销了)
        } else { // 不存在写出锁,释放可重入的读取锁
          // 重入次数-1
          assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
          info.readerCount -= 1
          // 获取当前任务对应的数据块列表
          val countsForTask = readLocksByTask(taskId)
          // 从数据块列表中移除一个数据块,作为释放一个锁的表示
          val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
          assert(newPinCountForTask >= 0,
            s"Task $taskId release lock on block $blockId more times than it acquired it")
        }
        4. 唤醒其他线程
        notifyAll()
    }
    
    def lockNewBlockForWriting(blockId: BlockId,newBlockInfo: BlockInfo): Boolean
    功能: 新建数据块进行锁定
    这里强制首次写为主的语义,当第一次写的时候,直接写且获取写出锁.否则,其他线程已经在写数据块,那么在获取读取锁之前,只需要等待其写出完成即可.
    返回参数: 为true表示写出成功
    val= lockForReading(blockId) match {
      case Some(info) =>
        false
      case None =>
        infos(blockId) = newBlockInfo
        lockForWriting(blockId)
        true
    }
    
    def size: Int = synchronized { infos.size }
    功能: 获取数据块的数量
    
    def getNumberOfMapEntries: Long
    功能: 获取总计Map的条目数量
    val= synchronized {
        size +
        readLocksByTask.size +
        readLocksByTask.map(_._2.size()).sum +
        writeLocksByTask.size +
        writeLocksByTask.map(_._2.size).sum
    }
    
    def entries: Iterator[(BlockId, BlockInfo)] = synchronized { infos.toArray.toIterator }
    功能: 获取所有数据块元数据副本迭代器
    
    def removeBlock(blockId: BlockId): Unit 
    功能: 移除指定数据块
    synchronized {
        1. 移除指定数据块
        infos.get(blockId) match {
          case Some(blockInfo) =>
            if (blockInfo.writerTask != currentTaskAttemptId) { 
              throw new IllegalStateException(
                s"Task $currentTaskAttemptId called remove() on block $blockId without a write lock")
            } else { // 移除数据块需要有写出锁
              // 移除数据块的索引记录
              infos.remove(blockId)
              // 重置元数据信息
              blockInfo.readerCount = 0
              blockInfo.writerTask = BlockInfo.NO_WRITER
              // 移除任务映射关系
              writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
            }
          case None =>
            throw new IllegalArgumentException(
              s"Task $currentTaskAttemptId called remove() on non-existent block $blockId")
        }
        2. 唤醒其他线程
        notifyAll()
    }
    
    def clear(): Unit
    功能: 删除所有状态,关闭时调用
    synchronized {
        infos.valuesIterator.foreach { blockInfo =>
          blockInfo.readerCount = 0
          blockInfo.writerTask = BlockInfo.NO_WRITER
        }
        infos.clear()
        readLocksByTask.clear()
        writeLocksByTask.clear()
        notifyAll()
    }
    
    def getTaskLockCount(taskAttemptId: TaskAttemptId): Int 
    功能: 获取当前任务锁持有的锁数量,用于测试
    val= readLocksByTask.get(taskAttemptId).map(_.size()).getOrElse(0) +
      writeLocksByTask.get(taskAttemptId).map(_.size).getOrElse(0)
    
    def releaseAllLocksForTask(taskAttemptId: TaskAttemptId): Seq[BlockId]
    功能: 释放当前任务持有的所有锁,并释放锁与数据块的映射关系,用于任务的最后,作为清理工作
    1. 获取当前读写锁列表
    val blocksWithReleasedLocks = mutable.ArrayBuffer[BlockId]()
    val readLocks = readLocksByTask.remove(taskAttemptId).getOrElse(ImmutableMultiset.of[BlockId]())
    val writeLocks = writeLocksByTask.remove(taskAttemptId).getOrElse(Seq.empty)
    2. 收集移除写出锁对应的数据块
    for (blockId <- writeLocks) {
      infos.get(blockId).foreach { info =>
        assert(info.writerTask == taskAttemptId)
        info.writerTask = BlockInfo.NO_WRITER
      }
      blocksWithReleasedLocks += blockId
    }
    3. 收集移除的读取锁对应的数据块
    readLocks.entrySet().iterator().asScala.foreach { entry =>
      val blockId = entry.getElement
      val lockCount = entry.getCount
      blocksWithReleasedLocks += blockId
      get(blockId).foreach { info =>
        info.readerCount -= lockCount
        assert(info.readerCount >= 0)
      }
    }
    4. 唤醒其他线程
    notifyAll()
    val= blocksWithReleasedLocks
}
```

#### BlockManager

```scala
private[spark] trait BlockData {
    介绍: 数据块数据
    这个抽象类提供了数据块如何存储的方法,以及不同读取底层数据块数据的方法,调用者在数据块操作完毕之后可以调用@dispose().
    操作集:
    def toInputStream(): InputStream
    功能: 将当前数据块数据转换成输入流,便于可以读取
    
    def toNetty(): Object
    功能: 返回一个可以使用netty读取的数据块对象,参考@ManagedBuffer.convertToNetty()获取更多
    
    def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer
    功能: 转换成块状字节缓冲区
    输入参数:
    allocator	分配函数
    
    def toByteBuffer(): ByteBuffer
    功能: 转换为字节缓冲区
    
    def size: Long
    功能: 转换为数据块的大小
    
    def dispose(): Unit
    功能: 向外界暴露当前数据块
}
```

```scala
private[spark] class ByteBufferBlockData(
    val buffer: ChunkedByteBuffer,
    val shouldDispose: Boolean) extends BlockData {
    介绍: 字节缓冲区数据块
    构造器参数:
    buffer	块状字节缓冲区
    操作集:
    def toInputStream(): InputStream = buffer.toInputStream(dispose = false)
    功能: 将数据块数据转换为输入流(缓冲区内容-> 输入流)
    
    def toNetty(): Object = buffer.toNetty
    功能: 转换为netty读取的对象
    
    def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer
    功能: 转换为块状字节缓冲区(将缓冲区内容拷贝到新的缓冲区并返回,进行了浅拷贝,只拷贝内容不拷贝引用)
    val= buffer.copy(allocator)
    
    def toByteBuffer(): ByteBuffer = buffer.toByteBuffer
    功能: 转换为字节缓冲区
    def size: Long = buffer.size
    
    def dispose(): Unit
    功能: 清除直接内存中以及内存映射的数据
    if (shouldDispose) {
      buffer.dispose()
    }
}
```

```scala
private[spark] class HostLocalDirManager(
    futureExecutionContext: ExecutionContext,
    cacheSize: Int,
    externalBlockStoreClient: ExternalBlockStoreClient,
    host: String,
    externalShuffleServicePort: Int) extends Logging {
    介绍: 本地目录管理器
    构造器参数:
        futureExecutionContext	异步任务执行器上下文
        cacheSize	缓存大小
        externalBlockStoreClient	外部数据块存储客户端
        host	主机名称
        externalShuffleServicePort	外部shuffle服务端口
    属性:
    #name @executorIdToLocalDirsCache	执行器编号->本地目录缓存映射
    val= CacheBuilder
      .newBuilder()
      .maximumSize(cacheSize)
      .build[String, Array[String]]()
    操作集:
    def getCachedHostLocalDirs()
      : scala.collection.Map[String, Array[String]] 
    功能: 获取缓存主机本地目录映射表@executorIdToLocalDirsCache
    val= executorIdToLocalDirsCache.synchronized {
        import scala.collection.JavaConverters._
        return executorIdToLocalDirsCache.asMap().asScala
    }
    
    def getHostLocalDirs(
      executorIds: Array[String])(
      callback: Try[java.util.Map[String, Array[String]]] => Unit): Unit
    功能: 获取主机本地目录
    输入参数:
    executorIds	获取执行器列表
    callback	执行器映射本地目录表回调函数
    1. 获取一个执行任务线程
    val hostLocalDirsCompletable = 
    	new CompletableFuture[java.util.Map[String, Array[String]]]
    2. 客户端获取本地执行目录
    externalBlockStoreClient.getHostLocalDirs(
      host,
      externalShuffleServicePort,
      executorIds,
      hostLocalDirsCompletable)
    3. 异步任务执行完成进行回调,发送任务执行成功/失败的消息
    val= hostLocalDirsCompletable.whenComplete { (hostLocalDirs, throwable) =>
      if (hostLocalDirs != null) {
        callback(Success(hostLocalDirs))
        executorIdToLocalDirsCache.synchronized {
          executorIdToLocalDirsCache.putAll(hostLocalDirs)
        }
      } else {
        callback(Failure(throwable))
      }
    }
}
```

```scala
private[spark] class BlockManager(
    executorId: String,
    rpcEnv: RpcEnv,
    val master: BlockManagerMaster,
    val serializerManager: SerializerManager,
    val conf: SparkConf,
    memoryManager: MemoryManager,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    val blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    externalBlockStoreClient: Option[ExternalBlockStoreClient])
extends BlockDataManager with BlockEvictionHandler with Logging {
    介绍: 数据块管理器
    构造器参数:
        executorId	执行器编号
        rpcEnv	RPC环境
        master	数据块管理器master
        serializerManager	序列化管理器
        mapOutputTracker	输出定位器
        shuffleManager	shuffle管理器
        blockTransferService	数据块转换服务
        securityManager	安全管理器
        externalBlockStoreClient	外部数据块存储客户端
    属性:
    #name @externalShuffleServiceEnabled: Boolean = externalBlockStoreClient.isDefined
    	是否允许外部shuffle
    #name @remoteReadNioBufferConversion	远端读取NIO缓冲区转换
    val= conf.get(Network.NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION)
    #name @subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)
    	每个本地目录的子目录数量(int类型)
    #name @diskBlockManager	磁盘数据块管理器
    val= {
        val deleteFilesOnStop =// 确定是否需要在数据块管理器停止的时候将文件删除
        !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
        new DiskBlockManager(conf, deleteFilesOnStop)
    }
    #name @blockInfoManager = new BlockInfoManager	数据块信息管理器(测试可见)
    #name @futureExecutionContext	异步执行任务线程池
    val=ExecutionContext.fromExecutorService(
    	ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))
    #name @memoryStore	内存存储器(保存数据块的内存部分)
    val= new MemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
    #name @diskStore = new DiskStore(conf, diskBlockManager, securityManager)	磁盘存储器
    #name @maxOnHeapMemory = memoryManager.maxOnHeapStorageMemory	最大堆内存
    #name @maxOffHeapMemory = memoryManager.maxOffHeapStorageMemory	最大非堆内存(方法区中)
    #name @externalShuffleServicePort = StorageUtils.externalShuffleServicePort(conf)
    	外部shuffle服务的端口
    #name @blockManagerId: BlockManagerId = _	数据块管理器编号
    #name @shuffleServerId: BlockManagerId = _	
    	执行执行器shuffle的服务器地址(外部shuffle服务或者当前执行器的数据块管理器都可以)
    #name @blockStoreClient = externalBlockStoreClient.getOrElse(blockTransferService)
    	数据块存储客户端(用于读取其他执行器数据块,既可以是外部shuffle服务,也可以是连接到其他执行器的数据块转换服务@BlockTransferService)
    #name @maxFailuresBeforeLocationRefresh 数据块管理器在刷新驱动器数据块位置前的最大失败次数
    val= conf.get(config.BLOCK_FAILURES_BEFORE_LOCATION_REFRESH)
    #name @slaveEndpoint	slaveRPC端点
    val= rpcEnv.setupEndpoint(
        "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
        new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))
    #name @asyncReregisterTask: Future[Unit] = null	异步注册任务
    	待定重新注册的动作(用于异步执行),获取的时候需要获取同步锁@asyncReregisterLock
    #name @asyncReregisterLock = new Object	异步注册锁
    #name @cachedPeers: Seq[BlockManagerId] = _	缓存的数据块管理器标识符
    #name @peerFetchLock = new Object	同级别数据块获取锁
    #name @lastPeerFetchTimeNs = 0L	上个数据块获取的时间
    #name @blockReplicationPolicy: BlockReplicationPolicy = _	数据块备份策略
    #name @remoteBlockTempFileManager	远端数据块临时文件管理器
    val= new BlockManager.RemoteBlockDownloadFileManager(this)
    使用@DownloadFileManager,用于定位所有远端数据块文件(超出存储容量的部分),并会基于引用删除文件
    #name @maxRemoteBlockToMem = conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)
    最大远端数据块转换到内存的量(默认200M)
    #name @hostLocalDirManager: Option[HostLocalDirManager] = None	本地数据块管理器
    
    初始化操作:
    memoryManager.setMemoryStore(memoryStore)
    功能: 设置内存管理器
    
    内部类:
    private[spark] abstract class BlockStoreUpdater[T](
      blockSize: Long,
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean,
      keepReadLock: Boolean) {
        介绍: 数据块存储更新器,用于存储数据块,无论是在内存上还是在磁盘上
        构造器参数:
        blockSize	数据块大小
        blockId	数据块标识符
        level	存储等级
        classTag	类标签
        tellMaster	更新是否告知master
        keepReadLock	是否保持读取锁
        操作集:
        def readToByteBuffer(): ChunkedByteBuffer
        功能: 读取数据块内容到内存中,如果数据块存储的更新基于临时文件,,这个就会导致将整个文件加载到块字节缓冲区中@ChunkedByteBuffer
        
        def blockData(): BlockData
        功能: 获取数据块数据
        
        def saveToDiskStore(): Unit
        功能: 将数据块内容保存到磁盘存储器中
        
        def saveDeserializedValuesToMemoryStore(inputStream: InputStream): Boolean
        功能: 将反序列化的值保存到内存存储器中
        try {
        val values = serializerManager.dataDeserializeStream(
            blockId, inputStream)(classTag)
        memoryStore.putIteratorAsValues(blockId, values, classTag) match {// 存储数据
          case Right(_) => true
          case Left(iter) =>
            // 如果存储到磁盘失败,将会直接将数据放置在磁盘上,这样就不需要这个迭代器信息,直接提前关闭
            // 并释放资源
            iter.close()
            false
        }
      } finally {
        IOUtils.closeQuietly(inputStream)
      }
    }
    
    def saveSerializedValuesToMemoryStore(bytes: ChunkedByteBuffer): Boolean
    功能: 保存反序列化的值到内存存储器中,反序列化之后的值在块字节缓冲区中@bytes
    val memoryMode = level.memoryMode 
    // 这里并不存在需要防止磁盘的操作,因为本身数据就在内存,只需要转移位置即可
      memoryStore.putBytes(blockId, blockSize, memoryMode, () => {
        if (memoryMode == MemoryMode.OFF_HEAP && bytes.chunks.exists(!_.isDirect)) {
          bytes.copy(Platform.allocateDirectBuffer)
        } else {
          bytes
        }
      })
    
    def save(): Boolean
    功能: 防止给定的数据到给定存储等级的存储器中,如果需要备份,则进行备份
    如果数据块已经存在了,这个方法不会覆盖.如果开启了读取锁保持@keepReadLock=true,这个方法会持有读取锁,并在返回的时候(尽管数据块已经存在)持有.如果没有设置,返回的时候就不会持有这个读取锁.
    返回: 如果数据块已经存在或者保持数据成功则返回true,否则返回false
    1. 放置数据,并检测放置的数据
    doPut(blockId, level, classTag, tellMaster, keepReadLock) { info =>
        val startTimeNs = System.nanoTime()
        // 因为是存储字节信息，需要在本地存储之前初始化备份，这个操作很快，因为数据已经序列化
        // 且马上就要发送,通过创建一个进行备份的异步任务进行备份处理.
        val replicationFuture = if (level.replication > 1) {
          Future {
              // 这个是一个阻塞的方法,需要运行在创建的线程池中@futureExecutionContext
            replicate(blockId, blockData(), level, classTag)
          }(futureExecutionContext)
        } else {
          null
        }
		// 优先填满内存存储器,尽管设置了使用磁盘的属性,但是还需要优先满足内存存储器要求
        if (level.useMemory) {
          val putSucceeded = if (level.deserialized) {
            saveDeserializedValuesToMemoryStore(blockData().toInputStream())
          } else {
            saveSerializedValuesToMemoryStore(readToByteBuffer())
          }
          if (!putSucceeded && level.useDisk) {
            logWarning(s"Persisting block $blockId to disk instead.")
            saveToDiskStore()
          }
        } else if (level.useDisk) {
            // 内存无法存储则转存磁盘
          saveToDiskStore()
        }
        // 告知master存储的情况(如果可能的话)
        val putBlockStatus = getCurrentBlockStatus(blockId, info)
        val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
        if (blockWasSuccessfullyStored) {
          info.size = blockSize
          if (tellMaster && info.tellMaster) {
            reportBlockStatus(blockId, putBlockStatus)
          }
          addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        }
        logDebug(s"Put block ${blockId} locally took 
        	${Utils.getUsedTimeNs(startTimeNs)}")
        // 存储完毕,等待异步备份工作的完成即可
        if (level.replication > 1) {
          // Wait for asynchronous replication to finish
          try {
            ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
          } catch {
            case NonFatal(t) =>
              throw new Exception("Error occurred while waiting for 
              replication to finish", t)
          }
        }
        if (blockWasSuccessfullyStored) {
          None
        } else {
          Some(blockSize)
        }
      }.isEmpty
    
    private case class ByteBufferBlockStoreUpdater[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      bytes: ChunkedByteBuffer,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false)
    extends BlockStoreUpdater[T](
        bytes.size, blockId, level, classTag, tellMaster, keepReadLock) {
        介绍: 字节缓冲区数据块存储更新器
        构造器参数:
            blockId	数据块标识符
            level	存储等级
            classTag	类型标签
            bytes	块字节缓冲区
            tellMaster	是否告知master
            keepReadLock	是否保持读取锁
        操作集:
        def readToByteBuffer(): ChunkedByteBuffer = bytes
        功能: 读取到字节缓冲区中,并返回字节缓冲区@ChunkedByteBuffer
        
        def blockData(): BlockData = new ByteBufferBlockData(bytes, false)
        功能: 获取数据块数据,返回字节缓冲区的包装的数据块信息,不会释放内存区域,用于避免释放用户调用的缓冲区
        
        def saveToDiskStore(): Unit = diskStore.putBytes(blockId, bytes)
        功能: 保存到磁盘存储器上
    }
    
    private[spark] case class TempFileBasedBlockStoreUpdater[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      tmpFile: File,
      blockSize: Long,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false)
    extends BlockStoreUpdater[T](blockSize, blockId, level, classTag, tellMaster, keepReadLock) {
        介绍: 基于临时文件的数据块存储更新器
        构造器参数:
        blockId	数据块标识符
        level	存储等级
        classTag	数据类型
        tmpFile	临时文件
        blockSize	数据块大小
        tellMaster	是否通知master
        keepReadLock	是否保持读取锁
        操作集:
        def readToByteBuffer(): ChunkedByteBuffe
        功能: 转换为字节缓冲区
        val allocator = level.memoryMode match { // 确定分配方式
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _ // 堆模式下,分配字节缓冲区
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _ // 非堆模式下,使用直接内存处理
        }
        val= blockData().toChunkedByteBuffer(allocator)
        
        def blockData(): BlockData = diskStore.getBytes(tmpFile, blockSize)
        功能: 获取数据块数据
        
        def saveToDiskStore(): Unit = diskStore.moveFileToBlock(tmpFile, blockSize, blockId)
        功能: 保存到磁盘存储器上
        
        def save(): Boolean
        功能: 保存数据块数据(保存完毕删除临时文件)
        val res = super.save()
        tmpFile.delete()
        val= res
    }
    
    操作集:
    def initialize(appId: String): Unit
    功能: 使用给定的应用编号@appId 初始化数据块管理器,不会表现在构造器中,因为应用编号@appId 不会再数据块管理器初始化时间获取(尤其是驱动器,只能在使用@TaskScheduler注册之后得知).
    这个方法初始化了@BlockTransferService 和@BlockStoreClient,使用@BlockManagerMaster注册,启动@BlockManagerWorker后台,如果配置了本地shuffle服务则还要将本地shuffle服务注册.
    1. 初始化数据块转换服务
    blockTransferService.init(this)
    2. 初始化外部数据块存储客户端
    externalBlockStoreClient.foreach { blockStoreClient =>
      blockStoreClient.init(appId)
    }
    3. 获取数据块备份策略
    blockReplicationPolicy = {
      val priorityClass = conf.get(config.STORAGE_REPLICATION_POLICY)
      val clazz = Utils.classForName(priorityClass)
      val ret = clazz.getConstructor().newInstance().asInstanceOf[BlockReplicationPolicy]
      logInfo(s"Using $priorityClass for block replication policy")
      ret
    }
    4. 获取数据块管理标识符,并注册
    val id =
      BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)
    val idFromMaster = master.registerBlockManager(
      id,
      diskBlockManager.localDirsString,
      maxOnHeapMemory,
      maxOffHeapMemory,
      slaveEndpoint)
    blockManagerId = if (idFromMaster != null) idFromMaster else id // 尽可能需要在master状态下的标识符
    5. 获取shuffle服务编号,并注册
    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }
    6. 获取本地目录管理器
    hostLocalDirManager =
      if (conf.get(config.SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED)) {
        externalBlockStoreClient.map { blockStoreClient =>
          new HostLocalDirManager(
            futureExecutionContext,
            conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE),
            blockStoreClient,
            blockManagerId.host,
            externalShuffleServicePort)
        }
      } else {
        None
      }
    logInfo(s"Initialized BlockManager: $blockManagerId")
    
    def shuffleMetricsSource: Source
    功能: 获取shuffle的度量资源(外部shuffle/非外部shuffle(netty数据块转换))
    if (externalShuffleServiceEnabled) {
      new ShuffleMetricsSource("ExternalShuffle", blockStoreClient.shuffleMetrics())
    } else {
      new ShuffleMetricsSource("NettyBlockTransfer", blockStoreClient.shuffleMetrics())
    }
    
    def registerWithExternalShuffleServer(): Unit 
    功能: 使用外部shuffle服务器注册
    1. 获取shuffle的配置参数
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirsString,
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)
    2. 周期性的向shuffle服务器发起请求,注册shuffle服务
    val MAX_ATTEMPTS = conf.get(config.SHUFFLE_REGISTRATION_MAX_ATTEMPTS)
    val SLEEP_TIME_SECS = 5
    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        blockStoreClient.asInstanceOf[ExternalBlockStoreClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000L)
        case NonFatal(e) =>
          throw new SparkException("Unable to register with external shuffle server due to : " +
            e.getMessage, e)
      }
    }
    
    def reportAllBlocks(): Unit
    功能: 汇报所有的数据块给数据块管理器,如果数据块管理器丢数据了,这个操作是必须的.或者在执行器宕机之后需要汇报数据块消息的时候,这个操作也是必须的.
    这个函数在master返回false的时候(表明从节点需要重新注册),会在后台特意的失败.错误条件会通过心跳信息,新建数据块注册,尝试重新注册所有数据块而断开连接.
    1. 汇报数据块管理器大小给master
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    2. 检测是否含有不能汇报给master的状态,有的话则显示出来,并结束
    for ((blockId, info) <- blockInfoManager.entries) {
      val status = getCurrentBlockStatus(blockId, info)
      if (info.tellMaster && !tryToReportBlockStatus(blockId, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
    
    def reregister(): Unit
    功能: 使用master重新注册,并汇报所有的数据块到master上,这个会被心跳线程调用.(如果心跳表明当前数据块没有注册到管理器的话)
    1. 向master注册数据块管理器
    logInfo(s"BlockManager $blockManagerId re-registering with master")
    master.registerBlockManager(blockManagerId, diskBlockManager.localDirsString, maxOnHeapMemory,
      maxOffHeapMemory, slaveEndpoint)
    2. 汇报所有的数据块
    reportAllBlocks()
    
    def asyncReregister(): Unit
    功能: 使用master重新注册(同步注册)
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] { // 异步注册任务
          // 这个是个阻塞动作，需要使用线程池@futureExecutionContext运行
          reregister()
          asyncReregisterLock.synchronized {
              // 双重检定,重置注册任务
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
    
    def waitForAsyncReregister(): Unit 
    功能: 等待待定的异步重新注册执行,否则什么都不做,测试可见
    val task = asyncReregisterTask
    if (task != null) {
      try {
        ThreadUtils.awaitReady(task, Duration.Inf)
      } catch {
        case NonFatal(t) =>
          throw new Exception("Error occurred while waiting for async. reregistration", t)
      }
    }
    
    def getHostLocalShuffleData(
      blockId: BlockId,
      dirs: Array[String]): ManagedBuffer
    功能: 获取本地shuffle数据
    输入参数:
    	dirs	指定的目录列表
    val= shuffleManager.shuffleBlockResolver.getBlockData(blockId, Some(dirs))
    
    def getLocalBlockData(blockId: BlockId): ManagedBuffer
    功能: 获取本地数据块信息
    val= if (blockId.isShuffle) {
      shuffleManager.shuffleBlockResolver.getBlockData(blockId)
    } else {
      getLocalBytes(blockId) match {
        case Some(blockData) =>
          new BlockManagerManagedBuffer(blockInfoManager, blockId, blockData, true)
        case None =>
          reportBlockStatus(blockId, BlockStatus.empty)
          throw new BlockNotFoundException(blockId.toString)
      }
    }
    
    def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean
    功能: 使用指定的存储等级,本地存放数据块
    val= putBytes(blockId, new ChunkedByteBuffer(data.nioByteBuffer()), level)(classTag)
    
    def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]): StreamCallbackWithID
    功能: 流式存储数据块
    1. 获取文件通道
    val (_, tmpFile) = diskBlockManager.createTempLocalBlock()
    val channel = new CountingWritableChannel(
      Channels.newChannel(serializerManager.wrapForEncryption(new FileOutputStream(tmpFile))))
    logTrace(s"Streaming block $blockId to tmp file $tmpFile")
    2. 获取带有回调函数的流
    val= new StreamCallbackWithID {
      override def getID: String = blockId.name
      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        while (buf.hasRemaining) {
          channel.write(buf)
        }
      }
      override def onComplete(streamId: String): Unit = {
        logTrace(s"Done receiving block $blockId, now putting into local blockManager")
        channel.close()
        val blockSize = channel.getCount
        TempFileBasedBlockStoreUpdater(blockId, level, classTag, tmpFile, blockSize).save()
      }
      override def onFailure(streamId: String, cause: Throwable): Unit = {
        channel.close()
        tmpFile.delete()
      }
    }
    
    def getStatus(blockId: BlockId): Option[BlockStatus]
    功能: 获取指定数据块对应的数据块状态@BlockStatus
    val= blockInfoManager.get(blockId).map { info =>
      // 确定内存空间大小
      val memSize = 
        if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      // 确定磁盘空间大小
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
    
    def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] 
    功能: 获取存在数据块的数据块编号列表(满足指定条件@filter),注意到这个会查询存储在磁盘数据块(数据块管理器可能不知道数据块的类型)
    val= (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
    
    def reportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit
    功能: 汇报数据块信息
    向master汇报指定数据块存储状态,会发送一个数据块更新消息,不是需要的存储等级,例如@MEMORY_AND_DISK的数据块可能仅仅落在磁盘上.抛弃的内存大小@droppedMemorySize 用于计算内存转换到磁盘的内存量.保证master的更新会弥补slave节点中的内存增长(slave内存增长-> master 内存数据转换到磁盘)
    输入参数:
    status	数据块状态
    droppedMemorySize	master端需要转换到磁盘的内存量
    0. 确定数据块是否需要注册
    val needReregister = !tryToReportBlockStatus(blockId, status, droppedMemorySize)
    1. 重新进行数据块的注册,并异步汇报新的数据块
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      asyncReregister()
    }
    2. 显示信息
    logDebug(s"Told master about block $blockId")
    
    def tryToReportBlockStatus(
      blockId: BlockId,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean
    功能: 尝试汇报数据块@blockId状态@status,返回master的响应,如果是true则表明,master成功接收到更新信息.如果是false表明,slave需要重新注册.
    1. 确定内存存储空间和磁盘存储空间
    val storageLevel = status.storageLevel
    val inMemSize = Math.max(status.memSize, droppedMemorySize)
    val onDiskSize = status.diskSize
    2. master端更新数据块,并返回更新的响应(成功与否)
    val= master.updateBlockInfo(
        blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
    
    def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus
    功能: 获取当前数据块状态
    返回给定数据块@blockId的更新状态@BlockStatus,如果释放内存存储并将其存储到磁盘中的时候,返回存储等级和相应的内存和磁盘存储空间大小.
    val= info.synchronized {
      info.level match {
        case null =>
          BlockStatus.empty
        case level =>
          // 确定存储等级需要的参数
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem  || onDisk) level.replication else 1
          val storageLevel = StorageLevel(
            useDisk = onDisk,
            useMemory = inMem,
            useOffHeap = level.useOffHeap,
            deserialized = deserialized,
            replication = replication)
          // 统计内存/磁盘存储量
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize)
      }
    }	
    
    def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]]
    功能: 获取数据块列表的存储位置列表
    val startTimeNs = System.nanoTime()
    val locations = master.getLocations(blockIds).toArray
    logDebug(s"Got multiple block location in ${Utils.getUsedTimeNs(startTimeNs)}")
    val= locations
    
    def handleLocalReadFailure(blockId: BlockId): Nothing
    功能: 处理本地读取失败,清空代码用于回应失败的本地读取,必须持有数据块读取锁才可以读取,当然在这里处理失败逻辑需要释放掉这个锁.
    1. 释放数据块读取锁
    releaseLock(blockId)
    2. 移除处理失败的数据块
    removeBlock(blockId)
    throw new SparkException(s"Block $blockId was not found even though it's read-locked")
    
    def getLocalValues(blockId: BlockId): Option[BlockResult] 
    功能: 从本地数据块管理器获取指定数据块@blockId
    0. 起始日志显示
    logDebug(s"Getting local block $blockId")
    1. 读取指定数据块,并进行处理
    blockInfoManager.lockForReading(blockId) match {
      case None => // 没有读取到数据块
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) => // 读取当数据块信息@BlockInfo
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        val taskContext = Option(TaskContext.get())
        if (level.useMemory && memoryStore.contains(blockId)) {
            // 存储等级为内存,获取内存相关的数据块结果@BlockResult信息
          val iter: Iterator[Any] = if (level.deserialized) {
            memoryStore.getValues(blockId).get
          } else {
            serializerManager.dataDeserializeStream(
              blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iter, {
            releaseLock(blockId, taskContext)
          })
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) {
            // 处理存储在磁盘的数据块结果@BlockResult
          val diskData = diskStore.getBytes(blockId)
          val iterToReturn: Iterator[Any] = {
            if (level.deserialized) {
              val diskValues = serializerManager.dataDeserializeStream(
                blockId,
                diskData.toInputStream())(info.classTag)
              maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
            } else {
              val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
                .map { _.toInputStream(dispose = false) }
                .getOrElse { diskData.toInputStream() }
              serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
            }
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {
            releaseLockAndDispose(blockId, diskData, taskContext)
          })
          Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
        } else {
            // 获取失败处理
          handleLocalReadFailure(blockId)
        }
    }
    
    def getLocalBytes(blockId: BlockId): Option[BlockData]
    功能: 获取本地数据块内容@BlockData
    logDebug(s"Getting local block $blockId as bytes")
    assert(!blockId.isShuffle, s"Unexpected ShuffleBlockId $blockId")
    val=blockInfoManager.lockForReading(blockId).map{info=>doGetLocalBytes(blockId,info)}
    
    def doGetLocalBytes(blockId: BlockId, info: BlockInfo): BlockData
    功能: 获取本地数据块管理器的数据,必须持有读取锁才可以访问,保持直到执行成功,除非出现异常处理需要释放锁.
    1. 获取存储等级
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    2. 尝试按照内存-> 磁盘的顺序读取数据,返回序列化的内存对象.如果数据块不存在抛出异常
    if (level.deserialized) {
        //通过读取磁盘上预先序列化的副本，避免消耗过大的序列化
      if (level.useDisk && diskStore.contains(blockId)) {
        // 注意: 不需要再这里将数据块放回内存,因为数据块可能仅仅的存储到内存中,没有进行序列化,不需要将反序列化的结果缓存到内存中,因为没有什么好处,反倒占了内存
        diskStore.getBytes(blockId)
      } else if (level.useMemory && memoryStore.contains(blockId)) {
        // 数据块在磁盘上找不到,序列化一个内存副本
        new ByteBufferBlockData(serializerManager.dataSerializeWithExplicitClassTag(
          blockId, memoryStore.getValues(blockId).get, info.classTag), true)
      } else {
        // 获取数据块失败,处理失败情况
        handleLocalReadFailure(blockId)
      }
    } else {  // storage level is serialized
        // 存储等级经过了序列化
      if (level.useMemory && memoryStore.contains(blockId)) {
        // 数据块处于内存中
        new ByteBufferBlockData(memoryStore.getBytes(blockId).get, false)
      } else if (level.useDisk && diskStore.contains(blockId)) {
        // 数据块处于磁盘中,name需要尽可能的将磁盘数据缓存到内存中
        val diskData = diskStore.getBytes(blockId)
        maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
          .map(new ByteBufferBlockData(_, false))
          .getOrElse(diskData)
      } else {
        handleLocalReadFailure(blockId)
      }
    }
    
    def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult]
    功能: 获取远端数据块的value@BlockResult
    val ct = implicitly[ClassTag[T]]
    val= getRemoteBlock(blockId, (data: ManagedBuffer) => {
      val values =
        serializerManager.dataDeserializeStream(blockId, data.createInputStream())(ct)
      new BlockResult(values, DataReadMethod.Network, data.size)
    })
    
    def getRemoteBlock[T](
      blockId: BlockId,
      bufferTransformer: ManagedBuffer => T): Option[T]
    功能: 获取远端数据块@blockId,且使用转换函数@bufferTransformer 将其转换为指定数据类型
    0. 数据块校验
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")
    1. 由于远端数据块都注册到驱动器中了,所以没有必要询问slave数据块的位置,只需要从驱动器中获取这个注册位置即可.
    val locationsAndStatusOption = master.getLocationsAndStatus(
        blockId, blockManagerId.host)
    2. 获取远端数据结果
    if (locationsAndStatusOption.isEmpty) {
      logDebug(s"Block $blockId is unknown by block manager master")
      None
    } else {
      // 确定远端数据元数据信息(文件大小/文件状态)
      val locationsAndStatus = locationsAndStatusOption.get
      val blockSize = locationsAndStatus.status.
        diskSize.max(locationsAndStatus.status.memSize)
      // 根据远端文件状态的本地目录,获取数据块数据@BlockData,并进行转换@bufferTransformer 获取结果
      locationsAndStatus.localDirs.flatMap { localDirs =>
        val blockDataOption =
          readDiskBlockFromSameHostExecutor(
              blockId, localDirs, locationsAndStatus.status.diskSize)
        val res = blockDataOption.flatMap { blockData =>
          try {
            Some(bufferTransformer(blockData))
          } catch {
            case NonFatal(e) =>
              logDebug("Block from the same host executor cannot be opened: ", e)
              None
          }
        }
        logInfo(s"Read $blockId from the disk of a same host executor is " +
          (if (res.isDefined) "successful." else "failed."))
        res
      }.orElse {// 本地目录表中记录为空,则必须从远端管理器中获取数据块才行,效率低,需要进过网络传输
        fetchRemoteManagedBuffer(
            blockId, blockSize, locationsAndStatus).map(bufferTransformer)
      }
    }
    
    def preferExecutors(locations: Seq[BlockManagerId]): Seq[BlockManagerId]
	功能: 获取之前数据块管理器@BlockManagerId 的最佳执行位置
    1. 分割执行器和shuffle服务
    val (executors, shuffleServers) = locations.partition(
        _.port != externalShuffleServicePort)
    2. 合并两部分内容并返回
    val= executors ++ shuffleServers
    
    def sortLocations(locations: Seq[BlockManagerId]): Seq[BlockManagerId]
    功能: 对给定数据块位置进行排序,优先使用本地机器.因为多个数据块管理器可以共享一台主机,然后多台主机可以共享一台机架.在每个原则信息(同主机,同机架,其他)在外部shuffle中也是需要使用的.
    1. 位置随机
    val locs = Random.shuffle(locations)
    2. 安装是否为本机进行分类
    val (preferredLocs, otherLocs) = locs.partition(_.host == blockManagerId.host)
    3. 再安装本地-> 同一个机架 -> 其他关键字进行排序
    val orderedParts = blockManagerId.topologyInfo match {
      case None => Seq(preferredLocs, otherLocs)
      case Some(_) =>
        val (sameRackLocs, differentRackLocs) = otherLocs.partition {
          loc => blockManagerId.topologyInfo == loc.topologyInfo
        }
        Seq(preferredLocs, sameRackLocs, differentRackLocs)
    }
    4. 按照执行位置的顺序,进行聚合
    val= orderedParts.map(preferExecutors).reduce(_ ++ _) // 安装@preferExecutors 聚合
    
    def fetchRemoteManagedBuffer(
      blockId: BlockId,
      blockSize: Long,
      locationsAndStatus: BlockManagerMessages.BlockLocationsAndStatus):
    Option[ManagedBuffer]
    功能: 获取远端数据块(以@ManagedBuffer 形式)
    1. 如果数据块大小超出了容量,需要将@FileManger 传递给数据块传输服务@BlockTransferService,这个会平衡溢写的数据块,如果不这样做会返回一个空值,表示数据块已经持久化到内存中.
    val tempFileManager = if (blockSize > maxRemoteBlockToMem) {
      remoteBlockTempFileManager // 用于平衡溢写的数据块
    } else {
      null // 表示持久化到内存中
    }
    2. 确定元数据信息和容错参数
    var runningFailureCount = 0 
    var totalFailureCount = 0
    val locations = sortLocations(locationsAndStatus.locations)
    val maxFetchFailures = locations.size
    var locationIterator = locations.iterator
    3. 计算数据块数据
    while (locationIterator.hasNext) {
      // 获取缓冲数据并返回
      val loc = locationIterator.next()
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        val buf = blockTransferService.fetchBlockSync(loc.host, loc.port, loc.executorId,
          blockId.toString, tempFileManager)
        if (blockSize > 0 && buf.size() == 0) {
          throw new IllegalStateException("Empty buffer received for non empty block")
        }
        buf
      } catch {
        case NonFatal(e) =>
          runningFailureCount += 1
          totalFailureCount += 1
          if (totalFailureCount >= maxFetchFailures) {
              // 容错超限处理
            logWarning(s"Failed to fetch block after $totalFailureCount
            fetch failures. " +
              s"Most recent failure cause:", e)
            return None
          }
          logWarning(s"Failed to fetch remote block $blockId " +
            s"from $loc (failed attempt $runningFailureCount)", e)
          // 容错超限处理
          if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
            locationIterator = sortLocations(master.getLocations(blockId)).iterator
            logDebug(s"Refreshed locations from the driver " +
              s"after ${runningFailureCount} fetch failures.")
            runningFailureCount = 0
          }
          null
      }
      if (data != null) {
        // 如果@ManagedBuffer是@BlockManagerManagedBuffer,name读取数据之后就会将其返回,在这种情		// 况下仅仅从远端获取结果,所以这里对类型进行了断言
        assert(!data.isInstanceOf[BlockManagerManagedBuffer])
        return Some(data)
      }
      logDebug(s"The value of block $blockId is null")
    }
    4.缺省值处理
    logDebug(s"Block $blockId not found")
    val= None
    
    def readDiskBlockFromSameHostExecutor(
      blockId: BlockId,
      localDirs: Array[String],
      blockSize: Long): Option[ManagedBuffer]
    功能: 从同一个主机执行器读取磁盘数据块
    1. 获取指定目录@localDirs的文件列表
    val file = ExecutorDiskUtils.getFile(localDirs, subDirsPerLocalDir, blockId.name)
    2. 读取数据对应的数据块数据,使用@ManagedBuffer 返回
    val= if (file.exists()) {
      val mangedBuffer = securityManager.getIOEncryptionKey() match {
        case Some(key) =>
          new EncryptedManagedBuffer(
            new EncryptedBlockData(file, blockSize, conf, key))
        case _ =>
          val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
          new FileSegmentManagedBuffer(transportConf, file, 0, file.length)
      }
      Some(mangedBuffer)
    } else {
      None
    }
    
    def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] 
    功能: 获取远端数据,以@ChunkedByteBuffer 字节缓冲区形式存储
    getRemoteBlock(blockId, (data: ManagedBuffer) => {
      if (remoteReadNioBufferConversion) {
        new ChunkedByteBuffer(data.nioByteBuffer())
      } else {
        ChunkedByteBuffer.fromManagedBuffer(data)
      }
    })
    
    def get[T: ClassTag](blockId: BlockId): Option[BlockResult] 
    功能: 从数据块管理器中获取数据块(本地/远端),如果是本地文件系统需要获取读取锁才可以进行,只有全部读取完毕才可以释放锁.
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemoteValues[T](blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
    
    def downgradeLock(blockId: BlockId): Unit
    功能: 将写排它锁降级为读取共享锁
    blockInfoManager.downgradeLock(blockId)
    
    def releaseLock(blockId: BlockId, taskContext: Option[TaskContext] = None): Unit
    功能: 释放指定数据块的读取锁,@taskContext在获取不到正确的任务上下文时需要传入
     val taskAttemptId = taskContext.map(_.taskAttemptId())
    // 注意到,当任务完成时候,spark会释放所有任务数据块的锁.如果@isCompleted=true表示不能再释放锁了
    // 参考SPARK-27666
    if (taskContext.isDefined && taskContext.get.isCompleted) {
      logWarning(s"Task ${taskAttemptId.get} already completed, 
      not releasing lock for $blockId")
    } else {
      blockInfoManager.unlock(blockId, taskAttemptId)
    }
    
    def registerTask(taskAttemptId: Long): Unit 
    功能: 使用数据块管理器注册指定任务
    blockInfoManager.registerTask(taskAttemptId)
    
    def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] 
    功能: 释放指定任务的所有锁,返回释放锁的数据块
    val= blockInfoManager.releaseAllLocksForTask(taskAttemptId)
    
    def getOrElseUpdate[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]]
    功能: 检索指定数据块,否则使用@makeIterator计算数据块,持久化(更新),并返回value值
    1. 尝试获取数据块内容
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
        // Need to compute the block.
    }
    2. 计算为空的时候的数据块内容,并持久化
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
      case None =>
        val blockResult = getLocalValues(blockId).getOrElse {
          releaseLock(blockId)
          throw new SparkException(s"get() failed for block $blockId 
          even though we held a lock")
        }
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) =>
       Right(iter)
    }
    
    def putIterator[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean 
    功能: 对指定数据块@blockId 使用values进行持久化,成功则返回true
    0. 参数校验
    require(values != null, "Values is null")
    1. 数据块持久化
    doPutIterator(blockId, () => values, level, implicitly[ClassTag[T]], tellMaster)	 	 match {
      case None =>
        true
      case Some(iter) =>
        iter.close()
        false
    }
    
    def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetricsReporter): DiskBlockObjectWriter
    功能: 获取指定数据块的磁盘写出器
    1. 确定是否需要同步写出(默认false,异步)
    val syncWrites = conf.get(config.SHUFFLE_SYNC)
    val= new DiskBlockObjectWriter(file, serializerManager, serializerInstance, bufferSize,syncWrites, writeMetrics, blockId)
    
    def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_],
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T]
    功能: 持久化指定数据块@blockId,使用指定函数@putBody 函数进行持久化
    1. 参数校验
    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    2. 确定持久化数据块的数据块信息@BlockInfo
    val putBlockInfo = {
      val newInfo = new BlockInfo(level, classTag, tellMaster)
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) {
          releaseLock(blockId)
        }
        return None
      }
    }
    3. 持久化数据,并返回持久化的结果
    val startTimeNs = System.nanoTime()
    var exceptionWasThrown: Boolean = true
    val result: Option[T] = try {
      val res = putBody(putBlockInfo)
      exceptionWasThrown = false
      if (res.isEmpty) {
        // the block was successfully stored
        if (keepReadLock) {
          blockInfoManager.downgradeLock(blockId)
        } else {
          blockInfoManager.unlock(blockId)
        }
      } else {
        removeBlockInternal(blockId, tellMaster = false)
        logWarning(s"Putting block $blockId failed")
      }
      res
    } catch {
      case NonFatal(e) =>
        logWarning(s"Putting block $blockId failed due to exception $e.")
        throw e
    } finally {
      if (exceptionWasThrown) { // 持久化失败处理
        removeBlockInternal(blockId, tellMaster = tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
      }
    }
    4. 备份参数显示
    val usedTimeMs = Utils.getUsedTimeNs(startTimeNs)
    if (level.replication > 1) {
      logDebug(s"Putting block ${blockId} with replication took $usedTimeMs")
    } else {
      logDebug(s"Putting block ${blockId} without replication took ${usedTimeMs}")
    }
    val= result
    
    def putBytes[T: ClassTag](
     blockId: BlockId,
     bytes: ChunkedByteBuffer,
     level: StorageLevel,
     tellMaster: Boolean = true): Boolean
    功能: 将指定数据缓冲区的数据@bytes 存储到数据块中
    1. 更新数据块存储情况
    val blockStoreUpdater =
      ByteBufferBlockStoreUpdater(
          blockId, level, implicitly[ClassTag[T]], bytes, tellMaster)
    2. 保存更新
    blockStoreUpdater.save()
    
    def doPutIterator[T](
      blockId: BlockId,
      iterator: () => Iterator[T],
      level: StorageLevel,
      classTag: ClassTag[T],
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Option[PartiallyUnrolledIterator[T]]
    功能: 存储迭代器信息@iterator,可以进行必要的备份,如果数据块已经存在,那么这个方法不会覆盖
    如果数据块存在或者存储成功,那么返回None,如果存储失败则返回@Some(iterator)
    doPut(
        blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { 	  info =>
      val startTimeNs = System.nanoTime()
      var iteratorFromFailedMemoryStorePut: Option[PartiallyUnrolledIterator[T]] = None
      var size = 0L
      if (level.useMemory) {.
        if (level.deserialized) {
          memoryStore.putIteratorAsValues(blockId, iterator(), classTag) match {
            case Right(s) =>
              size = s
            case Left(iter) =>
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  serializerManager.dataSerializeStream(blockId, out, iter)(classTag)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut = Some(iter)
              }
          }
        } else {
          memoryStore.putIteratorAsBytes(
              blockId, iterator(), classTag, level.memoryMode) match {
            case Right(s) =>
              size = s
            case Left(partiallySerializedValues) =>
              if (level.useDisk) {
                logWarning(s"Persisting block $blockId to disk instead.")
                diskStore.put(blockId) { channel =>
                  val out = Channels.newOutputStream(channel)
                  partiallySerializedValues.finishWritingToStream(out)
                }
                size = diskStore.getSize(blockId)
              } else {
                iteratorFromFailedMemoryStorePut =
                  Some(partiallySerializedValues.valuesIterator)
              }
          }
        }
      } else if (level.useDisk) {
        diskStore.put(blockId) { channel =>
          val out = Channels.newOutputStream(channel)
          serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
        }
        size = diskStore.getSize(blockId)
      }
      val putBlockStatus = getCurrentBlockStatus(blockId, info)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        info.size = size
        if (tellMaster && info.tellMaster) {
          reportBlockStatus(blockId, putBlockStatus)
        }
        addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
        logDebug(s"Put block $blockId locally took ${Utils.getUsedTimeNs(startTimeNs)}")
        if (level.replication > 1) {
          val remoteStartTimeNs = System.nanoTime()
          val bytesToReplicate = doGetLocalBytes(blockId, info)
          val remoteClassTag = if (!serializerManager.canUseKryo(classTag)) {
            scala.reflect.classTag[Any]
          } else {
            classTag
          }
          try {
            replicate(blockId, bytesToReplicate, level, remoteClassTag)
          } finally {
            bytesToReplicate.dispose()
          }
          logDebug(s"Put block $blockId remotely took
          ${Utils.getUsedTimeNs(remoteStartTimeNs)}")
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
    }
    
    def maybeCacheDiskBytesInMemory(
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskData: BlockData): Option[ChunkedByteBuffer]
    功能: 将磁盘移除数据缓存到内存中,为了加速读取,这个方法需要调用者持有读取锁
    返回: 存储成功则返回内存存储器的副本数据,否则为None
    1. 存储等级断言
    require(!level.deserialized)
    2. 请求获取磁盘移除数据对应内存存储器副本
    if (level.useMemory) {
      // 使用@blockInfo用于避免两个读取线程间的竞争条件(竞争读取磁盘内容写到内存中)
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          diskData.dispose()
          Some(memoryStore.getBytes(blockId).get)
        } else {
          val allocator = level.memoryMode match {
            case MemoryMode.ON_HEAP => ByteBuffer.allocate _
            case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
          }
          val putSucceeded = memoryStore.putBytes(
              blockId, diskData.size, level.memoryMode, () => {
            diskData.toChunkedByteBuffer(allocator)
          })
          if (putSucceeded) {
            diskData.dispose()
            Some(memoryStore.getBytes(blockId).get)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
    
    def maybeCacheDiskValuesInMemory[T](
      blockInfo: BlockInfo,
      blockId: BlockId,
      level: StorageLevel,
      diskIterator: Iterator[T]): Iterator[T]
    功能: 同上,但是返回的是迭代器的副本(返回后原先数据将不存在)
    1. 参数校验
    require(level.deserialized)
    2. 将磁盘数据持久化到内存中,并返回
    if (level.useMemory) {
        // 需要同步读取数据信息,避免线程竞争
      blockInfo.synchronized {
        if (memoryStore.contains(blockId)) {
          memoryStore.getValues(blockId).get
        } else {
          memoryStore.putIteratorAsValues(blockId, diskIterator, classTag) match {
            case Left(iter) =>
              iter
            case Right(_) =>
              memoryStore.getValues(blockId).get
          }
        }
      }.asInstanceOf[Iterator[T]]
    } else {
      diskIterator
    }
    
    def getPeers(forceFetch: Boolean): Seq[BlockManagerId]
    功能: 获取系统中同一个等级的数据块管理器
    val= peerFetchLock.synchronized {
      val cachedPeersTtl = conf.get(config.STORAGE_CACHED_PEERS_TTL) // milliseconds
      val diff = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastPeerFetchTimeNs)
      val timeout = diff > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTimeNs = System.nanoTime()
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
    
    def replicateBlock(
      blockId: BlockId,
      existingReplicas: Set[BlockManagerId],
      maxReplicas: Int): Unit
    功能: 备份指定数据块
    输入参数:
    existingReplicas	存在有副本的数据块管理器标识符集合
    maxReplicas	最大失败次数
    logInfo(s"Using $blockManagerId to pro-actively replicate $blockId")
    val= blockInfoManager.lockForReading(blockId).foreach { info =>
      val data = doGetLocalBytes(blockId, info)
      val storageLevel = StorageLevel(
        useDisk = info.level.useDisk,
        useMemory = info.level.useMemory,
        useOffHeap = info.level.useOffHeap,
        deserialized = info.level.deserialized,
        replication = maxReplicas)
      getPeers(forceFetch = true)
      try {
        replicate(blockId, data, storageLevel, info.classTag, existingReplicas)
      } finally {
        logDebug(s"Releasing lock for $blockId")
        releaseLockAndDispose(blockId, data)
      }
    }
    
    def replicate(
      blockId: BlockId,
      data: BlockData,
      level: StorageLevel,
      classTag: ClassTag[_],
      existingReplicas: Set[BlockManagerId] = Set.empty): Unit
    功能: 将数据块备份到其他节点上,注意这个是阻塞调用,数据块备份完毕之后才会返回
    1. 获取备份参数
    val maxReplicationFailures = conf.get(config.STORAGE_MAX_REPLICATION_FAILURE)
    val tLevel = StorageLevel(
      useDisk = level.useDisk,
      useMemory = level.useMemory,
      useOffHeap = level.useOffHeap,
      deserialized = level.deserialized,
      replication = 1)
    val numPeersToReplicateTo = level.replication - 1
    val startTime = System.nanoTime
    2. 获取需要备份的节点参数
    val peersReplicatedTo = mutable.HashSet.empty ++ existingReplicas
    val peersFailedToReplicateTo = mutable.HashSet.empty[BlockManagerId]
    var numFailures = 0
    val initialPeers = getPeers(false).filterNot(existingReplicas.contains)
    var peersForReplication = blockReplicationPolicy.prioritize(
      blockManagerId,
      initialPeers,
      peersReplicatedTo,
      blockId,
      numPeersToReplicateTo)
    3. 备份数据块数据(带容错)
    while(numFailures <= maxReplicationFailures &&
      !peersForReplication.isEmpty &&
      peersReplicatedTo.size < numPeersToReplicateTo) {
      val peer = peersForReplication.head
      try {
        val onePeerStartTime = System.nanoTime
        logTrace(s"Trying to replicate $blockId of ${data.size} bytes to $peer")
        val buffer = new BlockManagerManagedBuffer(
            blockInfoManager, blockId, data, false,unlockOnDeallocate = false)
        blockTransferService.uploadBlockSync(
          peer.host,
          peer.port,
          peer.executorId,
          blockId,
          buffer,
          tLevel,
          classTag)
        logTrace(s"Replicated $blockId of ${data.size} bytes to $peer" +
          s" in ${(System.nanoTime - onePeerStartTime).toDouble / 1e6} ms")
        peersForReplication = peersForReplication.tail
        peersReplicatedTo += peer
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to replicate $blockId to $peer, failure #$numFailures", e)
          peersFailedToReplicateTo += peer
          val filteredPeers = getPeers(true).filter { p =>
            !peersFailedToReplicateTo.contains(p) && !peersReplicatedTo.contains(p)
          }
          numFailures += 1
          peersForReplication = blockReplicationPolicy.prioritize(
            blockManagerId,
            filteredPeers,
            peersReplicatedTo,
            blockId,
            numPeersToReplicateTo - peersReplicatedTo.size)
      }
    }
    4. 显示备份相关信息
    logDebug(s"Replicating $blockId of ${data.size} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took 
      ${(System.nanoTime - startTime) / 1e6} ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }
    logDebug(s"block $blockId replicated to ${peersReplicatedTo.mkString(", ")}")
    
    def getSingle[T: ClassTag](blockId: BlockId): Option[T]
    功能: 读取包含单个对象的数据块(读取数据块-> 转换为类型T)
    val= get[T](blockId).map(_.data.next().asInstanceOf[T])
    
    def putSingle[T: ClassTag](
      blockId: BlockId,
      value: T,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean 
    功能: 写入一个@value 到指定数据块中
    val= putIterator(blockId, Iterator(value), level, tellMaster)
    
    def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
    功能: 从内存中移除指定数据块,如果磁盘可以接受就存放到磁盘中,在内存容量不足的时候调用.用于释放内存.
    如果@data 没有存放到磁盘上,那么数据就不会被创建,这个方法调用者必须持有一个可写锁.这个方法不会释放这个写锁.
    1. 如果可以释放的数据块存储到数据块上
    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level
    if (level.useDisk && !diskStore.contains(blockId)) {
      logInfo(s"Writing block $blockId to disk")
      data() match {
        case Left(elements) =>
          diskStore.put(blockId) { channel =>
            val out = Channels.newOutputStream(channel)
            serializerManager.dataSerializeStream(
              blockId,
              out,
              elements.toIterator)(info.classTag.asInstanceOf[ClassTag[T]])
          }
        case Right(bytes) =>
          diskStore.putBytes(blockId, bytes)
      }
      blockIsUpdated = true
    }
    2. 从内存区域移除数据块
    val droppedMemorySize =
      if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
    val blockIsRemoved = memoryStore.remove(blockId)
    if (blockIsRemoved) {
      blockIsUpdated = true
    } else {
      logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
    }
    3. 获取数据块的存储等级,并返回
    val status = getCurrentBlockStatus(blockId, info)
    if (info.tellMaster) {
      reportBlockStatus(blockId, status, droppedMemorySize)
    }
    if (blockIsUpdated) {
      addUpdatedBlockStatusToTaskMetrics(blockId, status)
    }
    val= status.storageLevel
    
    def removeRdd(rddId: Int): Int
    功能: 移除指定RDD的数据块
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfoManager.entries.flatMap(
        _._1.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    val= blocksToRemove.size
    
    def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int
    功能: 移除指定广播变量的数据块
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    val= blocksToRemove.size
    
    def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit
    功能: 从内存和磁盘上移除指定数据块
    logDebug(s"Removing block $blockId")
    blockInfoManager.lockForWriting(blockId) match {
      case None =>
        logWarning(s"Asked to remove block $blockId, which does not exist")
      case Some(info) =>
        removeBlockInternal(blockId, tellMaster = tellMaster && info.tellMaster)
        addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
    }
 
    def removeBlockInternal(blockId: BlockId, tellMaster: Boolean): Unit
    功能: 内部移除数据块@blockId,假定调用者持有了写出锁
    1. 获取移除数据块的状态
     val blockStatus = if (tellMaster) {
      val blockInfo = blockInfoManager.assertBlockIsLockedForWriting(blockId)
      Some(getCurrentBlockStatus(blockId, blockInfo))
    } else None
    2. 确定移除数据块的大小(内存,磁盘)
    val removedFromMemory = memoryStore.remove(blockId)
    val removedFromDisk = diskStore.remove(blockId)
    if (!removedFromMemory && !removedFromDisk) {
      logWarning(s"Block $blockId could not be removed as 
      it was not found on disk or in memory")
    }
    3. 移除数据块
    blockInfoManager.removeBlock(blockId)
    if (tellMaster) {
      reportBlockStatus(blockId, blockStatus.get.copy(storageLevel = StorageLevel.NONE))
    }
    
    def addUpdatedBlockStatusToTaskMetrics(blockId: BlockId, status: BlockStatus): Unit
    功能: 添加更新的数据块到任务度量器中
    if (conf.get(config.TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES)) {
      Option(TaskContext.get()).foreach { c =>
        c.taskMetrics().incUpdatedBlockStatuses(blockId -> status)
      }
    }
    
    def releaseLockAndDispose(
      blockId: BlockId,
      data: BlockData,
      taskContext: Option[TaskContext] = None): Unit
    功能: 释放锁并暴露数据
    releaseLock(blockId, taskContext)
    data.dispose()
    
    def stop(): Unit
    功能: 停止数据块管理器
    blockTransferService.close()
    if (blockStoreClient ne blockTransferService) {
      blockStoreClient.close()
    }
    remoteBlockTempFileManager.stop()
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
}
```

```scala
private[spark] object BlockManager {
    属性:
    #name @ID_GENERATOR = new IdGenerator	ID生成器
    操作集:
    def blockIdsToLocations(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]]
    功能: 获取数据块--> 执行位置映射表
    0. 参数断言
    assert(env != null || blockManagerMaster != null)
    1. 获取指定数据块的执行位置列表
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }
    2. 形成映射关系,并返回
    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map { loc =>
        ExecutorCacheTaskLocation(loc.host, loc.executorId).toString
      }
    }
    val= blockManagers.toMap
    
    内部类:
    private class ShuffleMetricsSource(
      override val sourceName: String,
      metricSet: MetricSet) extends Source {
        介绍: shuffle度量资源
        属性:
        #name @metricRegistry = new MetricRegistry	度量注册器
        初始化操作:
        metricRegistry.registerAll(metricSet)
        功能: 注册度量集@metricSet
    }
    
    class RemoteBlockDownloadFileManager(blockManager: BlockManager)
    extends DownloadFileManager with Logging {
        介绍: 远端数据块下载管理器
        属性:
        #name @encryptionKey = SparkEnv.get.securityManager.getIOEncryptionKey()
        	加密密钥
        #name @referenceQueue = new JReferenceQueue[DownloadFile]	下载文件引用队列
        #name @referenceBuffer	引用缓冲区
        val= Collections.newSetFromMap[ReferenceWithCleanup](new ConcurrentHashMap)
        #name @POLL_TIMEOUT = 1000	轮询周期
        #name @stopped = false	轮询周期 volatile
        #name @cleaningThread 清理线程
        val= new Thread() { override def run(): Unit = { keepCleaning() } }
        初始化操作:
        cleaningThread.setDaemon(true)
        cleaningThread.setName("RemoteBlock-temp-file-clean-thread")
        cleaningThread.start()
        功能: 清理线程设置
        
        def createTempFile(transportConf: TransportConf): DownloadFile 
        功能: 创建临时文件
        1. 获取需要创建的文件
        val file = blockManager.diskBlockManager.createTempLocalBlock()._2
        2. 对文件进行加密
        val= encryptionKey match {
            case Some(key) =>
            new EncryptedDownloadFile(file, key)
            case None =>
            new SimpleDownloadFile(file, transportConf)
        }
        
        def registerTempFileToClean(file: DownloadFile): Boolean 
        功能: 注册需要清理的临时文件
        referenceBuffer.add(new ReferenceWithCleanup(file, referenceQueue))
        
        def stop(): Unit
        功能: 停止远端数据块管理器
        stopped = true
        cleaningThread.interrupt()
        cleaningThread.join()
        
        def keepCleaning(): Unit 
        功能: 周期性的处理对文件引用队列的清除
        while (!stopped) {
            try {
              Option(referenceQueue.remove(POLL_TIMEOUT))
                .map(_.asInstanceOf[ReferenceWithCleanup])
                .foreach { ref =>
                  referenceBuffer.remove(ref)
                  ref.cleanUp()
                }
            } catch {
              case _: InterruptedException =>
                // no-op
              case NonFatal(e) =>
                logError("Error in cleaning thread", e)
            }
        }
        
        内部类:
        private class ReferenceWithCleanup(
            file: DownloadFile,
            referenceQueue: JReferenceQueue[DownloadFile]
            ) extends WeakReference[DownloadFile](file, referenceQueue) {
            介绍: 清除引用(弱引用)
            属性:
            #name @filePath = file.path()	文件路径
            def cleanUp(): Unit
            功能: 清空文件
            logDebug(s"Clean up file $filePath")
            if (!file.delete()) {
              logDebug(s"Fail to delete file $filePath")
            }
        }
    }
}
```

```scala
private class EncryptedDownloadFile(
      file: File,
      key: Array[Byte]) extends DownloadFile {
    介绍: 加密的下载文件
    构造器参数:
    file	文件
    key	加密密钥
    属性:
    #name @env = SparkEnv.get	spark配置
    #name @def delete(): Boolean = file.delete()	删除文件
    操作集:
    def openForWriting(): DownloadFileWritableChannel
    功能: 获取可写的文件通道
    val= new EncryptedDownloadWritableChannel()
    
    def path(): String = file.getAbsolutePath
    功能: 获取文件路径
    
    内部类:
    class EncryptedDownloadWritableChannel extends DownloadFileWritableChannel{
        介绍: 可写文件通道
        #name @countingOutput: CountingWritableChannel	输出通道
        val= new CountingWritableChannel(
        	Channels.newChannel(
            env.serializerManager.wrapForEncryption(new FileOutputStream(file))))
        操作集:
        def closeAndRead(): ManagedBuffer
        功能: 关闭写出同步,并开启读取,提供相应的缓冲区供读取
        countingOutput.close()
        val size = countingOutput.getCount
        new EncryptedManagedBuffer(new EncryptedBlockData(file, size, env.conf, key))
        
        def write(src: ByteBuffer): Int = countingOutput.write(src)
        功能: 写出指定内容@src
        
        def isOpen: Boolean = countingOutput.isOpen()
        功能: 确定通道是否启动
        
        def close(): Unit = countingOutput.close()
        功能: 关闭通道
    }
}
```



#### BlockManagerId

```markdown
介绍:
	块管理器标识,这个类代表块管理器@BlockManager 的唯一标识
	前两个构造器是私有的,保证了其只能使用apply方法去获取实例,允许对象的复制.此外构造器的参数是私有的,保证了参数不能在类的外部修改.
```

```scala
@DeveloperApi
class BlockManagerId private (
    private var executorId_ : String,
    private var host_ : String,
    private var port_ : Int,
    private var topologyInfo_ : Option[String])
extends Externalizable {
    构造器参数:
    	executorId_	执行器ID
    	host_	主机名
    	port_	端口号
    	topologyInfo_	拓扑信息
    初始化操作:
    private def this() = this(null, null, 0, None) 
    功能: 内部使用构造器,只能通过apply获取,只能使用与反序列化
    
   	if (null != host_) {
        Utils.checkHost(host_)
        assert (port_ > 0)
      }
    功能: host ,port参数合法性检查
    
    操作集:
    def executorId: String = executorId_
    功能: 获取执行器id
    
    def hostPort: String 
    功能: 获取主机端口信息
    0. 参数合法性检测
    Utils.checkHost(host)
    assert (port > 0)
    val= host + ":" + port
    
    def host: String = host_
    功能: 获取主机名称
    
    def port: Int = port_
    功能: 获取端口号
    
    def topologyInfo: Option[String] = topologyInfo_
    功能: 获取拓扑信息
    
    def isDriver: Boolean
    功能: 确认是否为驱动器
    val= executorId == SparkContext.DRIVER_IDENTIFIER
    
    def toString: String = s"BlockManagerId($executorId, $host, $port, $topologyInfo)"
    功能: 信息显示
    
    def hashCode: Int
    功能: 获取hashcode
    val= ((executorId.hashCode * 41 + host.hashCode) * 41 + port) * 41 + topologyInfo.hashCode
    
    def equals(that: Any): Boolean
    功能: 比较两个实例是否相等
    val= that match {
    case id: BlockManagerId =>
      	executorId == id.executorId &&
        port == id.port &&
        host == id.host &&
        topologyInfo == id.topologyInfo
        case _ =>
          false
      }
    
    def readExternal(in: ObjectInput): Unit
    功能: 读取外部实例,并按顺序赋值给本类
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
    val isTopologyInfoAvailable = in.readBoolean()
    topologyInfo_ = if (isTopologyInfoAvailable) Option(in.readUTF()) else None
    
    def writeExternal(out: ObjectOutput): Unit 
    功能: 写出当前属性
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
    out.writeBoolean(topologyInfo_.isDefined)
    topologyInfo.foreach(out.writeUTF(_)) // 只有拓扑信息存在时才写出
}
```

```scala
private[spark] object BlockManagerId {
    属性:
    #name @blockManagerIdCache #Type @CacheBuilder	块管理器标识符缓存
    val= CacheBuilder.newBuilder().maximumSize(10000).
    	build(new CacheLoader[BlockManagerId, BlockManagerId]() {
            override def load(id: BlockManagerId) = id
        }
    操作集:
    def getCachedBlockManagerId(id: BlockManagerId): BlockManagerId
    功能: 获取缓冲块管理器的标识符
    val= blockManagerIdCache.get(id)
              
    def apply(in: ObjectInput): BlockManagerId
    功能: 从输入中获取一个块管理器标识实例
    val obj = new BlockManagerId()
    obj.readExternal(in)
    val= getCachedBlockManagerId(obj)
              
    def apply(execId: String,host: String,port: Int,
      topologyInfo: Option[String] = None): BlockManagerId
    功能: 获取指定参数对应的块管理器标识@BlockManagerId
}
```

#### BlockManagerManagedBuffer

```markdown
介绍:
	块管理器管理的缓冲区.这个类是由管理缓冲区@ManagedBuffer 包装了来自块管理器的块数据@BlockData 域而形成的.这样的目的是,一旦缓冲区的锁释放了,对应的数据读取锁也就被释放了.
	如果处理@dispose 标记为true，当缓冲区计数指针等于0时。就会释放掉块数据的引用。
    这是一种对块管理器读取锁的概念和网络层保持和释放计数的沟通桥梁。
```

```scala
private[storage] class BlockManagerManagedBuffer(
    blockInfoManager: BlockInfoManager,
    blockId: BlockId,
    data: BlockData,
    dispose: Boolean,
unlockOnDeallocate: Boolean = true) extends ManagedBuffer {
    构造器参数:
    blockInfoManager	块信息管理器
    blockId		块管理器
    data	数据块
    dispose	是否需要处理标记
    unlockOnDeallocate	解除分配的锁是否打开(打开为true)
    属性:
    #name @refCount = new AtomicInteger(1)	参考计数
    操作集
    def size(): Long = data.size
    功能: 获取规模大小
    
    def nioByteBuffer(): ByteBuffer = data.toByteBuffer()
    功能: 获取数据对应的字节缓冲区
    
    def createInputStream(): InputStream = data.toInputStream()
    功能: 创建数据@data块对应的输入流
    
    def convertToNetty(): Object = data.toNetty()
    功能: 将数据块转化为netty对象
    
    def retain(): ManagedBuffer
    功能: 数据的留存(对读取进行上锁)
    refCount.incrementAndGet()
    val locked = blockInfoManager.lockForReading(blockId, blocking = false)
    assert(locked.isDefined)
    val= this
    
    def release(): ManagedBuffer
    功能: 释放读取锁
    if (unlockOnDeallocate) {
      blockInfoManager.unlock(blockId)
    }
    if (refCount.decrementAndGet() == 0 && dispose) {
      data.dispose()
    }
    val= this
}
```

#### BlockManagerMaster

```scala
private[spark] class BlockManagerMaster(
    var driverEndpoint: RpcEndpointRef,
    var driverHeartbeatEndPoint: RpcEndpointRef,
    conf: SparkConf,
    isDriver: Boolean)
extends Logging {
    构造器参数: 
    	driverEndpoint	驱动器RPC端点
    	driverHeartbeatEndPoint	驱动器心跳接受端点参考
    	conf	spark配置
    	isDriver	是否为驱动器
    属性:
    #name @timeout = RpcUtils.askRpcTimeout(conf)	RPC请求时间上限
    操作集:
    def removeExecutor(execId: String): Unit
    功能: 移除指定执行器编号@execId 对应的执行器(这个执行器是死亡状态,且只能在driver侧调用)
    1. 向master端点发送一个移除指定@execId执行器的消息
    tell(RemoveExecutor(execId)) // 发送的是单程消息,不需要对端发送回调
    logInfo("Removed " + execId + " successfully in removeExecutor")
    
    def removeExecutorAsync(execId: String): Unit
    功能: 从driver端请求移除死亡的执行器,只能在驱动器侧调用,异步执行
    1. driver侧发送一个移除执行器的消息
    driverEndpoint.ask[Boolean](RemoveExecutor(execId))
    logInfo("Removal of executor " + execId + " requested")
    
    def getLocations(blockId: BlockId): Seq[BlockManagerId]
    功能: 从driver端获取指定数据块@blockId 所处于的位置列表
    val= driverEndpoint.askSync[Seq[BlockManagerId]](GetLocations(blockId))
    
    def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]]
    功能: 获取多个块数据@blockIds所属的位置列表
    val= driverEndpoint.askSync[IndexedSeq[Seq[BlockManagerId]]](
     	 GetLocationsMultipleBlockIds(blockIds))
    
    def contains(blockId: BlockId): Boolean
    功能: 确定块管理器主管理器是否含有指定块@blockId
    val= !getLocations(blockId).isEmpty
    
    def getLocationsAndStatus(blockId: BlockId,
      requesterHost: String): Option[BlockLocationsAndStatus]
    功能: 从driver端获取指定数据块@blockId的位置和状态信息
    val= driverEndpoint.askSync[Option[BlockLocationsAndStatus]](
        GetLocationsAndStatus(blockId, requesterHost))
    
    def updateBlockInfo(blockManagerId: BlockManagerId,blockId: BlockId,storageLevel: StorageLevel,memSize: Long,diskSize: Long): Boolean
    功能: 更新块信息
    输入参数:
    	blockManagerId	块管理器标识符
    	blockId	块标识符
    	storageLevel	存储等级
    	memSize	内存大小
    	diskSize	磁盘大小
    1. driver端发起更新数据块消息
    val res = driverEndpoint.askSync[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    2. 返回更新消息结果
    logDebug(s"Updated info of block $blockId")
    val= res
    
    def registerBlockManager(id: BlockManagerId,localDirs: Array[String],
      maxOnHeapMemSize: Long,maxOffHeapMemSize: Long,
      slaveEndpoint: RpcEndpointRef): BlockManagerId 
    功能: 注册块管理器,输入的块管理器标识符不包含拓扑信息.这个拓扑信息从master中获取,使用这条信息,使用一个更新的管理器标识符@BlockManagerId所谓回应
    输入参数:
    	BlockManagerId	块管理器标识符
    	localDirs	本地目录列表
    	maxOnHeapMemSize	最大堆上内存
    	maxOffHeapMemSize	最大非堆模式下内存
    	slaveEndpoint	从RPC端点
    1. 发起注册指定块管理器消息
    val updatedId = driverEndpoint.askSync[BlockManagerId](
      RegisterBlockManager(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))
    2. 返回注册的块管理器
    val= updatedId
    
    def removeBlock(blockId: BlockId): Unit
	功能: 移除指定数据块@blockId(driver发送消息,从端点中移除指定数据块)
    val= driverEndpoint.askSync[Boolean](RemoveBlock(blockId))
    
    def removeRdd(rddId: Int, blocking: Boolean): Unit
	功能: 移除指定rdd的所有数据块
    输入参数:
    	rddId	rdd编号
    	blocking	是否阻塞执行
    1. driver向执行器发送移除RDD消息,获取执行任务
    val future = driverEndpoint.askSync[Future[Seq[Int]]](RemoveRdd(rddId))
    2. 失败处理
    future.failed.foreach(e =>
      logWarning(s"Failed to remove RDD $rddId - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    3. 进行可能的同步操作
    if (blocking) {
      timeout.awaitResult(future)
    }
    
    def removeShuffle(shuffleId: Int, blocking: Boolean): Unit
    功能: 移除指定shuffle中所有的数据块
    1. driver端向执行器同步发送移除指定shuffle中数据块消息,获取执行任务
    val future = driverEndpoint.askSync[Future[Seq[Boolean]]](RemoveShuffle(shuffleId))
    2. 执行失败处理
    future.failed.foreach(e =>
      logWarning(s"Failed to remove shuffle $shuffleId - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    3. 进行可能的同步操作
    if (blocking) {
      timeout.awaitResult(future)
    }
    
    def removeBroadcast(broadcastId: Long, removeFromMaster: Boolean, blocking: Boolean): Unit
    功能: 移除指定广播变量中所有的数据块
    1. RPC请求移除线程
    val future = driverEndpoint.askSync[Future[Seq[Int]]](
      RemoveBroadcast(broadcastId, removeFromMaster))
    2. 失败处理
    future.failed.foreach(e =>
      logWarning(s"Failed to remove broadcast $broadcastId" +
        s" with removeFromMaster = $removeFromMaster - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    3. 进行可能的同步操作
    if (blocking) {
      timeout.awaitResult(future)
    }
    
    def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef]
    功能: 获取执行器RPC端点参考
    1. driver端同步方式发送消息并获取结果
    val= driverEndpoint.askSync[Option[RpcEndpointRef]](GetExecutorEndpointRef(executorId))
    
    def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId]
    功能: 获取集群中其他节点列表
    val= driverEndpoint.askSync[Seq[BlockManagerId]](GetPeers(blockManagerId))
    
    def getMemoryStatus: Map[BlockManagerId, (Long, Long)] 
    功能: 获取每个块管理器的内存状态,value的二元组表示最大内存量和剩余内存量
    val= driverEndpoint.askSync[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
    
    def getStorageStatus: Array[StorageStatus]
    功能: 获取存储状态列表
    val= driverEndpoint.askSync[Array[StorageStatus]](GetStorageStatus)
    
    def getMatchingBlockIds(filter: BlockId => Boolean,askSlaves: Boolean): Seq[BlockId]
	功能:获取匹配的数据块列表
    输入参数:
    	filter	块过滤函数
    	askSlaves	是否询问从节点(设置true,就会使用master去询问每个块管理器,在主块管理器没有被其他块管理器通知时很有用)
    1. 获取消息格式
    val msg = GetMatchingBlockIds(filter, askSlaves)
    2. 发送消息获取执行结果
    val future = driverEndpoint.askSync[Future[Seq[BlockId]]](msg)
    timeout.awaitResult(future)
    
    def tell(message: Any): Unit
    功能: 发送单程消息给master端点,这里需要返回true
    if (!driverEndpoint.askSync[Boolean](message)) {
      throw new SparkException("BlockManagerMasterEndpoint returned false, expected true.")
    }
    
    def getBlockStatus(blockId: BlockId,askSlaves: Boolean = true): Map[BlockManagerId, BlockStatus]
	功能: 获取块状态列表信息
    1. 构建获取块状态的消息
    val msg = GetBlockStatus(blockId, askSlaves)
    2. 获取远端响应(这里必须要使用任务形式,否则会造成两端资源互斥,可能产生死锁)
    val response = driverEndpoint.
      askSync[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
    val (blockManagerIds, futures) = response.unzip
    3. 获取远端传输过来的数据,构建块状态列表
    implicit val sameThread = ThreadUtils.sameThread
    val cbf =
      implicitly[
        CanBuildFrom[Iterable[Future[Option[BlockStatus]]],
        Option[BlockStatus],
        Iterable[Option[BlockStatus]]]]
    val blockStatus = timeout.awaitResult(
      Future.sequence(futures)(cbf, ThreadUtils.sameThread))
    4. 结果校验和转换
    if (blockStatus == null) {
      throw new SparkException("BlockManager returned null for BlockStatus query: " + blockId)
    }
    val= blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
      status.map { s => (blockManagerId, s) }
    }.toMap
    
    def stop(): Unit 
    功能: 停止driver的端点,只有在driver节点上才可以使用
    if (driverEndpoint != null && isDriver) {
      tell(StopBlockManagerMaster)
      driverEndpoint = null
      if (driverHeartbeatEndPoint.askSync[Boolean](StopBlockManagerMaster)) {
        driverHeartbeatEndPoint = null
      } else {
        logWarning("Failed to stop BlockManagerMasterHeartbeatEndpoint")
      }
      logInfo("BlockManagerMaster stopped")
    }
}
```

```scala
private[spark] object BlockManagerMaster {
    属性:
    #name @DRIVER_ENDPOINT_NAME = "BlockManagerMaster"	driver端点名称
    #name @DRIVER_HEARTBEAT_ENDPOINT_NAME = "BlockManagerMasterHeartbeat"	driver侧心跳端点名称
}
```

#### BlockManagerMasterEndpoint

```scala
private[spark] class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,	
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    externalBlockStoreClient: Option[ExternalBlockStoreClient],
    blockManagerInfo: mutable.Map[BlockManagerId, BlockManagerInfo])
extends IsolatedRpcEndpoint with Logging {
    介绍: 块管理器主管理器端点是master节点的隔离RPC端点@IsolatedRpcEndpoint,用于定位所有从节点(slaves)的块管理器.
    构造器属性:
        rpcEnv	RPC环境参数
        isLocal	是否处于本地
        conf	spark配置
        listenerBus	监听总线
        externalBlockStoreClient	外部块存储客户端
        blockManagerInfo	块管理器映射表
    属性:
    #name @executorIdToLocalDirs #Type @CacheBuilder	执行器映射本地目录
    val= CacheBuilder
      .newBuilder()
      .maximumSize(conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE))
      .build[String, Array[String]]()
    #name @blockStatusByShuffleService #Type @HashMap 块管理器管理ID映射外部shuffle服务(块编号映射块状态)
    val= new mutable.HashMap[BlockManagerId, JHashMap[BlockId, BlockStatus]]
    #name @blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]
    执行器ID映射块管理器标识符
    #name @blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]
    块标识符映射其包含的该数据块的集合
    #name @askThreadPool #Type @ThreadPoolExecutor	块管理器询问线程池
    val= ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool", 100)
    #name @askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool) 询问执行器上下文
    #name @topologyMapper #Type @TopologyMapper	拓扑关系映射
    val= {
        val topologyMapperClassName = conf.get(
          config.STORAGE_REPLICATION_TOPOLOGY_MAPPER)
        val clazz = Utils.classForName(topologyMapperClassName)
        val mapper =
          clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
        logInfo(s"Using $topologyMapperClassName for getting topology information")
        mapper
      }
    #name @defaultRpcTimeout = RpcUtils.askRpcTimeout(conf)	默认RPC访问时间上限
    #name @proactivelyReplicate = conf.get(config.STORAGE_REPLICATION_PROACTIVE)	是否主动备份
    #name @externalShuffleServicePort: Int = StorageUtils.externalShuffleServicePort(conf)
    外部shuffle端口
    #name @externalShuffleServiceRddFetchEnabled: Boolean = externalBlockStoreClient.isDefined
    是否允许外部shuffle获取RDD
    操作集:
    def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
    功能: 接受和回应RPC消息
    // 回应注册块管理器消息
    case RegisterBlockManager(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint) =>
      context.reply(register(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))
    // 监听更新块信息,并发送监听消息到监听总线上
    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      context.reply(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size))
      listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))
    // 回复获取位置消息
    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))
    // 回复位置消息和状态消息
    case GetLocationsAndStatus(blockId, requesterHost) =>
      context.reply(getLocationsAndStatus(blockId, requesterHost))
    // 回复多个数据块列表的消息
    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))
    // 回复节点消息
    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))
    // 回去执行器端点参考消息
    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))
    // 回复内存状态消息
    case GetMemoryStatus =>
      context.reply(memoryStatus)
    // 回复存储状态消息
    case GetStorageStatus =>
      context.reply(storageStatus)
    // 回复数据块状态消息
    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))
    // 回复执行器存活状态消息
    case IsExecutorAlive(executorId) =>
      context.reply(blockManagerIdByExecutor.contains(executorId))
    // 回复满足指定条件的数据块列表
    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))
    // 回复移除指定RDD消息
    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))
    // 回复移除指定shuffle消息
    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))
    // 回复移除指定广播变量消息
    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))
    // 回复移除数据块消息
    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)
    // 回复指定执行器消息
    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)
    // 回复停止块管理器主管理器消息
    case StopBlockManagerMaster =>
      context.reply(true)
      stop()
    
    def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] 
    功能: 移除指定shuffle任务
    1. 获取移除消息
    val removeMsg = RemoveShuffle(shuffleId)
    2. 获取一个任务,主体为从从端点中移除指定shuffle
    val= Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg)
      }.toSeq
    )
    
    def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]]
    功能: 移除广播变量,需要指定是否从driver端移除
    1. 获取移除消息
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    2. 获取需要移除数据块
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    3. 从从端点中移除
    val futures = requiredBlockManagers.map { bm =>
      bm.slaveEndpoint.ask[Int](removeMsg).recover {
        case e: IOException =>
          logWarning(s"Error trying to remove broadcast $broadcastId from block manager " +
            s"${bm.blockManagerId}", e)
          0 
      }
    }.toSeq
    val= Future.sequence(futures)
    
    def removeExecutor(execId: String): Unit
    功能: 移除指定执行器
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
    
    def removeBlockFromWorkers(blockId: BlockId): Unit
    功能: 从worker中移除数据块
    1. 获取块管理器列表
    val locations = blockLocations.get(blockId)
    2. 向从端点发送移除数据块消息
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId))
        }
      }
    }
    
    def memoryStatus: Map[BlockManagerId, (Long, Long)]
    功能: 获取内存状态列表(最大内存,剩余内存的二元组列表)
    val= blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
    
    def storageStatus: Array[StorageStatus]
    功能: 获取存储状态列表
    val= blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, Some(info.maxOnHeapMem),
        Some(info.maxOffHeapMem), info.blocks.asScala)
    }.toArray
    
    def getMatchingBlockIds(filter: BlockId => Boolean,askSlaves: Boolean): Future[Seq[BlockId]]
    功能: 获取符合过滤条件的数据块列表,同样的为了避免同步执行出现死锁,返回@Future
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    val= Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
    
    def externalShuffleServiceIdOnHost(blockManagerId: BlockManagerId): BlockManagerId 
    功能: 获取主机上外部shuffle服务的块管理器标识符@BlockManagerId
    val= BlockManagerId(blockManagerId.executorId, blockManagerId.host, externalShuffleServicePort)
    
    def blockStatus(blockId: BlockId,askSlaves: Boolean):
    	Map[BlockManagerId, Future[Option[BlockStatus]]]
    功能: 获取指定数据块@blockId 所有数据块的块管理器,这个操作时需要很大的开销,只限于测试
    如果允许询问slave,主节点就会询问每个块管理器,获取最新的更新块状态.在master没有收到其他管理器的消息很有效.
    1. 获取指定数据块的块状态
    val getBlockStatus = GetBlockStatus(blockId)
    2. 获取数据块的状态列表
    由于阻塞式访问可能会导致死锁,所以只需要获取简单的任务@Future 即可
    val= blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
    
    def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]]
    
    def getMatchingBlockIds(filter: BlockId => Boolean,askSlaves: Boolean): Future[Seq[BlockId]]
    功能: 获取符合过滤条件的数据块列表
    
    def register(
      idWithoutTopologyInfo: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      slaveEndpoint: RpcEndpointRef): BlockManagerId
    功能: 有可能的话,使用携带有拓扑信息的块管理器标识符@BlockManagerId
    1. 获取块管理器编号
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host))
    2. 注册当前块管理器标识符@BlockManagerId 到执行器映射本地目录表中@executorIdToLocalDirs
    val time = System.currentTimeMillis()
    executorIdToLocalDirs.put(id.executorId, localDirs)
    3. 向执行器映射块管理标识符@blockManagerIdByExecutor 注册信息
    if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxOnHeapMemSize + maxOffHeapMemSize), id))
      blockManagerIdByExecutor(id.executorId) = id
      val externalShuffleServiceBlockStatus =
        if (externalShuffleServiceRddFetchEnabled) {
          val externalShuffleServiceBlocks = blockStatusByShuffleService
            .getOrElseUpdate(externalShuffleServiceIdOnHost(id), new JHashMap[BlockId, BlockStatus])
          Some(externalShuffleServiceBlocks)
        } else {
          None
        }
      blockManagerInfo(id) = new BlockManagerInfo(id, System.currentTimeMillis(),
        maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint, externalShuffleServiceBlockStatus)
    }
    4. 向监听总线发送消息
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxOnHeapMemSize + maxOffHeapMemSize,
        Some(maxOnHeapMemSize), Some(maxOffHeapMemSize)))
    val= id
    
    def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean
    功能: 更新数据块信息
    0. 无法寻找当前块管理器标识符@blockManagerId 处理
    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
		// 在master端不注册块信息(除非是运行在本地模式下)
        return true
      } else {
        return false
      }
    }
    1. 不存在指定的块标识符的处理
    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }
    2. 更新块管理器信息
    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)
    3. 获取块管理器列表
    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }
    4. 根据存储等级的合法性,判断是否可以将当前块管理标识符置入
    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }
    5. 确定是否可以引入外部shuffle服务
    if (blockId.isRDD && storageLevel.useDisk && externalShuffleServiceRddFetchEnabled) {
      val externalShuffleServiceId = externalShuffleServiceIdOnHost(blockManagerId)
      if (storageLevel.isValid) {
        locations.add(externalShuffleServiceId)
      } else {
        locations.remove(externalShuffleServiceId)
      }
    }
    6. 移除条目判断
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    val= true
    
    def getLocations(blockId: BlockId): Seq[BlockManagerId]
    功能: 获取当前数据块@blockId 所属位置列表@Seq[BlockManagerId]
    val= if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
    
    def getLocationsAndStatus(blockId: BlockId,requesterHost: String):
    	Option[BlockLocationsAndStatus]
    功能: 获取当前数据块@blockId 的位置信息以及其状态信息@BlockLocationsAndStatus
    1. 获取位置信息
    val locations = Option(blockLocations.get(blockId)).map(_.toSeq).getOrElse(Seq.empty)
    2. 获取状态信息
    val status = locations.headOption.flatMap { bmId =>
      if (externalShuffleServiceRddFetchEnabled && bmId.port == externalShuffleServicePort) {
        Option(blockStatusByShuffleService(bmId).get(blockId))
      } else {
        blockManagerInfo.get(bmId).flatMap(_.getStatus(blockId))
      }
    }
    3. 获取数据块位置和状态实例
    if (locations.nonEmpty && status.isDefined) {
      val localDirs = locations.find { loc =>
        loc.host == requesterHost &&
          (loc.port == externalShuffleServicePort ||
            blockManagerInfo
              .get(loc)
              .flatMap(_.getStatus(blockId).map(_.storageLevel.useDisk))
              .getOrElse(false))
      }.flatMap { bmId => Option(executorIdToLocalDirs.getIfPresent(bmId.executorId)) }
      Some(BlockLocationsAndStatus(locations, status.get, localDirs))
    } else {
      None
    }
    
    def getLocationsMultipleBlockIds(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]]
    功能: 获取当前数据块列表@blockIds 对于的数据块位置列表
    val= blockIds.map(blockId => getLocations(blockId))
    
    def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] 
    功能: 获取节点列表
    val blockManagerIds = blockManagerInfo.keySet
    val= if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
    
    def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] 
    功能: 获取执行器端点参考
    val= for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.slaveEndpoint
    }
    
    def onStop(): Unit 
    功能: 停止操作
    askThreadPool.shutdownNow()
    
    def removeBlockManager(blockManagerId: BlockManagerId): Unit
    功能: 移除块管理器
    1. 从执行器映射块管理器表中移除当前块管理器中移除指定执行器
    blockManagerIdByExecutor -= blockManagerId.executorId
    2. 从块管理信息中移除
    blockManagerInfo.remove(blockManagerId)
    3. 获取块标识符迭代器,迭代移除
    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId) // 获取位置列表信息
      locations -= blockManagerId // 移除需要溢出的块管理标识符@BlockManagerId
      if (locations.size == 0) {
        blockLocations.remove(blockId) // 移除块标识符记录
        logWarning(s"No more replicas available for $blockId !")
      } else if (proactivelyReplicate && (blockId.isRDD || blockId.isInstanceOf[TestBlockId])) {
          // 有备份,且当前数据块是RDD类型数据块
        val maxReplicas = locations.size + 1
        val i = (new Random(blockId.hashCode)).nextInt(locations.size)
        val blockLocations = locations.toSeq
        val candidateBMId = blockLocations(i)
        blockManagerInfo.get(candidateBMId).foreach { bm =>
          val remainingLocations = locations.toSeq.filter(bm => bm != candidateBMId)
          val replicateMsg = ReplicateBlock(blockId, remainingLocations, maxReplicas)
          bm.slaveEndpoint.ask[Boolean](replicateMsg)
        }
      }
   4. 发送监听消息
   listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))        
}
```

```scala
@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  介绍: 块数据状态
  构造器参数:
    storageLevel	存储等级
    memSize	内存大小
    diskSize	磁盘大小
  def isCached: Boolean = memSize + diskSize > 0
  功能: 确认是否存储存在缓存
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
  功能: 获取空数据块的状态
}
```

```scala
private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxOnHeapMem: Long,
    val maxOffHeapMem: Long,
    val slaveEndpoint: RpcEndpointRef,
    val externalShuffleServiceBlockStatus: Option[JHashMap[BlockId, BlockStatus]])
extends Logging{
    介绍: 块管理器信息	
    构造器参数:
        blockManagerId	块管理器标识符
        timeMs	时间(ms)
        maxOnHeapMem	最大堆内存
        maxOffHeapMem	最大非堆模式内存
        slaveEndpoint	RPC端点参考
        externalShuffleServiceBlockStatus	外部shuffle块状态信息映射表
    属性:
    #name @maxMem = maxOnHeapMem + maxOffHeapMem	最大内存
    #name @externalShuffleServiceEnabled = externalShuffleServiceBlockStatus.isDefined	
    	是否允许外部shuffle
    #name @_lastSeenMs: Long = timeMs	上次使用时间
    #name @_remainingMem: Long = maxMem	剩余内存
    #name @_blocks = new JHashMap[BlockId, BlockStatus]	数据块状态映射表
    操作集:
    def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))
    功能: 获取指定数据块的数据块状态
    
    def updateLastSeenMs(): Unit = { _lastSeenMs = System.currentTimeMillis() }
    功能: 设置使用时间
    
    def remainingMem: Long = _remainingMem
    功能: 获取剩余时间
    
    def lastSeenMs: Long = _lastSeenMs
    功能: 获取上次使用时间
    
    def blocks: JHashMap[BlockId, BlockStatus] = _blocks
    功能: 获取数据块映射数据块状态表
    
    def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem
    功能: 信息显示
    
    def clear(): Unit = { _blocks.clear() }
    功能: 清理取数据块映射数据块状态表数据
    
    def removeBlock(blockId: BlockId): Unit
    功能: 移除指定数据块
    if (_blocks.containsKey(blockId)) {
      // 释放数据块占用的内存
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId) // 移除映射条目
      externalShuffleServiceBlockStatus.foreach { blockStatus => // 移除shuffle信息
        blockStatus.remove(blockId)	
      }
    }
    
        def updateBlockInfo(
          blockId: BlockId,
          storageLevel: StorageLevel,
          memSize: Long,
          diskSize: Long): Unit 
    功能: 更新块信息
    1. 更新时间
    updateLastSeenMs()
    2. 获取初始化状态下属性
    val blockExists = _blocks.containsKey(blockId) // 确认映射中是否存在当前数据块信息
    var originalMemSize: Long = 0
    var originalDiskSize: Long = 0
    var originalLevel: StorageLevel = StorageLevel.NONE
    3. 更新初始状态下属性为slave上属性值,当前数据块不存在的情况直接JUMP 7
    if (blockExists) {
      val blockStatus: BlockStatus = _blocks.get(blockId)
      originalLevel = blockStatus.storageLevel
      originalMemSize = blockStatus.memSize
      originalDiskSize = blockStatus.diskSize
      if (originalLevel.useMemory) { // 更新可能使用的内存剩余量
        _remainingMem += originalMemSize
      }
    }
    4. 同时使用磁盘和内存存储的信息写入
    if (storageLevel.isValid) {
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        if (blockExists) {
          logInfo(s"Updated $blockId in memory on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(memSize)}," +
            s" original size: ${Utils.bytesToString(originalMemSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})")
        } else {
          logInfo(s"Added $blockId in memory on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(memSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})")
        }
      }
    }
    5. 使用磁盘存储的块信息存入
    if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        _blocks.put(blockId, blockStatus)
        if (blockExists) {
          logInfo(s"Updated $blockId on disk on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(diskSize)}," +
            s" original size: ${Utils.bytesToString(originalDiskSize)})")
        } else {
          logInfo(s"Added $blockId on disk on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(diskSize)})")
        }
      }
    6. shuffle块信息写入
    externalShuffleServiceBlockStatus.foreach { shuffleServiceBlocks =>
        if (!blockId.isBroadcast && blockStatus.diskSize > 0) {
          shuffleServiceBlocks.put(blockId, blockStatus)
        }
      }
    7. 块不存在的处理
     _blocks.remove(blockId)
      externalShuffleServiceBlockStatus.foreach { blockStatus =>
        blockStatus.remove(blockId)
      }
      if (originalLevel.useMemory) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} in memory" +
          s" (size: ${Utils.bytesToString(originalMemSize)}," +
          s" free: ${Utils.bytesToString(_remainingMem)})")
      }
      if (originalLevel.useDisk) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} on disk" +
          s" (size: ${Utils.bytesToString(originalDiskSize)})")
      }
}
```

#### BlockManagerMasterHeartbeatEndpoint

```markdown
介绍:
	由于性能的原因将心跳从块管理器主管理器中分离
```

```scala
private[spark] class BlockManagerMasterHeartbeatEndpoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean,
        blockManagerInfo: mutable.Map[BlockManagerId, BlockManagerInfo])
: mutable.Map[BlockManagerId, BlockManagerInfo])
extends ThreadSafeRpcEndpoint with Logging {
    构造器参数:
    	rpcEnv	RPC环境参数
    	isLocal	是否处于本地状态
    	blockManagerInfo	块管理器信息
    操作集:
    def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
    功能: 接受远端RPC数据并做出响应
    val= 
        case BlockManagerHeartbeat(blockManagerId) => // 收到心跳信息处理
          context.reply(heartbeatReceived(blockManagerId))
        case StopBlockManagerMaster => // 停止块管理器的处理
          stop()
          context.reply(true)
        case _ => // 其他信息不回应
    
    def heartbeatReceived(blockManagerId: BlockManagerId): Boolean
    功能: 确定指定@blockManagerId 是否接收到了心跳信息(即driver知道这个数据块的存在),返回false则表明需要重新注册
    val= if (!blockManagerInfo.contains(blockManagerId)) {
      // 只有数据块在driver节点上,且位于远端节点上才不需要注册,一般情况下是false
      blockManagerId.isDriver && !isLocal 
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs() // 更新最后一次出现的时间
      true // 表明存在有这个心跳信息
    }
}
```

#### BlockManagerMessages

```scala
private[spark] object BlockManagerMessages {
    
    Part 1:
    介绍: 块管理器消息(消息是从master发送到slaves上的)
    sealed trait ToBlockManagerSlave 
    介绍: 传送到从块管理器上的消息
    
    case class RemoveBlock(blockId: BlockId) extends ToBlockManagerSlave
    介绍:  移除指定@blockId 块的消息
    
    case class ReplicateBlock(blockId: BlockId, replicas: Seq[BlockManagerId], maxReplicas: Int)
    extends ToBlockManagerSlave
    介绍: 由于执行器执行失败而设置的副本块消息
    输入参数:
    	blockId	需要制作副本的数据块标识符
    	replicas	复制数据块标识符列表
    	maxReplicas	最大副本数量
    
    case class RemoveRdd(rddId: Int) extends ToBlockManagerSlave
    介绍: 移除指定RDD消息 
    
    case class RemoveShuffle(shuffleId: Int) extends ToBlockManagerSlave
    介绍: 移除指定shuffle消息
    
    case class RemoveBroadcast(broadcastId: Long, removeFromDriver: Boolean = true)
    extends ToBlockManagerSlave
    介绍: 移除指定广播变量消息,默认需要从driver端移除
    
    case object TriggerThreadDump extends ToBlockManagerSlave
    介绍: 线程转储触发器
    
    Part 2:
    介绍: slave发向master的消息
    sealed trait ToBlockManagerMaster
    介绍: 传送到主管理器的消息
    
    case class RegisterBlockManager(
      blockManagerId: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      sender: RpcEndpointRef)
    extends ToBlockManagerMaster
    介绍: 注册块管理器
    构造器属性:
    	blockManagerId	块管理器编号
    	localDirs	本地目录列表
    	maxOnHeapMemSize	最大堆上内存
    	maxOffHeapMemSize	最大非堆模式内存
    	sender	RPC发送端
    
    case class GetLocations(blockId: BlockId) extends ToBlockManagerMaster
    介绍: 获取位置的消息
    
    case class GetLocationsAndStatus(blockId: BlockId, requesterHost: String)
    extends ToBlockManagerMaster
    介绍: 获取位置和状态的消息
    
    case class GetLocationsMultipleBlockIds(blockIds: Array[BlockId]) extends ToBlockManagerMaster
    介绍: 获取多个块的位置消息
    
    case class GetPeers(blockManagerId: BlockManagerId) extends ToBlockManagerMaster
    介绍: 获取集群中其他节点消息
    
    case class GetExecutorEndpointRef(executorId: String) extends ToBlockManagerMaster
    介绍: 获取执行器RPC端点消息
    
    case class RemoveExecutor(execId: String) extends ToBlockManagerMaster
    介绍: 移除执行器消息
    
    case object StopBlockManagerMaster extends ToBlockManagerMaster
    介绍: 停止块管理器主管理器消息
    
    case object GetMemoryStatus extends ToBlockManagerMaster
    介绍: 获取内存状态消息
    
    case object GetStorageStatus extends ToBlockManagerMaster
    介绍: 获取存储状态消息
    
    case class GetBlockStatus(blockId: BlockId, askSlaves: Boolean = true)
    extends ToBlockManagerMaster
    介绍: 获取块数据状态消息
    
    case class GetMatchingBlockIds(filter: BlockId => Boolean, askSlaves: Boolean = true)
    extends ToBlockManagerMaster
    介绍: 获取匹配与指定规则的块消息列表消息
    
    case class BlockManagerHeartbeat(blockManagerId: BlockManagerId) extends ToBlockManagerMaster
    介绍: 块管理器心跳消息
    
    case class IsExecutorAlive(executorId: String) extends ToBlockManagerMaster
    介绍: 检测执行器存活状态
    
    case class BlockLocationsAndStatus(
      locations: Seq[BlockManagerId],
      status: BlockStatus,
      localDirs: Option[Array[String]]) {
        assert(locations.nonEmpty)
      }
    介绍: 获取块位置和状态的消息
    
    case class UpdateBlockInfo(
      var blockManagerId: BlockManagerId,
      var blockId: BlockId,
      var storageLevel: StorageLevel,
      var memSize: Long,
      var diskSize: Long)
    extends ToBlockManagerMaster
    with Externalizable {
        介绍: 更新块信息的消息
        构造器参数:
            blockManagerId	块管理器标识符
            block	块标识符
            storageLevel	存储等级
            memSize	内存大小
            diskSize	磁盘大小
        初始化操作:
        def this() = this(null, null, null, 0, 0) // 这个构造器只能反序列化
        操作集:
        def writeExternal(out: ObjectOutput): Unit
        功能: 写出内部配置
        blockManagerId.writeExternal(out)
        out.writeUTF(blockId.name)
        storageLevel.writeExternal(out)
        out.writeLong(memSize)
        out.writeLong(diskSize)
        
        def readExternal(in: ObjectInput): Unit
        功能: 读取外部配置
        blockManagerId = BlockManagerId(in)
     	blockId = BlockId(in.readUTF())
        storageLevel = StorageLevel(in)
        memSize = in.readLong()
        diskSize = in.readLong()
    }
}
```

#### BlockManagerSlaveEndpoint

```scala
private[storage] class BlockManagerSlaveEndpoint(
    override val rpcEnv: RpcEnv,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker)
extends IsolatedRpcEndpoint with Logging {
    介绍: 块管理器从管理器端点.RPC端点从主管理器上获取指令,并执行.例如,这是用于移动从管理器的块管理器@BlockManager中移除数据块的动作.
    构造器属性:
    	rpcEnv	RPC环境参数
    	blockManager	块管理器
    	mapOutputTracker	map输出定位器
    属性:
    #name @asyncThreadPool #Type @ThreadPoolExecutor	异步线程池
    val= ThreadUtils.newDaemonCachedThreadPool("block-manager-slave-async-thread-pool", 100)
    #name @asyncExecutionContext #Type @ExecutionContextExecutorService	异步执行器上下文
    val= ExecutionContext.fromExecutorService(asyncThreadPool)
    操作集:
    def onStop(): Unit
    功能: 停止块管理器从端点
    1. 直接停止溢出线程池即可
    asyncThreadPool.shutdownNow()
    
    def doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T): Unit 
    功能: 异步动作
    输入参数:
    	actionMessage	动作消息
    	context	RPC调用上下文
    	body	执行函数
    1. 获取执行任务
    val future = Future {
      logDebug(actionMessage)
      body
    }
    2. 回复响应信息给信息发出器
    future.foreach { response =>
      logDebug(s"Done $actionMessage, response is $response")
      context.reply(response) // 发送响应消息给RPC的sender
      logDebug(s"Sent response: $response to ${context.senderAddress}")
    }
    3. 失败处理
    future.failed.foreach { t =>
      logError(s"Error in $actionMessage", t)
      context.sendFailure(t) // 发送失败信息给RPC的sender
    }
    
    def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
    功能: 由于涉及到移除数据块的动作比较慢,所以使用异步执行方式
    // 移除动作较慢,使用异步执行
    case RemoveBlock(blockId) =>
      doAsync[Boolean]("removing block " + blockId, context) {
        blockManager.removeBlock(blockId)
        true
      }
    case RemoveRdd(rddId) =>
      doAsync[Int]("removing RDD " + rddId, context) {
        blockManager.removeRdd(rddId)
      }
    case RemoveShuffle(shuffleId) =>
      doAsync[Boolean]("removing shuffle " + shuffleId, context) {
        if (mapOutputTracker != null) {
          mapOutputTracker.unregisterShuffle(shuffleId)
        }
        SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
      }
    case RemoveBroadcast(broadcastId, _) =>
      doAsync[Int]("removing broadcast " + broadcastId, context) {
        blockManager.removeBroadcast(broadcastId, tellMaster = true)
      }
	// 使用RPC响应数据
    case GetBlockStatus(blockId, _) =>
      context.reply(blockManager.getStatus(blockId))
    case GetMatchingBlockIds(filter, _) =>
      context.reply(blockManager.getMatchingBlockIds(filter))
    case TriggerThreadDump =>
      context.reply(Utils.getThreadDump())
    case ReplicateBlock(blockId, replicas, maxReplicas) =>
      context.reply(blockManager.replicateBlock(blockId, replicas.toSet, maxReplicas))
}
```

#### BlockManagerSource

```scala
private[spark] class BlockManagerSource(val blockManager: BlockManager) extends Source {
    介绍: 块管理资源类
    构造器参数:
    	blockManager	块管理器
    属性:
    #name @metricRegistry = new MetricRegistry()	度量注册器
    #name @sourceName = "BlockManager"	资源名称
    操作集:
    def registerGauge(name: String, func: BlockManagerMaster => Long): Unit
    功能: 注册指定名称@name 的计量器
    输入参数:
    	name	计量器名称
    	func	度量转换函数(函数值为字节数)
    metricRegistry.register(name, new Gauge[Long] {
      // 获取度量值(MB)
      override def getValue: Long = func(blockManager.master) / 1024 / 1024
    })
    初始化操作:
    registerGauge(MetricRegistry.name("memory", "maxMem_MB"),
    _.getStorageStatus.map(_.maxMem).sum)
	功能: 注册最大内存值
    
  	registerGauge(MetricRegistry.name("memory", "maxOnHeapMem_MB"),
    _.getStorageStatus.map(_.maxOnHeapMem.getOrElse(0L)).sum)
	功能: 注册最大堆上内存
    
    registerGauge(MetricRegistry.name("memory", "maxOffHeapMem_MB"),
    _.getStorageStatus.map(_.maxOffHeapMem.getOrElse(0L)).sum)
	功能: 注册最大非堆模式下内存
    
    registerGauge(MetricRegistry.name("memory", "remainingMem_MB"),
    _.getStorageStatus.map(_.memRemaining).sum)
	功能: 注册剩余内存量
    
    registerGauge(MetricRegistry.name("memory", "remainingOnHeapMem_MB"),
    _.getStorageStatus.map(_.onHeapMemRemaining.getOrElse(0L)).sum)
	功能: 注册剩余堆上内存
    
    registerGauge(MetricRegistry.name("memory", "remainingOffHeapMem_MB"),
    _.getStorageStatus.map(_.offHeapMemRemaining.getOrElse(0L)).sum)
	功能: 注册剩余非堆模式下内存
    
    registerGauge(MetricRegistry.name("memory", "memUsed_MB"),
    _.getStorageStatus.map(_.memUsed).sum)
	功能: 注册内存使用量
    
    registerGauge(MetricRegistry.name("memory", "onHeapMemUsed_MB"),
    _.getStorageStatus.map(_.onHeapMemUsed.getOrElse(0L)).sum)
	功能: 注册堆上内存使用量
    
    registerGauge(MetricRegistry.name("memory", "offHeapMemUsed_MB"),
    _.getStorageStatus.map(_.offHeapMemUsed.getOrElse(0L)).sum)
	功能: 注册非堆模式下内存使用量
    
    registerGauge(MetricRegistry.name("disk", "diskSpaceUsed_MB"),
    _.getStorageStatus.map(_.diskUsed).sum)
    功能: 注册磁盘空间使用量大小
}
```

#### BlockNotFoundException

```scala
class BlockNotFoundException(blockId: String) extends Exception(s"Block $blockId not found")
介绍: 查找失败异常
```

#### BlockReplicationPolicy

```markdown
介绍:
	数据块副本策略
	数据块副本优先级 提供了对节点列表的优先化处理,,主要是用于对数据块的备份.块管理器@BlockManager 会在返回的节点列表中都进行备份,直到达到了需要的副本顺序.如果备份失败,@prioritize() 方法会再次调用,从而获取一个新的优先化策略.
```

```scala
@DeveloperApi
class RandomBlockReplicationPolicy extends BlockReplicationPolicy with Logging {
    操作集:
    def prioritize(blockManagerId: BlockManagerId,peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,numReplicas: Int): List[BlockManagerId]
    功能: 对于一个数据块的备选节点列表进行优先化处理,这个是基本实现,,只能保证将数据块放进不同的节点中.
    	返回列表中低位的节点具有高优先级
    输入参数:
    	blockManagerId	当前块管理器唯一标识符
    	peers	节点列表
    	peersReplicatedTo	已经备份的
    	blockId	块编号
    	numReplicas	副本数量
    1. 获取块编号哈希值的随机采样值,用于原始列表的洗牌操作
    val random = new Random(blockId.hashCode)
    2. 获取优先化列表
    val prioritizedPeers = if (peers.size > numReplicas) {
        // 节点列表长度大于副本数量,需要随机选取出几个节点进行备份
      BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
    } else {
        // 节点列表长度不大于副本数量,则对节点顺序进行洗牌分配
      if (peers.size < numReplicas) {
        logWarning(s"Expecting ${numReplicas} replicas with only ${peers.size} peer/s.")
      }
      random.shuffle(peers).toList
    }
    val= prioritizedPeers
}
```

```scala
@DeveloperApi
class RandomBlockReplicationPolicy extends BlockReplicationPolicy with Logging {
    介绍: 随机块备份策略
 	操作集:
    def prioritize(
      blockManagerId: BlockManagerId,
      peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,
      numReplicas: Int): List[BlockManagerId]
    功能: 获取带有优先级的备份副本节点列表
    val random = new Random(blockId.hashCode)
    logDebug(s"Input peers : ${peers.mkString(", ")}")
    val prioritizedPeers = if (peers.size > numReplicas) {
      BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
    } else {
      if (peers.size < numReplicas) {
        logWarning(s"Expecting ${numReplicas} replicas with only ${peers.size} peer/s.")
      }
      random.shuffle(peers).toList
    }
    logDebug(s"Prioritized peers : ${prioritizedPeers.mkString(", ")}")
    val= prioritizedPeers
}
```

```scala
@DeveloperApi
class BasicBlockReplicationPolicy extends BlockReplicationPolicy with Logging {
    介绍: 基本块备份策略
    操作集:
    def prioritize(
      blockManagerId: BlockManagerId,
      peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,
      numReplicas: Int): List[BlockManagerId] 
    1. 获取一个块标识符哈希值的随机值
    val random = new Random(blockId.hashCode)
    2. 根据是否提供了拓扑信息来决定优先列表的形成方式,如果没有提供则之间使用随机shuffle即可,如果存在,则需要根据副本数量@numReplicas 和 备份节点集合@peersReplicatedTo去决定需要哪些节点
    if (blockManagerId.topologyInfo.isEmpty || numReplicas == 0) 
    	 BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
    else JUMP 3
    3. 处理含有拓扑信息的优先列表形成逻辑
    // 确定拓扑信息中指定的逻辑信息
    val doneWithinRack = peersReplicatedTo.exists(_.topologyInfo == blockManagerId.topologyInfo)
    // 确定拓扑信息是否在框架外部执行
    val doneOutsideRack = peersReplicatedTo.exists { p =>
        p.topologyInfo.isDefined && p.topologyInfo != blockManagerId.topologyInfo
      }
      if (doneOutsideRack && doneWithinRack) { // 两种情况都满足,使用随机shuffle即可
        BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
      } else {
        // 分割内外部分
        val (inRackPeers, outOfRackPeers) = peers
            .filter(_.host != blockManagerId.host)
           	.partition(_.topologyInfo == blockManagerId.topologyInfo)
        val peerWithinRack = if (doneWithinRack) { // 获取内部(满足拓扑信息)的节点列表
          Seq.empty
        } else {
            // 拓扑内部进行随机处理
          if (inRackPeers.isEmpty) {
            Seq.empty
          } else {
            Seq(inRackPeers(random.nextInt(inRackPeers.size)))
          }
        }
          // 获取外部(不满足拓扑信息)的节点列表
        val peerOutsideRack = if (doneOutsideRack || numReplicas - peerWithinRack.size <= 0) {
          Seq.empty
        } else {
            // 在外部节点信息进行随机出来
          if (outOfRackPeers.isEmpty) {
            Seq.empty
          } else {
            Seq(outOfRackPeers(random.nextInt(outOfRackPeers.size)))
          }
        }
          // 合并列表
        val priorityPeers = peerWithinRack ++ peerOutsideRack
          // 获取剩余需要节点数量
        val numRemainingPeers = numReplicas - priorityPeers.size
          // 剩余节点处理
        val remainingPeers = if (numRemainingPeers > 0) {
          val rPeers = peers.filter(p => !priorityPeers.contains(p))
            // 使用随机采用获取剩余节点列表
          BlockReplicationUtils.getRandomSample(rPeers, numRemainingPeers, random)
        } else {
          Seq.empty
        }
          val= (priorityPeers ++ remainingPeers).toList
}
```

```scala
object BlockReplicationUtils {
    介绍: 数据块备份工具
    操作集:
    def getSampleIds(n: Int, m: Int, r: Random): List[Int]
    功能: 获取采样列表(使用Robert Floyd 采样算法,在O(n)的时间复杂度内找到随机值,且可以缩小内存使用)
    输入参数:
    	n	指明的总数
    	m	需要采样的数量
    	r	随机采样生成器
    1. 获取随机采样值列表
    // n-m+1 保证范围在(1,n-1) 整个循环在合法范围内
    val indices = (n - m + 1 to n).foldLeft(mutable.LinkedHashSet.empty[Int]) {case (set, i) =>
      val t = r.nextInt(i) + 1
      if (set.contains(t)) set + i else set + t
    }
    
    def getRandomSample[T](elems: Seq[T], m: Int, r: Random): List[T]
    功能: 获取指定数量@m 随机采样列表(数据来源于@elems)
    val= if (elems.size > m) {
      getSampleIds(elems.size, m, r).map(elems(_)) // 数据集元素足够多,使用Robert Floyd 采样算法产生数据
    } else {
      r.shuffle(elems).toList // 数据集不够多,直接采用随机shuffle
    }
}
```

#### BlockUpdatedInfo

```scala
@DeveloperApi
case class BlockUpdatedInfo(
    blockManagerId: BlockManagerId,
    blockId: BlockId,
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long)
介绍: 块管理器关于块状态的存储信息
构造器参数:
	blockManagerId	块管理器标识符
	blockId	块标识符
	storageLevel	存储等级
	memSize	内存大小
	diskSize	磁盘大小
```

```scala
private[spark] object BlockUpdatedInfo {
    操作集:
    def apply(updateBlockInfo: UpdateBlockInfo): BlockUpdatedInfo
    功能: 设置块更新信息,并返回
    val= BlockUpdatedInfo(
      updateBlockInfo.blockManagerId,
      updateBlockInfo.blockId,
      updateBlockInfo.storageLevel,
      updateBlockInfo.memSize,
      updateBlockInfo.diskSize)
}
```

#### DiskBlockManager

```scala
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {
    介绍: 创建和维护逻辑数据块和物理磁盘位置之间的映射关系,一个块映射到给定数据块标识符的文件上.
    在spark.local.dir  设置的目录列表中,块状文件在其中散列分布(hash)
    构造器属性:
    	conf spark配置
    	deleteFilesOnStop	是否停止时删除
    属性:
    #name @subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)	每个本地目录下的子目录
    #name @localDirs: Array[File] = createLocalDirs(conf)	本地目录列表
    #name @localDirsString: Array[String] = localDirs.map(_.toString)	本地目录名称
    #name @subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir)) 子目录内容列表
    	子目录@subDirs 是不可以改变的,但是subDirs(i)的内容是可以改变的,对其进行读写需要获取subDirs(i)的锁
    #name @shutdownHook = addShutdownHook()	关闭阻截器
    操作集:
    def getFile(blockId: BlockId): File = getFile(blockId.name)
    功能: 获取指定数据块对应的文件名称
    
    def containsBlock(blockId: BlockId): Boolean
    功能: 确认磁盘块中是否含有指定的数据块@blockId
    val= getFile(blockId.name).exists()
    
    def getAllFiles(): Seq[File]
    功能: 获取当前磁盘存储的所有文件
    subDirs.flatMap { dir =>
      dir.synchronized { // 复制一个副本,保证数据安全(其他线程可能修改)
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles() // 获取文件列表
      if (files != null) files else Seq.empty
    }
    
    def getAllBlocks(): Seq[BlockId]
    功能: 获取当前磁盘存储所有文件对应的数据块
    val= getAllFiles().flatMap { f =>
      try {
        Some(BlockId(f.getName))
      } catch {
        case _: UnrecognizedBlockId =>
          None
      }
    }
    
    def createTempLocalBlock(): (TempLocalBlockId, File)
    功能: 创建本地临时数据块
    1. 获取一个本地临时的块标识符
    var blockId = TempLocalBlockId(UUID.randomUUID())
    2. 获取本地临时文件
    var tempLocalFile = getFile(blockId)
    3. 循环探测,直到创建出文件
    while (!canCreateFile(tempLocalFile) && count < Utils.MAX_DIR_CREATION_ATTEMPTS) {
      blockId = TempLocalBlockId(UUID.randomUUID())
      tempLocalFile = getFile(blockId)
      count += 1
    }
    val= (blockId, tempLocalFile)
    
    def createTempShuffleBlock(): (TempShuffleBlockId, File)
    功能: 创建本地临时shuffle数据块
    1. 获取本地临时文件
    var blockId = TempShuffleBlockId(UUID.randomUUID())
    var tempShuffleFile = getFile(blockId)
    var count = 0
    2. 循环探测,直到创建出文件
    while (!canCreateFile(tempShuffleFile) && count < Utils.MAX_DIR_CREATION_ATTEMPTS) {
      blockId = TempShuffleBlockId(UUID.randomUUID())
      tempShuffleFile = getFile(blockId)
      count += 1
    }
    val= (blockId, tempShuffleFile)
    
    def canCreateFile(file: File): Boolean
    功能: 确认是否能够创建指定文件
    val= try {
      file.createNewFile()
    } catch {
      case NonFatal(_) =>
        logError("Failed to create temporary block file: " + file.getAbsoluteFile)
        false
    }
 	
    def createLocalDirs(conf: SparkConf): Array[File]
    功能: 创建本地目录(文件列表)
    val= Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
    
    def addShutdownHook(): AnyRef
    功能: 添加关闭连接点
    val= 
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
    
    def stop(): Unit
    功能: 停止(清除本地目录,停止shuffle发出)
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook) // 清除关闭连接点(否则会出现内存泄漏)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop() //停止后续动作
    
    def getFile(filename: String): File
    功能: 获取指定文件名称@filename的文件对象
    1. 获取文件名称hash
    val hash = Utils.nonNegativeHash(filename)
    2. 确定目录编号
    val dirId = hash % localDirs.length
    3. 确定子目录并啊哈
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir
    4. 获取子目录
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else { // 不存在该文件的时候需要创建
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }
    val= new File(subDir, filename)
    
    def doStop(): Unit
    功能: 停止后续动作,主要是当参数@deleteFilesOnStop 时,清除相关文件
    if (deleteFilesOnStop) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
}
```

#### DiskBlockObjectWriter

```markdown
介绍:
	磁盘块对象写出器,将JVM对象直接写入到磁盘上的文件中,这个类允许添加数据到存在的数据块中.为了提升效率,通过多次提交,保持了底层文件通道,这个通道保持开启状态,直到@close() 调用.故障的情况下,调用者需要使用@revertPartialWritesAndClose()去关闭,这个可以自动返回没有提交的部分写出数据.
	这个类不支持并发写,此外一个写出器打开的情况下,不允许再次打开.
```

```scala
private[spark] class DiskBlockObjectWriter(
    val file: File,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    writeMetrics: ShuffleWriteMetricsReporter,
    val blockId: BlockId = null)
extends OutputStream with Logging with PairsWriter {
  构造器参数:
    file	磁盘文件
    serializerManager	序列化管理器
    serializerInstance	序列化实例
    bufferSize	缓冲区大小
    syncWrites	是否同步写出
    writeMetrics	写出度量器
    blockId	数据块标识符
    属性:
    #name @channel: FileChannel = null	文件通道
    #name @mcs: ManualCloseOutputStream = null	手动关闭输出流
    #name @bs: OutputStream = null	输出流
    #name @fos: FileOutputStream = null	文件输出流
    #name @ts: TimeTrackingOutputStream = null	时间追踪输出流
    #name @objOut: SerializationStream = null	序列化流
    #name @initialized = false	初始化标志
    #name @streamOpen = false	是否开启数据量
    #name @hasBeenClosed = false 是否被关闭写出
    #name @committedPosition = file.length()	提交指针
    #name @reportedPosition = committedPosition	上报指针(将这个参数给度量系统)
    #name @numRecordsWritten = 0	写出记录数量
    操作集:
    def initialize(): Unit
    功能: 参数初始化
    fos = new FileOutputStream(file, true)
    channel = fos.getChannel()
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
    
    def open(): DiskBlockObjectWriter
    功能: 打开底层输出流
    0. 参数检验
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }
    1. 设置底层输出流
    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    val= this
    
    def closeResources(): Unit
    功能: 关闭和清空资源
    if (initialized) {
      Utils.tryWithSafeFinally {
        mcs.manualClose()
      } {
        channel = null
        mcs = null
        bs = null
        fos = null
        ts = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
    }
    
    def close(): Unit
    功能: 关闭写出器(需要先将剩余没写出的数据提交,才能清空资源)
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
      } {
        closeResources()
      }
    }
    
    def commitAndGet(): FileSegment 
    功能: 作为一个原子数据块提交,并刷写到磁盘上.为了构建一个原子数据块,一个提交中可能会写出额外的内容.
    if (streamOpen) JUMP 1 else val= new FileSegment(file, committedPosition, 0)
    1. 使用流式处理获取原子提交的内容
    // 刷写输出流中的数据,由于kryo不会刷写底层输出,可以通过序列化流或者低级流实现数据的刷写
    objOut.flush() 
    bs.flush()
    objOut.close()
    streamOpen = false
    if (syncWrites) { // 异步写出处理
        val start = System.nanoTime()
        fos.getFD.sync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
    }
    // 获取通道当前位置,并计算文件在通道中的范围为 pos - committedPosition
    val pos = channel.position()
    val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
    // 更新提交指针位置,并传送度量值到度量系统
    committedPosition = pos
    writeMetrics.incBytesWritten(committedPosition - reportedPosition)
    // 重置汇报指针,重设已经写出的记录数量
    reportedPosition = committedPosition
    numRecordsWritten = 0
    val= fileSegment
    
    def revertPartialWritesAndClose(): File
    功能: 返回部分结果,并关闭写出器
    Utils.tryWithSafeFinally {
      if (initialized) {
        // 由于处理失败,需要清空当前资源,并redo度量器的度量值
        writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }
    } {
        // 获取文件输出流,读取剩余未提交的数据
      var truncateStream: FileOutputStream = null
      try {
        truncateStream = new FileOutputStream(file, true)
        // 删除文件
        truncateStream.getChannel.truncate(committedPosition)
      } catch {
        case ce: ClosedByInterruptException =>
          logError("Exception occurred while reverting partial writes to file "
            + file + ", " + ce.getMessage)
        case e: Exception =>
          logError("Uncaught exception while reverting partial writes to file " + file, e)
      } finally {
        if (truncateStream != null) {
          truncateStream.close()
          truncateStream = null
        }
      }
    }
    val= file
    
    def write(b: Int): Unit = throw new UnsupportedOperationException()
    功能: 写出单个数据(不支持的操作)
    
    def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit 
    功能: 写出缓冲区内范围数据
    1. 确定打开输出流
    if (!streamOpen) {
      open()
    }
    2. 写出数据
    bs.write(kvBytes, offs, len)
    
    def write(key: Any, value: Any): Unit
    功能: 写出kv键值对
    1. 确定打开输出流
    if (!streamOpen) {
      open()
    }
    2. 写出kv数据
    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
    
    def recordWritten(): Unit
    功能: 提示写出器写出了一条记录
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)
    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
    
    def flush(): Unit
    功能: 刷写数据
    objOut.flush()
    bs.flush()
    
    def updateBytesWritten(): Unit
    功能: 更新写出字节量
    // 更新度量器度量值,修改汇报指针@reportedPosition
    val pos = channel.position()
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
}
```

#### DiskStore

```scala
private[spark] class DiskStore(conf: SparkConf,diskManager: DiskBlockManager,
    securityManager: SecurityManager) extends Logging {
    介绍: 磁盘存储器
    构造器参数: 
    	conf	spark配置
    	diskManager	磁盘管理器
    	securityManager	安全管理器
    属性:
    #name @minMemoryMapBytes = conf.get(config.STORAGE_MEMORY_MAP_THRESHOLD)	最小内存映射字节量
    #name @maxMemoryMapBytes = conf.get(config.MEMORY_MAP_LIMIT_FOR_TESTS)	最大内存映射字节量
    #name @blockSizes = new ConcurrentHashMap[BlockId, Long]()	数据块大小映射表
    操作集:
    def getSize(blockId: BlockId): Long = blockSizes.get(blockId)
    功能: 获取指定数据块@blockId 的数据块大小
    
    def put(blockId: BlockId)(writeFunc: WritableByteChannel => Unit): Unit
    功能: 调用指定的回调函数写出指定数据块
    输入参数:
    	blockId	数据块标识符
    	writeFunc	写出函数
    0. 数据块存在性校验
    if (contains(blockId)) {
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    1. 获取输出文件通道
    val startTimeNs = System.nanoTime()
    val file = diskManager.getFile(blockId)
    val out = new CountingWritableChannel(openForWrite(file))
    2. 写出通道内部数据
    var threwException: Boolean = true
    try {
      writeFunc(out)
      blockSizes.put(blockId, out.getCount)
      threwException = false
    } finally {
      try {
        out.close()
      } catch {
        case ioe: IOException =>
          if (!threwException) {
            threwException = true
            throw ioe
          }
      } finally {
         if (threwException) {
          remove(blockId)
        }
      }
    }
    
    def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit 
    功能: 写出指定数据块@blockId,指定字节缓冲区为@bytes
    put(blockId) { channel =>
      bytes.writeFully(channel)
    }
    
    def getBytes(blockId: BlockId): BlockData
    功能: 获取指定数据块@blockId 的数据块
    val= getBytes(diskManager.getFile(blockId.name), getSize(blockId))
    
    def getBytes(f: File, blockSize: Long): BlockData 
    功能: 获取指定文件@f 中数据块大小为@blockSize 的数据块
    val= securityManager.getIOEncryptionKey() match {
        // 加密数据块不可以用于内存映射
        case Some(key) => new EncryptedBlockData(f, blockSize, conf, key)
        case _ => new DiskBlockData(minMemoryMapBytes, maxMemoryMapBytes, f, blockSize)
    }
    
    def remove(blockId: BlockId): Boolean 
    功能: 移除指定数据块@blockId 
    1. 从数据块大小映射表中移除指定@blockId 的信息
    blockSizes.remove(blockId)
    2. 实际移除指定磁盘数据块
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
    
    def contains(blockId: BlockId): Boolean
    功能: 确认是否含有数据块@blockId
    val file = diskManager.getFile(blockId.name)
    val= file.exists()
    
    def openForWrite(file: File): WritableByteChannel
    功能: 获取指定文件@file 的字节通道 用于数据写出操作
    1. 获取通道
    val out = new FileOutputStream(file).getChannel()
    2. 对字节通道进行加密
    try {
      val= securityManager.getIOEncryptionKey().map { key =>
        CryptoStreamUtils.createWritableChannel(out, conf, key)
      }.getOrElse(out)
    } catch {
      case e: Exception =>
        Closeables.close(out, true)
        file.delete()
        throw e
    }
    
    def moveFileToBlock(sourceFile: File, blockSize: Long, targetBlockId: BlockId): Unit
    功能: 移动文件@file 到指定数据块@targetBlockId 
    1. 注册数据块信息
    blockSizes.put(targetBlockId, blockSize)
    2. 获取目标文件信息,并进行移动
    val targetFile = diskManager.getFile(targetBlockId.name)
    FileUtils.moveFile(sourceFile, targetFile)
}
```

```scala
private class DiskBlockData(
    minMemoryMapBytes: Long,
    maxMemoryMapBytes: Long,
    file: File,
    blockSize: Long) extends BlockData {
    介绍: 磁盘数据块
    构造器参数:
    minMemoryMapBytes	最小内存映射字节量
    maxMemoryMapBytes	最大内存映射字节量
    file	文件
    blockSize	数据块大小
    操作集:
    def size: Long = blockSize
    功能: 获取数据块大小
    
    def dispose(): Unit = {}
    功能: 是否进行最后的处理
    
    def open():FileChannel = new FileInputStream(file).getChannel
    功能: 打开指定文件的文件通道
    
    def toInputStream(): InputStream = new FileInputStream(file)
    功能: 转换为输入流
    
    def toNetty(): AnyRef = new DefaultFileRegion(file, 0, size)
    功能: 转化为用于netty的包装模式
    
    def toChunkedByteBuffer(allocator: (Int) => ByteBuffer): ChunkedByteBuffer
    功能: 转换为块状字节缓冲区
    输入参数:
    	allocator	缓冲区分配函数
    val= Utils.tryWithResource(open()) { channel =>
        var remaining = blockSize
        val chunks = new ListBuffer[ByteBuffer]()
        while (remaining > 0) {
            val chunkSize = math.min(remaining, maxMemoryMapBytes)
            val chunk = allocator(chunkSize.toInt)
            remaining -= chunkSize
            JavaUtils.readFully(channel, chunk)
            chunk.flip()
            chunks += chunk
        }
        val= new ChunkedByteBuffer(chunks.toArray)
    }
    
    def toByteBuffer(): ByteBuffer
    功能: 获取字节缓冲区
    1. 数据块大小断言
    require(blockSize < maxMemoryMapBytes,
      s"can't create a byte buffer of size $blockSize" +
      s" since it exceeds ${Utils.bytesToString(maxMemoryMapBytes)}.")
    2. 获取字节缓冲区
    val= Utils.tryWithResource(open()) { channel =>
      if (blockSize < minMemoryMapBytes) { // 小文件直接磁盘读取,不使用内存映射
        val buf = ByteBuffer.allocate(blockSize.toInt)
        JavaUtils.readFully(channel, buf)
        buf.flip()
        buf
      } else {
        channel.map(MapMode.READ_ONLY, 0, file.length)
      }
    }
}
```

```scala
private[spark] class EncryptedBlockData(
    file: File,
    blockSize: Long,
    conf: SparkConf,
    key: Array[Byte]) extends BlockData {
	介绍: 加密数据块
    构造器参数:
    	file	磁盘文件
    	blockSize	数据块大小
    	conf	spark配置
    	key	密码列表
    操作集:
    def toInputStream(): InputStream = Channels.newInputStream(open())
    功能: 转换为数据流
    
    def toNetty(): Object = new ReadableChannelFileRegion(open(), blockSize)
    功能: 转换为方便netty传输的形式
    
    def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer 
    功能: 转换为块状字节缓冲区
    val source = open()
    try {
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) {
        val chunkSize = math.min(remaining, ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)
        val chunk = allocator(chunkSize.toInt)
        remaining -= chunkSize
        JavaUtils.readFully(source, chunk)
        chunk.flip()
        chunks += chunk
      }
      val= new ChunkedByteBuffer(chunks.toArray)
    } finally {
      source.close()
    }
    
    def toByteBuffer(): ByteBuffer
    功能: 转换为字节缓冲区
    1. 块大小断言
    assert(blockSize <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
      "Block is too large to be wrapped in a byte buffer.")
    2. 转化为字节缓冲区
    val dst = ByteBuffer.allocate(blockSize.toInt)
    val in = open()
    try {
      JavaUtils.readFully(in, dst)
      dst.flip()
      dst
    } finally {
      Closeables.close(in, true)
    }
    
    def size: Long = blockSize
    功能: 获取数据块大小
    
    def dispose(): Unit = { }
    功能: 后续处理工作
    
    def open(): ReadableByteChannel
    功能: 打开数据块的字节通道
    val channel = new FileInputStream(file).getChannel()
    try {
      val= CryptoStreamUtils.createReadableChannel(channel, conf, key)
    } catch {
      case e: Exception =>
        Closeables.close(channel, true)
        throw e
    } 
}
```

```scala
private[spark] class EncryptedManagedBuffer(
    val blockData: EncryptedBlockData) extends ManagedBuffer {
    介绍: 加密管理缓冲区
    操作集:
    def size(): Long = blockData.size
    功能: 获取数据块大小
    
    def nioByteBuffer(): ByteBuffer = blockData.toByteBuffer()
    功能: 获取数据块对于的缓冲区
    
    def convertToNetty(): AnyRef = blockData.toNetty()
    功能: 将数据块转换为适合netty传输的数据
    
    def createInputStream(): InputStream = blockData.toInputStream()
    功能: 创建数据块的输入流
    
    def retain(): ManagedBuffer = this
    功能: 保留缓冲区内容
    
    def release(): ManagedBuffer = this
}
```

```scala
private class ReadableChannelFileRegion(source: ReadableByteChannel, blockSize: Long)
extends AbstractFileRegion {
    功能: 可读通道文件区域(适合netty传输)
    构造器属性:
        source	可读字节通道
        blockSize	数据块大小
    属性:
    #name @_transferred = 0L	转化字节量
    #name @buffer = ByteBuffer.allocateDirect(64 * 1024)	字节缓冲区
    操作集:
    def count(): Long = blockSize
    功能: 获取数据块大小
    
    def position(): Long = 0
    功能: 获取通道指针
    
    def transferred(): Long = _transferred
    功能: 获取转换字节量
    
    def transferTo(target: WritableByteChannel, pos: Long): Long
    功能: 将数据转换到目标字节通道
    0. 指针位置断言
    assert(pos == transferred(), "Invalid position.")
    1. 转移字节通道中的数据
    var written = 0L
    var lastWrite = -1L
    while (lastWrite != 0) {
      if (!buffer.hasRemaining()) {
        buffer.clear()
        source.read(buffer)
        buffer.flip()
      }
      if (buffer.hasRemaining()) {
        lastWrite = target.write(buffer)
        written += lastWrite
      } else {
        lastWrite = 0
      }
    }
    2. 更新字节转换量
    _transferred += written
    val= written
    
    def deallocate(): Unit = source.close()
    功能: 关闭字节通道
}
```

```scala
private class CountingWritableChannel(sink: WritableByteChannel) extends WritableByteChannel {
    构造器参数:
    sink	可写字节通道
    属性:
    #name @count = 0L	计数值
    操作集:
    def getCount: Long = count	
    功能: 获取计数值
    
    def write(src: ByteBuffer): Int 
    功能: 写入缓冲区内容
    val written = sink.write(src)
    if (written > 0) {
      count += written
    }
    val= written
    
    def isOpen(): Boolean = sink.isOpen()
    功能: 确定通道是否打开
    
    def close(): Unit = sink.close()
    功能: 关闭通道
}
```

#### FileSegment

```scala
private[spark] class FileSegment(val file: File, val offset: Long, val length: Long) {
    介绍: 指向一个文件的一个部分,也有可能时全文,基于偏移量@offset 和长度@length
    参数校验:
    require(offset >= 0, s"File segment offset cannot be negative (got $offset)")
    require(length >= 0, s"File segment length cannot be negative (got $length)")
    def toString: String= "(name=%s, offset=%d, length=%d)".format(file.getName, offset, length)
    功能: 信息显示
}
```

#### RDDInfo

```scala
@DeveloperApi
class RDDInfo(
    val id: Int,
    var name: String,
    val numPartitions: Int,
    var storageLevel: StorageLevel,
    val isBarrier: Boolean,
    val parentIds: Seq[Int],
    val callSite: String = "",
    val scope: Option[RDDOperationScope] = None)
extends Ordered[RDDInfo] {
    介绍: RDD信息
    构造器属性:
        id	RDD编号
        name	RDD名称
        numPartitions	分区数量
        storageLevel	存储等级
        isBarrier	是否收到阻碍
        parentIds	父级RDD编号列表
        callSite	调用地址
        scope	RDD操作范围
    属性:
    #name @numCachedPartitions = 0	缓冲分区数量
    #name @memSize = 0L	内存大小
    #name @diskSize = 0L	磁盘大小
    #name @externalBlockStoreSize = 0L	外部块存储器大小
    操作集:
    def toString: String
    功能: 信息显示
    val= ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; DiskSize: %s").format(
        name, id, storageLevel.toString, numCachedPartitions, numPartitions,
        bytesToString(memSize), bytesToString(diskSize))
    
    def compare(that: RDDInfo): Int
    功能: RDD比较逻辑
    
    def isCached: Boolean = (memSize + diskSize > 0) && numCachedPartitions > 0
    功能: 确认是否存在缓存
}
```

```scala
private[spark] object RDDInfo {
    操作集:
    def fromRdd(rdd: RDD[_]): RDDInfo 
    功能: 获取RDD信息
    1. 获取rdd所需属性值
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))
    val parentIds = rdd.dependencies.map(_.rdd.id)
    val callsiteLongForm = Option(SparkEnv.get)
      .map(_.conf.get(EVENT_LOG_CALLSITE_LONG_FORM))
      .getOrElse(false)
    val callSite = if (callsiteLongForm) {
      rdd.creationSite.longForm
    } else {
      rdd.creationSite.shortForm
    }
    val= new RDDInfo(rdd.id, rddName, rdd.partitions.length,
      rdd.getStorageLevel, rdd.isBarrier(), parentIds, callSite, rdd.scope)
}
```

#### ShuffleBlockFetcherIterator

```markdown
这是一个迭代器,用于获取多个数据块.对于本地数据块,可以从本地块管理器中获取本地数据块.通过提供的数据块传输服务从远端获取数据块.
创建一个迭代器(数据块,输入流)元组,所以调用者可以按照接受数据的管道形式处理数据块.
构造器参数:
	context	任务上下文(用于度量值更新)
	shuffleClient	shuffle客户端
	blockManager	块管理器
	blocksByAddress	块管理器列表
	streamWrapper	流包装器(包装输入流)
	maxBytesInFlight	在给定点远端获取的最大数据量(字节)
	maxReqsInFlight	在指定点远端请求获取的最大数据块
	maxBlocksInFlightPerAddress	对于指定主机端口号指定时间最大shuffle数据块数量
	maxReqSizeShuffleToMem	可以shuffle到内存中请求最大数据量
	detectCorrupt	是否弃用数据块
	shuffleMetrics	shuffle度量器
	doBatchFetch	是否批量获取数据块
```

```scala
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: BlockStoreClient,
    blockManager: BlockManager,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean,
    detectCorruptUseExtraMemory: Boolean,
    shuffleMetrics: ShuffleReadMetricsReporter,
    doBatchFetch: Boolean)
extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {
    属性:
    #name @targetRemoteRequestSize = math.max(maxBytesInFlight / 5, 1L)	目标远端请求量
    #name @numBlocksToFetch = 0	需要获取的数据块数量
    #name @numBlocksProcessed = 0	调用者使用的数据块数量
    	当numBlocksProcessed=numBlocksToFetch 时,迭代器不可以使用
    #name @startTimeNs = System.nanoTime()	开始时间
    #name @localBlocks = scala.collection.mutable.LinkedHashSet[(BlockId, Int)]()
    	本地数据块列表
    #name @hostLocalBlocksByExecutor #type @LinkHashMap	执行器本地数据块映射表
    val= LinkedHashMap[BlockManagerId, Seq[(BlockId, Long, Int)]]()
    #name @hostLocalBlocks = scala.collection.mutable.LinkedHashSet[(BlockId, Int)]()
    本地数据块列表
    #name @results = new LinkedBlockingQueue[FetchResult]	结果队列
    用于放置结果的阻塞队列,运行在同步模式下
    #name @currentResult: SuccessFetchResult = null	当前执行结果
    当前@FetchResult 正在执行,定位它,因此可以释放当前缓冲区,考虑到运行中的异常.
    #name @fetchRequests = new Queue[FetchRequest]	获取结果队列
    #name @deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()
    	延期获取请求映射表(就是第一次入队列时没有进行处理的数据块@BlockManagerId),当满足重试条件的时候会进行重试.
    #name @bytesInFlight = 0L	请求的字节数量
    #name @reqsInFlight = 0	当前的请求数量
    #name @numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()
    	每个地址数据块数量映射表
    #name @corruptedBlocks = mutable.HashSet[BlockId]()	弃用数据块集合
    #name @isZombie = false @GuardedBy("this")
    迭代器存活标记(为true表示回调接口不会将结果置入@results中)
    #name @shuffleFilesSet = mutable.HashSet[DownloadFile]() @GuardedBy("this")
    shuffle文件数据集（当执行cleanup时会清空，这个是为了防止磁盘文件泄漏）
    #name @onCompleteCallback = new ShuffleFetchCompletionListener(this)
    完成时回调事件
    操作集：
    def releaseCurrentResultBuffer(): Unit
    功能: 释放当前结果缓冲,并置null使得@cleanup时不会再次清除
    if (currentResult != null) {
      // 释放缓冲区
      currentResult.buf.release()
    }
    currentResult = null
    
    def createTempFile(transportConf: TransportConf): DownloadFile 
    功能: 创建临时文件
    val= new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
    
    def registerTempFileToClean(file: DownloadFile): Boolean 
    功能: 注册需要清理的临时文件@file
    val= synchronized {
        if (isZombie) {
          false
        } else {
          shuffleFilesSet += file
          true
        }
    }
    
    def cleanup(): Unit
    功能: 释放所有没有被反序列化的缓冲区
    1. 设置弃用标记
    synchronized {
      isZombie = true
    }
    2. 释放结果队列的缓冲区
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, _, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
    3. 删除shuffle文件集的内容
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.path())
      }
    }
    
    def sendRequest(req: FetchRequest): Unit
    功能: 发送获取请求@req
    1. 设置度量参数
    bytesInFlight += req.size // 更新读取字节量
    reqsInFlight += 1 / 请求数量+1
    2. 查找数据块信息ID
    val infoMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, mapIndex) => (blockId.toString, (size, mapIndex))
    }.toMap
    val remainingBlocks = new HashSet[String]() ++= infoMap.keys
    val blockIds = req.blocks.map(_.blockId.toString)
    val address = req.address
    3. 设置监听器信息
    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            buf.retain()
            remainingBlocks -= blockId
            results.put(new SuccessFetchResult(BlockId(blockId), infoMap(blockId)._2,
              address, infoMap(blockId)._1, buf, remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace(s"Got remote block $blockId after ${Utils.getUsedTimeNs(startTimeNs)}")
      }
      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(new FailureFetchResult(BlockId(blockId), infoMap(blockId)._2, address, e))
      }
    }
    4. 超出指定数量时,将shuffle数据块写出到磁盘上(由于shuffle数据块已经经过加密和压缩),所以不需要对其进行再加工,直接写出即可
    if (req.size > maxReqSizeShuffleToMem) {
      shuffleClient.fetchBlocks(address.host, address.port, 
                                address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, 
                                address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
    
    def partitionBlocksByFetchMode(): ArrayBuffer[FetchRequest]
    功能: 获取执行结果列表
    
    def assertPositiveBlockSize(blockId: BlockId, blockSize: Long): Unit
    功能: 断言数据块大小为正数
    if (blockSize < 0) {
      throw BlockException(blockId, "Negative block size " + size)
    } else if (blockSize == 0) {
      throw BlockException(blockId, "Zero-sized blocks should be excluded.")
    }
    
    def checkBlockSizes(blockInfos: Seq[(BlockId, Long, Int)]): Unit
    功能: 检查数据块大小
    
    def mergeFetchBlockInfo(toBeMerged: ArrayBuffer[FetchBlockInfo]): FetchBlockInfo
    功能: 获取数据块信息
    val= {
      val startBlockId = toBeMerged.head.blockId.asInstanceOf[ShuffleBlockId]
      FetchBlockInfo(
        ShuffleBlockBatchId(
          startBlockId.shuffleId,
          startBlockId.mapId,
          startBlockId.reduceId,
          toBeMerged.last.blockId.asInstanceOf[ShuffleBlockId].reduceId + 1),
        toBeMerged.map(_.size).sum,
        toBeMerged.head.mapIndex)
    }
    
    def mergeContinuousShuffleBlockIdsIfNeeded(
      blocks: ArrayBuffer[FetchBlockInfo]): ArrayBuffer[FetchBlockInfo]
    功能: 合并连续shuffle数据块列表
    1. 获取结果集
    val result = if (doBatchFetch) {
      var curBlocks = new ArrayBuffer[FetchBlockInfo]
      val mergedBlockInfo = new ArrayBuffer[FetchBlockInfo]
      val iter = blocks.iterator
      while (iter.hasNext) {
        val info = iter.next()
        val curBlockId = info.blockId.asInstanceOf[ShuffleBlockId]
        if (curBlocks.isEmpty) {
          curBlocks += info
        } else {
          if (curBlockId.mapId != curBlocks.head.blockId.asInstanceOf[ShuffleBlockId].mapId) {
            mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
            curBlocks.clear()
          }
          curBlocks += info
        }
      }
    }
    if (curBlocks.nonEmpty) {
        mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
      }
    mergedBlockInfo
    else
    	blocks
    2. 更新度量信息
    numBlocksToFetch += result.size
    val= result
    
    def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch
    功能: 确定是否有下一个元素
    
    def next(): (BlockId, InputStream) 
    功能: 获取下一个(BlockId,InputStream)原则,如果任务失败会使用@cleanUp清理所有的输入流,调用者需要关闭输入流,因为不会再被使用.主要目的是竟可能的释放空间.
    
    def toCompletionIterator: Iterator[(BlockId, InputStream)]
    功能: 获取需要完成的迭代器
    val= {
        CompletionIterator[(BlockId, InputStream), this.type](this,
          onCompleteCallback.onComplete(context))
    }
    
    def throwFetchFailedException(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable):Nothing 
    功能: 抛出获取失败异常
    
    def initialize(): Unit
    功能: 初始化
    1. 添加完成回调的监听事件
    context.addTaskCompletionListener(onCompleteCallback)
    2. 设置不同获取模式的分区数据块(本地,主机本地,远端数据块)
    val remoteRequests = partitionBlocksByFetchMode()
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)
    3. 发出数据块的初始化请求,跳转到最大字节处
    fetchUpToMaxBytes()
    4. 确定需要获取的数量
    val numFetches = remoteRequests.size - fetchRequests.size
    5. 获取本地数据块
    fetchLocalBlocks()
    if (hostLocalBlocks.nonEmpty) {
      blockManager.hostLocalDirManager.foreach(fetchHostLocalBlocks)
    }
    
    def fetchUpToMaxBytes(): Unit
    功能: 发送获取数据到最大字节处,如果你不能获取一个远端主机,则会将请求置入下次处理的队列
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress 
        with ${request.blocks.size} blocks")
      val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress,
                                                        new Queue[FetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        send(remoteAddress, request)
      }
    }
    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      sendRequest(request)
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }
    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
    
    def fetchLocalBlocks(): Unit
    功能: 获取本地数据块
    在获取远端数据块时允许获取本地数据块,由于`ManagedBuffer`内存的创建输入流时的懒加载方式
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val (blockId, mapIndex) = iter.next()
      try {
        val buf = blockManager.getLocalBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, mapIndex, 	blockManager.blockManagerId,buf.size(), buf, false))
      } catch {
        case e: Exception =>
          e match {
            case ce: ClosedByInterruptException =>
              logError("Error occurred while fetching local blocks, " + ce.getMessage)
            case ex: Exception => 
              logError("Error occurred while fetching local blocks", ex)
          }
          results.put(new FailureFetchResult(blockId, mapIndex,
                                             blockManager.blockManagerId, e))
          return
      }
    }
    
    def fetchHostLocalBlock(
      blockId: BlockId,
      mapIndex: Int,
      localDirs: Array[String],
      blockManagerId: BlockManagerId): Boolean
    功能: 确认是否获取主机本地数据块
    val= {
        try {
          val buf = blockManager.getHostLocalShuffleData(blockId, localDirs)
          buf.retain()
          results.put(SuccessFetchResult(blockId, mapIndex, blockManagerId
                                         , buf.size(), buf,isNetworkReqDone = false))
          true
        } catch {
          case e: Exception =>
            logError(s"Error occurred while fetching local blocks", e)
            results.put(FailureFetchResult(blockId, mapIndex, blockManagerId, e))
            false
        }
      }
    
    def fetchHostLocalBlocks(hostLocalDirManager: HostLocalDirManager): Unit
    功能: 获取主机本地数据块
    val cachedDirsByExec = hostLocalDirManager.getCachedHostLocalDirs()
    val (hostLocalBlocksWithCachedDirs, hostLocalBlocksWithMissingDirs) =
      hostLocalBlocksByExecutor
        .map { case (hostLocalBmId, bmInfos) =>
          (hostLocalBmId, bmInfos, cachedDirsByExec.get(hostLocalBmId.executorId))
        }.partition(_._3.isDefined)
    val bmId = blockManager.blockManagerId
    val immutableHostLocalBlocksWithoutDirs =
      hostLocalBlocksWithMissingDirs.map { case (hostLocalBmId, bmInfos, _) =>
        hostLocalBmId -> bmInfos
      }.toMap
    if (immutableHostLocalBlocksWithoutDirs.nonEmpty) {
      logDebug(s"Asynchronous fetching host-local blocks without cached executors' dir: " +
        s"${immutableHostLocalBlocksWithoutDirs.mkString(", ")}")
      val execIdsWithoutDirs = immutableHostLocalBlocksWithoutDirs.keys.map(_.executorId).toArray
      hostLocalDirManager.getHostLocalDirs(execIdsWithoutDirs) {
        case Success(dirs) =>
          immutableHostLocalBlocksWithoutDirs.foreach { case (hostLocalBmId, blockInfos) =>
            blockInfos.takeWhile { case (blockId, _, mapIndex) =>
              fetchHostLocalBlock(
                blockId,
                mapIndex,
                dirs.get(hostLocalBmId.executorId),
                hostLocalBmId)
            }
          }
          logDebug(s"Got host-local blocks (without cached executors' dir) in " +
            s"${Utils.getUsedTimeNs(startTimeNs)}")

        case Failure(throwable) =>
          logError(s"Error occurred while fetching host local blocks", throwable)
          val (hostLocalBmId, blockInfoSeq) = immutableHostLocalBlocksWithoutDirs.head
          val (blockId, _, mapIndex) = blockInfoSeq.head
          results.put(FailureFetchResult(blockId, mapIndex, hostLocalBmId, throwable))
      }
    }
    if (hostLocalBlocksWithCachedDirs.nonEmpty) {
      logDebug(s"Synchronous fetching host-local blocks with cached executors' dir: " +
          s"${hostLocalBlocksWithCachedDirs.mkString(", ")}")
      hostLocalBlocksWithCachedDirs.foreach { case (_, blockInfos, localDirs) =>
        blockInfos.foreach { case (blockId, _, mapIndex) =>
          if (!fetchHostLocalBlock(blockId, mapIndex, localDirs.get, bmId)) {
            return
          }
        }
      }
      logDebug(s"Got host-local blocks (with cached executors' dir) in " +
        s"${Utils.getUsedTimeNs(startTimeNs)}")
    }
}
```

```scala
private class BufferReleasingInputStream(
    private[storage] val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator,
    private val blockId: BlockId,
    private val mapIndex: Int,
    private val address: BlockManagerId,
    private val detectCorruption: Boolean)
extends InputStream {
    介绍: 辅助类,可以确保@ManagedBuffer 在输入流上被释放,且如果关闭输入流压缩和加密为true就会解除流的使用.
    构造器参数:
    	delegate	代理输入流
    	iterator	shuffle数据块基本迭代器
    	blockId	数据块标识符
    	mapIndex	map位置
    	address	块管理器标识符
    	detectCorruption	是否弃用的标记
    操作集:
    def read(): Int
    功能: 读取数据
    val= try {
      delegate.read()
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
    
    def close(): Unit
    功能: 关闭输入流
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
    
    def available(): Int = delegate.available()
    功能: 确认是否可用
    
    def mark(readlimit: Int): Unit = delegate.mark(readlimit)
    功能: 标记读取位置
    
    def skip(n: Long): Long
    功能: 跳读n位
    val= try {
      delegate.skip(n)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
    
    def markSupported(): Boolean = delegate.markSupported()
    功能: 标记是否支持
    
    def read(b: Array[Byte]): Int
    功能: 读取一个缓冲区内容
    val= try {
      delegate.read(b)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
    
    def read(b: Array[Byte], off: Int, len: Int): Int
    功能: 读取缓冲区的一个范围
    val= try {
      delegate.read(b, off, len)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
    
    def reset(): Unit = delegate.reset()
    功能: 重置
}
```

```scala
private[storage] object ShuffleBlockFetcherIterator {
    内部类:
    case class FetchBlockInfo(
    blockId: BlockId,
    size: Long,
    mapIndex: Int)
    介绍: 获取数据块信息
    
    case class FetchRequest(address: BlockManagerId, blocks: Seq[FetchBlockInfo]) {
        val size = blocks.map(_.size).sum
      }
    介绍: 获取请求
    
    sealed trait FetchResult {
        val blockId: BlockId
        val address: BlockManagerId
    }
    介绍: 获取结果
    
    case class SuccessFetchResult(
        blockId: BlockId,
        mapIndex: Int,
        address: BlockManagerId,
        size: Long,
        buf: ManagedBuffer,
        isNetworkReqDone: Boolean) extends FetchResult {
        require(buf != null)
        require(size >= 0)
    }
    介绍: 成功获取结果
    
    case class FailureFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
    介绍: 失败获取结果
}
```

#### StorageLevel

```scala
@DeveloperApi
class StorageLevel private(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useOffHeap: Boolean,
    private var _deserialized: Boolean,
    private var _replication: Int = 1)
extends Externalizable {
    介绍: 外部控制RDD的存储,每个存储等级记录可以选择是否使用内存或者外部数据块存储@ExternalBlockStore ,是否将RDD写到磁盘,是否将数据保存在内存(以一种序列化的方式).是否在多个节点上进行备份.
    构造器参数:
    _useDisk	是否使用磁盘
    _useMemory	是否使用内存
    _useOffHeap	是否使用非堆模式内存
    _deserialized	是否进行反序列化
    _replication	是否进行备份
    构造器:
    def this() = this(false, true, false, false)
    功能: 用于反序列
    
    def this(flags: Int, replication: Int) = 
    this((flags & 8) != 0, (flags & 4) != 0, (flags & 2) != 0, (flags & 1) != 0, replication)
    功能: 根据标志码@flages 设置工作模式
    
    初始化操作:
    assert(replication < 40, "Replication restricted to be less than 40 for calculating hash codes")
    功能: 副本上限限制
    
    if (useOffHeap) {
        require(!deserialized, "Off-heap storage level does not support deserialized storage")
    }
    功能: 非堆模式下不支持反序列化存储
    
    操作集:
    def memoryMode: MemoryMode
    功能: 获取内存模式
    val= if (useOffHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP
    
    def clone(): StorageLevel
    功能: 获取存储等级的副本
    val= new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication)
    
    def equals(other: Any): Boolean
    功能: 判断两个存储等级实例是否相等
    val= other match {
        case s: StorageLevel =>
          s.useDisk == useDisk &&
          s.useMemory == useMemory &&
          s.useOffHeap == useOffHeap &&
          s.deserialized == deserialized &&
          s.replication == replication
        case _ =>
          false
      }
    
    def isValid: Boolean = (useMemory || useDisk) && (replication > 0)
    功能: 确定当前存储系统是否可以使用
    
    def toInt: Int 
    功能: 参数值形成唯一的代表值
    val= {
            var ret = 0
            if (_useDisk) {
              ret |= 8
            }
            if (_useMemory) {
              ret |= 4
            }
            if (_useOffHeap) {
              ret |= 2
            }
            if (_deserialized) {
              ret |= 1
            }
            ret
      }
    
    def hashCode(): Int = toInt * 41 + replication
    功能: 求取当前存储等级的hash值
    
    def toString: String
    功能: 信息显示
    val disk = if (useDisk) "disk" else ""
    val memory = if (useMemory) "memory" else ""
    val heap = if (useOffHeap) "offheap" else ""
    val deserialize = if (deserialized) "deserialized" else ""
    val output =Seq(disk, memory, heap, deserialize, s"$replication replicas").filter(_.nonEmpty)
    val= s"StorageLevel(${output.mkString(", ")})"
    
    def description: String
    功能: 获取描述信息
    var result = ""
    result += (if (useDisk) "Disk " else "")
    if (useMemory) {
      result += (if (useOffHeap) "Memory (off heap) " else "Memory ")
    }
    result += (if (deserialized) "Deserialized " else "Serialized ")
    result += s"${replication}x Replicated"
    val= result
    
    def writeExternal(out: ObjectOutput): Unit
    功能: 写出配置信息
    out.writeByte(toInt)
    out.writeByte(_replication)
    
    def readExternal(in: ObjectInput): Unit
    功能: 读取配置信息
    val flags = in.readByte()
    _useDisk = (flags & 8) != 0
    _useMemory = (flags & 4) != 0
    _useOffHeap = (flags & 2) != 0
    _deserialized = (flags & 1) != 0
    _replication = in.readByte()
    
    @throws(classOf[IOException])
    private def readResolve(): Object = StorageLevel.getCachedStorageLevel(this)
    功能: 获取缓存等级
}
```

```scala
object StorageLevel {
    属性:
    #name @NONE = new StorageLevel(false, false, false, false)	不使用存储
    #name @DISK_ONLY = new StorageLevel(true, false, false, false)	只使用磁盘存储
    #name @DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)	使用冗余策略的磁盘存储
    #name @MEMORY_ONLY = new StorageLevel(false, true, false, true)	使用内存且序列化
    #name @MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2) 使用内存,序列化且使用冗余策略
    #name @MEMORY_ONLY_SER = new StorageLevel(false, true, false, false) 使用内存,且只使用反序列化
    #name @MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2) 
    	使用内容只使用反序列化,使用冗余策略
    #name @MEMORY_AND_DISK = new StorageLevel(true, true, false, true) 存储到内存和磁盘上
    #name @MEMORY_AND_DISK_2= new StorageLevel(true, true, false, true, 2)
    	存储到内存和磁盘并且使用冗余策略
    #name @MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)	
    	存储到内存和磁盘,并且进行序列化
    #name @MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
    	存储到内存和磁盘上,使用序列化,并使用冗余策略
    #name @OFF_HEAP = new StorageLevel(true, true, true, false, 1)	使用非堆模式下的内存
    #name @storageLevelCache = new ConcurrentHashMap[StorageLevel, StorageLevel]()	存储等级缓存
    
    操作集:
    @DeveloperApi
    def fromString(s: String): StorageLevel
    功能: 获取指定@s 的存储等级实例
    val= s match {
        case "NONE" => NONE
        case "DISK_ONLY" => DISK_ONLY
        case "DISK_ONLY_2" => DISK_ONLY_2
        case "MEMORY_ONLY" => MEMORY_ONLY
        case "MEMORY_ONLY_2" => MEMORY_ONLY_2
        case "MEMORY_ONLY_SER" => MEMORY_ONLY_SER
        case "MEMORY_ONLY_SER_2" => MEMORY_ONLY_SER_2
        case "MEMORY_AND_DISK" => MEMORY_AND_DISK
        case "MEMORY_AND_DISK_2" => MEMORY_AND_DISK_2
        case "MEMORY_AND_DISK_SER" => MEMORY_AND_DISK_SER
        case "MEMORY_AND_DISK_SER_2" => MEMORY_AND_DISK_SER_2
        case "OFF_HEAP" => OFF_HEAP
        case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $s")
    }
    
    @DeveloperApi
    def apply(
        useDisk: Boolean,
        useMemory: Boolean,
        useOffHeap: Boolean,
        deserialized: Boolean,
        replication: Int): StorageLevel 
    功能: 获取存储等级实例
    val= getCachedStorageLevel(
      	new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication))
	
    @DeveloperApi
    def apply(
        useDisk: Boolean,
        useMemory: Boolean,
        deserialized: Boolean,
        replication: Int = 1): StorageLevel
    功能: 获取存储等级实例(不使用冗余,不使用非堆内存)
    val= 
    getCachedStorageLevel(new StorageLevel(useDisk, useMemory, false, deserialized, replication))
    
    @DeveloperApi
    def apply(flags: Int, replication: Int): StorageLevel
    功能: 获取指定指令码@flags 的存储等级
    val= getCachedStorageLevel(new StorageLevel(flags, replication))
    
    @DeveloperApi
    def apply(in: ObjectInput): StorageLevel
    功能:读取配置,获取存储等级实例
    val obj = new StorageLevel()
    obj.readExternal(in)
    val= getCachedStorageLevel(obj)
    
    def getCachedStorageLevel(level: StorageLevel): StorageLevel
    功能: 获取缓冲的存储等级
    storageLevelCache.putIfAbsent(level, level) // 没有该数据则置入
    val= storageLevelCache.get(level)
}
```

#### StorageUtils

```scala
private[spark] class StorageStatus(
    val blockManagerId: BlockManagerId,
    val maxMemory: Long,
    val maxOnHeapMem: Option[Long],
    val maxOffHeapMem: Option[Long]) {
    介绍: 存储状态(用于存储@BlockManager 的存储信息),这个类假定@BlockId 和@BlockStatus 是不可变的,这个类的消费者不能改变信息的源头,获取时线程不安全的.
    构造器属性:
        blockManagerId	块管理器标识符
        maxMemory	最大内存
        maxOnHeapMem	最大堆上内存
        maxOffHeapMem	最大非堆模式下内存
    属性:
    #name @_rddBlocks = new mutable.HashMap[Int, mutable.Map[BlockId, BlockStatus]]	RDD数据块列表
    #name @_nonRddBlocks = new mutable.HashMap[BlockId, BlockStatus]	非RDD数据块
    #name @_rddStorageInfo = new mutable.HashMap[Int, RddStorageInfo]	RDD存储信息映射表
    #name @_nonRddStorageInfo = NonRddStorageInfo(0L, 0L, 0L)	非EDD存储信息
    样例类:
    case class RddStorageInfo(memoryUsage: Long, diskUsage: Long, level: StorageLevel)
    介绍: RDD存储信息
    构造器参数:
        memoryUsage	内存使用量
        diskUsage	磁盘写出量
        level	存储等级
    
    case class NonRddStorageInfo(var onHeapUsage: Long, var offHeapUsage: Long,
      var diskUsage: Long)
    介绍: 非RDD存储信息
    构造器参数:
        onHeapUsage	堆内存使用量
        offHeapUsage	非堆模式内存使用量
        diskUsage	磁盘使用量
    
    构造器:
    def this(
          bmid: BlockManagerId,
          maxMemory: Long,
          maxOnHeapMem: Option[Long],
          maxOffHeapMem: Option[Long],
          initialBlocks: Map[BlockId, BlockStatus]) {
        this(bmid, maxMemory, maxOnHeapMem, maxOffHeapMem)
        initialBlocks.foreach { case (bid, bstatus) => addBlock(bid, bstatus) }
      }
    功能: 使用初始化数据块列表来创建存储状态,使得信息源不会被修改
    
    操作集:
    def blocks: Map[BlockId, BlockStatus] = _nonRddBlocks ++ rddBlocks
    功能: 获取数据块与数据块状态的映射表
    
    def rddBlocks: Map[BlockId, BlockStatus] = _rddBlocks.flatMap { case (_, blocks) => blocks }
    功能: 获取存储于块管理器的RDD数据块
    
    def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit
    功能: 添加指定数据块@blockId 到数据块列表中(需要区分是不是RDD类型)
    1. 更新存储信息
    updateStorageInfo(blockId, blockStatus)
    2. 添加数据块信息到映射表中
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.getOrElseUpdate(rddId, new mutable.HashMap)(blockId) = blockStatus
      case _ =>
        _nonRddBlocks(blockId) = blockStatus
    }
    
    def getBlock(blockId: BlockId): Option[BlockStatus]
    功能: 在O(1)的时间复杂度下获取指定数据块的状态@BlockStatus
    val=  blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).flatMap(_.get(blockId))
      case _ =>
        _nonRddBlocks.get(blockId)
    }
    
    def maxMem: Long = maxMemory
    功能: 获取最大内存
    
    def memRemaining: Long = maxMem - memUsed
    功能: 获取内存剩余量
    
    def memUsed: Long = onHeapMemUsed.getOrElse(0L) + offHeapMemUsed.getOrElse(0L)
    功能: 获取内存使用量
    
    def onHeapMemRemaining: Option[Long] =
    for (m <- maxOnHeapMem; o <- onHeapMemUsed) yield m - o
    功能: 获取堆模式内存剩余量
    
    def offHeapMemRemaining: Option[Long] =
    	for (m <- maxOffHeapMem; o <- offHeapMemUsed) yield m - o
    功能: 获取非堆模式下内存剩余量
    
    def onHeapMemUsed: Option[Long] = onHeapCacheSize.map(_ + _nonRddStorageInfo.onHeapUsage)
    功能: 获取块管理器使用的堆模式内存
    
    def offHeapMemUsed: Option[Long] = offHeapCacheSize.map(_ + _nonRddStorageInfo.offHeapUsage)
    功能: 获取非堆模式内存的使用量
    
    def onHeapCacheSize: Option[Long]
    功能: 获取堆模式下缓存大小
    val= maxOnHeapMem.map { _ =>
        _rddStorageInfo.collect {
          case (_, storageInfo) if !storageInfo.level.useOffHeap => storageInfo.memoryUsage
        }.sum
      }
    
    def offHeapCacheSize: Option[Long]
    功能: 获取非堆模式下缓存大小
    val= maxOffHeapMem.map { _ =>
        _rddStorageInfo.collect {
          case (_, storageInfo) if storageInfo.level.useOffHeap => storageInfo.memoryUsage
        }.sum
      }
    
    def diskUsed: Long = _nonRddStorageInfo.diskUsage + _rddBlocks.keys.toSeq.map(diskUsedByRdd).sum
    功能: 获取磁盘使用量
    
    def diskUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_.diskUsage).getOrElse(0L)
    功能: 获取磁盘RDD使用量
    
    def updateStorageInfo(blockId: BlockId, newBlockStatus: BlockStatus): Unit
    功能: 更新存储信息
    1. 获取旧数据块状态
    val oldBlockStatus = getBlock(blockId).getOrElse(BlockStatus.empty)
    2. 获取内存磁盘新增度量值
    val changeInMem = newBlockStatus.memSize - oldBlockStatus.memSize
    val changeInDisk = newBlockStatus.diskSize - oldBlockStatus.diskSize
    val level = newBlockStatus.storageLevel
    3. 从旧信息的基础上计算新值
    val (oldMem, oldDisk) = blockId match {
      case RDDBlockId(rddId, _) =>
        _rddStorageInfo.get(rddId)
          .map { case RddStorageInfo(mem, disk, _) => (mem, disk) }
          .getOrElse((0L, 0L))
      case _ if !level.useOffHeap =>
        (_nonRddStorageInfo.onHeapUsage, _nonRddStorageInfo.diskUsage)
      case _ if level.useOffHeap =>
        (_nonRddStorageInfo.offHeapUsage, _nonRddStorageInfo.diskUsage)
    }
    val newMem = math.max(oldMem + changeInMem, 0L)
    val newDisk = math.max(oldDisk + changeInDisk, 0L)
    4. 对上述计算结果进行修正
    blockId match {
      case RDDBlockId(rddId, _) =>
        // RDD不能再持久化,移除
        if (newMem + newDisk == 0) {
          _rddStorageInfo.remove(rddId)
        } else {
          _rddStorageInfo(rddId) = RddStorageInfo(newMem, newDisk, level)
        }
      case _ =>
        if (!level.useOffHeap) {
          _nonRddStorageInfo.onHeapUsage = newMem
        } else {
          _nonRddStorageInfo.offHeapUsage = newMem
        }
        _nonRddStorageInfo.diskUsage = newDisk
    }
}
```

```scala
private[spark] object StorageUtils extends Logging {
    属性:
    #name @bufferCleaner #Type @DirectBuffer => Unit	缓冲清理函数
    if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) { // JDK8以上处理方案
      val cleanerMethod =
        Utils.classForName("sun.misc.Unsafe").getMethod("invokeCleaner", classOf[ByteBuffer])
      val unsafeField = classOf[Unsafe].getDeclaredField("theUnsafe")
      unsafeField.setAccessible(true)
      val unsafe = unsafeField.get(null).asInstanceOf[Unsafe]
      buffer: DirectBuffer => cleanerMethod.invoke(unsafe, buffer)
    }else {
      val cleanerMethod = Utils.classForName("sun.misc.Cleaner").getMethod("clean")
      buffer: DirectBuffer => {
        // Careful to avoid the return type of .cleaner(), which changes with JDK
        val cleaner: AnyRef = buffer.cleaner()
        if (cleaner != null) {
          cleanerMethod.invoke(cleaner)
        }
      }
    }
    
    操作集:
    def dispose(buffer: ByteBuffer): Unit
    功能: 对缓冲数据的最后处理(清除缓冲(如果他是内存映射或者是这直接缓冲区,避免内存泄漏))
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Disposing of $buffer")
      bufferCleaner(buffer.asInstanceOf[DirectBuffer])
    }
    
    def externalShuffleServicePort(conf: SparkConf): Int
    功能: 获取外部shuffle服务的端口
    val tmpPort = Utils.getSparkOrYarnConfig(conf, config.SHUFFLE_SERVICE_PORT.key,
      config.SHUFFLE_SERVICE_PORT.defaultValueString).toInt
    val= if (tmpPort == 0) conf.get(config.SHUFFLE_SERVICE_PORT.key).toInt else tmpPort
}
```

#### TopologyMapper

```scala
@DeveloperApi
abstract class TopologyMapper(conf: SparkConf) {
    介绍: 为给定节点提供拓扑关系
    def getTopologyForHost(hostname: String): Option[String]
    功能: 获取当前节点的拓扑关系
    可以使用拓扑定界符进行分割,比如说/myrack/myhost中,/就是拓扑定界符.myrack时拓扑标识符,myhost时个人主机名.
    这个方法只返回拓扑标识符,不返回主机名
}
```

```scala
@DeveloperApi
class DefaultTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
    介绍: 默认拓扑逻辑映射(假设所有节点都在一个机架上)
    操作集:
    def getTopologyForHost(hostname: String): Option[String] = {
        logDebug(s"Got a request for $hostname")
        None
      }
    功能: 获取主机的拓扑标识符
}
```

```scala
@DeveloperApi
class FileBasedTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
    属性:
    #name @topologyFile = conf.get(config.STORAGE_REPLICATION_TOPOLOGY_FILE)	拓扑名称
    #name @topologyMap = Utils.getPropertiesFromFile(topologyFile.get)	拓扑图
    参数校验:
    require(topologyFile.isDefined, "Please specify topology file via " +
    "spark.storage.replication.topologyFile for FileBasedTopologyMapper.")
    功能: 断言拓扑文件存在
    
    def getTopologyForHost(hostname: String): Option[String]
    功能: 获取指定主机的拓扑标识符
    val topology = topologyMap.get(hostname)
    if (topology.isDefined) {
      logDebug(s"$hostname -> ${topology.get}")
    } else {
      logWarning(s"$hostname does not have any topology information")
    }
    topology
}
```

#### 基础拓展

1.  容错性的保证 -- 冗余策略
2.  [Robert Floyd采样算法](# https://math.stackexchange.com/q/178690)
3.  TTL
4.  弱引用