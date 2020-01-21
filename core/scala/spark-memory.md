## **spark-memory**

---

1.  [ExecutionMemoryPool.scala](# ExecutionMemoryPool)
2.  [MemoryManager.scala](# MemoryManager)
3.  [MemoryPool.scala](# MemoryPool)
4.  [StorageMemoryPool.scala](# StorageMemoryPool)
5.  [UnifiedMemoryManager.scala](# UnifiedMemoryManager)

---

#### ExecutionMemoryPool

```markdown
介绍:
	实现保险(policies)和记录(bookkeeping),用于多任务之间共享一个可调整大小的内存池.
	尝试去保证每一个任务获取一个内存副本,而非是一个任务增大到内存使用到一个较大的值,使得其他任务不得不去反复的溢写磁盘。假设有N个任务，那么每个任务在它需要溢写之前至少需要1/(2*N)个内存占有量。至多为1/N个内存占有量。由于N是动态的，需要保证对活动任务的追踪，且需要在等待任务中重新计算1/N以及1/2*N值。这个通过同步方法去改变状态，使用@wait()和@notifyAll()去提示调用者。在Spark 1.6之前是通过@ShuffleMemoryManager完成。
```
```markdown
private[memory] class ExecutionMemoryPool(lock: Object,memoryMode: MemoryMode){
	关系: father --> MemoryPool(lock)
		sibling --> Logging
	构造器属性: 
		lock: Object	@MemoryManager实例，用于同步
		memoryMode: MemoryMode	内存模式
	属性:
	#name @poolName #type @String	内存池名称
	val= memoryMode match{
	    case MemoryMode.ON_HEAP => "on-heap execution"
        case MemoryMode.OFF_HEAP => "off-heap execution"
  	}
  	#name @memoryForTask=new mutable.HashMap[Long, Long]() @GuardedBy("lock")
  		存储taskAttemptId -> 内存消耗量的键值对
  	
  	操作集:
  	def memoryUsed: Long = lock.synchronized{memoryForTask.values.sum}
  	功能: 获取内存使用量
  	
  	def getMemoryUsageForTask(taskAttemptId: Long): Long=
  	功能: 获取指定任务id@taskAttemptId的内存消耗量(字节计数)
  	val= lock.synchronized {memoryForTask.getOrElse(taskAttemptId, 0L)}
  	
  	def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit
  	功能: 释放给定任务@taskAttemptId指定大小@numBytes的内存
  	1，获取当前任务所需内存量
  		val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
  	2. 获取需要释放的内存量
  		val memoryToFree= curMem < numBytes? curMem : numBytes
  	3. 处理任务内存列表中指定任务@taskAttemptId
  	memoryForTask(taskAttemptId) -= memoryToFree
  	+ 释放之后内存占用非正
  	memoryForTask.remove(taskAttemptId)
  	4. 释放完毕，唤醒@acquireMemory()方法等待队列的首个元素
  	
  	def releaseAllMemoryForTask(taskAttemptId: Long): Long
  	功能: 释放给定任务的所有内存，并标记其为不活动(inactive),返回释放内存量
  	val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
  	val=numBytesToFree
  	
  	def acquireMemory(numBytes: Long,taskAttemptId: Long,
     	maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => (),
      	computeMaxPoolSize: () => Long = () => poolSize): Long 
  	功能: 试着获取@numBytes个字节给指定任务,返回获得的字节数，如果返回0就什么也没有获得。
  	这个方法是阻塞的，直到有拥有足够的内存时候才会执行。为了确保每个任务在它溢写前都能达到1/(2*N) *pool_size(
  	N是活动任务数量)。这个可以发生在任务数量增加但是老任务占有了大量的内存。
  	输入参数:
  		numBytes	获取内存量
  		taskAttemptId	任务id
  		maybeGrowPool	内存池扩容函数
  			maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => ()
  		computeMaxPoolSize	计算指定时刻，内存池所允许的最大可容纳容量
  	计算条件: numBytes>0
  	1. 任务内存记录表创建新的记录@taskAttemptId=key
  	 if (!memoryForTask.contains(taskAttemptId)) {
      	memoryForTask(taskAttemptId) = 0L
      	// This will later cause waiting tasks to wake up and check numTasks again
      	lock.notifyAll()
      }
    2. 保持循环知道拥有足够空闲内存量(>=1/(2*N))
    while (true) {
      // 获取激活任务数量
      val numActiveTasks = memoryForTask.keys.size
      // 获取指定任务@taskAttemptId 所占有的内存量
      val curMem = memoryForTask(taskAttemptId)
      // 扩展内存池的大小 delta=numBytes - memoryFree
      maybeGrowPool(numBytes - memoryFree)
      // 计算此时内存池最大容量
      val maxPoolSize = computeMaxPoolSize()
      // 单任务最大内存量
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      // 单任务最小内存量
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      val toGrant = math.min(maxToGrant, memoryFree)
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()
      } else {
        memoryForTask(taskAttemptId) += toGrant
        // 返回申请完毕时候，当前任务的内存量
        return toGrant
      }
}
```

#### MemoryManager

```markdown
介绍： 
	抽象内存管理器，可以实现执行器与存储之间内存共享。
	在这里，执行器内存时用于执行shuffle,join，sort,aggregations。而存储器指向缓存和集群间传播的外部数据。且在每个JVM中存在有一个内存管理器@MemoryManager
```

```markdown
private[spark] abstract class MemoryManager(conf: SparkConf,numCores: Int,onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long){
	关系: father --> Logging
    构造器属性:
        conf	应用程序配置集
        numCores	核心数量
        onHeapStorageMemory	堆模式下存储器内存
        onHeapExecutionMemory	堆模式下执行器内存
	属性：
	#name @onHeapStorageMemoryPool=new StorageMemoryPool(this, MemoryMode.ON_HEAP) @GuardedBy("this")
		堆模式下存储器内存池
	#name @offHeapStorageMemoryPool=new StorageMemoryPool(this,MemoryMode.OFF_HEAP)  
		@GuardedBy("this")
		非堆模式下存储器内存池
	#name @onHeapExecutionMemoryPool=new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
		@GuardedBy("this")
		堆模式下执行器内存池
	#name @offHeapExecutionMemoryPool=new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)
		@GuardedBy("this")
		非堆模式下执行器内存池
	#name @maxOffHeapMemory=conf.get(MEMORY_OFFHEAP_SIZE)
		最大非堆模式内存
	#name @offHeapStorageMemory=(maxOffHeapMemory * conf.get(MEMORY_STORAGE_FRACTION)).toLong
		非堆模式下存储内存大小
	#name @tungstenMemoryMode Tungsten内存模式
	#name @tungstenMemoryAllocator #type @MemoryAllocator
		Tungster内存分配器
		val=tungstenMemoryMode match {
                case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
                case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE }
    #name @pageSizeBytes #type @Long	页大小
    1. 获取最大/最小页大小
        val minPageSize = 1L * 1024 * 1024   // 1MB
        val maxPageSize = 64L * minPageSize  // 64MB
        val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
	2. 获取内存管理器内存池大小
		val maxTungstenMemory: Long = tungstenMemoryMode match {
      		case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
      		case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize }
     3. 获取默认页大小
     val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
     val default = math.min(maxPageSize, math.max(minPageSize, size))
     conf.get(BUFFER_PAGESIZE).getOrElse(default)
	初始化操作:
	初始化堆模式下存储器/执行器内存池
        onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
        onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)
	
	初始化非堆模式下存储器/执行器内存池
	  	offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  		offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)
	
	操作集:
	def maxOnHeapStorageMemory: Long
	功能: 获取最大堆模式下存储器内存，这个模式下值与执行器未占用内存相等
	
	def maxOffHeapStorageMemory: Long
	功能: 获取非堆模式下最大存储器内存
	
	def setMemoryStore(store: MemoryStore): Unit
	功能: 设置新的存储器，以逐出旧的存储器
	synchronized {
        onHeapStorageMemoryPool.setMemoryStore(store)
        offHeapStorageMemoryPool.setMemoryStore(store) }
  	
  	def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean
  	功能: 获取指定字节@numBytes的内存给指定的块缓存,有必要则会逐出部分内存，全部成功执行返回true
  	
  	def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean
  	功能: 获取指定大小@numBytes的内存给指定的块展开,如有必要则会逐出部分内存。
  	
    def acquireExecutionMemory(numBytes: Long,taskAttemptId: Long,memoryMode: MemoryMode): Long
	功能: 获取指定大小@numBytes内存给指定任务@taskAttemptId，分配时采用@memoryMode的模式
	
	def releaseExecutionMemory(numBytes: Long,taskAttemptId: Long,memoryMode: MemoryMode): Unit
	功能: 是否执行器内存
	synchronized {
       memoryMode match {
      	case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      	case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    	}
  	}
  	
  	def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long
  	功能: 是否任务@taskAttemptId的所有执行器内存
  	val= synchronized {
    	onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      	offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  	}
  	
  	def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit 
  	功能: 是否指定大小@numBytes的存储器内存
  	synchronized {
    	memoryMode match {
      	case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
      	case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
    	}
  	}
  	
  	def releaseAllStorageMemory(): Unit 
  	功能: 释放所有存储器内存
  	synchronized {
    	onHeapStorageMemoryPool.releaseAllMemory()
    	offHeapStorageMemoryPool.releaseAllMemory()
  	}
  	
  	def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit 
  	功能: 是否指定大小@numBytes展开内存
		releaseStorageMemory(numBytes, memoryMode)
	
	def executionMemoryUsed: Long 
	功能: 获取执行器内存使用量
	val= synchronized {onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed}
	
	def storageMemoryUsed: Long
	功能: 获取存储器内存使用量
	val= onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
	
	def onHeapExecutionMemoryUsed: Long
	功能: 获取堆模式下执行器内存使用量
	val= synchronized { onHeapExecutionMemoryPool.memoryUsed }
	
	def offHeapExecutionMemoryUsed: Long
	功能: 获取非堆模式下内存使用量
	val=  offHeapExecutionMemoryPool.memoryUsed
	
	def onHeapStorageMemoryUsed: Long 
	功能: 获取堆模式下存储器内存使用量
	val= onHeapStorageMemoryPool.memoryUsed
	
	def offHeapStorageMemoryUsed: Long
	功能: 获取非堆模式下内存使用量
	val= offHeapStorageMemoryPool.memoryUsed
	
	def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long
	功能: 获取指定任务@taskAttemptId 执行器内存使用量
	val= onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)	
}
```

#### MemoryPool

```markdown
介绍:
	管理一个可调整大小的内存区域的记录(bookkeeping),这个类是#class @MemoryManager的子类。详情请看其子类。
```

```markdown
private[memory] abstract class MemoryPool(lock: Object){
	构造器属性:
		lock	一个@MemoryManager实例，用于同步，使用Object主要是去避免程序出错。
	属性:
		#name @_poolSize=0 @GuardedBy("lock") 内存池大小
	操作集:
	def poolSize: Long = lock.synchronized {_poolSize}
	功能: 获取内存池的大小
	
	def memoryFree: Long = lock.synchronized {_poolSize - memoryUsed}
	功能: 获取释放内存量
	
	def incrementPoolSize(delta: Long): Unit 
	功能: 内存池扩充delta个字节
	val= lock.synchronized { require(delta >= 0) 
    	_poolSize += delta}
	
	def decrementPoolSize(delta: Long): Unit 
	功能: 内存池收缩delta个字节
	val= lock.synchronized{
		require(delta >= 0)
    	require(delta <= _poolSize)
    	require(_poolSize - delta >= memoryUsed)
    	_poolSize -= delta
	}
	
	def memoryUsed: Long
	功能: 获取内存使用量
}
```

#### StorageMemoryPool

```markdown
介绍:
	管理一个可调整大小的线程池的记录(bookkeeping),这个记录表用于存储(缓存)
```

```markdown
private[memory] class StorageMemoryPool(lock: Object,memoryMode: MemoryMode){
	关系: father --> MemoryPool(Pool) 
		sibling --> Logging
	构造器属性:
		lock	@MemoryManager实例，用于同步
		memoryMode	内存池的内存模式(堆模式/非堆模式)
	属性:
	#name @poolName	#type @String		线程名称
	val= memoryMode match{
		    case MemoryMode.ON_HEAP => "on-heap storage"
    		case MemoryMode.OFF_HEAP => "off-heap storage"}
	#name @_memoryUsed=0 @GuardedBy("lock")		内存使用量
	#name @_memoryStore #type @MemoryStore var	内存存储器
	操作集:
	def memoryUsed: Long = lock.synchronized {_memoryUsed}
	功能: 获取内存使用量@_memoryUsed
	
	def memoryStore: MemoryStore 
	功能: 获取内存存储器
	val= _memoryStore == null ? throw IllegalStateException : _memoryStore
	
	def setMemoryStore(store: MemoryStore): Unit={_memoryStore = store}
	功能: 设置内存存储器内容，用于将之前的缓存块去除
	
	def acquireMemory(blockId: BlockId, numBytes: Long): Boolean
	功能: 获取你指定字节的内存去对指定的块@BlockId进行缓存,如果由必要对之前进入的缓存内容逐出.当所有字节都		成功执行成功则返回true
	1. 获取需要申请的内存大小
	val numBytesToFree = math.max(0, numBytes - memoryFree)
	2. 申请numBytes大小,numBytesToFree逐出内存大小的内存块给@blockId
	acquireMemory(blockId, numBytes, numBytesToFree)
	
	def acquireMemory(blockId: BlockId,numBytesToAcquire: Long,numBytesToFree: Long): Boolean
	功能: 获取指定字节的内存,分配给给定的块,如有需要可以逐出内存中的部分内存
	输入参数:
		blockId	块编号
		numBytesToAcquire	需要获取内存大小(块的大小)
		numBytesToFree	通过逐出块内存的内存释放量
	操作条件: 内存释放量非负,块大小非负,内存在使用的合法范围内(memoryUsed <= poolSize)
	操作逻辑:
	1. 释放对应数据块的内存
	numBytesToFree > 0 ? 
		memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode) : Nop
	2. 为块分配内存
	val enoughMemory = numBytesToAcquire <= memoryFree // 确定是否由足够内存去分配
	if (enoughMemory) {_memoryUsed += numBytesToAcquire}
	val= enoughMemory
	
	def releaseAllMemory(): Unit = lock.synchronized {_memoryUsed = 0}
	功能: 是否所有内存
	
	def releaseMemory(size: Long): Unit 
	功能: 释放指定大小@size的内存空间
	size > _memoryUsed ? _memoryUsed = 0 : _memoryUsed -= size
	
	def freeSpaceToShrinkPool(spaceToFree: Long): Long
	功能: 通过内存池设置@spaceToFree参数,释放空间以缩小存储的大小
		这个方法不会真正的减少内存池的大小,但是需要调用者这么做.返回从内存池中移除的字节数.
	操作逻辑:
	1. 获取未使用内存大小
	val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
	2. 获取剩余需要释放内存大小
	val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
	3. 释放剩余需要释放的内存(remainingSpaceToFree > 0)
	+ 逐出内存存储器中剩余空间的大小
	val spaceFreedByEviction = 
    	memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
    + 返回总计内存释放量
    val=remainingSpaceToFree > 0?
    	spaceFreedByReleasingUnusedMemory + spaceFreedByEviction:spaceFreedByReleasingUnusedMemory
}
```

#### UnifiedMemoryManager

```markdown
介绍:
	这是一种@MemoryManager , 它实现了执行器和存储器的软界限，以至于两种类型内存空间可以相互借用内存。
	执行器和存储器之间共享区域是一个分数(合计堆空间300MB)，这个分数可以通过spark.memory.fraction指定(默认值为0.6)。空间内部界限值由@spark.memory.storageFraction(默认0.5)决定。这就意味着存储区域范围为0.6*0.5=0.3倍的堆空间大小。
	存储器可以借用执行器中空闲的内存空间。知道执行器声明了它的内存空间。当它发生时，缓存块会从内存中逐出直到有效的借用内存释放了，且满足执行器内存请求。
	相似的，执行器页可以借用存储器空闲内存空间。然而，执行器所占内存是不会被逐出的(由于其复杂的实现)。这种实现在执行器使用了存储器的大部分空闲空间时会造成缓冲块失效。在这种情况下，新产生的块会因为存储等级的问题而被立即逐出。
```

```markdown
private[spark] class UnifiedMemoryManager(conf: SparkConf,val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,numCores: Int){
	关系: father --> MemoryManager(conf,numCores,onHeapStorageRegionSize,
    				maxHeapMemory - onHeapStorageRegionSize)    
	构造器属性:
		conf	应用配置集
		maxHeapMemory	最大堆内存
		onHeapStorageRegionSize	堆模式下存储空间范围
		numCores	核心数
	操作集:
	def assertInvariants(): Unit
	功能: 对于内存池参数断言
	assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
	assert(offHeapExecutionMemoryPool.poolSize + 
		offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
	
	def maxOnHeapStorageMemory: Long
	功能: 获取最大堆模式下存储器内存
	val= synchronized { maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed }
	
	def maxOffHeapStorageMemory: Long
	功能: 获取最大非堆模式存储器内存
	val= synchronized { maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed }
	
	def acquireExecutionMemory(numBytes: Long,taskAttemptId: Long,memoryMode: MemoryMode): Long
	功能: 获取执行器内存
		获取指定大学@numBytes的执行器内存，给当前任务，返回获取的内存量大小，这个方法可能会引起阻塞，直到有足		够空闲内存，且保证每个任务的内存量保证在[1/(2*N),1/N]之间。
	操作条件: @numBytes>0 且满足之前的断言条件@assertInvariants()
	操作逻辑: 
	根据堆模式与否分为下述两种情况
	val (executionPool, storagePool, storageRegionSize, maxMemory)=MemoryMode.ON_HEAP==memoryMode ?
		(onHeapExecutionMemoryPool,onHeapStorageMemoryPool,onHeapStorageRegionSize,maxHeapMemory)
    	: (offHeapExecutionMemoryPool,offHeapStorageMemoryPool,offHeapStorageMemory,maxOffHeapMemory)
	
	def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit
	功能: 通过逐出缓存块来增加执行器内存池，从而缩小存储内存池。当获取一个任务的内存的时候，执行内存池需要发起	多个请求。每个请求必须要能够逐出存储器内容(假设请求之前有任务缓存了一大块内存)。每个请求被调用一次。
	操作条件: extraMemoryNeeded> 0
	1. 计算存储器内存池可以用于转换部分大小
	val memoryReclaimableFromStorage = math.max(storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
	2. 存储器内存池空可转换部分转换成执行器内存池
        // 存储器内存池缩小
        val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
        math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
        storagePool.decrementPoolSize(spaceToReclaim)
        // 执行器内存扩大
        executionPool.incrementPoolSize(spaceToReclaim)
        
    def acquireUnrollMemory(blockId: BlockId,numBytes: Long,memoryMode: MemoryMode): Boolean
    功能: 获取扩展内存
    val = synchronized { acquireStorageMemory(blockId, numBytes, memoryMode) }
    
    def acquireStorageMemory(blockId: BlockId,numBytes: Long,memoryMode: MemoryMode): Boolean 
	操作条件: 符合断言条件@assertInvariants() 且@numBytes非负
	1. 获取执行器内存池，存储器内存池，最大容量
	val (executionPool, storagePool, maxMemory)= MemoryMode.ON_HEAP== memoryMode ?
		(onHeapExecutionMemoryPool,onHeapStorageMemoryPool,maxOnHeapStorageMemory) :
		(offHeapExecutionMemoryPool,offHeapStorageMemoryPool,maxOffHeapStorageMemory)
	2. 分支情况处理  
	+ 当最大内存不足分配@numBytes，直接快速失败
	+ @numBytes > storagePool.memoryFree 即存储器内存池剩余容量不足，需要向执行器内存池中借用内存
    求解内存借用量: 
    val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,numBytes -
    	storagePool.memoryFree)    
    缩小执行器内存池，扩大存储器内存值
    executionPool.decrementPoolSize(memoryBorrowedFromExecution)
    storagePool.incrementPoolSize(memoryBorrowedFromExecution)
	
	3. 为@blockId获取内存为@numBytes的空间，并返回获取结果
	val= storagePool.acquireMemory(blockId, numBytes)
}
```

```markdown
object UnifiedMemoryManager{
	属性:
	#name @RESERVED_SYSTEM_MEMORY_BYTES=300 * 1024 * 1024 系统保有内存
		默认情况下用于给执行器和存储器的内存=(1024 - 300) * 0.6 = 434MB
	
	操作集:
	def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager 
	功能: 返回由指定应用程序配置集@conf和核心数@numCores组成的内存管理器@UnifiedMemoryManager
	val maxMemory = getMaxMemory(conf)
	val= UnifiedMemoryManager(conf,maxHeapMemory = maxMemory,
      onHeapStorageRegionSize =(maxMemory * conf.get(config.MEMORY_STORAGE_FRACTION)).toLong,
      numCores = numCores)
      
     def getMaxMemory(conf: SparkConf): Long
     功能: 获取@conf下的最大分配内存量
     1. 获取系统内存量和保留内存量
     val systemMemory = conf.get(TEST_MEMORY)
     val reservedMemory = conf.getLong(TEST_RESERVED_MEMORY.key,
      	if (conf.contains(IS_TESTING)) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
	2. 系统内存量不满足最小内存要求，快速失败
	if (systemMemory < minSystemMemory) throw IllegalArgumentException	
	3. 执行器内存不满足要求，快速失败
	val executorMemory = conf.getSizeAsBytes(config.EXECUTOR_MEMORY.key)
	if (executorMemory < minSystemMemory) throw IllegalArgumentException
    4. 求解最大可使用内存量
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.get(config.MEMORY_FRACTION)
    (usableMemory * memoryFraction).toLong
}
```

