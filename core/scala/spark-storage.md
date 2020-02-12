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
25.  [TopologyMapper.scala](# TopologyMapper)
26.  [基础拓展](# 基础拓展)

---

#### MemoryStore

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

#### BlockManager

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
    
    def updateBlockInfo(blockManagerId: BlockManagerId,blockId: BlockId,storageLevel: StorageLevel,
      memSize: Long,diskSize: Long): Boolean
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

#### DiskStore

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