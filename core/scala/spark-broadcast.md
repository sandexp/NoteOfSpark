## **spark - broadcast**

---

1.  [Broadcast.scala](# Broadcast)
2.  [BroadcastFactory.scala](# BroadcastFactory)
3.  [BroadcastManager.scala](# BroadcastManager)
4.  [TorrentBroadcast.scala](# TorrentBroadcast)
5.  [TorrentBroadcastFactory.scala](# TorrentBroadcastFactory)
6. [scala基础拓展](# scala基础拓展)

---

#### Broadcast

```markdown
介绍:
广播变量: 广播变量允许程序员保存一份只读的变量,将之缓存在每台机器上.而不是通过任务去传送一份副本.举例来说,它可以用于,它可以通过一种高效的方式给每个节点机器一份大规模数据集(dataset)的副本.同时spark也通过一种高效的广播算法,将广播变量分发给每台机器,有效的降低了建立连接的成本.

Broadcast变量创建方法:
	常见Broadcast方法:
	scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
	获取广播变量的值
	scala> broadcastVar.value

Broadcast创建完毕之后,在集群当中应当使用广播变量@broadcastVar而非值v.且需要注意到,一旦值v被广播之后,其值是不可以修改的,以便于确保所有的节点都获得了同一个值.
```

```scala
abstract class Broadcast[T: ClassTag] (val id: Long){
	id : 广播变量唯一标识
	关系:
		father -> Serializable
		mixed -> Logging
	属性:
		1. @volatile private var _isValid
			指示广播变量是否可以使用 true为默认表示可以使用
		2. private var _destroySite = ""
			销毁的地址参数
	操作集:
		def value: T 
		功能: 获取广播变量的值
		操作条件: 广播变量可以使用 使用#method @assertValid()
		
		def unpersist(): Unit
		功能: 异步删除本广播变量在执行器(executors)上的缓存副本,且如果调用该方法之后,广播变量又使用了,需要重		新发送到每台执行器上.
		操作逻辑: #method @unpersist(false)
		
		def unpersist(blocking: Boolean): Unit
		功能: 同@unpersist()相似,只是这里的block指示是否阻塞到解除持久化完成(blocking)
		操作条件: 广播变量可以使用 @assertValid()
		操作逻辑: @doUnpersist(blocking)
		
		def destroy(): Unit 
		功能: 销毁所有与广播变量相关的数据和元数据.这里需要注意到的是一旦广播变量销毁,就不能再使用了.
		操作逻辑: #method @destroy(blocking=false)
		
		private[spark] def destroy(blocking: Boolean): Unit
		功能: 销毁所有与广播变量相关的数据和元数据.这里需要注意到的是一旦广播变量销毁,就不能再使用了.
			blocking 指示是否要阻塞到销毁工作完成
		操作条件: 广播变量可以使用 @assertValid()
		操作逻辑:
			1. 广播变量可用标志@_isValid置为false
			2. 设置释放地址@_destroySite=Utils.getCallSite().shortForm
        	 3. 使用doDestroy释放空间
        	 
        private[spark] def isValid: Boolean 
        功能: 获取广播变量可用标志@_isValid
        
        protected def getValue(): T
        功能: 获取广播变量的值,是一个抽象的函数,必须再子类区实现这个逻辑
        
        protected def doUnpersist(blocking: Boolean): Unit
        功能: 再执行器(executors)上解除广播变量值的持久化工作,需要通过子类实现该逻辑
        
        protected def doDestroy(blocking: Boolean): Unit
        功能: 释放广播变量的数据以及元数据信息,需要再子类中定义实现方案
        
        protected def assertValid(): Unit 
        功能: 检查广播变量是否可以使用,不能使用则抛出异常@SparkException
        
        def toString: String 
        功能: 获取字符串信息 Broadcast( @id )
}
```

---

#### BroadcastFactory

```markdown
这是一个对于所有广播变量在spark中实现的接口(允许多个广播变量实现).spark上下文管理器@SparkContext使用广播变量工厂@BroadcastFactory实现对整个spark job的广播变量的实例化.
```

```scala
private[spark] trait BroadcastFactory{
	操作集:
	def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager): Unit	
	功能: 初始化工作,逻辑有子类实现
	输入参数:
		驱动器标志		isDriver	Boolean
		应用程序配置集	   conf	   	   SparkConf
         安全管理器		 securityMgr SecurityManager
	
	def newBroadcast[T: ClassTag](value: T, isLocal: Boolean, id: Long): Broadcast[T]
	功能: 创建一个广播变量,逻辑由子类实现
		value 广播变量的值
		isLocal 是否为本地模式(本地模式仅有单个JVM进程)
		id 广播变量唯一标识符
	
	def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit
	功能: 解除广播变量,逻辑由子类实现
	输入参数:
		id	广播变量唯一标识符
		removeFromDriver	是否从驱动器(driver中移除)
		blocking	是否采用阻塞式执行方式
		
	def stop(): Unit
	功能: 停止,逻辑由子类实现
}
```

----

#### BroadcastManager

```scala
private[spark] class BroadcastManager(val isDriver: Boolean,conf: SparkConf,
securityManager: SecurityManager){
	关系: 
		father -> Logging
	属性:
	1. 构造器属性
		#name @isDriver #type @val Boolean 驱动器标志
		#name @conf #type @SparkConf  spark应用程序配置集
		#name @securityManager #type @SecurityManager 安全管理器
	2. 其他属性
		#name @initialized=false #type @private var	初始化状态位(初始为false)
		#name @broadcastFactory #type @BroadcastFactory 广播变量工厂(初始为null)
		#name @nextBroadcastId #type @AtomicLong val 下一个广播变量Id
         #name @cachedValues #type @SynchronizedMap private[broadcast] val  广播变量缓存值
	操作集:
		private def initialize(): Unit
		功能: 使用广播变量前调用SparkContext和Executor(互斥操作需要同步)
		执行条件: 初始化状态@initialized为false
		操作逻辑: 
			1. 实例化广播变量工厂 @broadcastFactory = new @TorrentBroadcastFactory
			2. 初始化构造器中的信息 #class @BroadcastFactory #method @initialize
			3. 置位初始化状态位@initialized	
		
		def stop(): Unit
		功能: 停止工厂类
			broadcastFactory.stop()
			
		def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T]
		功能: 新建广播变量
			1. 计算bid=#class @AtomicLong #method @getAndIncrement()
			2. 通过工厂新建一个广播变量
				其中
					value=value_
					isLocal=isLocal
					id=id
				需要注意的是,当你要使用python的共享变量时,需要设置id与底层数据文件之间的映射具体参照
				@PythodRDD.scala
		
		def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit
		功能: 使用工厂类移除广播变量,参数意义参照#class @BroadcastFactory #method @unbroadcast
			broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
		
}
```

----

#### TorrentBroadcast

```markdown
Broadcast类的一种比特流实现方式
原理:
	驱动器(driver)将序列化完成的对象分割成多个小块(chunk),并将这些小块存储在驱动器的块管理器中@BlockManager.
	在执行器中(executor),执行器首先尝试获取自己来自自己的块管理器@BlockManager中的对象.如果不存在,则使用远程获取的方式拿到驱动器或者其他可以获得的执行器上的对象.一旦获取到这些小块(chunks).便将其置入自己的块管理器中.以便其他执行器获取.
	这个操作过程禁止驱动器成为发送多个广播变量副本到执行器中的瓶颈.
	初始化过程,本类会读取SparkEnv中的配置信息.
```

```scala
private[spark] class TorrentBroadcast [T: ClassTag] (obj: T, id: Long){
	关系:
		father-> Broadcast[T](id)
		mixed --> Logging --blockSize> Serializable
	属性:
		#name @_value #type @SoftReference[T] transient var 
			执行器上广播变量的值,可以通过@readBroadcastBlock重新构造(通过读取驱动器/其他执行器上的块来执行			此操作)
		#name @compressionCodec #type Option[CompressionCodec]  transient var
        	压缩方式参数,如果位Null则不使用压缩
        #name @blockSize #type @int transient var
        	块大小/每块默认大小为4MB,这个只能被广播者读取
        #name @broadcastId #type @BroadcastBlockId(id)
        	块编号
        #name @numBlocks #type #int
        	块数量
        #name @checksumEnabled=false #type @Boolean
        	是否产生校验数
        #name @checksums #type @Array[int]
        	校验数列表
   
     操作集:
     	def setConf(conf: SparkConf): Unit
     	功能: 设置压缩属性@compressionCodec 块大小@blockSize 可用数量@checksumEnabled
     	这三个值分别是
     		 blockSize = conf.get(config.BROADCAST_BLOCKSIZE).toInt * 1024
     		 checksumEnabled = conf.get(config.BROADCAST_CHECKSUM)
     		 compressionCodec= 
     		 	if(conf.get(config.BROADCAST_COMPRESS)) CompressionCodec.createCodec(conf) else None
     	setConf(SparkEnv.get.conf)
     	功能: 类中自动加载SparkEnv中的配置
   		
        protected def getValue() 
        功能: 获取广播变量的值(临界资源,需要同步访问)
        分为两种情况:
        	1. 当前执行器命中: (_value.get!=null) return _value.get
        	2. 当前执行器没有命中: 
        		+ 使用#method @readBroadcastBlock() 从其他位置读取块信息newlyred
        		+ 重设_value=new SoftReference[T](newlyRead) --> 方便其他执行器调用
        		return newlyred
        
        def calcChecksum(block: ByteBuffer): Int
     	功能: 对NIO的字节缓冲区计算器校验码
     	计算方法:
     		使用Adler-32对数据流进行编码(比CRC-32编码快速),作为拓展内容拓展
     	分为两种情况:
     		1. 字节缓冲@ByteBuffer 不能够一个字节数组
     			使用Adler-32编码,对缓冲区中剩余的字节进行编码(大小要小于字节缓冲数组的大小)
     		2. 字节缓冲@ByteBuffer 能够一个字节数组
     			使用Adler-32编码,对缓冲区中下一个缓冲字节数组进行Alert-32编码
     	最终返回编码获得的值
     	
     	def writeBlocks(value: T): Int
     	功能: 写块信息(将对象写出多个块,并将这些块放入内存管理器@BlockManager中接受调度)
     	输入参数: value 需要写的对象
     	返回: 这个共享变量被分成的块数量
     	操作逻辑:
     		1. 引入存储等级#class @StorageLevel
     		2. 获取块管理器@blockManager (从SparkEnv中获取),将广播变量写到块管理器中,其中存储等级
     			@StorageLevel=MEMORY_AND_DISK
     			(存储广播变量到驱动器,以便于任务运行于驱动器,不需要创建广播变量值的副本)
     		3. 如果需要校验码@checksumEnabled,则计算校验码@checksums(缓冲)
     		4. 使用@blockifyObject()将对象切分成多个块(Array[ByteBuffer]),对每个块的内容进行zip压缩,参照
     		#class @IndexedSeqOptimized #method @zipWithIndex 对块对象组@blocks中的每一个元素#type 
     		@ByteBuffer进行:
     			1. 计算该块@block(ByteBuffer)的校验码并存入到对应的位置checksum[i]
     			2. 使用#class @BlockId计算块编号@pieceId
     			3. 计算当前块对应的块字节数组@ChunkedByteBuffer 为bytes
     			4. 将计算的bytes内容写入到块管理器中@blockManager,存储等级为MEMORY_AND_DISK_SER
     				存储失败则抛出异常@SparkException
     	
     	def readBlocks(): Array[BlockData]
     	功能: 从驱动器/其他执行器中获取流式块信息 #type Array[BlockData]
     	注意: 所有数据块(chunk),注意这些块存储在@BlockManager中,且汇报给驱动器,所以其他执行器也可以从这里拉			取数据块.
     	操作逻辑:
     		0. 新建一个数据块数组Array[BlockData] 维度为numBlocks @blocks 用于放置读取数据块信息
     		1. 首先需要将块编号0-->numblocks 使用#class @Random #method @shuffle随机打乱顺序,详情参照
     		@shuffle,形成一个新的块编号序列(pids)
     		2. 对这个序列中的每一个元素pid进行如下计算
     			1. 获取广播变量块编号@pieceId=BroadcastBlockId(id, "piece" + pid)
     			对于这里块管理器是否能在本地节点获取编号为pieceId的数据块(使用@getLocalBytes)分成如下
     			两种情况:
     				1. 能够获取到
     					+ 首先获取数据时,读取pieceId时操作是互斥的,所以@getLocalBytes内部即加上了锁
     					+ 读取到的数据卡写入到对应的数据块数组对应位置@blocks[pieceId]=block
     					+ 释放块管理器的读取锁@releaseBlockManagerLock(pieceId) 
     				2. 获取不到	
     					+ 从远端获取块信息@getRemoteBytes(驱动器/其他执行器) 相同参照#class
                        @BlockManager #type Option[ChunkedByteBuffer]
     						- 获取成功
     							获取块字节缓冲@ChunkedByteBuffer的第一个元素,并对其计算校验码
     							@calcChecksum如果当前块编号@pid与之校验码相同,则对其进行写入操作.
     						- 获取失败
     							抛出异常@SparkException
     	
     	protected def doUnpersist(blocking: Boolean): Unit
     	功能: 解除持久化操作 (仅在执行器上)
     	输入参数: blocking 是否阻塞进行
     		@TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
     	
     	protected def doDestroy(blocking: Boolean): Unit
     	功能: 解除持久化的状态(驱动器+执行器)
     		@TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
     		
     	def writeObject(out: ObjectOutputStream): Unit
     	功能: JVM在序列化对象时使用
     	操作条件: 广播变量可以使用 @assertValid()
     		使用底层输出流写出对象,出现IO异常则抛出
     		
     	def readBroadcastBlock(): T 
		功能: 读取广播变量数据块
		1. 获取互斥资源块编号@broadcastId
			由于只对@blockId做了锁操作,所以无论何时使用@broadcastCache,只需要获取@blockId即可
			获取广播变量缓存:
				val broadcastCache = SparkEnv.get.broadcastManager.cachedValues
		2. 获取广播变量缓存中指定广播变量编号@broadcastId 对应的广播变量
			Option(broadcastCache.get(broadcastId)).map(_.asInstanceOf[T])
		3. 获取块管理器,使用块管理器对指定内容进行读取,这里根据是否能在本地执行器获取数据块分为两种情况:
			1. 本地执行器获取到数据块
				读取本地执行器@broadcastId #type @BlockResult中迭代器中对应的数据域值
					blockResult.data.hasNext==true
					x= blockResult.data.next().asInstanceOf[T]
				释放当前@broadcastId 对应的块管理器锁#method @releaseBlockManagerLock
				将@blockId,x键值对写入广播变量缓存器@broadcastCache中.
				返回这个x即可	
			2. 本地执行器没有对应数据块
				1. 通过#method @readBlocks()将数据从驱动器/其他执行器读取到本地执行器中
				2. 通过#method @unBlockifyObject() 将数据块还原成对象@T 
				3. 将数据以MEMORY_AND_DISK的存储级别存储到块管理器中,存储失败抛出异常@SparkException
				4. 在广播变量缓存中写入@broadcastId和obj键值对
		4. 全部工作完成后
			块数据@BlockData需要调用@dispose()取处理数据块
		
		def releaseBlockManagerLock(blockId: BlockId): Unit
		功能: 释放块管理器的锁
			运行任务的时候,注册已经分配好的块锁,根据任务的完成去释放锁.因此一旦任务不允许应当立即释放锁.
		向任务上下文管理器中添加释放锁的监听事件:
			taskContext.addTaskCompletionListener[Unit](_ => blockManager.releaseLock(blockId))
}
```
```scala
private object TorrentBroadcast{
	关系: father->Logging
    属性:
    	#name @torrentBroadcastLock #type KeyLock[BroadcastBlockId] 流式广播变量锁
    操作集:
    def blockifyObject[T: ClassTag](obj: T,blockSize: Int,serializer: Serializer,
      compressionCodec: Option[CompressionCodec]):  Array[ByteBuffer]
	功能: 切分指定对象@obj
		1. 获取定长块状字节数组: @ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)=cbbo
		2. 获取压缩后的输出流@out
			out=compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
		3. 获取序列化实例
			@ser=serializer.newInstance()
		4. 获取序列化输出流
			@serout=ser.serializeStream(out)
		5. 使用压缩且序列化的流@serOut写出对象@obj
	返回 块状数组的数组对象(Array[ByteBuffer]) cbbos.toChunkedByteBuffer.getChunks()
	
	def unBlockifyObject[T: ClassTag](blocks: Array[InputStream],serializer: Serializer,
      	compressionCodec: Option[CompressionCodec]): T 
	功能: 对象还原
	操作条件: 给定输入流列表@blocks非空 [scala中不使用assert关键字进行条件过滤,使用#class @predef 
    #method @require]
    操作步骤:
    	1. 形成输入流列表@blocks的序列化输入流@SequenceInputStream结果几位is
        2. 对is进行压缩,获得输入流@in
        3. 获取序列化实例 @ser
        4. 获取反序列化器@serIn
        5. 使用反序列化器读入数据到obj中
        返回 obj
       
   	def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit
   	功能: 解除持久化
   	输入参数: 
   		id	广播变量唯一标识符
   		removeFromDriver 驱动器移除标志 为true则会从驱动器上移除
   		blocking 是否阻塞执行
}
```



---

#### TorrentBroadcastFactory

```markdown
介绍:
	这是对Broadcast的一种实现,使用类似比特流的协议,去将广播变量数据做分布式传输给执行器.实现细节参照
	#class #TorrentBroadcast
```

```scala
private[spark] class TorrentBroadcastFactory{
	关系:
		father -> BroadcastFactory
		
	操作集:
		def initialize(isDriver: Boolean, conf: SparkConf,securityMgr: SecurityManager): Unit
		功能: 初始化 --> 空实现
		
		override def stop(): Unit
		功能: 停止 --> 空实现
		
		def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long): Broadcast[T]
		功能: 创建流式共享变量@TorrentBroadcast
		
		def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit
		功能: 移除指定id的流式共享变量的持久化状态
		   输入参数: 
		   		removeFromDriver 是否从驱动器中移除状态
		   		blocking 是否阻塞选项	
            TorrentBroadcast.unpersist(id, removeFromDriver, blocking)
}
```

---

#### scala基础拓展

1. 抽象类
2. 继承与混合
3. 特征
4. 注解@volatile @transient
5. scala变量的命名方式
6. 权限修饰符
7. 模式匹配
8. java synchronizedMap(线程安全的map)
9. 科普: 比特流
10.  Adler-32编码 (java.util.zip.Adler32)
11.  class 与 object

​    