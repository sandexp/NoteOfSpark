### **spark-network**

---

1. [spark.network.netty](# spark.network.netty)
2.  [BlockDataManager.scala](# BlockDataManager)
3.  [BlockTransferService.scala](# BlockTransferService)

---

#### spark.network.netty

1.  [NettyBlockRpcServer.scala](# NettyBlockRpcServer)

2.  [NettyBlockTransferService.scala](# NettyBlockTransferService)

3.  [TransportConf.scala](# SparkTransportConf)

   ---

   #### NettyBlockRpcServer

   ```markdown
   介绍:
   	通过简单的注册一个大块(chunk)来发起打开数据块@blocks的服务请求。处理打开和上传任意块管理器
   	@BlockManager所管理的块。
   	打开的块使用"一对一"的策略注册。意味着每个运输层的块(chunk)对等于一个spark-level 的shuffle块。
   ```

   ```scala
   class NettyBlockRpcServer(appId: String,serializer: Serializer,blockManager: BlockDataManager){
   	关系: father --> RpcHandler
   		sibling --> Logging
   	构造器属性:
   		appId	app编号
   		serializer	序列化器
   		blockManager	块数据管理器
   	属性:
   	#name @streamManager #type @OneForOneStreamManager	流管理器(一对一策略管理器)
   	操作集:
   	def getStreamManager(): StreamManager = streamManager
   	功能: 获取流管理器@streamManager
   	
   	def deserializeMetadata[T](metadata: Array[Byte]): (StorageLevel, ClassTag[T])
   	功能: 反序列化元数据信息
   	val = serializer.newInstance().deserialize(ByteBuffer.wrap(metadata))
         	.asInstanceOf[(StorageLevel, ClassTag[T])]
   	
   	def receiveStream(client: TransportClient,messageHeader: ByteBuffer,
         	responseContext: RpcResponseCallback): StreamCallbackWithID
   	功能: 获取接受流
   	输入参数:
   		client 	传输客户端
   		messageHeader	信息头
   		responseContext 相应上下文
   	1. 根据信息头获取信息(使用块转化信息类@BlockTransferMessage去@Decoder解码)
   	val message
	=BlockTransferMessage.Decoder.fromByteBuffer(messageHeader).asInstanceOf[UploadBlockStream]
   	2. 反序列化元数据(得到一个存储级别/类@ClassTag键值对)
		val (level, classTag) = deserializeMetadata(message.metadata)
   		并获得块id信息
		val blockId = BlockId(message.blockId)
   	3. 使用块管理器@blockManager 将数据流式地传入到放置到数据块中#method @putBlockDataAsStream
	
   	def receive(client: TransportClient,rpcMessage: ByteBuffer,
         	responseContext: RpcResponseCallback): Unit 
   	功能: 接受客户端发送的信息
   	输入参数: 
		client	传输客户端
   		rpcMessage	rpc信息(@ByteBuffer)
   		responseContext	RPC响应
   	1. 获取rpc传输过来地信息,并对其进行解码
   	val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
   		@BlockTransferMessage
   	2. 基于以及通过RPC收到地信息@message,分为如下几类
   	+ 多个块文件(列表)@OpenBlocks
   		1. 获取块总数量 @blocksNum = openBlocks.blockIds.length
   		2. 对于编号0-blocksum之间的块:
   			+ 获取当前块编号i下对应的块编号
   				val blockId = BlockId.apply(openBlocks.blockIds(i))
   			+ 使用块管理器获取本地块数据
   				blocks[i]=blockManager.getLocalBlockData(blockId)
   		3. 通过流管理器获取流编号
   		val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
             		client.getChannel)			
   		4. RPC响应成功@responseContext(服务器反序列化成功)的处理措施
   			responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)
   			#method @onSuccess(ByteBuffer bf) 成功响应之后,bf可以循环利用,但是它的内容就无效了.
   		如果响应成功之后,你还要使用bf的内容,请拷贝一份
   			#class @StreamHandle 流处理器: 通过"open blocks"创建指定数量的块,用于使用流式阅读
   	+ shuffle块列表@FetchShuffleBlocks
   		 + 对于shuffle块@FetchShuffleBlocks的每个个map(这里获取的是mapId，即mapIds[i]).使用		  		   zipWithIndex转化为kv形式.(mapId,index).这是需要考虑到数据块是否支持批量读取
   		 @batchFetchEnabled
   		1. 不支持批量读取
   		先获取mapId对应index再获取reduceId(int[][])
   			fetchShuffleBlocks.reduceIds(index) => reduceId
   		对这个reduceId进行处理，获取本地数据块
   			blockManager.getLocalBlockData(ShuffleBlockId(fetchShuffleBlocks.shuffleId, 
   				mapId, reduceId))
   		2. 支持批量读取
           	+ 获取index对应的reducedIds(int [])
           	当这个长度不为2时，不能形成一个区间，抛出异常@IllegalStateException
           	+ 否则返回读取返回数据的数据块
           	  Array(blockManager.getLocalBlockData(ShuffleBlockBatchId(
                   fetchShuffleBlocks.shuffleId, mapId, startAndEndId(0), startAndEndId(1))))
            + 获取块数量(需要根据是否支持批量读取来定)
            	val = fetchShuffleBlocks.batchFetchEnabled? fetchShuffleBlocks.mapIds.length:
            		fetchShuffleBlocks.reduceIds.map(_.length).sum
            + 获取流Id@streamId
            	val= streamManager.registerStream(appId, blocks.iterator.asJava,client.getChannel)
            + 处理相应成功时的动作
            	responseContext.onSuccess(new StreamHandle(streamId, numBlockIds).toByteBuffer)
   	+ 数据块上传列表@UploadBlock
   		1. 获取反序列化键值对(存储等级,类标签)
   		val (level, classTag) = deserializeMetadata(uploadBlock.metadata)
   		2. 将上传数据块中的数据作为字节缓冲@ByteBuffer形式包装起来,形成缓冲数据:
   		val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
   		3. 获取块编号
   		val blockId = BlockId(uploadBlock.blockId)
   		4. 块管理器将数据存入
   		存入时需要4个必要属性: 块编号,数据,存储等级,类标签即上述所得,具体参照@BlockManager
   		blockManager.putBlockData(blockId, data, level, classTag)
   }
   ```
   
   ####  NettyBlockTransferService
   
   ```markdown
   介绍: 这个类在一段时间内，使用netty去获取数据块
   ```
   
   ```scala
   private[spark] class NettyBlockTransferService(conf: SparkConf,securityManager: SecurityManager,bindAddress: String,override val hostName: String,_port: Int,numCores: Int,driverEndPointRef: RpcEndpointRef = null){
   	关系: father --> BlockTransferService
       构造器参数:
       	conf	应用程序配置集
       	securityManager	安全管理器
       	bindAddress		绑定地址
       	hostName		主机名称
       	_port			端口号
       	numCores		核心数量
       	driverEndPointRef	RPC参照结束点
       属性:
       	#name @serializer=new JavaSerializer(conf) #type @JavaSerializer 序列化器
       	#name @authEnabled=securityManager.isAuthenticationEnabled() #type @Boolean
       		是否允许授权
       	#name @transportConf=SparkTransportConf.fromSparkConf(conf, "shuffle", numCores)
       		传输配置
       	#name @transportContext #type @TransportContext 	传输上下文
       	#name @server #type @TransportServer	传输服务器
       	#name @clientFactory #type @TransportClientFactory	客户端工厂
       	#name @appId #type @String appID
      	
      	操作集:
      		def init(blockDataManager: BlockDataManager): Unit 
      		功能: 初始化
      		1. 初始化rpc处理器@rpcHandler
      		val= NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
      		2. 初始化服务端启动器@serverBootstrap #type @Option[TransportServerBootstrap]
      		val=None
      		3. 初始化客户端启动器@clientBootstrap #type @Option[TransportClientBootstrap]
      		val=None
      		4. 权限足够，则分配客户端和服务端
      		    if (authEnabled) {
         		serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
         		clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, 					securityManager))
       		}
      		5. 初始化传输上下文@transportContext
      		val= new TransportContext(transportConf, rpcHandler)
      		6. 初始化客户端工厂@clientFactory
      		val= transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
      		7. 初始化服务端
      		server = createServer(serverBootstrap.toList)
      		8. 初始化appId
      		appId = conf.getAppId
      		
      		def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer
      		功能: 创建服务器
   			创建并绑定传输服务器，可能需要尝试多个端口
   		1. 使用传输上下文创建服务器，并获取启动信息
   			@def startService(port: Int): (TransportServer, Int)
   				val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
   			返回值: (server, server.getPort)
   		2. 开启端口上的服务
   			#class #Utils 
   			#method @startServiceOnPort(_port, startService, conf, getClass.getName)._1
      		
      		def port: Int = server.getPort
      		功能: 获取服务端端口号
      		
      		def close(): Unit 
      		功能: 关闭服务器@sever,关闭@clientFactory客户端工厂，关闭传输上下文@transportContext
      		
           def uploadBlock(hostname: String,port: Int,execId: String,blockId: BlockId,
         		blockData: ManagedBuffer,level: StorageLevel,classTag: ClassTag[_]): Future[Unit]
   		功能: 上传数据块
   		输入参数:
   			hostname	主机名
   			port		端口号
   			execId		执行Id
   			blockId		块编号
   			blockData	块数据
   			level		存储等级
   			classTag	类标签
   		1. 根据主机名称以及端口号创建一个客户端@client
   		val =clientFactory.createClient(hostname, port)
   		2. 获取元数据信息
   			存储等级和类标签被@JavaSerializer序列化称为了字节，任何的东西都可以使用二进制协议加密
   		3. 确定是否需要流式读取
   		val asStream = blockData.size() > conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)
            4. 获取回调信号
            val callback = new RpcResponseCallback()
            在回调信号中需要设置处理成功的措施[对获取数据进行处理]
            def onSuccess(response: ByteBuffer): Unit
            	result.success((): Unit)
            处理失败状态[对异常/错误进行处理]
            def onFailure(e: Throwable): Unit
            	result.failure(e)
   		5. 处理读取数据
   		+ 采用流式读取@asStram
   			1. 获取流处理器@streamHeader
   			val= UploadBlockStream(blockId.name, metadata).toByteBuffer
   			2. 流式的将数据有客户端发送到服务端
   			client.uploadStream(new NioManagedBuffer(streamHeader), blockData, callback)
   		+ 非流式读取
   			1. 将NIO缓冲转化为数据,以便去序列化
   			val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())
   			2. 从客户端将数据发送到远端
   			client.sendRpc(new UploadBlock(appId, execId, blockId.name, metadata, 							array).toByteBuffer,callback)
   		
   		def fetchBlocks(host: String,port: Int,execId: String,blockIds: Array[String],
   listener: BlockFetchingListener,tempFileManager: DownloadFileManager): Unit
   		功能: 获取数据块(服务端动作)
   		输入参数:
   			host 	主机名
   			port	端口号
   			execId	执行Id
   			blockIds	块id数组
   			listener	块获取监听器@BlockFetchingListener
   			tempFileManager	临时文件管理器(下载文件管理器)
   		操作逻辑:
   		1. 获取一个块获取器@blockFetchStarter,但是这个获取器是可以重试的@RetryingBlockFetcher，其中内部包含一个块获取器@BlockFetchStarter，具体设计参考#class @RetryingBlockFetcher
   			+ 获取客户端
   			val client = clientFactory.createClient(host, port)
   			+ 由客户端对接起一个一对一块获取处理器@OneForOneBlockFetcher
   			OneForOneBlockFetcher(client, appId, execId, blockIds, listener,
                 	transportConf, tempFileManager).start()
   		2. 设定最大获取次数
   		val maxRetries = transportConf.maxIORetries()
   		3. 尝试获取块数据内容
           	+ 尝试次数大于0
           	new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds,
               	listener).start()
   			+ 尝试次数不大于0 (为了避免bug,全部按照为0处理)
   			blockFetchStarter.createAndStart(blockIds, listener)
       
   		def shuffleMetrics(): MetricSet 
   		功能: shuffle度量器
   		操作条件: 服务器存在且客户端存在
   		require(server != null && clientFactory != null, "NettyBlockTransferServer is not initialized")	
   		操作逻辑: 新建一个度量器集合实例@MetricSet
   			度量器中需要设置客户端和服务端信息
   		        allMetrics.putAll(clientFactory.getAllMetrics.getMetrics)
           		allMetrics.putAll(server.getAllMetrics.getMetrics)
   }
   ```
   
   #### SparkTransportConf
   
   ```markdown
   介绍:
   	这里提供了实用的转化方式，将Spark JVM(比如说: 执行器，驱动器，脱机shuffle服务)中的#class @SparkConf 转化为#class @TransportConf。且需要携带环境信息，比如说JVM分配的核心数量。
   ```
   
   ```scala
   操作集:
   	def fromSparkConf(_conf: SparkConf,module: String,numUsableCores: Int = 0,role: Option[String] = None): TransportConf
   	功能: 使用SparkConf创建@TransportConf
   	输入参数: 
   		_conf 应用程序配置集
   		module	模块名称
   		numUsableCores	可用核心数量(可以限制客户端以及服务端核心上限使用量)
   		role	身份(可以是驱动器，执行器，工作者@worker，调度者@master)。默认为none
   	操作步骤:
   	1. 克隆一份配置集@conf=_conf.clone
   	2. 获取线程数量@numThreads= NettyUtils.defaultNumThreads(numUsableCores)
   		指定默认线程配置，根据JVM分配的核心数，而非所有机器上的核心。
   	3. 根据获得的参数获取配置信息
   	new TransportConf(module, new ConfigProvider {
         override def get(name: String): String = conf.get(name)
         override def get(name: String, defaultValue: String): String = conf.get(name, defaultValue)
         override def getAll(): java.lang.Iterable[java.util.Map.Entry[String, String]] = 		{
           conf.getAll.toMap.asJava.entrySet()
         }
       })
   ```
---

#### BlockDataManager

```scala
private[spark] trait BlockDataManager{
	操作集:
	def getHostLocalShuffleData(blockId: BlockId, dirs: Array[String]): ManagedBuffer
	功能: 这个接口主要是获取主机本地(host-local)的shuffle数据块。如果块找不到或者是读取不成功(比如权限问题)，会抛出异常。
	
	def getLocalBlockData(blockId: BlockId): ManagedBuffer
	功能: 这个接口获取本地块数据,如果块找不到或是读取不成功则会抛出异常
    
    def putBlockData(blockId: BlockId,data: ManagedBuffer,level: StorageLevel,classTag: ClassTag[_]): Boolean
	功能: 使用给定的存储等级@level，本地存放数据块。
		如果: 块存储成功返回true，存储失败或块已经存在则返回false
	
    def putBlockDataAsStream(blockId: BlockId,level: StorageLevel,classTag: ClassTag[_]): StreamCallbackWithID
	功能: 存储将被作为流接受的指定数据块。
		调用此方法的时候，数据块本身时不可以被获得的。它会被传递给返回的回调流@StreamCallbackWithID
		
	def releaseLock(blockId: BlockId, taskContext: Option[TaskContext]): Unit
	功能: 释放锁(释放@putBlockData(),@getLocalBlockData()中所获取的锁)
}
```

#### BlockTransferService

```scala
private[spark] abstract class BlockTransferService{
	关系: father --> BlockStoreClient
		sibling --> Logging
	介绍: 块转换服务@BlockTransferService 是用于获取在一段时间内获取一组数据块@blocks。每个块转换服务
	@BlockTransferService 内部包含客户端和服务端。
	操作集:
	def init(blockDataManager: BlockDataManager): Unit
	功能: 通过给定一个块状数据管理器@blockDataManager 初始化转换服务。这个块状数据管理器可以对本地数据块的获	取和存储。获取块数据时在#class @BlockStoreClient 中提供获取的方法。但是需要者一步的初始化。
	
	def port: Int
	功能: 监听端口号，@init之后才可以使用
	
	def hostName: String
	功能: 监听的主机名称，@init()之后才可以使用
	
	def uploadBlock(hostname: String,port: Int,execId: String,blockId: BlockId,blockData: ManagedBuffer,level: StorageLevel,classTag: ClassTag[_]): Future[Unit]
	功能: 上传单个数据块到远程节点,@init()之后可以使用
	
	def uploadBlockSync(hostname: String,port: Int,execId: String,blockId: BlockId,blockData: ManagedBuffer,level: StorageLevel,classTag: ClassTag[_]): Unit
	功能: 同步上传
	    val future = uploadBlock(hostname, port, execId, blockId, blockData, level, classTag)
    	ThreadUtils.awaitResult(future, Duration.Inf)
	
	def fetchBlockSync(host: String,port: Int,execId: String,blockId: String,tempFileManager: DownloadFileManager): ManagedBuffer 
	功能: 同步获取数据块，返回一个缓冲结构@ManagedBuffer
	1. 使用#class @BlockStoreClient #method @fetchBlocks 获取块信息
	2. 读取成功同步读取下一个数据块
		ThreadUtils.awaitResult(result.future, Duration.Inf)
}
```
