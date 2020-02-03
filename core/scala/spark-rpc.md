

## **spark-rpc**

---

1.  [netty](# netty)
2.  [RpcAddress.scala](# RpcAddress)
3.  [RpcCallContext.scala](# RpcCallContext)
4.  [RpcEndpoint.scala](# RpcEndpoint)
5.  [RpcEndpointAddress.scala](# RpcEndpointAddress)
6.  [RpcEndpointNotFoundException.scala](# RpcEndpointNotFoundException)
7.  [RpcEndpointRef.scala](# RpcEndpointRef)
8.  [RpcEnv.scala](# RpcEnv)
9.  [RpcEnvStoppedException.scala](# RpcEnvStoppedException)
10.  [RpcTimeout.scala](# RpcTimeout)
11.   [基础拓展](# 基础拓展)

---

#### netty

1.  [Dispatcher.scala](# Dispatcher)
2.  [Inbox.scala](# Inbox)
3.  [MessageLoop.scala](# MessageLoop)
4.  [NettyRpcCallContext.scala](# NettyRpcCallContext)
5.  [NettyRpcEnv.scala](# NettyRpcEnv)
6.  [NettyStreamManager.scala](# NettyStreamManager)
7.  [Outbox.scala](# Outbox)
8.  [RpcEndpointVerifier.scala](# RpcEndpointVerifier)

---

####  Dispatcher

```markdown
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int){
	介绍: 消息分配器，用于定为RPC消息到合适的RPC端点
	关系: father --> Logging
	构造器参数:
		nettyEnv	netty环境
		numUsableCores	可使用核心数量，设置为0则会使用主机上的CPU核心
	属性:
		#name @endpoints=@ConcurrentHashMap[String, MessageLoop]	RPC端点映射表
		#name @endpointRefs=@ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]	RPC端点与端点参考映射
		#name @shutdownLatch=@CountDownLatch(1)	关闭闭锁
		#name @sharedLoop=@SharedMessageLoop(nettyEnv.conf, this, numUsableCores)	共享消息环
		#name @stopped = false @GuardedBy("this") 分发器状态位(为true表示分发器停止工作)
			一旦分发器停止工作，所有发送的消息都会反弹
    操作集:
    def getMessageLoop(name: String, endpoint: RpcEndpoint): MessageLoop
	功能: 获取消息环
	根据端点类型确定消息环的类型，如果是独立RPC端点@IsolatedRpcEndpoint，则需要返回一个专用的消息环，否则只
    需要将端点信息注册到消息环并返回默认消息环@sharedLoop
        endpoint match {
          case e: IsolatedRpcEndpoint =>
            new DedicatedMessageLoop(name, e, this)
          case _ =>
            sharedLoop.register(name, endpoint)
            sharedLoop
        }
        
    def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef
    功能: 注册RPC端点
    1. 获取RPC端点地址以及RPC端点参考信息
        val addr = RpcEndpointAddress(nettyEnv.address, name)
        val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
	2. 状态检测
	synchronized {
      if (stopped) throw IllegalStateException
      if (endpoints.putIfAbsent(name, getMessageLoop(name, endpoint)) != null) 
        throw IllegalArgumentException
    }
    3. 将RPC端点信息存入RPC端点与端点参考映射中@endpointRefs
    endpointRefs.put(endpoint, endpointRef)
    val= endpointRef
    
    def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)
    功能: 获取指定RPC端点@endpoint的RPC端点参考
    val= endpointRefs.get(endpoint)
    
    def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)
    功能: 移除指定RPC端点@endpoint 的RPC端点参考
    
    def unregisterRpcEndpoint(name: String): Unit
    功能: 解除注册名称为@name的RPC端点信息
    1. 解除端点映射@endpoints 中的记录
    val loop = endpoints.remove(name)
    2. 从消息环中解除注册@name信息
    if (loop != null) loop.unregister(na
   	注意: 这里不需要解除注册RPC端点参考映射关系@endpointRefs,因为可能有部分信息正在处理,它们可能在使用
   	@getRpcEndpointRef 获取RPC端点参考,所有@endpointRefs 的清除工作通过#class @Inbox 的@method 
   	@removeRpcEndpointRef 方法清理。
   	
   	def stop(rpcEndpointRef: RpcEndpointRef): Unit 
   	功能: 停止指定的RPC端点参考@rpcEndpointRef(解除@rpcEndpointRef 的注册信息)
   	synchronized {
      if (stopped) 
        return
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
    
    def postToAll(message: InboxMessage): Unit
    功能: 发送指定消息@message给此进程中登记的所有RPC端点@RpcEndpoint
 	这个可以用于制造与其他端点的网络链接
 	1. 获取RPC端点信息
 	val iter = endpoints.keySet().iterator()
    2. 发送消息到RPC端点中
    while (iter.hasNext) {
      val name = iter.next
        postMessage(name, message, (e) => { e match {
          case e: RpcEnvStoppedException => logDebug (s"Message $message dropped. ${e.getMessage}")
          case e: Throwable => logWarning(s"Message $message dropped. ${e.getMessage}")
        }}
      )}
      
   def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit
   功能: 发送指定消息@message给远程RPC端点
   1. 获取远程RPC端点回调
   val rpcCallContext =new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
   2. 产生RPC消息
   val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
   3. 发送RPC消息到远程RPC端点
   @postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
    
   def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit
   功能: 发送指定消息@message给本地端点
   1. 获取本地RPC回调
   val rpcCallContext =new LocalNettyRpcCallContext(message.senderAddress, p)
   2. 产生RPC消息
   val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
   3. 发送消息给本地
   postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
   
   def postOneWayMessage(message: RequestMessage): Unit 
   功能: 发送消息@message 传输是单程的，不需要回应
   postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
   
   def postMessage(endpointName: String,message: InboxMessage,
   	callbackIfStopped: (Exception) => Unit): Unit
   功能: 给指定RPC端点@endpointName 发送消息@message,并给出了异常处理函数@callbackIfStopped
   1. 获取消息环信息，并使用消息环@MessageLoop 发送消息给指定RPC端点,同时统计错误信息
       val error = synchronized {
          val loop = endpoints.get(endpointName)
          if (stopped) Some(new RpcEnvStoppedException())
          else if (loop == null) Some(new SparkException)
          else {
            loop.post(endpointName, message)
            None
          }
        }
    2. 统一处理异常
    error.foreach(callbackIfStopped)
    
    def stop(): Unit
    功能: 停止分发器
    1. 停止分发器
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    2. 停止共享消息环@sharedLoop
    var stopSharedLoop = false
    endpoints.asScala.foreach { case (name, loop) =>
      unregisterRpcEndpoint(name) // 解除所有注册的RPC端点信息
      if (!loop.isInstanceOf[SharedMessageLoop]) {
        loop.stop()
      } else {
        stopSharedLoop = true // 标记停止
      }
    }
    if (stopSharedLoop) { // 停止共享消息环
      sharedLoop.stop()
    }
    3. 关闭闭锁资源量-1
    shutdownLatch.countDown()
    
    def awaitTermination(): Unit
    功能: 等待线程终止
    shutdownLatch.await()
    
    def verify(name: String): Boolean
    功能: 确认是否存在指定的RPC端点描述@name
    val= endpoints.containsKey(name)
}
```

#### Inbox

收件箱

```markdown
private[netty] sealed trait InboxMessage 
介绍: 收件箱消息

private[netty] case class OneWayMessage(senderAddress: RpcAddress,content: Any) extends InboxMessage
介绍: 单程消息
参数:
	senderAddress	发送器RPC地址
	content		发送内容

private[netty] case class RpcMessage(senderAddress: RpcAddress,content: Any,
    context: NettyRpcCallContext)
介绍: RPC 消息
参数:
	senderAddress	发送器RPC地址
	content	发送内容
	context	回调信息

private[netty] case object OnStart extends InboxMessage
功能: 启动实例

private[netty] case object OnStop extends InboxMessage
功能: 关闭实例

private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage
功能: 远程处理连接，用于告知所有RPC端点连接远端成功

private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage
功能: 远程关闭连接，告知所有RPC端点断开与远端的连接

private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage
功能: 远端连接错误
```

```markdown
private[netty] class Inbox(val endpointName: String, val endpoint: RpcEndpoint){
	关系: father --> Logging
	构造器属性：
		endpointName	端点名称
		endpoint	RPC端点
	属性:
	#name @messages = new java.util.LinkedList[InboxMessage]() @GuardedBy("this")	收件箱消息列表
	#name @stopped = false @GuardedBy("this")	收件箱停止状态位
	#name @enableConcurrent = false @GuardedBy("this")	是否允许并发处理消息
	#name @numActiveThreads = 0 @GuardedBy("this") 收件箱中处理消息的线程数量
	初始化操作:
	inbox.synchronized {
    	messages.add(OnStart)
  	}
  	功能: 添加初始化消息
  	
  	def post(message: InboxMessage): Unit
  	功能: 发送消息@message到消息列表@messages，如果收件箱管理了，则扔掉该消息
  	inbox.synchronized {
        if (stopped) {
          @onDrop(message) // 扔掉该消息
        } else {
          messages.add(message)
          false
        } 
  	}
  	
  	def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }
  	功能: 确认消息列表是否为空
  	
  	def onDrop(message: InboxMessage): Unit
  	功能: 放弃消息时调用，用于测试
  		logWarning(s"Drop $message because endpoint $endpointName is stopped")
  	
  	def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit
  	功能: 调用动作关闭，在异常的情况下调用RPC端点的错误处理函数@onError
        try action catch {
          case NonFatal(e) =>	// RPC异常
            try endpoint.onError(e) catch {
              case NonFatal(ee) =>
                if (stopped) {
                  logDebug("Ignoring error", ee)
                } else {
                  logError("Ignoring error", ee)
                }
            }
        }
  	
	def stop(): Unit
	功能: 关闭收件箱
	inbox.synchronized {
  		// 下面的代码需要同步，主要是确保@OnStop 是最后一条消息
        if (!stopped) {
		// 注意这里需要关闭并发,当RpcEndpoint.onStop 调用时,仅仅只有这个线程在处理消息,这样
		// RpcEndpoint.onStop 能够安全的释放资源	
          enableConcurrent = false
          stopped = true
          messages.add(OnStop)
        }
  	}
  	
  	def process(dispatcher: Dispatcher): Unit 
  	功能: 处理存储起来的消息
  	1. 获取消息已经处理线程数量
        var message: InboxMessage = null
        inbox.synchronized {
          if (!enableConcurrent && numActiveThreads != 0) {
            return
          }
          message = messages.poll() // 拿去消息列表中第一个元素
          if (message != null) {
            numActiveThreads += 1 // 增加1个处理线程
          } else {
            return
          }
        }
     2. 根据消息类型,进行不同的处理
     safelyCall(endpoint) {
        message match {
        	...
        }
     + 收到RPC类型消息
	case RpcMessage(_sender, content, context) =>
		endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
        	throw new SparkException(s"Unsupported message $message from ${_sender}")})
     + 收到单程消息
     case OneWayMessage(_sender, content) =>
     	endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")})
          
     + 收到启动消息
     case OnStart =>
     	endpoint.onStart()
        if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
        	inbox.synchronized {
                if (!stopped) { enableConcurrent = true }
             }
        }
     
     + 收到停止消息
     case OnStop =>
         val activeThreads = inbox.synchronized { inbox.numActiveThreads }
         // 线程数量断言
         assert(activeThreads == 1,
         s"There should be only a single active thread but found $activeThreads threads.")
         // 移除RPC端点信息
         dispatcher.removeRpcEndpointRef(endpoint)
         // 停止RPC端点
         endpoint.onStop()
         // 断言检查收件箱是否为空
         assert(isEmpty, "OnStop should be the last message")
            
     + 收到远端进程连接
     case RemoteProcessConnected(remoteAddress) =>
     	endpoint.onConnected(remoteAddress)
     
     + 收到远端断开连接
     case RemoteProcessDisconnected(remoteAddress) =>
     	endpoint.onDisconnected(remoteAddress)
     
     + 收到远端进程连接出错信息
     case RemoteProcessConnectionError(cause, remoteAddress) =>
     	endpoint.onNetworkError(cause, remoteAddress)
}
```

#### MessageLoop

消息环

```markdown
private sealed abstract class MessageLoop(dispatcher: Dispatcher) {
	关系: father --> Logging
	介绍: 分配器@Dispatcher所使用的消息环，用于发送数据到RPC端点
	属性:
	#name @active = new LinkedBlockingQueue[Inbox]() 
		带有待定消息的收件箱列表，使用消息环处理
	#name @receiveLoopRunnable=new Runnable() {override def run(): Unit = receiveLoop() }
		接受环任务
	#name @threadpool #type @ExecutorService	线程池
	#name @stopped = false	停止标志
	操作集:
	def post(endpointName: String, message: InboxMessage): Unit
	功能: 发送消息@message到指定的RPC端点@endpointName
	
	def unregister(name: String): Unit
	功能: 解除RPC端点名称为name的注册信息
	
	def stop(): Unit
	功能: 停止消息环
	1. 停止消息环
	synchronized {
      if (!stopped) {
        setActive(MessageLoop.PoisonPill)
        threadpool.shutdown()
        stopped = true
      }
    }
    2. 等待线程执行完毕
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    
    def setActive(inbox: Inbox): Unit = active.offer(inbox)
    功能: 将指定收件箱插@inbox插入激活队列@active尾部
    
    def receiveLoop(): Unit 
    功能： 接受环操作
    对激活队列@active中的每一个元素进行处理
    while(true){
    	try {
          // 获取一个收件箱
          val inbox = active.take()
          // 检测到时阻碍收件箱则将其放回队列并停止处理
          if (inbox == MessageLoop.PoisonPill) {
            setActive(MessageLoop.PoisonPill)
            return
          }
          // 处理分发器的消息
          inbox.process(dispatcher)
        } catch {
          case NonFatal(e) => logError(e.getMessage, e)
        }
    }
}
```

```markdown
private object MessageLoop {
	属性:
	#name @PoisonPill = new Inbox(null, null) 
		阻碍收件箱，表名消息环应当终止读取消息
}
```

```markdown
private class SharedMessageLoop(conf: SparkConf,dispatcher: Dispatcher,numUsableCores: Int){
	关系: father --> MessageLoop(dispatcher)
	介绍: 服务于多个RPC端点的消息环，使用共享线程池
	构造器属性:
		conf	应用程序配置集
		dispatcher	分发器
		numUsableCores	可用核心数量
	属性:
	#name=endpoints = new ConcurrentHashMap[String, Inbox]() 端点映射表
	#name=threadpool #type @ThreadPoolExecutor	线程池执行器(用于分发消息到RPC端点)
	val= {
        // 获取线程数量
        val numThreads = getNumOfThreads(conf)
        val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
        for (i <- 0 until numThreads) {
          // 执行分配工作
          pool.execute(receiveLoopRunnable)
        }
        pool
      }
	操作集:
	def getNumOfThreads(conf: SparkConf): Int
	功能: 获取线程数量
	1. 获取可使用核心数量,如果给定核心数量合法则为用户分配的核心数量，否则系统给定核心数量
	val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
     2. 获取分配器线程数量
     val modNumThreads = conf.get(RPC_NETTY_DISPATCHER_NUM_THREADS)
      .getOrElse(math.max(2, availableCores))
     3. 确定执行ID对应的身份以及线程数量
     conf.get(EXECUTOR_ID).map { id =>
          val role = if (id == SparkContext.DRIVER_IDENTIFIER) "driver" else "executor"
          conf.getInt(s"spark.$role.rpc.netty.dispatcher.numThreads", modNumThreads)
     }.getOrElse(modNumThreads)
	
	def post(endpointName: String, message: InboxMessage): Unit
	功能: 发送消息到指定RPC端点
        val inbox = endpoints.get(endpointName)
        inbox.post(message)
        setActive(inbox)
	
	def unregister(name: String): Unit
	功能: 解除注册@name对应的RPC端点
        val inbox = endpoints.remove(name)
        if (inbox != null) {
          inbox.stop()
          //设置active用户处理结束@onStop消息
          setActive(inbox)
        }
    
    def register(name: String, endpoint: RpcEndpoint): Unit
    功能: 注册名称为@name RPC端点为@endpoint的收件箱
    val inbox = new Inbox(name, endpoint)
    endpoints.put(name, inbox)
    // Mark active to handle the OnStart message.
    setActive(inbox)
}
```

```markdown
private class DedicatedMessageLoop(name: String,endpoint: IsolatedRpcEndpoint,dispatcher: Dispatcher){
	介绍: 专用消息环，用于处理单个RPC端点问题
	关系: father --> MessageLoop(dispatcher)
	构造器属性:
		name	RPC端点名称
		endpoint	RPC端点
		dispatch	分发器
	属性:
		#name @inbox = new Inbox(name, endpoint)	收件箱
		#name @threadpool 线程池
			val= if (endpoint.threadCount() > 1)
			ThreadUtils.newDaemonCachedThreadPool(s"dispatcher-$name", endpoint.threadCount())
			else
			ThreadUtils.newDaemonSingleThreadExecutor(s"dispatcher-$name")
	初始化操作:
	(1 to endpoint.threadCount()).foreach { _ =>
    	threadpool.submit(receiveLoopRunnable)
  	}
  	功能: 线程池提交接受环任务
  	
  	setActive(inbox)
  	功能: 注册收件箱，并用其处理onStart消息
    
	操作集:
	def post(endpointName: String, message: InboxMessage): Unit 
	功能: 发送消息@message 给指定端点@endpointName
	    require(endpointName == name)
        inbox.post(message)
        setActive(inbox)
	
	def unregister(endpointName: String): Unit
	功能: 解除名称为@endpointName的端点注册
     synchronized {
        require(endpointName == name)
        inbox.stop()
        // Mark active to handle the OnStop message.
        setActive(inbox)
        setActive(MessageLoop.PoisonPill)
        threadpool.shutdown()
      }
}
```



#### NettyRpcCallContext

```markdown
private[netty] abstract class NettyRpcCallContext(override val senderAddress: RpcAddress){
	关系: father --> RpcCallContext
		sibling --> Logging
	操作集:
	def send(message: Any): Unit
	功能: 发送消息@message
	
	def reply(response: Any): Unit
	功能: 回应消息@response
	
	def sendFailure(e: Throwable): Unit
	功能: 发送错误信息
}
```

```markdown
private[netty] class LocalNettyRpcCallContext(senderAddress: RpcAddress,p: Promise[Any]){
	关系: father --> NettyRpcCallContext(senderAddress)
	构造器参数:
	senderAddress	发送器RPC地址
	p		许诺值(失败抛出异常，成功返回值)
	
	操作集:
	def send(message: Any): Unit 
	功能: 发送信息@message
}
```

```markdown
private[netty] class RemoteNettyRpcCallContext(nettyEnv: NettyRpcEnv,callback: RpcResponseCallback,
    senderAddress: RpcAddress){
	关系: father --> NettyRpcCallContext(senderAddress)
    构造器参数:
        nettyEnv	netty RPC环境
        callback	RPC回调
        senderAddress	发送器RPC地址
    操作集:
    def send(message: Any): Unit
    功能: 发送消息@message
    1. netty Env段接受消息，并做出回应
    val reply = nettyEnv.serialize(message)
    2. 接受回调
    callback.onSuccess(reply)
}
```

#### NettyRpcEnv

```markdown
private[netty] class NettyRpcEnv(val conf: SparkConf,javaSerializerInstance: JavaSerializerInstance,
    host: String,securityManager: SecurityManager,numUsableCores: Int){
	关系: father --> RpcEnv(conf)
    	sibling --> Logging
    构造器属性:
    	conf	spark属性配置集
    	javaSerializerInstance	java序列化实例
    	host	主机名称
    	securityManager	安全管理器
    	numUsableCores	可使用的核心数量
    属性:
    #name @role #type @String 当前任务@EXECUTOR_ID 身份
    val= conf.get(EXECUTOR_ID).map { id =>
    	if (id == SparkContext.DRIVER_IDENTIFIER) "driver" else "executor" }
    
    #name @transportConf #type @TransportConf 传输配置信息
    val= SparkTransportConf.fromSparkConf(
   	 	conf.clone.set(RPC_IO_NUM_CONNECTIONS_PER_PEER, 1),"rpc",
    	conf.get(RPC_IO_THREADS).getOrElse(numUsableCores),role)
	
	#name @dispatcher #type @Dispatcher	RPC分发器
	val= new Dispatcher(this, numUsableCores)
	
	#name @streamManager #type @NettyStreamManager 	流管理器
	val= new NettyStreamManager(this)
	
	#name @transportContext #type @TransportContext	传输上下文
	val= new TransportContext(transportConf,new NettyRpcHandler(dispatcher, this, streamManager))
	
	#name @clientFactory=transportContext.createClientFactory(createClientBootstraps())	客户端工厂
	
	#name @fileDownloadFactory #type @TransportClientFactory  volatile	文件下载工厂(传输客户端工厂)
		这是一个单独用于文件下载的客户端工厂,为了避免使用主RPC上下文中相同的RPC处理器.所以这些客户端引起的事		件保存在主RPC网之外的独立空间中.同样,它允许特定属性的不同配置,比如单点链接数量等等.
	 	
	#name @timeoutScheduler=
		ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")
		超时调度器
	
	#name clientConnectionExecutor
    val = ThreadUtils.newDaemonCachedThreadPool("netty-rpc-connection",conf.get(RPC_CONNECT_THREADS))
	客户端连接执行器
		由于TransportClientFactory.createClient 是阻塞的,需要将其运行在一个线程池中,去实现非阻塞式消息的发		送与请求
	
	#name @server #type @TransportServer 	传输服务器
	
	#name @stopped=new AtomicBoolean(false)	停止标记位(原子性)
	
	#name @outboxes=new ConcurrentHashMap[RpcAddress, Outbox]()
	发件箱映射表
	-> 维护RPC地址@RpcAddress与收件箱@Outbox的映射关系,当连接上远端RPC时,只需要将消息置入发件箱,去实现一个非		阻塞式的@send 方法.
	
	#name @address #type @RpcAddress lazy Nullable
	RPC地址信息
	val=if (server != null) RpcAddress(host, server.getPort()) else null
	
	操作集:
	def removeOutbox(address: RpcAddress): Unit
	功能: 移除指定RPC地址@address的发件箱
    val outbox = outboxes.remove(address) // 移除记录
    if (outbox != null) { // 停止发件箱
      outbox.stop()}
	
	def createClientBootstraps(): java.util.List[TransportClientBootstrap]
	功能: 创建客户端启动器
	val= securityManager.isAuthenticationEnabled() ?
		java.util.Arrays.asList(new AuthClientBootstrap(transportConf,
        	securityManager.getSaslUser(), securityManager)) :
         java.util.Collections.emptyList[TransportClientBootstrap] // 权限不足,返回空表
	
	def startServer(bindAddress: String, port: Int): Unit
	功能: 开启服务器
	1. 获取启动的客户端列表
	val bootstraps: java.util.List[TransportServerBootstrap] =
      if (securityManager.isAuthenticationEnabled()) {
        java.util.Arrays.asList(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        java.util.Collections.emptyList()
      }
    2. 创建服务器
    server = transportContext.createServer(bindAddress, port, bootstraps)
    3. 注册RPC端点信息
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
      
    def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef
    功能: 创建RPC端点
    val= dispatcher.registerRpcEndpoint(name, endpoint)
    
    def stop(endpointRef: RpcEndpointRef): Unit
    功能: 停止指定的RPC端点@endpointRef
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
    
    def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]
    功能: 通过URI异步创建RPC端点,获取的是一个任务
    1. 获取RPC端点参考信息@RpcEndpointRef
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    2. 获取RPC端点验证器
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    3. 检查端点@endpointRef,是否存在,存在则进行成功处理任务,否则处理失败场景
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
    
    def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit
    功能: 发送消息@message到发件箱
    1.如果接收端存在,则直接将消息发送到接收端
     if (receiver.client != null) {
      message.sendWith(receiver.client)}
    2. 否则需要先创建目标发件箱(RPC地址月给定@receive一致)
    if(...){
    	...
    }else{
    	// 存在性断言
    	require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
    	val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
    }
 	3. 考虑到发送消息时,可能RPC环境已经停止了@stop.value=true,所以需要清理发件箱
 	if(){
 		...
 	}else{
 		...
 		if (stopped.get) {
            // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
            outboxes.remove(receiver.address)
            targetOutbox.stop()
          } else {
            targetOutbox.send(message)
          }
 	}
    
    def send(message: RequestMessage): Unit
    功能: 发送请求消息
    分为两种情况,一种时发送到本地,另一种是发送到远程
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      // 发送到本地RPC端点
      try {
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: RpcEnvStoppedException => logDebug(e.getMessage)
      }
    }else{
    	// 发送到远程RPC端点
    	postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
    }
    
    def createClient(address: RpcAddress): TransportClient
    功能: 创建客户端
    
    def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T]
    功能: 在规定时间内@timeout 获取@message的响应
    val= askAbortable(message, timeout).toFuture
    
    def askAbortable[T: ClassTag](
      message: RequestMessage, timeout: RpcTimeout): AbortableRpcFuture[T]
    功能: 可放弃读取式请求,时间限制为@timeout,发出消息为@message,获取一个可以RPC任务@AbortableRpcFuture
    需要对这个RPC任务设置:
    1. 失败处理方案
    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        e match {
          case e : RpcEnvStoppedException => logDebug (s"Ignored failure: $e")
          case _ => logWarning(s"Ignored failure: $e")
        }
      }
    }
    2. 处理成功处理方案
    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(s"Ignored message: $reply")
        }
    }
    3. 放弃请求处理方案
    def onAbort(reason: String): Unit = {
      onFailure(new RpcAbortException(reason))
    }
    4. 初始设置(线程进去就会执行的内容)
    + 发送信息给目的RPC地址(分为本地和远程地址)
        if (remoteAddr == address) {
            val p = Promise[Any]()
            p.future.onComplete {
              case Success(response) => onSuccess(response)
              case Failure(e) => onFailure(e)
            }(ThreadUtils.sameThread)
            dispatcher.postLocalMessage(message, p)
          } else {
            val rpcMessage = RpcOutboxMessage(message.serialize(this),
              onFailure,
              (client, response) => onSuccess(deserialize[Any](client, response)))
            postToOutbox(message.receiver, rpcMessage)
            promise.future.failed.foreach {
              case _: TimeoutException => rpcMessage.onTimeout()
              case _: RpcAbortException => rpcMessage.onAbort()
              case _ =>
            }(ThreadUtils.sameThread)
          }
     + 使用超时调度器@timeoutScheduler 调度连接任务
         val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
            override def run(): Unit = {
              onFailure(new TimeoutException(s"Cannot receive any reply from ${remoteAddr} " +
                s"in ${timeout.duration}"))
            }
          }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
    + 任务完成处理(取消超时)
    promise.future.onComplete { v =>
        timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    + 返回经过上述设置的任务信息
    val= new AbortableRpcFuture[T](
      promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread),
      onAbort)
    
    def serialize(content: Any): ByteBuffer 
    功能: 序列化消息@content,使用java序列化
    val= javaSerializerInstance.serialize(content)
    
    def serializeStream(out: OutputStream): SerializationStream
    功能: 获取指定输出流@out的序列化流@SerializationStream
    val= javaSerializerInstance.serializeStream(out)
    
    def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T
    功能: 客户端反序列化@bytes
    val= NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () => avaSerializerInstance.deserialize[T](bytes)}
    }
    
    def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef
    功能: 获取指定RPC端点的参考信息
    val= dispatcher.getRpcEndpointRef(endpoint)
    
    def shutdown(): Unit
    功能: 关闭当前RPC环境
    	cleanup()
    
    def awaitTermination(): Unit
    功能: 等待分发器终止
    	dispatcher.awaitTermination()
    
     def cleanup(): Unit
     功能: 清除全部(超时调度器,分发器,服务器,客户端工厂,客户端连接执行器,文件下载工厂,传输上下文关闭)
     	if (!stopped.compareAndSet(false, true)) {
          return
        }

        val iter = outboxes.values().iterator()
        while (iter.hasNext()) {
          val outbox = iter.next()
          outboxes.remove(outbox.address)
          outbox.stop()
        }
        if (timeoutScheduler != null) {
          timeoutScheduler.shutdownNow()
        }
        if (dispatcher != null) {
          dispatcher.stop()
        }
        if (server != null) {
          server.close()
        }
        if (clientFactory != null) {
          clientFactory.close()
        }
        if (clientConnectionExecutor != null) {
          clientConnectionExecutor.shutdownNow()
        }
        if (fileDownloadFactory != null) {
          fileDownloadFactory.close()
        }
        if (transportContext != null) {
          transportContext.close()
        }
    
     def deserialize[T](deserializationAction: () => T): T
     功能: 反序列化
     输出参数: deserializationAction	反序列化函数
     val=  NettyRpcEnv.currentEnv.withValue(this) {
      	deserializationAction()}
	
    def fileServer: RpcEnvFileServer = streamManager
    功能: 获取RPC文件服务器
    
    def openChannel(uri: String): ReadableByteChannel
    功能: 读取指定uri通道中的字节
    1. 获取URI,并进行断言检测
    val parsedUri = new URI(uri)
    require(parsedUri.getHost() != null, "Host name must be defined.")
    require(parsedUri.getPort() > 0, "Port must be defined.")
    require(parsedUri.getPath() != null && parsedUri.getPath().nonEmpty, "Path must be defined.")
    2. 获取需要下载文件信息
    val pipe = Pipe.open()
    val source = new FileDownloadChannel(pipe.source())
    3. 获取客户端中传输的流式数据
    val= Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      val client = downloadClient(parsedUri.getHost(), parsedUri.getPort())
      val callback = new FileDownloadCallback(pipe.sink(), source, client)
      client.stream(parsedUri.getPath(), callback)
    })(catchBlock = {
      pipe.sink().close()
      source.close()
    })
    
    def downloadClient(host: String, port: Int): TransportClient
    功能: 获取指定主机名和端口号的传输客户端@TransportClient
    if (fileDownloadFactory == null) synchronized {
      if (fileDownloadFactory == null) {
   		// 双重检索,保证线程安全
   		* 1. 获取模块名称,前缀名称,克隆配置信息
   		val module = "files"
        val prefix = "spark.rpc.io."
        val clone = conf.clone()
        * 2. 拷贝没有spark文件属性信息的RPC配置信息
        conf.getAll.foreach { case (key, value) =>
          if (key.startsWith(prefix)) {
            val opt = key.substring(prefix.length())
            clone.setIfMissing(s"spark.$module.io.$opt", value)
          }
        }
        
        * 3.设置io线程数量为1,配置下载(拷贝了一份conf配置)配置信息,已经配置上下文@TransportContext
        val ioThreads = clone.getInt("spark.files.io.threads", 1)
        val downloadConf = SparkTransportConf.fromSparkConf(clone, module, ioThreads)
        val downloadContext = new TransportContext(downloadConf, new NoOpRpcHandler(), true)
        
        * 4.设置文件下载工厂(产生这个类型的传输客户端@TransportClient)
        fileDownloadFactory = downloadContext.createClientFactory(createClientBootstraps())
      }
      5. 从工厂中获取一个实例
      val= fileDownloadFactory.createClient(host, port)
   }
}
```
#subclass @NettyRpcEnv.FileDownloadChannel

```markdown
private class FileDownloadChannel(source: Pipe.SourceChannel){
	关系: father --> ReadableByteChannel
	介绍: 文件下载通道 
	构造器属性:
		source	数据源(通道)
	属性:
	#name @error #type @Throwable volatile	错误
	操作集:
	def setError(e: Throwable): Unit
	功能: 设置错误原因
	
	def close(): Unit = source.close()
	功能: 关闭通道读取
	
	def isOpen(): Boolean = source.isOpen()
	功能: 确认通道是否开启
	
	def read(dst: ByteBuffer): Int
	功能: 读取/下载目标@dst缓冲区
	val= Try(source.read(dst)) match {
	    case _ if error != null => throw error
        case Success(bytesRead) => bytesRead
        case Failure(readErr) => throw readErr
	}
}
```

#subclass @NettyRpcEnv.FileDownloadCallback

```markdown
private class FileDownloadCallback(sink: WritableByteChannel,source: FileDownloadChannel,
      client: TransportClient){
	介绍: 文件下载回调信息处理
    构造器参数:
    	sink	目标通道位置
    	source	来源通道位置
    	client	传输客户端
    操作集:
    def onData(streamId: String, buf: ByteBuffer): Unit
    功能: 处理缓冲区中的数据
    while (buf.remaining() > 0) {sink.write(buf)}
	
	def onComplete(streamId: String): Unit
	功能: 完成处理措施(关闭sink链接)
	sink.close()
	
	def onFailure(streamId: String, cause: Throwable): Unit
	功能: 失败处理措施
	logDebug(s"Error downloading stream $streamId.", cause)
    source.setError(cause)
    sink.close()
}
```

```markdown
private[netty] object NettyRpcEnv{
	关系: father --> Logging
	属性:
	#name @currentEnv = new DynamicVariable[NettyRpcEnv](null)	当前环境变量
		反序列化@NettyRpcEndpointRef，需要一个@NettyRpcEnv的引用，使用@currentEnv去包装反序列化代码。
		例如:
		{{{
			NettyRpcEnv.currentEnv.withValue(this) {
   		   	 	your deserialization codes
   			}
		}}}
	#name @currentClient = new DynamicVariable[TransportClient](null)	当前客户端
		与当前环境变量@currentEnv相似,这个可变的参考客户端实例，与RPC相关，在这种情况下，需要在反序列化过	程中找到远端地址
}
```
```markdown
private[netty] class NettyRpcEndpointRef(@transient private val conf: SparkConf,
    private val endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv){
	关系: father --> RpcEndpointRef(conf)
    介绍:	RPC端点参考的NettyRPC环境变量。这个类根据创建位置的不同表现形式不同。在拥有RPC端点节点上,它就是一个	具有保证简单RPC端点地址信息的实例对象。
    在其他机器上,接受序列化完成的参考值,这样行为就发生了变化。这个实例会保持对发送参考值的客户端
    @TransportClient的定位,所以发送给RPC端点的信息通过客户端连接发送，而非使用一个新的链接。
    RPC地址的参考值可以为空,意味着参考值ref可能仅仅的通过客户端连接，因此处理RPC端点不是通过监听即将到来的节	点。这些参考值refs需要被第三方共享，因此它们不会发送信息给RPC端点。
    构造器属性:
    conf	spark配置
    endpointAddress	RPC端点地址
    nettyEnv	这个参考值的RPC环境
    属性:
    #name @client: TransportClient = _  传输客户度
    操作集:
    def address: RpcAddress
    功能: 获取RPC地址
    val= if (endpointAddress.rpcAddress != null) endpointAddress.rpcAddress else null
    
    def readObject(in: ObjectInputStream): Unit
    功能: 读取数据
        in.defaultReadObject()
        nettyEnv = NettyRpcEnv.currentEnv.value
        client = NettyRpcEnv.currentClient.value
   	
   	def writeObject(out: ObjectOutputStream): Unit
   	功能: 写出数据
   		out.defaultWriteObject()
   	
   	def name: String = endpointAddress.name
   	功能: 获取端点地址名称
   	
   	def askAbortable[T: ClassTag](
      message: Any, timeout: RpcTimeout): AbortableRpcFuture[T]
    功能: 可放弃式读取数据
    val= nettyEnv.askAbortable(new RequestMessage(nettyEnv.address, this, message), timeout)
    
    def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]
    功能: 使用@message请求消息
	
	def send(message: Any): Unit
	功能: 发送消息
        require(message != null, "Message is null")
        nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
	
	def toString: String = s"NettyRpcEndpointRef(${endpointAddress})"
	功能: 显示信息
	
	def equals(that: Any): Boolean
	功能: 判断对象相等逻辑
	val= Boolean = that match {
    	case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    	case _ => false}
	
	def hashCode(): Int
	功能: 计算hash值
	val= if (endpointAddress == null) 0 else endpointAddress.hashCode()
}
```

```markdown
private[rpc] class NettyRpcEnvFactory{
	关系: father --> RpcEnvFactory
		sibling --> Logging
	介绍: Netty的RPC环境工程
	操作集:
	def create(config: RpcEnvConfig): RpcEnv
	功能: 创建RPC环境
	输入参数: config	RPC环境配置
	1. 序列化配置信息
	val sparkConf = config.conf
	val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
     注意: 这里使用java序列化，是线程安全的，未来打算使用Kryo序列化，因此必须使用@ThreadLocal去存储序列化对象
	2. 获取netty环境
	val nettyEnv =new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
        config.securityManager, config.numUsableCores)
	3. 在客户端模式关闭时,需要根据实际端口映射到netty的环境变量和端口地址
	if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
      	// 启动端口上的服务
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
}
```
```markdown
private[netty] object RequestMessage{
	操作集:
	def readRpcAddress(in: DataInputStream): RpcAddress
	功能: 读取RPC地址
	val= val hasRpcAddress = in.readBoolean()
        if (hasRpcAddress) {
          RpcAddress(in.readUTF(), in.readInt())
        } else {
          null
        }
	
	def apply(nettyEnv: NettyRpcEnv, client: TransportClient, bytes: ByteBuffer): RequestMessage
	功能: 获取一个请求消息实例
	输入参数:
		nettyEnv	netty环境
		client	客户端
		bytes	字节缓冲
	1. 读取sender地址信息
    val bis = new ByteBufferInputStream(bytes)
    val in = new DataInputStream(bis)
    val senderAddress = readRpcAddress(in)
    2. 读取端点参考信息
    val endpointAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())
    val ref = new NettyRpcEndpointRef(nettyEnv.conf, endpointAddress, nettyEnv)
    3. 设置RPC端点参考的客户端
    ref.client = client
    4. 获取实例
    new RequestMessage(
        senderAddress,
        ref,
        // 剩余字节内容是消息内容
        nettyEnv.deserialize(client, bytes))
}
```
```markdown
private[netty] class RequestMessage(val senderAddress: RpcAddress,val receiver: NettyRpcEndpointRef,
    val content: Any){
	介绍: 请求消息
    构造器属性:
    	senderAddress	发送器地址
    	receiver	接收端RPC端点参考
    	content		消息内容
    操作集:
    def serialize(nettyEnv: NettyRpcEnv): ByteBuffer
    功能: 手动序列化@RequestMessage 去缩小消息大小
    1. 写出消息内容到缓冲
        val bos = new ByteBufferOutputStream()
        val out = new DataOutputStream(bos)
        try {
          writeRpcAddress(out, senderAddress)
          writeRpcAddress(out, receiver.address)
          out.writeUTF(receiver.name)
          val s = nettyEnv.serializeStream(out)
          try {
            s.writeObject(content)
          } finally {
            s.close()
          }
        } finally {
          out.close()
        }
	2. 获取写出的内容
		val= bos.toByteBuffer
	
	def writeRpcAddress(out: DataOutputStream, rpcAddress: RpcAddress): Unit
	功能: 写出RPC地址
        if (rpcAddress == null) {
          out.writeBoolean(false)
        } else {
          out.writeBoolean(true)
          out.writeUTF(rpcAddress.host)
          out.writeInt(rpcAddress.port)
        }
    
    def toString: String = s"RequestMessage($senderAddress, $receiver, $content)"
    功能: 信息显示
}
```

```markdown
private[netty] case class RpcFailure(e: Throwable)
功能: 接受侧异常的回应表示
```

```markdown
private[netty] class NettyRpcHandler(dispatcher: Dispatcher,nettyEnv: NettyRpcEnv,
    streamManager: StreamManager){
	关系: father --> RPCHandler
    	sibling --> Logging
    构造器属性:
    	dispatch	分发器
    	nettyEnv	netty环境
    	streamManager	流管理器
    属性:
    #name @remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]() 远端地址映射表
    操作集:
    def receive(client: TransportClient,message: ByteBuffer,callback: RpcResponseCallback): Unit
	功能: 使用客户端@client接受消息@message,回调函数为@callback
	1. 接受来自远端的消息
	val messageToDispatch = internalReceive(client, message)
	2. 分发消息到指定的RPC端点
	dispatcher.postRemoteMessage(messageToDispatch, callback)

	def receive(client: TransportClient,message: ByteBuffer): Unit
	功能: 接受消息@message,本地发送单向消息
        val messageToDispatch = internalReceive(client, message)
        dispatcher.postOneWayMessage(messageToDispatch)
    
    def getStreamManager: StreamManager = streamManager
    功能: 获取流管理器@streamManager
    
    def exceptionCaught(cause: Throwable, client: TransportClient): Unit
    功能: 异常捕获
    1. 将异常分发到所有RPC端点上，以供监听
    val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
    if (addr != null) {
    	val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
      	// 监听本机
      	dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))
	   // 监听远端
		val remoteEnvAddress = remoteAddresses.get(clientAddr)
      	if (remoteEnvAddress != null) {
        	dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
      	}
	}
	
	def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage 
	功能: 远端接受请求消息@RequestMessage
        val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
        assert(addr != null)
        val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
        val requestMessage = RequestMessage(nettyEnv, client, message)
        if (requestMessage.senderAddress == null) {
          new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
        } else {
          val remoteEnvAddress = requestMessage.senderAddress
          if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
            dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
          }
          requestMessage
        }
	
	def channelActive(client: TransportClient): Unit
	功能: 激活通道
        val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
        assert(addr != null)
        val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
        dispatcher.postToAll(RemoteProcessConnected(clientAddr))
	
	def channelInactive(client: TransportClient): Unit
	功能: 通道失活(从发件箱@OutBox中移除客户端)
        val addr = client.getChannel.remoteAddress().asInstanceOf[InetSocketAddress]
        if (addr != null) {
          val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
          nettyEnv.removeOutbox(clientAddr)
          dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))
          val remoteEnvAddress = remoteAddresses.remove(clientAddr)
          // If the remove RpcEnv listens to some address, we should also  fire a
          // RemoteProcessDisconnected for the remote RpcEnv listening address
          if (remoteEnvAddress != null) {
            dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
          }
}
```

#### NettyStreamManager


```markdown
private[netty] class NettyStreamManager(rpcEnv: NettyRpcEnv){
	关系: father --> StreamManager
		sibling --> RpcEnvFileServer
	属性:
	#name @files = new ConcurrentHashMap[String, File]()	文件映射表
	#name @jars = new ConcurrentHashMap[String, File]()	jar映射表
	#name @dirs = new ConcurrentHashMap[String, File]()	目录映射表
	操作集:
	def addFile(file: File): String
	功能: 添加文件到文件映射表中
	val existingPath = files.putIfAbsent(file.getName, file)
	require(existingPath == null || existingPath == file) //路径存在性断言
	val= s"${rpcEnv.address.toSparkURL}/files/${Utils.encodeFileNameToURIRawPath(file.getName())}"
	
	def addJar(file: File): String
	功能: 添加jar包到jar映射表
	val existingPath = jars.putIfAbsent(file.getName, file)
	require(existingPath == null || existingPath == file) //路径存在性断言
	val= s"${rpcEnv.address.toSparkURL}/jars/${Utils.encodeFileNameToURIRawPath(file.getName())}"
	
	def addDirectory(baseUri: String, path: File): String
	功能: 添加目录到目录映射器
	val fixedBaseUri = validateDirectoryUri(baseUri) // 目录格式修正
	require(dirs.putIfAbsent(fixedBaseUri.stripPrefix("/"), path) == null) // 存放新键值对，并断言
	val= s"${rpcEnv.address.toSparkURL}$fixedBaseUri"
	
	def getChunk(streamId: Long, chunkIndex: Int): ManagedBuffer
	功能: 获取缓冲对应的块信息@ManagedBuffer
		不支持该操作
		val= throw UnsupportedOperationException
		
	def openStream(streamId: String): ManagedBuffer
	功能: 读取数据信息到缓冲@ManagedBuffer
	1. 获取类型和名称
	val Array(ftype, fname) = streamId.stripPrefix("/").split("/", 2)
	2. 读取文件
        val file = ftype match {
          case "files" => files.get(fname)
          case "jars" => jars.get(fname)
          case other =>
            val dir = dirs.get(ftype)
            require(dir != null, s"Invalid stream URI: $ftype not found.")
            new File(dir, fname)
        }
    3. 获取文件对于的缓冲内容
    val= file != null && file.isFile() ?
    	FileSegmentManagedBuffer(rpcEnv.transportConf, file, 0, file.length()) : null
}
```

#### Outbox

发件箱

```markdown
private[netty] sealed trait OutboxMessage {
	介绍: 发件箱消息
	操作集:
	def sendWith(client: TransportClient): Unit
	功能: 客户端发送消息
	
	def onFailure(e: Throwable): Unit
	功能: 异常处理
}

private[netty] case class OneWayOutboxMessage(content: ByteBuffer){
	关系: father --> OutboxMessage
		sibling --> Logging
	介绍: 单向消息发件箱
	构造器参数:
	content	消息内容
	
	操作集:
	def sendWith(client: TransportClient): Unit =  client.send(content)
	功能: 客户端发送详细内容
	
	def onFailure(e: Throwable): Unit 
	功能: 错误/异常处理
        e match {
          case e1: RpcEnvStoppedException => logDebug(e1.getMessage)
          case e1: Throwable => logWarning(s"Failed to send one-way RPC.", e1) }
}

private[netty] case class RpcOutboxMessage(content: ByteBuffer,
    _onFailure: (Throwable) => Unit,_onSuccess: (TransportClient, ByteBuffer) => Unit){
	关系: father --> OutboxMessage
    	sibling --> RpcResponseCallback --> Logging
    介绍: RPC发件箱消息
    构造器参数:
    	content	消息内容
    	_onFailure	异常处理函数
    	_onSuccess	成功处理函数
    属性:
    #name @client=_ #type @TransportClient	客户端
    #name @requestId=_ #type @Long	请求ID
    操作集:
    def sendWith(client: TransportClient): Unit
    功能: 设置客户端,即请求编号属性,并发送消息@content
        this.client = client
        this.requestId = client.sendRpc(content, this)
    
    def removeRpcRequest(): Unit
    功能: 移除RPC请求(客户端中移除编号为@requestId的请求)
    	client != null ? client.removeRpcRequest(requestId) : 
    	logError("Ask terminated before connecting successfully")
    
    def onTimeout(): Unit
    功能: 超时处理
    removeRpcRequest() // 移除当前请求id
    
    def onAbort(): Unit 
    功能: 放弃处理
    removeRpcRequest()
    
    def onFailure(e: Throwable): Unit
    功能: 失败处理
    _onFailure(e)
    
    def onSuccess(response: ByteBuffer): Unit
    功能: 成功处理
    _onSuccess(client, response)
}
```

```markdown
private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {
	介绍: RPC收件箱
	构造器参数:
		nettyEnv	RPC环境
		address		RPC地址
	属性:
	#name @messages = new java.util.LinkedList[OutboxMessage] @GuardedBy("this") 消息列表
	#name @client=null #type @TransportClient GuardedBy("this") 客户端
	#name @connectFuture=null #type @java.util.concurrent.Future[Unit] GuardedBy("this") 连接任务
		指向连接任务,没有连接任务的时候,这个值为null
	#name @stopped=false GuardedBy("this") 	发件箱状态
	#name @draining=false GuardedBy("this")	是否设置一个线程排出消息队列
	操作集:
	def send(message: OutboxMessage): Unit
	功能: 发送消息，如果不存在可以使用的链接，则对消息进行缓存且启动一个新的链接。如果发件箱@Outbox被停止了，	那么发送器会被@SparkException通知
	1. 获取停止标记位
        val dropped = synchronized {
          if (stopped)
            true
          else 
            messages.add(message)
            false
        }
	2. 如果发件箱停止了，则需要发送器处理异常。否则从消息队列中排出消息。
	if(dropped) 
		message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
	else
		drainOutbox()
	
	def drainOutbox(): Unit
	功能: 排出发件箱中的消息。消费消息队列中的消息，如果有其他消费线程就退出。如果链接还没有建立，则会运行
	@nettyEnv.clientConnectionExecutor 去创建一个链接。
	1. 同步检定
        var message: OutboxMessage = null
        synchronized {
          if (stopped) {// 发件箱停止退出
            return
          }
          if (connectFuture != null) {
            // 链接任务存在退出
            return
          }
          if (client == null) {
           // 客户端不存在处理
            launchConnectTask()
            return
          }
          if (draining) {
            // 存在有其他消费队列 退出
            return
          }
          // 获取消息列表中的首个元素
          message = messages.poll()
          if (message == null) {
            return
          }
          draining = true
        }
	2. 使用while(true)循环发出消息
         while (true) {
              try {
              	1. 发送消息
                    val _client = synchronized { client }
                    if (_client != null) {
                      message.sendWith(_client)
                    } else {
                      // 客户端不存在下状态断言
                      assert(stopped)
                    }
                  } catch {
                    case NonFatal(e) =>
                      // 处理网络异常
                      handleNetworkFailure(e)
                      return
                  }
                2. 检定当前消息队列是否为空  
                  synchronized {
                    if (stopped) {
                      return
                    }
                    message = messages.poll()
                    if (message == null) {
                      draining = false
                      return
                    }
              }
        }
     
       def launchConnectTask(): Unit
       功能: 运行连接任务
       val callable=new Callable[Unit]{
       override def call(): Unit = {
            try {
             1. 初始化客户端
              val _client = nettyEnv.createClient(address)
              outbox.synchronized {
                client = _client
                if (stopped) {
                  closeClient()
                }
              }
            } catch {
              case ie: InterruptedException =>
                return
              case NonFatal(e) =>
                outbox.synchronized { connectFuture = null }
                handleNetworkFailure(e)
                return
            }
            outbox.synchronized { connectFuture = null }
            // 从这里就需要开始排出数据了，否则必须等到下一条消息到达才能排出消息
            drainOutbox()	
       }
       connectFuture = nettyEnv.clientConnectionExecutor.submit(callable)

       def handleNetworkFailure(e: Throwable): Unit
       功能: 处理网络异常/错误
       1. 关闭客户端，停止发件箱
           synchronized {
              assert(connectFuture == null)
              if (stopped) {
                return
              }
              stopped = true
              closeClient()
            }
       2. 移除当前RPC地址对应的发件箱，以便于新进入的消息会使用新链接创建新的发件箱 
       nettyEnv.removeOutbox(address)
       3. 排出消息队列中剩余的消息
        由于在更新消息时，需要检查stopped标志，所以可以保证没有线程可以更新这些消息，所以排出这些消息时安全的
           var message = messages.poll()
            while (message != null) {
              message.onFailure(e)
              message = messages.poll()
            }
            // 最后最检定
            assert(messages.isEmpty)

        def stop(): Unit
        功能: 停止发件箱@OutBox,发件箱的剩余消息会使用@SparkException提示
        1. 停止客户端,发件箱,取消链接任务(先停止发件箱，再取消链接任务，最后关闭客户端)
            synchronized {
              if (stopped) {
                return
              }
              stopped = true
              if (connectFuture != null) {
                connectFuture.cancel(true)
              }
              closeClient()
            }
         2. 剩余消息处理
            直接将消息排出，由于stop=true，其他线程不会更新这些消息，所以可以直接处理，如果处理消息引发异常，			则进行异常处理
        	var message = messages.poll()
         	while (message != null) {
              message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
              message = messages.poll() }
         
}
```

#### RpcEndpointVerifier

```markdown
private[netty] class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher){
	关系： father --> RpcEndpoint
	介绍: RPC 断点验证器
		一个RPC端点@RpcEndpoint 对于远程RPC环境询问RPC端点@RpcEndpoint是否存在
		使用在创建远程端点参考的时候。
	构造器属性:
		rpcEnv	RPC环境
		dispatcher	分配器
	操作集:
	def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
	功能: 接受回调信息，并做出响应
	case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
}
```

```markdown
private[netty] object RpcEndpointVerifier {
	属性:
	#name @NAME="endpoint-verifier"名称
	
	case class CheckExistence(name: String)
	功能: 检查远程RPC端点验证器，是否有端点存在@RpcEndpoint
}
```

#### RpcAddress

```markdown
介绍: 
	RPC环境的地址属性(主机名,端口)
```

```markdown
private[spark] case class RpcAddress(host: String, port: Int) {
	操作集:
	def hostPort: String = host + ":" + port
	功能: 获取主机名端口信息
	
	def toSparkURL: String = "spark://" + hostPort
	功能: 获取spark资源定位符信息
	
	def toString: String = hostPort
	功能: 信息显示
}
```

```markdown
private[spark] object RpcAddress {
	操作集:
	def fromURIString(uri: String): RpcAddress
	功能: 获取指定资源标识符@uri对应的RPC信息@RpcAddress
        val uriObj = new java.net.URI(uri)
        val= RpcAddress(uriObj.getHost, uriObj.getPort)
	
	def fromSparkURL(sparkUrl: String): RpcAddress
	功能: 获取指定spark地址对应的RPC信息@RpcAddress
		val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    	val= RpcAddress(host, port)
}
```

#### RpcCallContext

```markdown
private[spark] trait RpcCallContext {
	介绍: 这是@RpcEndpoint使用的回调，用户发送消息或者失败信息，这是线程安全的且可以在任		意线程调用。
	
	操作集:
	def reply(response: Any): Unit
	功能: 给发送器@RpcAddress 回应消息。如果发送器是@RpcEndpoint类型，则会调用
    @RpcEndpoint.receive
	
	def sendFailure(e: Throwable): Unit
	功能: 报告失败@e给发送器
	
	def senderAddress: RpcAddress
	功能: 获取这条消息的发送器@RpcAddress
}
```

#### RpcEndpoint

```markdown
private[spark] trait RpcEnvFactory {
	介绍: 创建@RpcEnv的工厂类,必须要有无参构造器以便反射
	操作集:
  	def create(config: RpcEnvConfig): RpcEnv
	功能: 根据指定配置@config常见一个RpcEnv实例
}
```

```markdown
private[spark] trait RpcEndpoint{	
	介绍:
        RPC端点,定义了什么函数会触发给定消息.这里确保@onStart,@receive,@onStop会被调用
        RPC端点的生命周期:
        constructor -> onStart -> receive* -> onStop
        注意: 其中,@receive是可并发的,如果你想线程安全,请使用@ThreadSafeRpcEndpoint
        如果中途发送错误,会通过@onError 处理错误事件
	属性:
		#name @rpcEnv #type @RpcEnv RPC端点需要注册的RPC环境
	操作集:
	final def self: RpcEndpointRef 
	功能:当前RPC端点的参考@RpcEndpointRef,当使用@onStart方法时,@self设置的参数会失效(变成null)
	注意: 在@onStart 调用之前,端点@RpcEndpoint没有被注册,因此没有对应的参考@RpcEndpointRef,所以不要在
	@onStart之前调用@self
	
	def receive: PartialFunction[Any, Unit]
	功能: 处理来自@RpcEndpointRef.send 发送的消息或者@RpcCallContext.reply 回应的信息,如果接受的消息不匹
	配,抛出异常,交由@onError 处理
		val= case _ => throw new SparkException(self + " does not implement 'receive'")
		(这里没有支持其实现,需要在实现类中自己写入逻辑)
	
	def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
	功能: 处理来自@RpcEndpointRef.ask 的请求消息,如果收到了不匹配的消息,会抛出异常并交由@onError处理
		val=case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
		(这里斌没有给实现,需要实现类中自己写入逻辑)
	
	def onError(cause: Throwable): Unit= throw cause
	功能: 错误处理(默认抛出异常/错误,可自定义)
	
	def onConnected(remoteAddress: RpcAddress): Unit = {}
	功能: 连接到指定RPC地址的操作(子类实现)
	
	def onDisconnected(remoteAddress: RpcAddress): Unit = {}
	功能: 与指定RPC地址@remoteAddress 失去连接操作(子类实现)
	
	def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit 
	功能: 与目的RPC地址@remoteAddress 发生网络错误的处理方式(子类实现)
	
	def onStart(): Unit = {}
	功能: 启动时初始化操作(子类实现)
	
	def onStop(): Unit = {}
	功能: 关闭时操作,这个方法里@self 会变成null,你不能使用它来收发信息(生命周期到头了) --> 子类实现逻辑
	
	final def stop(): Unit
	功能: RPC端点@RpcEndpoint的便捷式关闭方法
        val _self = self
        if (_self != null) {
          rpcEnv.stop(_self)
        }
}
```

```markdown
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint
介绍: 线程安全的RPC端点
	线程安全意味着处理一条消息之后才会接受下一条消息.换句话说,改变@ThreadSafeRpcEndpoint的外部属性,使得当其处理下一条消息时能够看到这个属性,且在@ThreadSafeRpcEndpoint 不应当是不稳定的或是相等的.
	然而并不保证同一个线程对于不同消息会执行同一个RPC端点@ThreadSafeRpcEndpoint.
```

```markdown
private[spark] trait IsolatedRpcEndpoint extends RpcEndpoint {
	介绍: 端点使用专用线程池发送消息
	操作集:
	def threadCount(): Int = 1
	功能: 用于传输消息的线程数量,默认单线程传送.
	注意到请求超过一个线程,意味着这个端点需要立即能够处理来自多个线程的消息,而且其他信息也要同时获得(包括没有传送到端点消息的信息)
}
```



#### RpcEndpointAddress

```markdown
private[spark] case class RpcEndpointAddress(rpcAddress: RpcAddress, name: String) {
	属性: 
	#name @toString #type @String	显示信息
	val= rpcAddress != null ?
    	s"spark://$name@${rpcAddress.host}:${rpcAddress.port}" :
    	s"spark-client://$name"
    初始化操作:
    require(name != null, "RpcEndpoint name must be provided.")
	功能: 名称断言
	
	def this(host: String, port: Int, name: String) 
	功能: 获取Rpc端点地址@RpcEndpointAddress
		val= this(RpcAddress(host, port), name)
}
```

```markdown
private[spark] object RpcEndpointAddress {
	操作集:
    def apply(host: String, port: Int, name: String): RpcEndpointAddress
	功能: 根据主机和端口信息,获取一个RPC端点地址实例@RpcEndpointAddress
		val= new RpcEndpointAddress(host, port, name)
	
	def apply(sparkUrl: String): RpcEndpointAddress
	功能: 根据指定spark地址信息@RpcEndpointAddress获取一个实例@RpcEndpointAddress
	1. 获取配置信息
	val uri = new java.net.URI(sparkUrl)
    val host = uri.getHost
    val port = uri.getPort
    val name = uri.getUserInfo
	2. 排除schema不是spark，非法host/name/port信息
	if (uri.getScheme != "spark" ||
          host == null ||
          port < 0 ||
          name == null ||
          (uri.getPath != null && !uri.getPath.isEmpty) || 
          uri.getFragment != null ||
          uri.getQuery != null)
         throw SparkException
	3. 获取实例
	val= new RpcEndpointAddress(host, port, name)
}
```

#### RpcEndpointNotFoundException

```markdown
private[rpc] class RpcEndpointNotFoundException(uri: String){
	关系: father --> SparkException
	介绍: 当找不到指定uri对应的RPC端点时抛出的异常
}
```

#### RpcEndpointRef

```markdown
private[spark] abstract class RpcEndpointRef(conf: SparkConf){
	关系: father --> Serializable
		sibling --> Logging
	属性:
	#name @maxRetries=RpcUtils.numRetries(conf) #type @int 最大尝试次数
	#name @retryWaitMs=RpcUtils.retryWaitMs(conf) #type @long 重试等待时间
	#name @defaultAskTimeout= RpcUtils.askRpcTimeout(conf)	
		默认询问最长时间
	操作集:
	def address: RpcAddress
	功能: 获取RPC地址
	
	def name: String
	功能: 获取名称
	
	def send(message: Any): Unit
	功能: 发送一条异步消息
	
	def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] 
	功能: 可放弃式访问
		发送消息到相应的@RpcEndpoint.receiveAndReply 中,并返回一个可以抛弃的RPC任务@AbortableRpcFuture,主		要用于接受规定时间限制@timeout内的响应.
		@AbortableRpcFuture 是对@Future的一层包装,提供了抛弃@abort方法.
		
	def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]
	功能: 在规定时间内获取消息@message的响应
		发送消息@message到指定RPC端点@RpcEndpoint.receiveAndReply.返回一个任务@Future,去获取指定时间限制	内的响应.这个方法只发送一次,且不会重试.
		
	def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T 
	功能: 同步访问
		发送消息到相应的RPC端点@RpcEndpoint.receiveAndReply 并在指定时限内部获取结果,如果失败抛出异常.
		注意: 阻塞操作需要耗费大量的时间,所以不要在循环的RPC端点@RpcEndpoint消息中调用它,因为会引发不同端点		之间资源争抢,从而会阻塞某些线程.
	1. 获取任务
	val future = ask[T](message, timeout)
	2. 等待结构
	timeout.awaitResult(future)
	
	 def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)
	 功能: 同步访问(采取默认访问时间)
	 输入参数: message 需要发送的消息
	 val= askSync(message, defaultAskTimeout)
}
```

```markdown
class RpcAbortException(message: String){
	关系: father --> Exception(message)
	介绍: 如果RPC被废弃则抛出此异常
}
```

```markdown
private[spark] class AbortableRpcFuture[T: ClassTag] (future: Future[T],onAbort: String => Unit){
	介绍: 添加@abort 方法的任务@Future,适用于长期允许的RPC,提供抛弃RPC的方法
	构造器参数:
		future	任务
		onAbort	抛弃方法
	操作集:
	def abort(reason: String): Unit = onAbort(reason)
	功能: 指定抛弃原因@reason,抛弃RPC
	
	def toFuture: Future[T] = future
	功能: 获取任务
}
```



#### RpcEnv


```markdown
private[spark] object RpcEnv {
	介绍: RpcEnv的实现必须要含有一个带有无参构造器@RpcEnvFactory,以便于其可以通过反射创建.
	操作集:
	def create(name: String,host: String,port: Int,conf: SparkConf,
      securityManager: SecurityManager,clientMode: Boolean = false): RpcEnv
	功能: 从RPC工厂创建一个实例,默认使用核心数为0
		val= @create(name, host, host, port, conf, securityManager, 0, clientMode)
	
	def create(name: String,bindAddress: String,advertiseAddress: String,
      port: Int,conf: SparkConf,securityManager: SecurityManager,
      numUsableCores: Int,clientMode: Boolean): RpcEnv
	功能: 从RPC工厂中获取一个实例
	1. 获取配置信息
	val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
      numUsableCores, clientMode)
    2. 获取实例
    val= new NettyRpcEnvFactory().create(config)
}
```

```markdown
private[spark] abstract class RpcEnv(conf: SparkConf) {
	介绍: RPC环境,RPC端点@RpcEndpoint 需要注册自己的名字给RPC环境@RpcEnv,以便于接受信息.@RpcEnv 会加工来自于参考端点@RpcEndpointRef或者远程节点的消息.且发送消息通过相应的RPC端点@RpcEndpoint.对于维捕捉到的异常,@RpcEnv会使用RPC回调@RpcCallContext.sendFailure 去设置异常,并将异常信息发送回给发送器@sender.当然@RPCEnv也提供检索@RpcEndpointRef 名称的方法.
	
	属性: 
	#name @defaultLookupTimeout=RpcUtils.lookupRpcTimeout(conf)	默认查找时间上限
	操作集:
	def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef
	功能: 返回记录有RPC端点信息的RPC端点参考@RpcEndpointRef
	
	def address: RpcAddress
	功能: 获取RPC环境@RPCEnv监听的端口信息
	
	def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef
	功能: 根据指定的名称@name注册RPC端点,并返回RPC端点参考@RpcEndpointRef,RPC环境@RpcEnv 不保证是线程安全的
	
	def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]
	功能: 通过@uri异步检索RPC端点参考@RpcEndpointRef
	
	def setupEndpointRefByURI(uri: String): RpcEndpointRef
	功能: 通过@uri 检索RPC端点参考@RpcEndpointRef,这是一个阻塞操作
		val= defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
	
	def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef
	功能: 根据指定RPC地址@address和指定端点名称@endpointName检索RPC端点参考@RpcEndpointRef
		val= setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
		
	def stop(endpoint: RpcEndpointRef): Unit
	功能: 停止指定的@endpoint 的RPC端点参考@RpcEndpointRef
	
	def shutdown(): Unit
	功能: 异步关闭@RPCEnv,如果确认@RPCEnv关闭成功,在关闭之后调用@awaitTermination 等待终止
	
	def awaitTermination(): Unit
	功能: 等待@RPCEnv退出
	
	def deserialize[T](deserializationAction: () => T): T
	功能: Rpc端点参考@RpcEndpointRef 不能使用@RPCEnv反序列化,所以反序列需要使用这个函数
	输入参数: 
		deserializationAction: () => T	反序列化函数
	
	def fileServer: RpcEnvFileServer
	功能: 获取RPC文件服务器(不处于服务器模式时,返回null)
	
	def openChannel(uri: String): ReadableByteChannel
	功能: 从指定@uri中,打开通道下载文件.如果文件服务器中的uri地址使用spark.的schema,该方法会调用工具类@Utils		去检索文件.
}
```
```markdown
private[spark] trait RpcEnvFileServer {
	介绍: RPCEnv文件服务器(用于给该应用的其他进程提供文件服务)
	文件服务器返回URI信息,可以是普通信息(http,hdfs)或者由@RpcEnv#fetchFile 处理的spark URI信息
	操作集:
	def addFile(file: File): String
	功能: 添加指定文件@File到RPCEnv的文件服务器中,当其存储于driver侧的本地文件系统时,用于服务文件由driver到
	executor的功能.返回文件的URI信息.
	
	def addJar(file: File): String
	功能: 添加Jar包到这个RPCEnv中,但是添加jar使用@SparkContext.addJar,返回jar的URI信息
	
	def addDirectory(baseUri: String, path: File): String
	功能: 添加目录,返回文件服务器中的文件路径名称
	输入参数:
		baseuri	引入URI路径
		path	本地目录
	
	def validateDirectoryUri(baseUri: String): String
	功能: 格式化基本URI地址
	val= = "/" + baseUri.stripPrefix("/").stripSuffix("/")
}
```
```markdown
private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    securityManager: SecurityManager,
    numUsableCores: Int,
    clientMode: Boolean)
介绍: RPC配置样例类
```
#### RpcEnvStoppedException

```markdown
private[rpc] class RpcEnvStoppedException(){
	关系: father --> IllegalStateException
	介绍: rpc环境停止异常
}
```

#### RpcTimeout

```markdown
private[rpc] class RpcTimeoutException(message: String, cause: TimeoutException){
	关系: father --> TimeoutException(message)
	初始化操作:
	initCause(cause)
	功能: 初始化异常原因
}
```

```markdown
private[spark] class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String){
	关系: father --> Serializable
	构造器属性:
		duration	限制周期
		timeoutProp	超时属性
	操作集:
	createRpcTimeoutException(te: TimeoutException): RpcTimeoutException
	功能: 创建rpc超时异常
	val=new RpcTimeoutException(te.getMessage + ". This timeout is controlled by " + timeoutProp, te)
	
	def addMessageIfTimeout[T]: PartialFunction[Throwable, T]
	功能: 超时添加消息
		匹配异常类型，并添加超时描述到消息中
	case rte: RpcTimeoutException => throw rte	// RPC型异常，可以直接使用
	case te: TimeoutException => throw createRpcTimeoutException(te) // 其他类型需要通过转换
	
	def awaitResult[T](future: Future[T]): T
	功能: 等待结果
		等待完整结果并返回，如果结果在规定时间内不可以接收到，这回抛出异常@RpcTimeoutException
	输入参数:	future 需要等待的线程
	val= try {
		// 等待线程结果(等待周期为duration)
      	ThreadUtils.awaitResult(future, duration)
    } 
    	catch addMessageIfTimeout	// 等待超时，则捕获异常
}
```

```markdown
private[spark] object RpcTimeout {
	操作集:
	 def apply(conf: SparkConf, timeoutProp: String): RpcTimeout
	 功能: 根据应用程序配置集@conf和超时属性@timeoutProp 获取一个实例
	 	查找配置中的超时属性，创建一个带有属性key描述的@RpcTimeout
     1. 获取超时时间
     val timeout = { conf.getTimeAsSeconds(timeoutProp).seconds }
     2. 获取实例
     val = new RpcTimeout(timeout, timeoutProp)
	
	def apply(conf: SparkConf, timeoutProp: String, defaultValue: String): RpcTimeout
	功能: 查找配置的超时属性，创建一个带有属性key描述的@RpcTimeout,如果没有设置时使用给定默认参数
        val timeout = { conf.getTimeAsSeconds(timeoutProp, defaultValue).seconds }
        val= new RpcTimeout(timeout, timeoutProp)
	
	def apply(conf: SparkConf, timeoutPropList: Seq[String], defaultValue: String): RpcTimeout
	功能: 查找配置@conf的超时属性，创建一个带有属性key描述的@RpcTimeout，没有设置时使用默认参数@default
        val itr = timeoutPropList.iterator
        var foundProp: Option[(String, String)] = None
        while (itr.hasNext && foundProp.isEmpty) {
          val propKey = itr.next()
          conf.getOption(propKey).foreach { prop => foundProp = Some((propKey, prop)) }
        }
        val finalProp = foundProp.getOrElse((timeoutPropList.head, defaultValue))
        val timeout = { Utils.timeStringAsSeconds(finalProp._2).seconds }
        val= new RpcTimeout(timeout, finalProp._1)
}
```

#### 基础拓展

1.  幂等性