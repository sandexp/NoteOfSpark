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
}
```

#### Inbox

#### MessageLoop

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
	#name @NAME="endpoint-verifier" 	名称
	
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

