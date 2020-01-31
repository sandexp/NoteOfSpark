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
		发送消息到@
}
```



#### RpcEnv

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

