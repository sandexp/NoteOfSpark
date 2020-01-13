## **spark-launcher**

---

1.  [LauncherBackend.scala](# LauncherBackend)
2.  [SparkSubmitArgumentsParser.scala](# SparkSubmitArgumentsParser)
3.  [WorkerCommandBuilder.scala](# WorkerCommandBuilder)

---

#### LauncherBackend

```markdown
介绍:
	这个类可以与运行服务器通信,用户需要实现这个抽象类,并实现其中的抽象方法
	请参照#class @LauncherServer(org.apache.spark.launcher.LauncherServer.class) 
	寻找如何运行器是如何去沟通的
```

```markdown
private[spark] abstract class LauncherBackend {
	属性:
		#name @clientThread #type @Thread
		#name @connection #type @BackendConnection
		#name @lastState #type @SparkAppHandle.State
		#name @_isConnected=false #type @volatile 
		#name @def conf: SparkConf #type @SparkConf
	操作集:
		def connect(): Unit
		功能: 连接运行服务器
		1. 获取服务器端口号
		@port= conf.getOption(LauncherProtocol.CONF_LAUNCHER_PORT)
      			.orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_PORT)).map(_.toInt)
		优先级: 配置属性@conf > 环境属性 sys.env
		2. 获取连接密码
		@secret= conf.getOption(LauncherProtocol.CONF_LAUNCHER_SECRET)
			.orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_SECRET))
		3. 当获取服务器端口号与密码则建立起两者之间的连接
			+ socker流:
			val s=new Socket(InetAddress.getLoopbackAddress(), port.get)
			+ 连接@conn
			conn=new BackendConnection(s)
			+ 客户端向服务器发送Hello信息
			conn.send(new Hello(secret.get, SPARK_VERSION))
			+ 设置客户端线程信息
			从本类的线程池中获取一个线程,并启动
			clientThread = LauncherBackend.threadFactory.newThread(connection)
      		clientThread.start()
			+ 设置连接标志位
			_isConnected = true
			
		def close(): Unit
		功能: 关闭连接
			1. connection.close()
			2. clientThread.join()
		
		def setAppId(appId: String): Unit
		功能: 设置appId(在服务器端,需要通过socket发送)
		connection.send(new SetAppId(appId))
		
		def setState(state: SparkAppHandle.State): Unit
		功能: 设置状态(服务器端设置,通过socket发送)
		执行条件: 最后状态,与需要发送状态不同
        	connection.send(new SetState(state))
			lastState = state
			
		def isConnected(): Boolean = _isConnected
         功能: 返回连接状态
        
		def onStopRequest(): Unit
		功能: 抽象方法,需要子类实现,功能是尝试去优雅(gracefully)的关闭应用程序
		
		def onDisconnected() : Unit
		功能: 当服务器端与客户端断开连接时,会调用此方法
		
		def fireStopRequest(): Unit
		功能: 解除停止请求@onStopRequest()状态,并重新开始请求
		val thread = LauncherBackend.threadFactory.newThread(() => 
			Utils.tryLogNonFatalError { onStopRequest() })
		thred.start()
}
```
#subclass @BackendConnection

```markdown
private class BackendConnection(s: Socket){
	关系: father --> LauncherConnection(s)
	操作集:
	protected def handle(m: Message): Unit
	功能: 处理给定信息@m对应事件
		@m=Stop则重新连接服务器@fireStopRequest()
		为其他信息时则抛出异常
	
	def close(): Unit
	功能: 关闭连接
	1. 设置连接标志位	_isConnected = false
	2. 关闭连接	onDisconnected()
}
```

#object @LauncherBackend

```markdown
val threadFactory = ThreadUtils.namedThreadFactory("LauncherBackend")
功能: 创建一个线程工厂,用于获取线程
```


#### SparkSubmitArgumentsParser

```markdown
介绍:
	这个类使得SparkSubmitOptionParser对于spark code的处于launcher.由于java中不像scala这样支持private[spark]这样的权限修饰符,所以这个类不应当时public的
	private[spark] abstract class SparkSubmitArgumentsParser
	关系: father --> SparkSubmitOptionParser
```

#### WorkerCommandBuilder

```markdown
介绍:
	这个类由CommandUtils使用.它使用一些SparkLauncher中一些包私有的API,由于java不支持类似scala支持private[spark]的权限修饰符,所以这个类不应当时public的.需要生存在相同包,作为库的剩余部分.
```

```markdown
private[spark] class WorkerCommandBuilder(sparkHome: String, memoryMb: Int, command: Command){
	关系: father --> AbstractCommandBuilder
	属性:
		初始化设置:
		子环境@childEnv
			设置子环境位输入cmd对应列表,设置SPARK_HOME=给定sparkHome
			childEnv.putAll(command.environment.asJava)
			childEnv.put(CommandBuilderUtils.ENV_SPARK_HOME, sparkHome)
			
		def buildCommand(): JList[String] = buildCommand(new JHashMap[String, String]())
		功能: 根据指定的@JHashMap 创建对应的环境变量列表@JList
		
		def buildCommand(env: JMap[String, String]): JList[String]
		功能: 本类对应生成环境变量列表的实现
		1. 设置cmd加载java基本类路径@buildJavaCommand(String extraClassPath) 
		2. 设置 -Xmx${memoryMb}M
		3. 对command的每个java配置项@javaOpts 进行迭代,并加入到cmd中
}
```

