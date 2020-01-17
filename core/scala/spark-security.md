## **spark-security**

---

1.  [CryptoStreamUtils.scala](# CryptoStreamUtils)
2.  [GroupMappingServiceProvider.scala](# GroupMappingServiceProvider)
3.  [HadoopDelegationTokenProvider.scala](# HadoopDelegationTokenProvider)
4.  [ShellBasedGroupsMappingProvider.scala](# ShellBasedGroupsMappingProvider)
5.  [SocketAuthHelper.scala](# SocketAuthHelper)
6.  [SocketAuthServer.scala](# SocketAuthServer)
7.  [基础拓展](# 基础拓展)

---

#### CryptoStreamUtils

```markdown
介绍: [私密流工具类]
	这个一个可以操作IO加密以及流加密的工具类
```

```markdown
private[spark] object CryptoStreamUtils{
	关系: father --> Logging
	属性:
	#name @IV_LENGTH_IN_BYTES=16	初始化字节向量长度
	#name @SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX="spark.io.encryption.commons.config."
		初始化io加密配置前缀
	操作集:
	def createCryptoOutputStream(os: OutputStream,sparkConf: SparkConf,
		key: Array[Byte]): OutputStream 
	功能: 创建私密输出流
	1. 获取私密属性
		val params = new CryptoParams(key, sparkConf)
	2. 获取初始化向量表
		val iv = createInitializationVector(params.conf)
	3. 写出初始化向量表信息
		os.write(iv)
	4. 返回一个可以错误处理的私密输出流
		val= new ErrorHandlingOutputStream(
     		 new CryptoOutputStream(params.transformation, params.conf, os, params.keySpec,
        	new IvParameterSpec(iv)),
      		os)
    
     def createWritableChannel(channel: WritableByteChannel,sparkConf: SparkConf,key: Array[Byte]): 		WritableByteChannel
	功能: 加密包装@WritableByteChannel
	1. 获取私密属性
		val params = new CryptoParams(key, sparkConf)
	2. 获取初始化向量表
		val iv = createInitializationVector(params.conf)
	3. 获取并写出辅助信息
		val helper = new CryptoHelperChannel(channel)
		helper.write(ByteBuffer.wrap(iv))
	4. 返回私密化的字节通道
	    new ErrorHandlingWritableChannel(
     		 new CryptoOutputStream(params.transformation, params.conf, helper, params.keySpec,
       		 new IvParameterSpec(iv)),helper)
       		 
    def createReadableChannel(channel: ReadableByteChannel,sparkConf: SparkConf,
      key: Array[Byte]): ReadableByteChannel
    功能: 创建私密化输出通道
    
    def toCryptoConf(conf: SparkConf): Properties
    功能: 转化为私密配置(spark.io.encryption.commons.config.*中的信息)
    val= CryptoUtils.toCryptoConf(SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX,
      conf.getAll.toMap.asJava.entrySet())
     
    def createKey(conf: SparkConf): Array[Byte] 
    功能: 创建加密的key
    1. 获取加密字节长度
    	val keyLen = conf.get(IO_ENCRYPTION_KEY_SIZE_BITS)
    2. 获取加密算法类型
    	val ioKeyGenAlgorithm = conf.get(IO_ENCRYPTION_KEYGEN_ALGORITHM)
    3. 获取key生成器
    	val keyGen = KeyGenerator.getInstance(ioKeyGenAlgorithm)
    4. key生成器初始化
    	keyGen.init(keyLen)
    5. 返回加密后的key
    	keyGen.generateKey().getEncoded()
    
    def createInitializationVector(properties: Properties): Array[Byte]
    功能: 创建初始化向量表(使用安全随机的方式)
    1. 分配初始化向量表空间
    val iv = new Array[Byte](IV_LENGTH_IN_BYTES)
    2. 创建随机对象，并对iv中的每一位进行随机赋值
    CryptoRandomFactory.getCryptoRandom(properties).nextBytes(iv)
    随机处理方式请参照接口#interface @CryptoRandom
}
```
```markdown
private class CryptoHelperChannel(sink: WritableByteChannel){
	关系: father --> WritableByteChannel
	介绍: 这个类与CRYPTO-125有关。使得所有字节写到底层通道中。因此这个API的调用者使用BIO，所有没有CPU使用的考		虑。
	构造器属性:
		sink	可写字节通道
	操作集:
	def isOpen(): Boolean = sink.isOpen()
	功能: 确定当前通道是否打开
	
	def close(): Unit = sink.close()
	功能: 确定当前通道是否关闭
	
	def write(src: ByteBuffer): Int
	功能: 将src写出到底层通道@sink
}
```
```markdown
trait BaseErrorHandler{
	关系: father --> Closeable
	介绍: commons-cryto库在出现错误时，会报出@InternalError错误。由于在java包装类中留下来失败状态,之后再使	用就不安全了。这个包装发现避免进一步的调用commons-crypto代码，此时仍然允许底层流保持关闭状态。
	属性:
	#name @closed=false	底层流关闭状态标记
	
	操作集:
	def cipherStream: Closeable
	功能: 获取可能导致不健康状态的加密流
	
	def original: Closeable
	功能: 获取有加密流包装的底层流。以便于其能够正常在私密流(crypto layer)出错情况下工作
	
	def safeCall[T](fn: => T): T
	功能: 安全调用函数fn
	 ！closed ? IOExcpetion : fn
	 catch(fn) ? InternalError => closed = true original.close() throw ie
	
	def close(): Unit
	功能: 关流
}
```
```markdown
class ErrorHandlingReadableChannel(
      protected val cipherStream: ReadableByteChannel,
      protected val original: ReadableByteChannel){
	关系: father --> ReadableByteChannel
    	sibling --> BaseErrorHandler
    操作集:
    	def read(src: ByteBuffer): Int={ cipherStream.read(src)}
    	功能: 使用安全调用的方式(忽略私密层错误)，读取字节缓冲数据
    	
    	def isOpen(): Boolean = cipherStream.isOpen()
    	功能: 获取字节通道当前状态
}
```
```markdown
private class ErrorHandlingInputStream(protected val cipherStream: InputStream,
      protected val original: InputStream){
 	关系: father --> InputStream
 		sibling --> BaseErrorHandler
 	操作集: 
 		def read(b: Array[Byte]): Int
 		功能: 使用安全调用的方式，读取一组数据
 		
 		def read(): Int 
 		功能: 使用安全调用的方式，读取一个字节
 		
 		def read(b: Array[Byte], off: Int, len: Int): Int 
 		功能: 使用安全调用的方式，读取一组数据
 }
```

```markdown
private class ErrorHandlingWritableChannel(
      protected val cipherStream: WritableByteChannel,
      protected val original: WritableByteChannel){
	关系: father --> WritableByteChannel
    sibling --> BaseErrorHandler
    操作集:
    def isOpen(): Boolean = cipherStream.isOpen()
    功能: 通道当前状态
    
    def write(src: ByteBuffer): Int 
    功能: 使用安全调用的方式，写出数据
}
```

```markdown
  private class ErrorHandlingOutputStream(protected val cipherStream: OutputStream,
      protected val original: OutputStream){
 	关系: father --> OutputStream
    	sibling --> BaseErrorHandler
    操作集:
    def flush(): Unit 
    功能: 使用安全调用的方式刷写数据到磁盘中
    
    def write(b: Array[Byte]): Unit
    def write(b: Array[Byte], off: Int, len: Int): Unit
    def write(b: Int): Unit
    功能: 使用安全调用的方式写出数据
 }
```

```markdown
private class CryptoParams(key: Array[Byte], sparkConf: SparkConf){
	构造器属性:
		key			key值列表
		sparkConf	应用程序配置集
	属性:
		#name @keySpec=new SecretKeySpec(key, "AES")	key值密码
		#name @transformation=sparkConf.get(IO_CRYPTO_CIPHER_TRANSFORMATION)	转换
		#name @conf=toCryptoConf(sparkConf)		私密配置文件
}
```



#### GroupMappingServiceProvider

```markdown
介绍:
	这个特征用于处理对给定用户名映射到其所属组
	对于指定给一个组(admins/developers),提供其admin权限,修改或查找其使用权限.基于是否使用
	@spark.acls.enable使能权限检查,每次当用户尝试去使用或者修改应用时.安全管理器@SecurityManager获取用户所属的权限组,通过由@spark.user.groups.mapping指定的组映射
```

```markdown
trait GroupMappingServiceProvider {
	def getGroups(userName : String) : Set[String]
	功能: 获取用户所属组
}
```

#### HadoopDelegationTokenProvider

```markdown
介绍:	
	这个特征的实现可交给用户实现.主要是提供hadoop的委托令牌(delegation token)
```

```markdown
@DeveloperApi
trait HadoopDelegationTokenProvider{
	操作集:
	def serviceName: String
	功能: 服务名称
		这个服务名称用于提供委托令牌,名称需要保证唯一性.spark会内部的使用这个名称@serviceName去调用
		(differentiate)委托令牌提供者.
	
	def delegationTokensRequired(sparkConf: SparkConf, hadoopConf: Configuration): Boolean
	功能: 是否需要委托令牌
	为true则表示需要委托令牌.默认情况下,根据Hadoop security来决定.
	
	def obtainDelegationTokens(hadoopConf: Configuration,sparkConf: SparkConf,
    	creds: Credentials): Option[Long]
	功能: 获取委托令牌
		获取这个服务的委托令牌,且获取下个恢复时间
	输入参数:
		hadoopConf	当前hadoop兼容系统的配置
		creds		添加令牌的证书,以及安全密钥
	返回参数:
		如果获得的令牌是可以更新的,则返回下次更新的时间,否则返回None
}
```

#### ShellBasedGroupsMappingProvider

```markdown
介绍:
	这个类负责获取特点Unix环境下用户所属的组.这个实现使用Unix Shell,基于id commond去获取指定用户所属的用户组,但是它不缓存用户组,因为调用的不频繁的.
```

```markdown
private[spark] class ShellBasedGroupsMappingProvider{
	关系: father --> GroupMappingServiceProvider
	sibling --> Logging
	操作集:
	def getGroups(username: String): Set[String]
	功能: 获取用户@username的组名称
	val=  getUnixGroups(username)
	
	private def getUnixGroups(username: String): Set[String]
	功能: 获取unix环境下用户所属组
	1. val cmd =Seq("bash", "-c", "id -Gn " + username)
	2. 执行获取执行结果并返回
	val= Utils.executeAndGetOutput(cmdSeq).stripLineEnd.split(" ").toSet
}
```

#### SocketAuthHelper

```markdown
介绍: 
	这是一个可以将单个证实协议添加到socket通信的类
	协议很简单:
		将授权密码写入到socket中,其他方面检查密码并写入ok/err到输出中.如果证实失败,那么这个socket将不再合法.
	这之中没有保密,所以这个依赖于本地socket或者某种程度上的加密socket。
```

```markdown
private[spark] class SocketAuthHelper(conf: SparkConf){
	构造器属性:
		conf	应用程序配置集
	属性:
		#name @secret=Utils.createSecret(conf)	密码
	操作集:
	def authClient(s: Socket): Unit 
	功能: 读取来自于socket的密码，并与期望值作比较。将应答(reply)返回给socket
		如果证实失败,或者过程中抛出了异常，那么这个方法会关闭这个socket
	1. 设定关闭指示标记@shouldClose=true
	2. 获取socket中的证实数据
		val clientSecret = readUtf8(s)
	3. 判断证实数据的合法性
		secret == clientSecret ? 
		{writeUtf8("ok", s) shouldClose=false} :
		writeUtf8("err", s) throw new IllegalArgumentException("Authentication failed.")
	4. 设置验证时间
		s.setSoTimeout(currentTimeout)
	5. 需要的情况下，关闭socket
		shouldClose ? JavaUtils.closeQuietly(s) : Nop
	
	def authToServer(s: Socket): Unit
	功能: 使用服务端验证.通过检查权限密码和检验服务端相应(reply)
	输入参数:
		s	连接到服务端的socket
	1. 设置指示关闭标记shouldClose=true
	2. 将权限密码写出
		writeUtf8(secret, s)
	3. 从客户端收取相应
		val reply = readUtf8(s)
	4. 确定相应
		reply != "ok" ? throw new IllegalArgumentException : shouldClose = false
	5. 需要情况下关闭socket
		shouldClose ? JavaUtils.closeQuietly(s) : Nop
		
	def readUtf8(s: Socket): String
	功能: 读取socket中的数据内容
	
	def writeUtf8(str: String, s: Socket): Unit 
	功能: 写入str到socket中
}
```

#### SocketAuthServer

```markdown
介绍:
	在JVM中创建一个服务器，用于和外部程序进行交互(Python R).主要用户处理批量数据，包括权限证实和错误处理。
	socket服务器仅能接受一个连接,如果连接在15s内依旧不存在，那么就要关闭连接。
```

```markdown
private[spark] abstract class SocketAuthServer[T] (authHelper: SocketAuthHelper,threadName: String){
	构造器属性:
		authHelper	权限辅助器
		threadName	线程名称
	属性:
	val promise = Promise[T]()
	初始化加载:
	def this(env: SparkEnv, threadName: String) = this(new SocketAuthHelper(env.conf), threadName)
	功能: 初始化spark环境@env,初始化线程名称
	
	def this(threadName: String) = this(SparkEnv.get, threadName)
	功能: 初始化线程名，初始化spark环境为默认
	
	服务器信息
	val (port, secret) = startServer()
	1. 创建服务端socket
		val serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))
	2. 设定最大延时时间(无连接时间)
		serverSocket.setSoTimeout(15000)
	3. 设置线程处理数据，和身份验证工作
	    new Thread(threadName) {
          setDaemon(true)
          override def run(): Unit = {
            var sock: Socket = null
            try {
              sock = serverSocket.accept()
              authHelper.authClient(sock)	// 身份验证
              promise.complete(Try(handleConnection(sock))) // 处理连接
            } finally {
              JavaUtils.closeQuietly(serverSocket)
              JavaUtils.closeQuietly(sock)
            }
          }
    	}.start()
    4. 返回服务器信息
    val= (serverSocket.getLocalPort, authHelper.secret)
    
	操作集:
	def startServer(): (Int, String) 
	功能: 启动服务器
	
	def handleConnection(sock: Socket): T
	功能: 处理一个已经被授权的连接，任意错误存在与方法中，都会清空连接和整个服务器，且会传播给@getResult
	
	def getResult(): T = {getResult(Duration.Inf)}
	功能: 无限期的阻塞直到@handleConnection完成，且会返回那个结果。如果@handleConnection抛出异常，那么这里	也会抛出异常
	
	def getResult(wait: Duration): T ={ThreadUtils.awaitResult(promise.future, wait)}
	功能: 等待@promise的线程，最大等待时间为wait
} 
```

```markdown
private[spark] class SocketFuncServer(authHelper: SocketAuthHelper,threadName: String,
    func: Socket => Unit){
    介绍: 创建一个socket服务器，在后台线程上运行用户功能。这个线程可以读写socket输入输出。函数会被传送给一个已	经连接且授权的socket。
	关系: father --> SocketAuthServer[Unit](authHelper, threadName)
    操作集:
    def handleConnection(sock: Socket): Unit ={func(sock) }
    功能: 处理连接
}
```

```markdown
private[spark] object SocketAuthServer {
	操作集:
	def serveToStream(threadName: String,authHelper: SocketAuthHelper)
		(writeFunc: OutputStream => Unit): Array[Any]
	功能: 是一个创建socket的便利途径,在后台线程中运行用户函数去写输出流
		这个socket服务器，仅接受一个连接。如果超过15s不连接则会关闭。
	输入参数:
		threadName	后台服务线程名称
		authHelper	用于证实的辅助类@SocketAuthHelper
		writeFunc	写函数(写输出的用户函数)
	1. 获取处理函数
	val handleFunc = (sock: Socket) => {
		val out = new BufferedOutputStream(sock.getOutputStream())
      	 Utils.tryWithSafeFinally {
        	writeFunc(out)
      	} {
        	out.close()
      	}
	}
	2. 获取socket服务器
	val server = new SocketFuncServer(authHelper, threadName, handleFunc)
	3. 获取socket服务器信息，并返回
	val =  Array(server.port, server.secret, server)
}
```

#### 基础拓展

1.  证书,令牌与安全密钥
2.  加密算法 -- HmacSHA1
3.   加密算法-- AES