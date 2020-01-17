## **spark-serilizer**

---

1.  [GenericAvroSerializer.scala](# GenericAvroSerializer)
2.  [JavaSerializer.scala](# JavaSerializer)
3.  [KryoSerializer](# KryoSerializer)
4.  [SerializationDebugger](# SerializationDebugger)
5.  [Serializer](# Serializer)
6.  [SerializerManager](# SerializerManager)

---

#### GenericAvroSerializer

```markdown
介绍:
	为Avro记录订制的序列化方案。如果用户在时间之前注册schema，然后schema的副本(指纹...fingerprint)会随着每条消息发送,而不是发送schema本体。目的是减少网络IO。
	诸如转化(parsing)和压缩(compression)schema的行动从计算方面来说是昂贵的。使得序列化缓存语言被视作values，这样可以降低需要做的工作。
```

```markdown
private[serializer] class GenericAvroSerializer(schemas: Map[Long, String]){
	关系: father --> KSerializer[GenericRecord]
    构造器属性:
    	schemas:schema信息(一个Id与Avro Schema的映射关系，value为Schema的String表示，用于减轻描述复杂度)
    属性:
    // 用于减少压缩schema的时间参数
    #name @compressCache #type @mutable.HashMap[Schema, Array[Byte]]	压缩缓存
    #name @decompressCache	#type @mutable.HashMap[ByteBuffer, Schema]	解压缓存
    // 重用数据读/写器,因为同样schema的会使用很多次
    #name @writerCache #type @mutable.HashMap[Schema, DatumWriter[_]]	写出缓存
    #name @readerCache #type @mutable.HashMap[Schema, DatumReader[_]]	读取缓存
    // 减轻制造副本的开销参数
    #name @fingerprintCache #type @mutable.HashMap[Schema, Long]	副本缓存
    #name @schemaCache #type @mutable.HashMap[Long, Schema]			schema缓存
   	#name @codec #type @CompressionCodec val					压缩方式
   		通用Avro序列化器,不能采用sparkConf在构造器中。且他会作为@KryoSerializer序列化器中的一部分。这就会使	得@KryoSerializer无法序列化。在这里使用懒加载用于一些单元测试。
     
     操作集：
     def compress(schema: Schema): Array[Byte] 
     功能: 压缩schema
     	当schema通过电报发送时，压缩结果会被存储起来，用于减少压缩的次数。因为一个schema可能会重复很多次
     1. 获取schema对应的数据@Array[Byte]
     2. 如果当前value不存在，则插入一个键值对进去
     	val bos = new ByteArrayOutputStream()
   		val out = codec.compressedOutputStream(bos)
   		此时需要将schema信息写出去(标准输出流写出)
   			out.write(schema.toString.getBytes(StandardCharsets.UTF_8))
     	其中返回值:value=bos.toByteArray
     3. 如果schema存在，直接返回value
     	详情参照@mutable.hashMap #method @getOrElseUpdate 的使用方法
     
     def decompress(schemaBytes: ByteBuffer): Schema 
     功能: 解压schema,使其成为实际的内存对象.需要保证内部缓存以及可以作为value访问,用于限制解压需要操作的次	数
     1. 从解压缓存@decompressCache中获取schema对应的信息#class @mutable.hashMap #method @getOrElseUpdate
     + 如果存在 返回value
     + 如果不存在这个key 流式读取一条压缩完成的数据，并将其转化为@Schema
     	new Schema.Parser().parse(new String(bytes, StandardCharsets.UTF_8))
     
     def serializeDatum[R <: GenericRecord](datum: R, output: KryoOutput): Unit 
     功能: 序列化一条记录，比将其只有指定输出流中。它缓存了很多外部数据，主要用于不去重做重复的工作
     输入参数: 
     	datum	记录数据
     	output	kryo序列化流
     1. 获取副本信息
     	+ 获取输入对应的schema
     	val schema = datum.getSchema	
     	+ 从副本缓存中获取当前schema的副本
     	val fingerprint = fingerprintCache.getOrElseUpdate(schema, {
      		SchemaNormalization.parsingFingerprint64(schema)
    	})
     2. 处理副本数据
     	+ 本类中含有输入的schema信息:
     		写出: 在内部查找到标记为true，并写出查找到的副本信息
     		output.writeBoolean(true)
        	output.writeLong(fingerprint)
        + 本类中不包含输入的schema信息:
     		写出内部没有查找到状态false
     		   output.writeBoolean(false)
     		获取输入schema的压缩对象，并写出相关元数据信息
                val compressedSchema = compress(schema)
                output.writeInt(compressedSchema.length)	// 指示压缩schema大小
                output.writeBytes(compressedSchema)
     3. 写缓存@writerCache写出记录信息
        writerCache.getOrElseUpdate(schema, GenericData.get.createDatumWriter(schema))
      		.asInstanceOf[DatumWriter[R]].write(datum, encoder)
     	encoder.flush()
     
     def deserializeDatum(input: KryoInput): GenericRecord 
     功能: 反序列化通用记录使其成为内存对象，存在有一个外部状态用于维护一个schema缓存和数据阅读器
     1. 获取schema
     	读取序列化输出的状态标记为flag=input.readBoolean()
     	+ 标记位flag=true
     		这里对应于序列化中找到副本情况，先获取副本key值
     		val fingerprint = input.readLong()
     		再从schema缓存中找到副本对应的schema，如果@fingerprint 下没有schema，则从本类存在的schema映射		@schemas中获取schema	
     	+ 标记位flag=false
     		这种情况对应于序列化中，找不到schema，新建副本写出的情况
     	先获取长度
     		val length = input.readInt()
     	根据长度指示读取数据，并解压
     		decompress(ByteBuffer.wrap(input.readBytes(length)))
     2. 读取缓存@readerCache中读取一条记录信息
     	获取解码信息
     	val decoder = DecoderFactory.get.directBinaryDecoder(input, null)
     	读取缓存根据decoder信息读取一条记录
     	readerCache.getOrElseUpdate(schema, GenericData.get.createDatumReader(schema))
      		.asInstanceOf[DatumReader[GenericRecord]].read(null, decoder)
     	
     def write(kryo: Kryo, output: KryoOutput, datum: GenericRecord): Unit 
     功能: 写出数据
     	@serializeDatum(datum, output)
     
     def read(kryo: Kryo, input: KryoInput, datumClass: Class[GenericRecord]): GenericRecord 
	功能: 读取数据
		@deserializeDatum(input)
}
```

#### JavaSerializer

```markdown
private[spark] class JavaSerializationStream(out: OutputStream, counterReset: Int,
	extraDebugInfo: Boolean){
	关系: father --> SerializationStream
	构造器属性:
		out				 流式输出
		counterReset	 重置计数器
		extraDebugInfo	 是否需要Debug信息标志
	属性:
		#name @objOut=new ObjectOutputStream(out) #type @ObjectOutputStream	对象输出流
		#name @counter=0	计数器
	操作集:
		def close(): Unit = { objOut.close() } 
		功能: 关流
		
		def flush(): Unit = { objOut.flush() }
		功能: 刷写到磁盘上
		
		def writeObject[T: ClassTag](t: T): SerializationStream 
		功能: 写入对象
		1. 写入成功更新计数器@counter+=1
		2. 满足条件: 重置次数@counterReset>0 且计数器@counter >= counterReset
		则重置输出流@objOut 以及计数器@counter=0
		至于重置的原因，是为了避免内存泄漏，请参照:
			<http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standrd-api>
}
```

```markdown
private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader){
	关系: father --> DeserializationStream
	构造器属性: 
		in		输入流
		loader	类加载器
	属性:
		#name @objIn #type @ObjectInputStream(in)	对象输入流
		
	操作集:
		def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
		功能: 读取对象
	
		def close(): Unit = { objIn.close() }
		功能: 关流
}
```

```markdown
private object JavaDeserializationStream{
	属性: 
	原始类型映射:
	val primitiveMappings = Map[String, Class[_]](
    	"boolean" -> classOf[Boolean],
    	"byte" -> classOf[Byte],
    	"char" -> classOf[Char],
    	"short" -> classOf[Short],
    	"int" -> classOf[Int],
    	"long" -> classOf[Long],
    	"float" -> classOf[Float],
    	"double" -> classOf[Double],
    	"void" -> classOf[Void]
  	)
}
```

```markdown
@DeveloperApi
class JavaSerializer(conf: SparkConf) {
	关系: father --> Serializer
		sibling --> Externalizable
	介绍: 使用java构建的spark序列化器
	注意: 这个序列化器不保证不同版本spark之间相互兼容。用于使用在单个spark应用程序中的序列化/反序列化器。
	构造器属性:
		conf	spark应用程序配置集
	属性:
		#name @counterReset=conf.get(SERIALIZER_OBJECT_STREAM_RESET) 	重置次数
		#name @extraDebugInfo=conf.get(SERIALIZER_EXTRA_DEBUG_INFO)		额外debug信息标志
	操作集:
		def newInstance(): SerializerInstance 
		功能: 获取一个新的序列化实例
		 val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    	 new JavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
	
    	def writeExternal(out: ObjectOutput): Unit 
    	功能: 外部写出
    	Utils.tryOrIOException {
    		out.writeInt(counterReset)
    		out.writeBoolean(extraDebugInfo)
  		}
  		
  		def readExternal(in: ObjectInput): Unit 
  		功能: 外部读取
  		Utils.tryOrIOException {
    		counterReset = in.readInt()
    		extraDebugInfo = in.readBoolean()
  		}
}
```

```markdown
private[spark] class JavaSerializerInstance(counterReset: Int, extraDebugInfo: Boolean, 				defaultClassLoader: ClassLoader){
	关系: father --> SerializerInstance
	操作集:
	def serialize[T: ClassTag](t: T): ByteBuffer 
	功能: 将输入T序列化称为@ByteBuffer
		val bos = new ByteBufferOutputStream()
    	val out = serializeStream(bos)
    	out.writeObject(t)
    	out.close()
    	bos.toByteBuffer
    	def deserialize[T: ClassTag](bytes: ByteBuffer): T 
    功能: 将@ByteBuffer反序列化为T
        val bis = new ByteBufferInputStream(bytes)
        val in = deserializeStream(bis)
        in.readObject()

    def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T
    功能: 指定类加载器，对输入进行反序列化
        val bis = new ByteBufferInputStream(bytes)
        val in = deserializeStream(bis, loader)
        in.readObject()
    
    def serializeStream(s: OutputStream): SerializationStream 
    功能: 对输入的输出流进行序列化，形成序列化流@SerializationStream
        val=new JavaSerializationStream(s, counterReset, extraDebugInfo)
    
    def deserializeStream(s: InputStream): DeserializationStream
    功能: 对输入的输入流进行反序列化
        val=new JavaDeserializationStream(s, defaultClassLoader)
    
    def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream 
    功能: 指定类加载器，反序列化输入流
        val= new JavaDeserializationStream(s, loader)
}
```

#### KryoSerializer

```markdown
介绍:
	这是spark的一种序列化器，kryo序列化类库请参照<https://code.google.com/p/kryo/>
	注意: 这个类不保证在不同版本的spark兼容。所以使用与单个spark程序中用于序列化/反序列化。
```

```markdown
class KryoSerializer(conf: SparkConf){
	关系: father --> org.apache.spark.serializer.Serializer
	sibling --> Logging  --> java.io.Serializable
	属性:
	#name @bufferSizeKb #type @int		缓冲大小(KB)
	#name @bufferSize=ByteUnit.KiB.toBytes(bufferSizeKb).toInt  缓冲大小
	#name @maxBufferSizeMb=conf.get(KRYO_SERIALIZER_MAX_BUFFER_SIZE).toInt 最大缓冲大小(MB)
    #name @maxBufferSize=ByteUnit.MiB.toBytes(maxBufferSizeMb).toInt 最大缓冲大小
    #name @referenceTracking=conf.get(KRYO_REFERENCE_TRACKING) #type @Boolean 参考追踪标志
    #name @registrationRequired=conf.get(KRYO_REGISTRATION_REQUIRED) #type @Boolean	是否需要注册
    #name @userRegistrators=conf.get(KRYO_USER_REGISTRATORS) 	注册用户列表
	#name @classesToRegister=conf.get(KRYO_CLASSES_TO_REGISTER)	需要注册的类
	#name @avroSchemas=conf.getAvroSchema #type @Map[Long, String] 	Avro的schema
	#name @useUnsafe=conf.get(KRYO_USE_UNSAFE) #type @Boolean 是否使用不安全的IO用于序列化
	#name @usePool=conf.get(KRYO_USE_POOL) @Boolean	是否使用池
	#name @factory=new KryoFactory() {override def create: Kryo = {newKryo()} transient	kryo工厂
	#name @internalPool=new PoolWrapper	#type @PoolWrapper	外部序列化池
	#name @supportsRelocationOfSerializedObjects @Boolean 是否支持重定位序列化对象
		val= newInstance().asInstanceOf[KryoSerializerInstance].getAutoReset()
	操作集:
	def newKryoOutput(): KryoOutput
	功能: 新建kryo序列化输出(#class @Output)
	val= if(useUnsafe) new KryoUnsafeOutput(bufferSize, math.max(bufferSize, maxBufferSize))
    	else new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))
    
    def setDefaultClassLoader(classLoader: ClassLoader): Serializer 
    功能: 设置默认类加载器，并重置外部序列化池@internalPool
    	super.setDefaultClassLoader(classLoader)
    	internalPool.reset()

    def newInstance(): SerializerInstance 
    功能: 获取一个序列化实例
    val= new KryoSerializerInstance(this, useUnsafe, usePool)
    
    def newKryo(): Kryo 
    功能: 新建一个序列化类@Kryo
    1. 获取keyo对象
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    2. kryo实例注册被已设置属性
    + 设定注册标志 
    	kryo.setRegistrationRequired(registrationRequired)
    + 获取类加载器
    	val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    + 注册kryo序列化器@KryoSerializer中需要注册的常用对象
        for (cls <- KryoSerializer.toRegister) {kryo.register(cls)}
    + 注册kryo序列化器中需要注册的序列化器
    	for ((cls, ser) <- KryoSerializer.toRegisterSerializer) {kryo.register(cls, ser)}
    + 注册java迭代器的类到序列化器中
    	kryo.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)
    + 运行使用java序列化器传输类
        kryo.register(classOf[SerializableWritable[_]], new KryoJavaSerializer())
    	kryo.register(classOf[SerializableConfiguration], new KryoJavaSerializer())
    	kryo.register(classOf[SerializableJobConf], new KryoJavaSerializer())
    	kryo.register(classOf[PythonBroadcast], new KryoJavaSerializer())
  		kryo.register(classOf[GenericRecord], new GenericAvroSerializer(avroSchemas))
    	kryo.register(classOf[GenericData.Record], new GenericAvroSerializer(avroSchemas))
  	+ 调用用户注册器时，加载默认的类加载器

    	```scala
    		Utils.withContextClassLoader(classLoader){
    		try {
    			// 注册需要注册的类列表
                	classesToRegister.foreach { className =>
          		kryo.register(Utils.classForName(className, noSparkClassLoader = true))}
                	// 注册用户需要注册的类列表
            userRegistrators
              .map(Utils.classForName[KryoRegistrator](_, noSparkClassLoader = true).
                getConstructor().newInstance())
              .foreach { reg => reg.registerClasses(kryo) }
          	 } catch {
        	case e: Exception =>
          	throw new SparkException(s"Failed to register classes with Kryo", e)
            		}
    		}
  	+ 提供零散的注册类型

    		kryo.register(classOf[Array[Tuplex[Any...(x个)]]])
    		共计22个的注册方式
  	+ 注册一些其他特殊类型的
    	kryo.register(None.getClass)
    	kryo.register(Nil.getClass)
    	kryo.register(Utils.classForName("scala.collection.immutable.$colon$colon"))
    	kryo.register(Utils.classForName("scala.collection.immutable.Map$EmptyMap$"))
    	kryo.register(classOf[ArrayBuffer[Any]])
  	+ 对于一些不能直接加载的类，为了避免jar依赖。在加载的时候，如果没有找到则忽略它
        KryoSerializer.loadableSparkClasses.foreach { clazz =>
            try {
                kryo.register(clazz)
                } catch {
                    case NonFatal(_) => // do nothing
                    case _: NoClassDefFoundError if Utils.isTesting => // See SPARK-23422.
                }
            }
        其中@loadableSparkClasses是一些spark特有的类
  	+ kryo设置类加载器为@classLoader，用于解决未注册类名的问题
    		kryo.setClassLoader(classLoader)
    }
}
```

```markdown
private[spark] class KryoSerializationStream(serInstance: KryoSerializerInstance,
	outStream: OutputStream,useUnsafe: Boolean){
	关系: father --> SerializationStream
    构造器属性:
    	serInstance	序列化实例
    	outStream	输出流
    	useUnsafe	是否安全(IO)
    属性:
    	#name @output #type @KryoOutput	kryo输出
    		val= if (useUnsafe) new KryoUnsafeOutput(outStream) else new KryoOutput(outStream)
    	#name @kryo=serInstance.borrowKryo() #type @Kryo kryo实例
    操作集:
    	def writeObject[T: ClassTag](t: T): SerializationStream
    	功能: 写对象T 
    	 kryo.writeClassAndObject(output, t)

    	 def flush(): Unit 
    	 功能: 将输出@output刷新到到磁盘上
    	
    	def close(): Unit 
    	功能： 关流
    	操作条件: 输出对象存在
} 
```

```markdown
private[spark] class KryoDeserializationStream(serInstance: KryoSerializerInstance,
    inStream: InputStream,useUnsafe: Boolean){
	关系： father --> DeserializationStream
    构造器属性:
    	serInstance		kryo序列化实例
    	inStream		输入流
    	useUnsafe		是否安全(IO)
	属性:
	#name @input #type @KryoInput 	kryo输入
	val= if (useUnsafe) new KryoUnsafeInput(inStream) else new KryoInput(inStream)
	#name @kryo=serInstance.borrowKryo() #type @Kryo 	kryo实例
	操作集:
	def readObject[T: ClassTag](): T 
	功能: 读取@input输入，并返回反序列化结果T
	val= kryo.readClassAndObject(input).asInstanceOf[T]
	
	def close(): Unit
	功能: 关流
	操作条件: 输入存在
} 
```

```markdown
private[spark] class KryoSerializerInstance(ks: KryoSerializer, useUnsafe: Boolean, usePool: Boolean){
	关系: father --> SerializerInstance
	构造器属性:
		ks	kryo序列化器
		useUnsafe	是否安全(IO)
		usePool		是否使用了序列化池	
	属性:
		#name @cachedKryo=if (usePool) null else borrowKryo() #type @Kryo	缓存kryo实例
		这是一个可以重用的kryo实例。方法会通过@borrowKryo()借用一个实例。借完之后进行它的活动。活动完毕，调		用@releaseKryo()释放实例。逻辑上来说，这是一个长度为1的缓存池。序列化实例不是线程安全的，假定在获取这个		 属性没有进行同步。
		// 输入输出为懒加载属性，避免在使用之前创建buffer
		#name @output #type @KryoOutput		kryo输出
		#name @input=if (useUnsafe) new KryoUnsafeInput() else new KryoInput() #type @Input	kryo输入
	操作集:
	private[serializer] def borrowKryo(): Kryo 
	功能: 借用一个kryo实例
	根据是否使用了序列化缓存池，分为如下两种情况
	1. 使用缓存池
		val kryo = ks.pool.borrow() // 从缓存池中获取一个实例
      	kryo.reset() //	重置kryo中的未注册类名
      	val= kryo
	2. 没有使用缓存池
		从临时缓存池@cachedKryo中获取原始(cachedKryo != null)
		val kryo = cachedKryo
		kryo.reset()
		cachedKryo = null
		val= kryo
	3. 其他情况
		这种无法从缓存池中获取的情况，需要自己新建
		val= ks.newKryo()
	
	private[serializer] def releaseKryo(kryo: Kryo): Unit
	功能: 是否kryo实例
	1. 存在缓存池
		归还到缓存池中
		ks.pool.release(kryo)
	2. 不存在缓存池
		归还到临时缓存@cachedKryo
		cachedKryo==null?cachedKryo = kryo
	
	def serialize[T: ClassTag](t: T): ByteBuffer 
	功能: 序列化T-->ByteBuffer
	1. 情况输出,借一个kryo实例
		output.clear()
		val kryo = borrowKryo()
	2. 使用kryo将对象t，写出到输出@output
		kryo.writeClassAndObject(output, t)
	3. 将kryo还给缓存池/临时缓存池
		releaseKryo(kryo)
	4. 返回输出形成的缓存数组
		val= ByteBuffer.wrap(output.toBytes)
		
	def deserialize[T: ClassTag](bytes: ByteBuffer): T
	功能: 反序列化ByteBuffer --> T
	1. 先借用一个实例
		val kryo = borrowKryo()
	2. 读取缓冲数组中的数据,并将其转化为对象
	  	+ 读取缓存/流式读取数据到输入@input中
	  	if (bytes.hasArray) {
	    	input.setBuffer(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining())
	  	} else {
	    	input.setBuffer(new Array[Byte](4096))
	    	input.setInputStream(new ByteBufferInputStream(bytes))
	  	}
	  	+ 将输入中的数据反序列化为T
	  	val= kryo.readClassAndObject(input).asInstanceOf[T]
	3. 将实例还给缓存池
		releaseKryo(kryo)
		
	def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T
	功能: 指定类加载器@loader对缓冲数组的反序列化
	1. 借用实例
		val kryo = borrowKryo()
		val oldClassLoader = kryo.getClassLoader // 获取旧的类加载器(kryo的加载器)
	2. 读取缓冲数组中的数据，并将读取到的数据反序列化为T
		+ 指定工作的类加载器
		kryo.setClassLoader(loader)
		+ 读取数据到input
		if (bytes.hasArray) {
	    	input.setBuffer(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining())
	  	} else {
	    	input.setBuffer(new Array[Byte](4096))
	    	input.setInputStream(new ByteBufferInputStream(bytes))
	  	}
	  	+ 将输入中的数据反序列化为T
	  	kryo.readClassAndObject(input).asInstanceOf[T]
	3. 归还实例
		kryo.setClassLoader(oldClassLoader) // 重设类加载器为kryo的加载器
	  	releaseKryo(kryo)
	
	def serializeStream(s: OutputStream): SerializationStream 
	功能: 流式序列化
	val= new KryoSerializationStream(this, s, useUnsafe)
	
	def deserializeStream(s: InputStream): DeserializationStream
	功能: 流式反序列化
	val= new KryoDeserializationStream(this, s, useUnsafe)
	
	def getAutoReset(): Boolean
	功能: 获取自动重置标记
	1. 获取自动重置属性
		val field = classOf[Kryo].getDeclaredField("autoReset")
		field.setAccessible(true)
	2. 借用一个实例
		val kryo = borrowKryo()
	3. 获取实例的自动重置属性值，并返回
		val= field.get(kryo).asInstanceOf[Boolean]
	4. 归还实例
}
```

```markdown
private[spark] class KryoInputObjectInputBridge(kryo: Kryo, input: KryoInput){
	关系: father --> FilterInputStream(input)
		sibling --> ObjectInput
	介绍: 这个桥类是用来包装KryoInput，使其作为InputStream以及ObjectInput的特性。使得KryoInput具有
    @InputStream以及@ObjectInput的方法。方便使用这个类的方法。
    构造器属性:
    	kryo	kryo实例
    	input	kryo输入
    操作集:
    def readLong(): Long = input.readLong()
    功能: 读取long型数据
    
    def readChar(): Char = input.readChar()
    功能: 读取char型数据
    
    def readFloat(): Float = input.readFloat()
    功能: 读取float型数据
    
    def readByte(): Byte = input.readByte()
    功能: 读取字节型数据
    
    def readShort(): Short = input.readShort()
    功能: 读取short型数据
    
    def readInt(): Int = input.readInt()
    功能: 读取int
    
    def readUnsignedShort(): Int = input.readShortUnsigned()
    功能: 读取无符号short数据
    
    def skipBytes(n: Int): Int = {input.skip(n) n}
    功能: 跳读n位
    
    def readFully(b: Array[Byte]): Unit = input.read(b)
    功能: 读取一个字节数组的数据
    
    def readFully(b: Array[Byte], off: Int, len: Int): Unit = input.read(b, off, len)
    功能: 读取范围数据
    
    def readLine(): String = throw new UnsupportedOperationException("readLine")
    功能: 禁止读取一行数据
    
    def readBoolean(): Boolean = input.readBoolean()
    功能: 读取boolean数据
    
    def readUnsignedByte(): Int = input.readByteUnsigned()
    功能: 读取无符号byte数据
    
    def readDouble(): Double = input.readDouble()
    功能: 读取double型数据
    
    def readObject(): AnyRef = kryo.readClassAndObject(input)
    功能: 读取对象
}
```

```markdown
private[spark] class KryoOutputObjectOutputBridge(kryo: Kryo, output: KryoOutput){
	关系: father --> FilterOutputStream(output)
		sibling --> ObjectOutput
	介绍: 与@KryoOutputObjectInputBridge,此处不再详述
	操作集:
	def writeFloat(v: Float): Unit = output.writeFloat(v)
	def writeChars(s: String): Unit = throw new UnsupportedOperationException("writeChars")
	def writeDouble(v: Double): Unit = output.writeDouble(v)
	def writeUTF(s: String): Unit = output.writeString(s) 
	def writeShort(v: Int): Unit = output.writeShort(v)
	def writeInt(v: Int): Unit = output.writeInt(v)
	def writeBoolean(v: Boolean): Unit = output.writeBoolean(v)
	def write(b: Int): Unit = output.write(b)
	def write(b: Array[Byte]): Unit = output.write(b)
	def write(b: Array[Byte], off: Int, len: Int): Unit = output.write(b, off, len)
	def writeBytes(s: String): Unit = output.writeString(s)
	def writeChar(v: Int): Unit = output.writeChar(v.toChar)
	def writeLong(v: Long): Unit = output.writeLong(v)
	def writeByte(v: Int): Unit = output.writeByte(v)
	def writeObject(obj: AnyRef): Unit = kryo.writeClassAndObject(output, obj)
}
```

```markdown
private class JavaIterableWrapperSerializer{
	关系: father --> com.esotericsoftware.kryo.Serializer[java.lang.Iterable[_]]
	操作集:
	def write(kryo: Kryo, out: KryoOutput, obj: java.lang.Iterable[_]): Unit
	功能:  序列化obj
	1. 对象被包装，且简单的序列化底层scala迭代器对象
		(obj.getClass == wrapperClass && underlyingMethodOpt.isDefined)
		kryo.writeClassAndObject(out, underlyingMethodOpt.get.invoke(obj))
	2. 其他情况
		kryo.writeClassAndObject(out, obj)
	
	def read(kryo: Kryo, in: KryoInput, clz: Class[java.lang.Iterable[_]]):java.lang.Iterable[_]
	功能: 反序列化in
	kryo.readClassAndObject(in) match {
	  case scalaIterable: Iterable[_] => scalaIterable.asJava
	  case javaIterable: java.lang.Iterable[_] => javaIterable
	}
}
```

```markdown
private object JavaIterableWrapperSerializer{
	关系: father --> Logging
	介绍: java迭代器包装序列化器
	属性:
	#name @wrapperClass #type @Class<?> 	包装类
	#name @underlyingMethodOpt 底层方法
		val= Some(wrapperClass.getDeclaredMethod("underlying")) or None
}
```

```markdown
trait KryoRegistrator {
  	操作集:
  	def registerClasses(kryo: Kryo): Unit
	功能: 使用kryo序列化器注册类
}
```

```markdown
private[serializer] object KryoSerializer {
	属性:
	#name @toRegister 通用注册列表
	val=seq(
	    ByteBuffer.allocate(1).getClass,
        classOf[StorageLevel],
        classOf[CompressedMapStatus],
        classOf[HighlyCompressedMapStatus],
        classOf[CompactBuffer[_]],
        classOf[BlockManagerId],
        classOf[Array[Boolean]],
        classOf[Array[Byte]],
        classOf[Array[Short]],
        classOf[Array[Int]],
        classOf[Array[Long]],
        classOf[Array[Float]],
        classOf[Array[Double]],
        classOf[Array[Char]],
        classOf[Array[String]],
        classOf[Array[Array[String]]],
        classOf[BoundedPriorityQueue[_]],
        classOf[SparkConf],
        classOf[TaskCommitMessage]
	)
	
	#name @loadableSparkClasses lazy	特殊注册类列表(主要分布于sql,ML,MLLib中)
	val=seq(
	  "org.apache.spark.sql.catalyst.expressions.UnsafeRow",
      "org.apache.spark.sql.catalyst.expressions.UnsafeArrayData",
      "org.apache.spark.sql.catalyst.expressions.UnsafeMapData",
      "org.apache.spark.ml.attribute.Attribute",
      "org.apache.spark.ml.attribute.AttributeGroup",
      "org.apache.spark.ml.attribute.BinaryAttribute",
      "org.apache.spark.ml.attribute.NominalAttribute",
      "org.apache.spark.ml.attribute.NumericAttribute",
      "org.apache.spark.ml.feature.Instance",
      "org.apache.spark.ml.feature.LabeledPoint",
      "org.apache.spark.ml.feature.OffsetInstance",
      "org.apache.spark.ml.linalg.DenseMatrix",
      "org.apache.spark.ml.linalg.DenseVector",
      "org.apache.spark.ml.linalg.Matrix",
      "org.apache.spark.ml.linalg.SparseMatrix",
      "org.apache.spark.ml.linalg.SparseVector",
      "org.apache.spark.ml.linalg.Vector",
      "org.apache.spark.ml.stat.distribution.MultivariateGaussian",
      "org.apache.spark.ml.tree.impl.TreePoint",
      "org.apache.spark.mllib.clustering.VectorWithNorm",
      "org.apache.spark.mllib.linalg.DenseMatrix",
      "org.apache.spark.mllib.linalg.DenseVector",
      "org.apache.spark.mllib.linalg.Matrix",
      "org.apache.spark.mllib.linalg.SparseMatrix",
      "org.apache.spark.mllib.linalg.SparseVector",
      "org.apache.spark.mllib.linalg.Vector",
      "org.apache.spark.mllib.regression.LabeledPoint",
      "org.apache.spark.mllib.stat.distribution.MultivariateGaussian"
	)
	val.flatMap(name => Some[Class[_]](Utils.classForName(name)) or None)

	#name @toRegisterSerializer		需要注册的序列化器列表
	val= Map[Class[_], KryoClassSerializer[_]](
		classOf[RoaringBitmap] -> new KryoClassSerializer[RoaringBitmap]()	
	)
	其中@KryoClassSerializer 需要重写write和read方法
	def write(kryo: Kryo, output: KryoOutput, bitmap: RoaringBitmap): Unit
		bitmap.serialize(new KryoOutputObjectOutputBridge(kryo, output))
	def read(kryo: Kryo, input: KryoInput, cls: Class[RoaringBitmap]): RoaringBitmap
		val ret = new RoaringBitmap
        ret.deserialize(new KryoInputObjectInputBridge(kryo, input))
		return ret
}
```

#### SerializationDebugger

```markdown
private[spark] object SerializationDebugger{
	关系: father --> Logging
	属性: 
	#name @enableDebugging #type @Boolean	允许debugger标志位
	#name @reflect #type @ObjectStreamClassReflection	对象流类反射
	操作集:
	def improveException(obj: Any, e: NotSerializableException): NotSerializableException 
	功能: 提升@NotSerializableException的功能,使之带有从问题对象引来的序列化路径.如果JVM的
	@sun.io.serialization.extendedDebugInfo flag标记开启,这个功能将会自动关闭
	
	private[serializer] def find(obj: Any): List[String] 
	功能: 查找导向非序列化对象	
		这个功能暂时不能处理写对象的重新问题,但是其不能太过复杂
	val = new SerializationDebugger().visit(obj, List.empty)
    
    def findObjectAndDescriptor(o: Object): (Object, ObjectStreamClass) 
    功能: 查找对象及其描述
    查找对象并序列化,将其与@ObjectStreamClass 组合成键值对,
    1. 这种方法处理序列化中存在写替代问题.其开始于对象本身,一直调用@writeReplace方法直到没有对象可以被调用.
      val replaced = desc.invokeWriteReplace(o)
      if (replaced.getClass == o.getClass) (replaced, desc)
      else findObjectAndDescriptor(replaced)
    2. 对象中不存在写替代问题
    	val= (o, desc)
    	val desc = ObjectStreamClass.lookupAny(o.getClass)
}
```
```markdown
private class ListObjectOutputStream{
	关系: father --> ObjectOutputStream(new NullOutputStream)
	介绍: 这是一个假的@ObjectOutput,可以简单的保存由外部写入的对象列表
	属性:
		#name @output #type @mutable.ArrayBuffer[Any]	输出
	初始化操作:
		this.enableReplaceObject(true) // 允许对象替换
	操作集:
		def outputArray: Array[Any] = output.toArray
		功能: 获取输出内容
		
		def replaceObject(obj: Object): Object={ output += obj obj }
		功能: 替换对象
}
```
```markdown
  private class NullOutputStream extends ObjectOutput{
  	介绍: 模拟/dev/null的输出流
    def write(b: Int): Unit = { }
  }
```
```markdown
private class ObjectStreamClassReflection{
	属性:
	val GetClassDataLayout: Method ={
	    val f = classOf[ObjectStreamClass].getDeclaredMethod("getClassDataLayout")
      	f.setAccessible(true)
      	f
	}
	获取getClassDataLayout()方法
	
	val HasWriteObjectMethod: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("hasWriteObjectMethod")
      f.setAccessible(true)
      f
    }
    获取hasWriteObjectMethod()方法
    
    val HasWriteReplaceMethod: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("hasWriteReplaceMethod")
      f.setAccessible(true)
      f
    }
    获取hasWriteReplaceMethod()方法
    
    val InvokeWriteReplace: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("invokeWriteReplace", classOf[Object])
      f.setAccessible(true)
      f
    }
    获取invokeWriteReplace()方法
    
    val GetNumObjFields: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("getNumObjFields")
      f.setAccessible(true)
      f
    }
    获取getNumObjFields()方法
    
    val GetObjFieldValues: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod(
        "getObjFieldValues", classOf[Object], classOf[Array[Object]])
      f.setAccessible(true)
      f
    }
    获取getObjFieldValues()方法
    
     val DescField: Field = {
      val f = Class.forName("java.io.ObjectStreamClass$ClassDataSlot").getDeclaredField("desc")
      f.setAccessible(true)
      f
    }
    获取属性desc
}
```
```markdown
implicit class ObjectStreamClassMethods(val desc: ObjectStreamClass){
 	关系: father --> AnyVal
 	操作集:
 	def hasWriteObjectMethod: Boolean
 	功能: 确定是否有写对象的方法
 	val = reflect.HasWriteObjectMethod.invoke(desc).asInstanceOf[Boolean]
 	
 	def hasWriteReplaceMethod: Boolean
 	功能: 确定是否有替换方法
 	reflect.HasWriteReplaceMethod.invoke(desc).asInstanceOf[Boolean]
 	
 	def invokeWriteReplace(obj: Object): Object
 	功能: 调用写替代
 	val= reflect.InvokeWriteReplace.invoke(desc, obj)
 	
 	def getNumObjFields: Int 
 	功能: 获取对象属性数量
 	val= reflect.GetNumObjFields.invoke(desc).asInstanceOf[Int]
 	
 	def getObjFieldValues(obj: Object, out: Array[Object]): Unit
 	功能: 获取属性值
 	val= reflect.GetObjFieldValues.invoke(desc, obj, out)
 }
```
```markdown
private class ListObjectOutput{
	关系: father --> ObjectOutput
	介绍: 这是一个伪@ObjectOutput,用于简单的保存外部写入的数据
	属性:
		#name @output=new mutable.ArrayBuffer[Any]	输出
	操作集:
	def outputArray: Array[Any] = output.toArray
	功能: 获取操作集元素
	
	def writeObject(o: Any): Unit = output += o
	功能: 写对象o到输出中
	
	def flush(): Unit = {}
	功能: 空实现
	
	def write(i: Int): Unit = {}
	功能: 空实现
	
	def write(bytes: Array[Byte]): Unit = {}
	功能: 空实现
	
	def write(bytes: Array[Byte], i: Int, i1: Int): Unit = {}
	功能: 空实现
	
	def close(): Unit = {}
	功能: 空实现
	
	def writeFloat(v: Float): Unit = {}
	功能: 空实现
	
	def writeChars(s: String): Unit = {}
	功能: 空实现
	
	def writeDouble(v: Double): Unit = {}
	功能: 空实现
	
	def writeUTF(s: String): Unit = {}
	功能: 空实现
	
	def writeShort(i: Int): Unit = {}
	功能: 空实现
	
	def writeInt(i: Int): Unit = {}
	功能: 空实现
	
	def writeBoolean(b: Boolean): Unit = {}
	功能: 空实现
	
	def writeBytes(s: String): Unit = {}
	功能: 空实现
	
	def writeChar(i: Int): Unit = {}
	功能: 空实现
	
	def writeLong(l: Long): Unit = {}
	功能: 空实现
    
	def writeByte(i: Int): Unit = {}
	功能: 空实现
}
```

#### Serializer

```markdown
介绍:
	由于一些序列化类库时线程不安全的，这个类用于创建org.apache.spark.serializer.SerializerInstance对象，
	这个对象保证可以做到序列化，且在一个时刻只能被一个线程调用。
	对于这个特征，需要实现:
	1. 定义各个无参构造器，或者带有一个@SparkConf类型的构造器。两者都有的话，采取第二种构造器。
	2. java序列化器接口
	注意: 序列化器在不同版本spark之间可能不通用。用于单个spark程序内部比较合适。
```

```markdown
@DeveloperApi
abstract class Serializer{
	属性:
	#name @defaultClassLoader #type @Option[ClassLoader] volatile 默认类加载器
	操作集:
	def setDefaultClassLoader(classLoader: ClassLoader): Serializer
	功能: 设置默认类加载器 
	
	def newInstance(): SerializerInstance
	功能: 抽象方法，获取一个序列化实例
	
	@Private
  	private[spark] def supportsRelocationOfSerializedObjects: Boolean = false
	功能: 抽象方法，检查是否支持重定位序列化对象
	基本逻辑:
		序列化器支持序列化对象重定位时，返回true。当且仅当在序列化流输出中的序列化对象字节 等于 排序完成的这	些元素。进一步说明，满足如下关系:
	{{{
        serOut.open()
        position = 0
        serOut.write(obj1)
        serOut.flush()
        position = # of bytes written to stream so far
        obj1Bytes = output[0:position-1]
        serOut.write(obj2)
        serOut.flush()
        position2 = # of bytes written to stream so far
        obj2Bytes = output[position:position2-1]
        serIn.open([obj2bytes] concatenate [obj1bytes]) should return (obj2, obj1)
 	}}}	
}
```

```markdown
@DeveloperApi
@NotThreadSafe
abstract class SerializerInstance {
	操作集:
	def serialize[T: ClassTag](t: T): ByteBuffer
	功能: 序列化T-->ByteBuffer
	
	def deserialize[T: ClassTag](bytes: ByteBuffer): T
	功能: 反序列化 ByteBuffer --> T
	
	def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T
	功能: 指定类加载器的反序列化
	
	def serializeStream(s: OutputStream): SerializationStream
	功能: 流式序列化 --> SerializationStream
	
	 def deserializeStream(s: InputStream): DeserializationStream
	功能: 流式反序列化 --> DeserializationStream
}
```

```markdown
abstract class DeserializationStream{
	关系: father --> Closable
	操作集: 
	def readObject[T: ClassTag](): T
	功能: 抽象方法，用于第一读取对象
	
	def readKey[T: ClassTag](): T = readObject[T]()
	功能: 用于读取key
	val = readObject[T]()
	
	def readValue[T: ClassTag](): T = readObject[T]()
	功能: 用于读取value
	val = readObject[T]()
	
	override def close(): Unit
	功能: 关流(抽象方法)
	
	def asIterator: Iterator[Any] = new NextIterator[Any] 
	功能: 获取迭代器
	+ 设置获取迭代器下一个元素的方法
	override protected def getNext()={
		try {
        	readObject[Any]()
      	} catch {
        	case eof: EOFException =>
          	finished = true
          	null
      	}
	}
	+ 关流
	override protected def close(): Unit = {
      	DeserializationStream.this.close()
    }
	
	def asKeyValueIterator: Iterator[(Any, Any)] 
	功能: 获取kv迭代器
	+ 获取迭代器的一个元素
	 override protected def getNext() = {
      	try {
        	(readKey[Any](), readValue[Any]())
      	} catch {
        	case eof: EOFException =>
          		finished = true
          		null
      	}
    }
    + 关流
    override protected def close(): Unit = {
      DeserializationStream.this.close()
    }
}
```

#### SerializerManager

```markdown
介绍:
	这是一个用于给不同spark组件配置序列化，压缩，加密的组件。包括自动选择用于shuffle的@Serializer
```

```markdown
private[spark] class SerializerManager(defaultSerializer: Serializer,conf: SparkConf,
    encryptionKey: Option[Array[Byte]]){
	构造器属性:
    	defaultSerializer	默认序列化器
    	conf				spark配置集
    	encryptionKey		加密key列表
    属性:
    	#name @kryoSerializer=new KryoSerializer(conf)	kryo序列化器
    	#name @stringClassTag=implicitly[ClassTag[String]] 类标签名称
    	#name @primitiveAndPrimitiveArrayClassTags #type @Set[ClassTag[_]]	原始标签列表
            val =
            {
                  val primitiveClassTags = Set[ClassTag[_]](
                  ClassTag.Boolean,
                  ClassTag.Byte,
                  ClassTag.Char,
                  ClassTag.Double,
                  ClassTag.Float,
                  ClassTag.Int,
                  ClassTag.Long,
                  ClassTag.Null,
                  ClassTag.Short
                )
            	val arrayClassTags = primitiveClassTags.map(_.wrap)
            	primitiveClassTags ++ arrayClassTags
            }
         #name @compressBroadcast= conf.get(config.BROADCAST_COMPRESS) Boolean  是否压缩广播变量标志
		#name @compressShuffle=conf.get(config.SHUFFLE_COMPRESS)  是否压缩shuffle的输出
		#name @compressRdds=conf.get(config.RDD_COMPRESS)	是否压缩已经存储的RDD分区
		#name @compressShuffleSpill=conf.get(config.SHUFFLE_SPILL_COMPRESS)	
			是否压缩暂时溢写到磁盘的shuffle输出
		#name @codec= CompressionCodec.createCodec(conf) lazy 	压缩参数
			这里使用懒加载方式,由于这里期望延迟初始化的工作,知道使用这个参数时采取加载。原因是spark项目可能			在使用用户定义的第三方jar包，这个jar包会加载Executor.updateDependencies。一旦块管理器
		@BlockManager初始化，用户等级的jar包仍然未被加载，所有使用懒加载去优先加载用户级别的jar。
		#name @encryptionEnabled=encryptionKey.isDefined	加密使能状态位
	操作集:
		def setDefaultClassLoader(classLoader: ClassLoader): Unit
		功能: 设置默认类加载器
		
		def canUseKryo(ct: ClassTag[_]): Boolean 
		功能: 确定是否可以使用kryo序列化
		val = primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
		
		def getSerializer(ct: ClassTag[_], autoPick: Boolean): Serializer
		功能: 获取序列化器
		SPARK-18617: 在SPARK-13990中，feature不能使用在spark-streaming中。最坏的情况是: 工作在Reciver模			式下的job不能适当的运行在spark 2.x上了。这时候使用相关措施关闭streaming job的kryo auto pick是第一		步。
		val = autoPick && canUseKryo(ct) ? kryoSerializer : defaultSerializer
		
		def getSerializer(keyClassTag: ClassTag[_], valueClassTag: ClassTag[_]): Serializer
		功能: 获取序列化器(获取shuffle RDD的最优序列化器)
		val = canUseKryo(keyClassTag) && canUseKryo(valueClassTag)? kryoSerializer: defaultSerializer
        
        def shouldCompress(blockId: BlockId): Boolean 
        功能: 决策指定@blockId是否该压缩
        val = {
        	  	 blockId match {
                  case _: ShuffleBlockId => compressShuffle
                  case _: BroadcastBlockId => compressBroadcast
                  case _: RDDBlockId => compressRdds
                  case _: TempLocalBlockId => compressShuffleSpill
                  case _: TempShuffleBlockId => compressShuffle
                  case _: ShuffleBlockBatchId => compressShuffle
                  case _ => false
   			 	}
        	}
        
        def wrapStream(blockId: BlockId, s: InputStream): InputStream 
        功能: 包装输入流，且进行压缩和加密 (加密 --> 压缩 --> 包装)
        val = wrapForCompression(blockId, wrapForEncryption(s)) 
		
	   def wrapStream(blockId: BlockId, s: OutputStream): OutputStream
	   功能: 包装输出流，且进行压缩和加密 (加密 --> 压缩 --> 包装)
		val = wrapForCompression(blockId, wrapForEncryption(s))
		
	   def wrapForEncryption(s: InputStream): InputStream 
       功能: 在shuffle加密功能运行的情况下进行加密
       val = encryptionKey.map { key => CryptoStreamUtils.createCryptoInputStream(s, conf, key) }
      		.getOrElse(s)
	  
	   def wrapForEncryption(s: OutputStream): OutputStream 
	   功能: 在shuffle加密功能运行的情况下进行加密
	   val= encryptionKey.map { key => CryptoStreamUtils.createCryptoOutputStream(s, conf, key) }
      		.getOrElse(s)
	   
	   def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream
	   功能: 压缩输出流
	   val = if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
	   
	   def wrapForCompression(blockId: BlockId, s: InputStream): InputStream 
	   功能: 压缩输入流
	   val = if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
	   
	   def dataSerialize[T: ClassTag](blockId: BlockId,values: Iterator[T]): ChunkedByteBuffer
	   功能: 数据序列化 T --> ChunkedByteBuffer
	   val = dataSerializeWithExplicitClassTag(blockId, values, implicitly[ClassTag[T]])
	   
	   def dataSerializeWithExplicitClassTag(blockId: BlockId,values: Iterator[_],
      		classTag: ClassTag[_]): ChunkedByteBuffer
	   功能: 将指定的数据块序列化为ChunkedByteBuffer
	   1. 创建一个4M的块状字节缓冲输出流@bbos
	   = new ChunkedByteBufferOutputStream(1024 * 1024 * 4, ByteBuffer.allocate)
	   	val byteStream = new BufferedOutputStream(bbos)
       2. 设置自动拾取标志@autoPick= !blockId.isInstanceOf[StreamBlockId]
       	即 当为streaming job时@autoPick=false，用以避免上面论述的bug问题。
       3. 根据指定的@autoPick获取序列化器
       val ser = getSerializer(classTag, autoPick).newInstance()
       4. 流式序列化，并且将指定数据@values写出，写出完毕关闭流
       ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
       5. 返回以及序列化写出完毕后的@ChunkedByteBuffer
       val= bbos.toChunkedByteBuffer
       
       def dataDeserializeStream[T](blockId: BlockId,inputStream: InputStream)
      		(classTag: ClassTag[T]): Iterator[T]
	   功能: 数据流式反序列  inputStream --> Iterator[T]
	   1. 获取@autoPick标志,用以避免streaming job的bug
	       val stream = new BufferedInputStream(inputStream)
    		val autoPick = !blockId.isInstanceOf[StreamBlockId]
	   2. 获取序列化实例
	   		val= getSerializer(classTag, autoPick).newInstance()
	   	 在此基础上反序列化获得#method @deserializeStream #type @DeserializationStream	
	   	 	val= val.deserializeStream(wrapForCompression(blockId, stream))
	   	 获取反序列化后对象迭代器
	   	 	val= val.asIterator.asInstanceOf[Iterator[T]]
} 
```

#### 基础拓展

1.  注解@tailrec
2.   implicit关键字