## **spark-input**

---

1.  [FixedLengthBinaryInputFormat.scala](# FixedLengthBinaryInputFormat)
2.  [FixedLengthBinaryRecordReader](# FixedLengthBinaryRecordReader)
3.  [PortableDataStream](# PortableDataStream)
4.  [WholeTextFileInputFormat](# WholeTextFileInputFormat)
5. [WholeTextFileyRecordReader](# WholeTextFileyRecordReader) 
6.  [scala基础拓展](# scala基础拓展)

---

#### FixedLengthBinaryInputFormat

```markdown
介绍:
	这时一个用于读取和分割二进制文件的客户输入形式.这类二进制文件中包含有记录信息,且每个都是定长,这个定长的大小(size)通过设置Hadoop配置来指定.
```

```markdown
private[spark] object FixedLengthBinaryInputFormat{
	属性:
	1. 记录长度参数@RECORD_LENGTH_PROPERTY 
		val= org.apache.spark.input.FixedLengthBinaryInputFormat.recordLength
		通过Hadoop 的JobConfs设置记录长度参数
	操作集:
	def getRecordLength(context: JobContext): Int
	功能: 从hadoop conf中获取记录长度属性值
	val= context.getConfiguration.get(RECORD_LENGTH_PROPERTY).toInt
}
```

```markdown
private[spark] class FixedLengthBinaryInputFormat{
	关系: father --> FileInputFormat[LongWritable, BytesWritable]
		sibling --> Logging
	属性:
	#name @recordLength=-1 @int var
	操作集:
	def isSplitable(context: JobContext, filename: Path): Boolean
	功能: 判断文件名称为@filename @Path 是否可以分割
	1. 如果当前长度为初始长度@recordLength==-1,则获取MR中配置的记录长度:
		recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)
	2. 此时如果@recordLength<=0 则证明文件不可分割,返回false.反之返回true
	
	def computeSplitSize(blockSize: Long, minSize: Long, maxSize: Long): Long
	输出参数:
		blockSize 	块大小
		minSize		最小大小
		maxSize		最大大小
	功能: 计算分片大小,这里重写主要是要保证每个分片都要包含整数条记录.每个输入分片@InputSplit将转化为
	FixedLengthBinaryRecordReader,会开始于首个字节,且最后一个字节指向记录的最后一个字节
		1. 计算默认情况下(使用父类InputSplit计算)分片大小@defaultSize
		2. val=(defaultSize < recordLength)?recordLength.toLong:
			(Math.floor(defaultSize / recordLength) * recordLength).toLong
			默认大小大的话则会截取.小则取记录长度
	
	def createRecordReader(split: InputSplit, context: TaskAttemptContext)
      	: RecordReader[LongWritable, BytesWritable]
	功能: 创建定长二进制记录阅读器@FixedLengthBinaryRecordReader
	输入参数:
		split	输入分片
		context	任务请求上下文
	val= new FixedLengthBinaryRecordReader
}
```

#### FixedLengthBinaryRecordReader

```markdown
介绍:
	定长二进制记录阅读器由定长二进制文件输入格式@FixedLengthBinaryInputFormat返回.
		使用设置在定长二进制文件输入格式@FixedLengthBinaryInputFormat 中的记录长度,用于在某一时刻读取给定输	入@InputSplit的一条记录.
		每次调用@nextKeyValue()就会更新key和value,其中:
			key = record index [Long] 记录编号
			value = record	[BytesWritable]
```

```markdown
private[spark] class FixedLengthBinaryRecordReader{
	关系: father--> RecordReader[LongWritable, BytesWritable]
	属性:
	#name @splitStart=0 #type @var long			分片开始位置
	#name @splitEnd=0 #type @var Long			分片结束位置
	#name @currentPosition=0 #type @var Long	 当前位置指针
	#name @recordLength=0 #type @var Int			记录指针
	#name @fileInputStream=null #type @FSDataInputStream (分布式)文件输入流
	#name @recordKey=null #type @LongWritable		记录Key值
	#name @recordValue=null #type @BytesWritable	记录value值
	操作集:
	def close(): Unit
	功能: 关闭文件流@fileInputStream
	操作条件: 文件流@fileInputStream 存在
	
	def getCurrentKey: LongWritable 
	功能: 获取当前记录key值(记录位置编号)@recordKey
	
	def getCurrentValue: BytesWritable
	功能: 获取当前记录value值(记录本身)@recordValue
	
	def getProgress: Float 
	功能: 获取当前记录在分片中的记录条信息
	val=Math.min(((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0).toFloat
	
	def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit 
	功能: 初始化操作
	输入参数:
		inputSplit	输入分片
		context	任务请求上下文
	操作逻辑:
		1. 初始化分片开始@splitStart和分片结束@splitEnd
			splitStart = fileSplit.getStart
			splitEnd = splitStart + fileSplit.getLength
		2. 检查输入是否存在由压缩情况,以及初始化文件(分布式)输入流
			+ 检查是否存在由输入压缩
				val conf = context.getConfiguration
				val codec = new CompressionCodecFactory(conf).getCodec(file)
				如果codec存在,则之间抛出异常,原因为定长记录阅读器不支持读取压缩文件
			+ 初始化文件流相关信息
				recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)
				val fs = file.getFileSystem(conf)
				fileInputStream = fs.open(file)
		3. 当前阅读指针的定位
			+ 找到文件开始位置指针
			fileInputStream.seek(splitStart)
			+ 设置当前记录指针@currentPosition
			currentPosition = splitStart
            
	def nextKeyValue(): Boolean 
    功能: 确认是否有下一条记录
    操作逻辑:
    1. 根据初始化操作后值，设置@recordKey和@recordValue
    	recordKey = new LongWritable()
    	recordValue = new BytesWritable(new Array[Byte](recordLength))
    	且对于key来说是一个线性的long型数据，根据分块的思想，是一个块大小为recordLength(对应于value空间),编		号即key。所以:
    	recordKey.set(currentPosition / recordLength)
    2. 读取记录信息，并返回状态
    	能够读取到数据(currentPosition < splitEnd)
            读取数据
            fileInputStream.readFully(recordValue.getBytes) 
            更新指针
            currentPosition = currentPosition + recordLength
            
}
```

#### PortableDataStream

```markdown
介绍:
	这个类允许数据流序列化，且可以在不创建数据流的情况下保证数据的移动。除非它们需要被去读取。
```

```markdown
class PortableDataStream(isplit: CombineFileSplit,context: TaskAttemptContext,index: Integer){
	关系: father --> Serializable
	构造器属性:
		isplit  @CombineFileSplit	输入文件子集
		context	@TaskAttemptContext	任务请求上下文
		index @Integer 文件编号
	属性:
		#name @confBytes = baos.toByteArraym #type @byte[]		配置字节数组
			context.getConfiguration.write(new DataOutputStream(baos))
		#name @splitBytes = baos.toByteArraym #type @byte[]
			isplit.write(new DataOutputStream(baos))			分片字节数组
		#name @split #type @CombineFileSplit lazy transient		输入文件子集合(分片)
			val bias=new ByteArrayInputStream(splitBytes)
			nsplit.readFields(new DataInputStream(bais)) -->  分片读取数据
		#name @conf #type @Configuration lazy transient		配置信息
			从confBytes读取配置信息
			val bais = new ByteArrayInputStream(confBytes)
			nconf.readFields(new DataInputStream(bais))
		#name @path #type @String lazy transient 		获取index对应路径名称		
	操作集:
    	def open(): DataInputStream
    	功能: 打开index所对应的文件分片，用户读取完毕之后需要自行关流
    	
    	def toArray(): Array[Byte]
    	功能: 将文件读取，并转化为字节数组
    	
    	def getPath(): String=path
    	功能: 获取文件分片路径
    	
    	def getConfiguration: Configuration = conf
    	功能: 获取文件配置
}
```

```markdown
private[spark] abstract class StreamFileInputFormat[T]{
	关系: father --> CombineFileInputFormat[String,T]
	介绍: 将全文读取成流，字节数组或者其他添加的函数形式的全局格式，
	操作集:
	def isSplitable(context: JobContext, file: Path): Boolean = false
	功能: 确认当前文件是否可以分片，初始化为false
	
	def createRecordReader(split: InputSplit, taContext: TaskAttemptContext): RecordReader[String, T]
	功能: 创建记录阅读器，由子类实现
	
	 def setMinPartitions(sc: SparkContext, context: JobContext, minPartitions: Int): Unit 
	 功能: 使得用户设置的最小分区数@minPartitions入旧的hadoop API保持一致，这个通过@setMaxSplitSize实现
	 1. 计算默认参数
	 	默认最大分片大小@defaultMaxSplitBytes= sc.getConf.get(config.FILES_MAX_PARTITION_BYTES)
	 	读取字节消耗@openCostInBytes= sc.getConf.get(config.FILES_OPEN_COST_IN_BYTES)
	 	默认并行度@defaultParallelism= Math.max(sc.defaultParallelism, minPartitions)
	 2. 获取文件相关信息
	 	获取文件列表@files = listStatus(context).asScala
	 	获取文件总字节数
	 		@totalBytes= files.filterNot(_.isDirectory).map(_.getLen + openCostInBytes).sum
		计算单核字节数 @bytesPerCore = totalBytes / defaultParallelism
		计算最大分片大小 
			@maxSplitSize=Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
	3. 计算单个节点/齿条(rack)分片属性
		单节点分片最小大小:
		minSplitSizePerNode = jobConfig.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERNODE, 0L)
		单齿条(rack)分片最小属性
		minSplitSizePerRack = jobConfig.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERRACK, 0L)
	4. 设置分片大小参数
		设置单个节点最小分片大小:
		setMinSplitSizeNode(maxSplitSize)
		设置单个齿条(rack)最小分片大小:
		setMinSplitSizeRack(maxSplitSize)
		设置最大分片大小
		setMaxSplitSize(maxSplitSize)
}
```

```markdown
private[spark] class StreamRecordReader(split: CombineFileSplit,context: TaskAttemptContext,
    index: Integer){
	关系: father --> StreamBasedRecordReader[PortableDataStream](split,context,index)
     介绍: 这个类直接流式读取记录，供给其他对象操作处理
    操作集:
     def parseStream(inStream: PortableDataStream): PortableDataStream =instream
     功能: 转换流对象
}
```

```markdown
private[spark] class StreamInputFormat{
	关系: father --> StreamFileInputFormat[PortableDataStream]
	介绍: @PortableDataStream文件的格式
	操作集:
	 def createRecordReader(split: InputSplit, taContext: TaskAttemptContext)
    	: CombineFileRecordReader[String, PortableDataStream]
     功能: 创建记录阅读器@CombineFileRecordReader
     val= CombineFileRecordReader[String, PortableDataStream](
      		split.asInstanceOf[CombineFileSplit], taContext, classOf[StreamRecordReader])
}
```

```markdown
private[spark] abstract class StreamBasedRecordReader[T] (split: CombineFileSplit,
    context: TaskAttemptContext,index: Integer)
{
	关系: father --> RecordReader[String,T]
    属性:
    #name @processed=false 	文件处于处理中标志(false 则指示当前文件会跳过)
    #name @key 		key值
    #name @value	value值
    操作集:
    def initialize(split: InputSplit, context: TaskAttemptContext): Unit
    功能: 初始化
    
    def close(): Unit = {}
    功能: 关流
    
    def getProgress: Float 
    功能: 获取当前进度
    val = if (processed) 1.0f else 0.0f
    
    def getCurrentKey: String = key
    功能: 获取当前key值
    
    def getCurrentValue: T = value
    功能: 获取当前value值
    
    def parseStream(inStream: PortableDataStream): T
    功能: 转化当前流对象@PortableDataStream，返回value的值，其格式为T
    
    def nextKeyValue: Boolean
    功能: 获取下一个kv
    + processed为false
    	val fileIn = new PortableDataStream(split, context, index)
    	@value=@parseStream(fileIn)
	    @key=fileIn.getPath
		processed=true
	  return true
	+ processed为false
	  return false
}
```

#### WholeTextFileInputFormat

````markdown
介绍:
	一个org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat的处理全文读取的类。每个文件读取按照kv形式存储，key是文件路径，value是文件的全文。
````

```markdown
private[spark] class WholeTextFileInputFormat{
	关系: father --> CombineFileInputFormat[Text, Text]
		sibling --> Configurable
	操作集:
	def isSplitable(context: JobContext, file: Path): Boolean
    功能: 获取当前文件是否可以分片(split) 初始状态下为false
    
    def createRecordReader(split: InputSplit,
      context: TaskAttemptContext): RecordReader[Text, Text]
	功能: 创建记录阅读器@RecordReader
	创建一个CombineFileRecordReader类的记录阅读器
	val reader=ConfigurableCombineFileRecordReader(split, context,
    		classOf[WholeTextFileRecordReader])
	且将reader的conf信息设置为@getConf
	返回reader
	
	def setMinPartitions(context: JobContext, minPartitions: Int): Unit 
	功能: 设置最小的分区
		允许用户设置的最小分区，用于与旧版的Hadoop API保持一致，这是通过@setMaxSplitSize完成
	1. 获取文件的状态列表@files=listStatus(context).asScala
		获取文件夹下文件的数量@totalLen=
			files.map(file => if (file.isDirectory) 0L else file.getLen).sum
		获取最大分片数量@maxSplitSize
			= Math.ceil(totalLen * 1.0 /(if (minPartitions == 0) 1 else minPartitions)).toLong
	2. 对于小文件需要保证在每个节点/每个齿条(rack)上最小分片大小小于maxSplitSize
		+ 获取MR job的配置信息
		val config = context.getConfiguration
		+ 获取单个节点最小分片大小
		minSplitSizePerNode = config.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERNODE, 0L)
		+ 获取单个齿条(rack)最小分片大小
		minSplitSizePerRack = config.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERRACK, 0L)
		- 上述两个值，与@org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat中设置常数有关
	3. 设置单个节点/齿条(rack)上最大分片大小@maxSplitSize
		maxSplitSize < minSplitSizePerNode? super.setMinSplitSizeNode(maxSplitSize)
		maxSplitSize < minSplitSizePerRack? super.setMinSplitSizeRack(maxSplitSize)
	4. 设置全局最大分片大小
		super.setMaxSplitSize(maxSplitSize)
	具体设置，请参照hadoop源码中的分析，这里不做进一步分析。
}
```

#### WholeTextFileyRecordReader

```markdown
介绍:
	这个类主要是用于处理: 读取全文文件kv对的措施(@org.apache.hadoop.mapreduce.RecordReader) ,其中key为文件路径，value为文件全文。
```

```markdown
private[spark] class WholeTextFileRecordReader(split: CombineFileSplit,context: TaskAttemptContext,
    index: Integer)
{
	关系: father --> RecordReader[Text, Text] 
		sibling --> Configurable\
	构造器属性:
		split	文件分片
		context 任务请求上下文
		index 顺序编号
	属性:
		#name @path=split.getPath(index)	文件路径
		#name @fs= path.getFileSystem(context.getConfiguration)	文件系统
         #name @processed=false 处理标记(为true则表示已经处理过了，跳过对其处理)
         #name @key=new Text(path.toString) #type= @Text key值(文件名称)
         #name @value=null #type @Text value值	为文本内容初始化为null
	操作集:
		def initialize(split: InputSplit, context: TaskAttemptContext): Unit 
		功能: 根据指定输入分片@split 任务请求上下文@context
		
		def close(): Unit = {}
		功能: 关闭读取
		
		def getProgress: Float
		功能: 获取当前读取的进度条 
		val= if (processed) 1.0f else 0.0f
		
		def getCurrentKey: Text 
		功能: 获取key值 
		val=@key
		
		def getCurrentValue: Text
		功能: 获取value值
		val=@value
		
		def nextKeyValue(): Boolean 
		功能: 确定是否有下一个kv值
		操作条件: 处理标记为false才可以进行处理，否则跳过处理直接返回false
		读取时分为两种情况:
		innerBuffer= 
            1. 读取的内容经过了压缩
                ByteStreams.toByteArray(codec.createInputStream(fileIn))
            2. 读取过的内容没有压缩
                ByteStreams.toByteArray(fileIn)
		文本内容 value=new Text(innerBuffer)
		标记置位 processed = true
}
```

```markdown
private[spark] trait Configurable{
	关系: father --> org.apache.hadoop.conf.HConfigurable
	介绍: 这个特征实现了接口@org.apache.hadoop.conf.Configurable
	属性:
	#name @conf #type @Configuration	配置信息
    操作集:
    def setConf(c: Configuration): Unit
    功能: 设置配置信息@conf
    
    def getConf: Configuration = conf
    功能: 获取配置信息@conf
}
```

```markdown
private[spark] class ConfigurableCombineFileRecordReader[K, V] (split: InputSplit,
    context: TaskAttemptContext,recordReaderClass: Class[_ <: RecordReader[K, V] 
    with HConfigurable])){
	关系: 
	father --> CombineFileRecordReader[K,V](split.asInstanceOf[CombineFileSplit],context
	,recordReaderClass)
	sibling --> Configurable
	在这之中对Configurable进行配置:
	def initNextRecordReader(): Boolean 
	功能: 初始化记录阅读器
	```scala
        val r = super.initNextRecordReader()
        if (r) {
          if (getConf != null) {
            this.curReader.asInstanceOf[HConfigurable].setConf(getConf)
          }
        }
        r
```
	def setConf(c: Configuration): Unit 
	功能: 设置指定的configuration
	```scala
	    super.setConf(c)
	    if (this.curReader != null) {
	      this.curReader.asInstanceOf[HConfigurable].setConf(c)
	    }
	```
}

#### scala基础拓展

1.  @since注解