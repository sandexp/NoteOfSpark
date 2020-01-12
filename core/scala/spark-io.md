## **spark-io**

---

这部分关于io主要时用于处理压缩的,不是处理数据读写方面的

1.  [CompressionCodec.scala](# CompressionCodec)
2.  [scala基础拓展](# scala基础拓展)

---

#### CompressionCodec

```markdown
介绍:
	压缩编码解码器@CompressionCodec 允许用户选择不同的压缩实现,用于块存储中.
	注意:
		一个编码解码器电报协议(wire protocol)在spark的不同版本中可能是不同的,可能会出现不兼容的情况.
		因此只要将其使用在单个spark应用程序中,用于内部的压缩使用.
```

```markdown
@DeveloperApi
trait CompressionCodec{
	def compressedOutputStream(s: OutputStream): OutputStream
		压缩输出流
		
	def compressedContinuousOutputStream(s: OutputStream): OutputStream 
		连续型压缩输出流
		
	def compressedInputStream(s: InputStream): InputStream
		压缩输入流
		
	def compressedContinuousInputStream(s: InputStream): InputStream 
		连续压缩输入流
}
```

```markdown
private[spark] object CompressionCodec {
	属性:
		#name @configKey=IO_COMPRESSION_CODEC.key	属性key值
		#name @FALLBACK_COMPRESSION_CODEC="snappy"	#type val 	后备编码类型
		#name @DEFAULT_COMPRESSION_CODEC="lz4"	#type val 默认编码类型
		#name @ALL_COMPRESSION_CODECS #type seq 全部编码列表
			=shortCompressionCodecNames.values.toSeq
		#name @shortCompressionCodecNames #type map val 压缩码简称
			lz4	lzf	snappy	zstd
			
	操作集:
	def getShortName(codecName: String): String 
	功能: 获取给定编码名称的简称版本(即查找@shortCompressionCodecNames)
	
	def getCodecName(conf: SparkConf): String 
	功能: 获取编码名称@IO_COMPRESSION_CODEC
	
	def supportsConcatenationOfSerializedStreams(codec: CompressionCodec): Boolean
	功能: 支持序列化流的连接
	val= codec.isInstanceOf[SnappyCompressionCodec] || 
		codec.isInstanceOf[LZFCompressionCodec] || 
		codec.isInstanceOf[LZ4CompressionCodec] || 
		codec.isInstanceOf[ZStdCompressionCodec]
		
	def createCodec(conf: SparkConf, codecName: String): CompressionCodec
	功能: 创建压缩编码器@CompressionCodec
	1. 获取编码器类 
		codeclass=shortCompressionCodecNames.getOrElse(codecName.toLowerCase(Locale.ROOT), codecName)
    2. 返回当前获取类的一个实例,如果引发了异常则返回None

	def createCodec(conf: SparkConf): CompressionCodec
   	功能: 获取由conf指定的压缩编码器@CompressionCodec
   		val=@createCodec(conf, getCodecName(conf))
}
```

```markdown
@DeveloperApi
class LZ4CompressionCodec(conf: SparkConf) {
	关系: father-> CompressionCodec
	介绍: lz4型编码方式,块大小@blockSize可以通过 @spark.io.compression.lz4.blockSize 设置
	注意: 
		1. 不同版本spark可能存在不兼容的情况,主要用于单个spark应用程序的内部压缩方式.
		2. SPARK-28102 : 
			如果lz4的JNI库(java Native Interface) 初始化失败,然后#method @fastestInstance()的调用引发退			回到非JNI的实现上.这就忽略了一个事实,就是JNI加载失败,所以重复的调用会引起JNI的重复尝试.而这种方式			  是很慢的,原因是它会从一个静态的同步的方法中抛出异常(涉及到资源的争抢).为了试图避免这个问题:
			对 @fastestInstance()的结果进行缓存,且调用自己.(两个工厂都是安全的@lz4Factory
			@xxHashFactory)
	属性:
	#name @lz4Factory #type @LZ4Factory lz4实例工厂
    #name @xxHashFactory #type @XXHashFactory 可选hash工厂(hash32 hash64)
    #name @defaultSeed=0x9747b28c #type @int LZ4BlockOutputStream的默认种子
    
    操作集:
    def compressedOutputStream(s: OutputStream): OutputStream 
    功能: 获取压缩输出流
    1. 块大小@blocksize=conf.get(IO_COMPRESSION_LZ4_BLOCKSIZE).toInt
    2. 设置同步刷新状态位syncFlush=false
    返回一个@LZ4BlockOutputStream 实例参数如下:
    new LZ4BlockOutputStream(s,blocksize,lz4Factory.fastCompressor(),
    	xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,syncFlush)
    采取hash32的方式,使用默认种子的hash32值作为校验码
    
    def compressedInputStream(s: InputStream): InputStream 
    功能: 获取压缩输入流
    1. 设置字节流连接状态位@disableConcatenationOfByteStream=false
    返回一个@LZ4BlockInputStream 实例,参数如下:
    LZ4BlockInputStream(s,lz4Factory.fastDecompressor(),
      xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,disableConcatenationOfByteStream)
}
```

```markdown
@DeveloperApi
class LZFCompressionCodec(conf: SparkConf){
	关系: father --> CompressionCodec
	介绍:
		块大小@blockSize可以通过 @org.apache.spark.io.CompressionCodec 
	操作集:
	def compressedOutputStream(s: OutputStream): OutputStream
	功能: 返回LZF模式下的压缩输出流
	返回 @LZFOutputStream 实例,且设置完成后刷新 @setFinishBlockOnFlush(true)
	
	def compressedInputStream(s: InputStream): InputStream 
	功能: 返回LZF模式下压缩输入流
	返回 @LZFInputStream 实例
}
```

```markdown
@DeveloperApi
class SnappyCompressionCodec(conf: SparkConf){
	关系: father--> CompressionCodec
	介绍: 块大小@blockSize可以通过 @spark.io.compression.snappy.blockSize
	属性:
		自动加载snappy的本地库版本#class @Snappy ,出现异常则抛出
	
	操作集:
	def compressedOutputStream(s: OutputStream): OutputStream 
	功能: 获取给定输出流s的压缩输出流
	返回 @SnappyOutputStream 实例,参数如下
		SnappyOutputStream(s, blockSize)
		blockSize = conf.get(IO_COMPRESSION_SNAPPY_BLOCKSIZE).toInt
	
    def compressedInputStream(s: InputStream): InputStream 
    功能: 获取给定输入流s的压缩输入流
    返回 @SnappyInputStream() 实例,参数如下:
    	SnappyInputStream(s)
}
```

```markdown
@DeveloperApi
class ZStdCompressionCodec(conf: SparkConf){
	关系: father --> CompressionCodec
	介绍: 是ZStandard压缩编码方式的实现,这种压缩方式详细请参照#link <http://facebook.github.io/zstd/>
	属性:
	#name @bufferSize=conf.get(IO_COMPRESSION_ZSTD_BUFFERSIZE).toInt #type @int	缓冲大小
	#name @level=conf.get(IO_COMPRESSION_ZSTD_LEVEL) 
		IO_COMPRESSION_ZSTD_LEVEL越大,会以更多的CPU和内存消耗,从而获得更好的压缩效果
	操作集:
	def compressedOutputStream(s: OutputStream): OutputStream 
	功能: 获取输出流s对应的zstd方式的压缩输出流
		使用缓冲流包装zstd输出流,避免过于频繁的JNI对小量数据的压缩.
		val=  new BufferedOutputStream  (new ZstdOutputStream(s, level), bufferSize)
	
	private[spark] def compressedContinuousOutputStream(s: OutputStream)
	功能: 获取连续压缩输出流
	SPARK-29322	:
		设置setCloseFrameOnFlush 位true防止连续输入流,
	val =BufferedOutputStream(new ZstdOutputStream(s, level).setCloseFrameOnFlush(true), bufferSize)
	
	def compressedInputStream(s: InputStream): InputStream
	功能: 获取输入流s对应的zstd方式的压缩输入流
		使用缓冲流包装zstd输入流,避免过于频繁的JNI对小量数据的压缩.
		val= new BufferedInputStream(new ZstdInputStream(s), bufferSize)
	def compressedContinuousInputStream(s: InputStream): InputStream
	功能: 创建连续输入流
	val= new BufferedInputStream(new ZstdInputStream(s).setContinuous(true), bufferSize)
}
```

#### scala基础拓展

1. 注解@DeveloperApi

   ```markdown
   介绍:
   	为开发者打造的,低级的不稳定的注解
   	在低级版本的spark中,@DeveloperApi可能不存在或者是以另一种状态存在
   ```

2.  lazy关键字

3.  工厂模式

   