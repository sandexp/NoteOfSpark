## **spark-util**

---

1.  [collection](# collection)
2.  [io](# io)
3.  [logging](# logging)
4.  [random](# random)

---

#### collection

1.  [AppendOnlyMap.scala](# AppendOnlyMap)

2.  [BitSet.scala](# BitSet)

3.  [CompactBuffer.scala](# CompactBuffer)

4.  [ExternalAppendOnlyMap.scala](# ExternalAppendOnlyMap)

5.  [ExternalSorter.scala](# ExternalSorter)

6.  [MedianHeap.scala](# MedianHeap)

7.  [OpenHashMap.scala](# OpenHashMap)

8.  [OpenHashSet.scala](# OpenHashSet)

9. [PairsWriter.scala](# PairsWriter)

10.  [PartitionedAppendOnlyMap.scala](# PartitionedAppendOnlyMap)

11.  [PartitionedPairBuffer.scala](# PartitionedPairBuffer)

12.  [PrimitiveKeyOpenHashMap.scala](# PrimitiveKeyOpenHashMap)

13.  [PrimitiveVector.scala](# PrimitiveVector)

14.  [SizeTracker.scala](# SizeTracker)

15.  [SizeTrackingAppendOnlyMap.scala](# SizeTrackingAppendOnlyMap)

16.  [SizeTrackingVector.scala](# SizeTrackingVector)

17.  [SortDataFormat.scala](# SortDataFormat)

18.  [Sorter.scala](# Sorter)

19.  [Spillable.scala](# Spillable)

20.  [Utils.scala](# Utils)

21.  [WritablePartitionedPairCollection.scala](# WritablePartitionedPairCollection)

    ---

    #### AppendOnlyMap

    #### BitSet

    #### CompactBuffer

    #### ExternalAppendOnlyMap

    #### ExternalSorter

    #### MedianHeap

    #### OpenHashMap

    #### OpenHashSet

    #### PairsWriter

    ```scala
/*
    	对于kv对消费者的抽象,持久化分区的数据时首先被使用.尽管也是可以通过shuffle写插件或者盘块写出器
	@DiskBlockObjectWriter 来持久化.
    */
private[spark] trait PairsWriter {
    	操作集:
  	def write(key: Any, value: Any): Unit
        功能: 写出指定kv值
}
    ```

    #### PartitionedAppendOnlyMap

    #### PartitionedPairBuffer

    #### PrimitiveKeyOpenHashMap

    #### PrimitiveVector

    #### SizeTracker

    #### SizeTrackingAppendOnlyMap
    
    #### SizeTrackingVector
    
    #### SortDataFormat
    
    #### Sorter
    
    #### Spillable
    
    #### Utils
    
    #### WritablePartitionedPairCollection

#### io

1.  [ChunkedByteBuffer.scala](# ChunkedByteBuffer)

2.  [ChunkedByteBufferFileRegion.scala](# ChunkedByteBufferFileRegion)

3.  [ChunkedByteBufferOutputStream.scala](# ChunkedByteBufferOutputStream)

   ---

   #### ChunkedByteBuffer

   ```markdown
介绍:
   	只读形式的字节缓冲区,物理存储于多个数据块(chunk)中,而非简单的储存在连续的数组空间内存中.
   构造器参数:
   	chunk: 字节缓冲区@ByteBuffer 的数组,每个字节缓冲区读写指针开始都是0,这个缓冲区的使用权移交给
   	@ChunkedByteBuffer,所以如果这些缓冲区也可以用在其他地方,调用者需要将它们复制出来.
   ```
   
   ```scala
   private[spark] class ChunkedByteBuffer(var chunks: Array[ByteBuffer]) {
       属性:
       #name @bufferWriteChunkSize #Type @Int	写缓冲块大小
       val= Option(SparkEnv.get).map(_.conf.get(config.BUFFER_WRITE_CHUNK_SIZE))
         .getOrElse(config.BUFFER_WRITE_CHUNK_SIZE.defaultValue.get).toInt
       #name @disposed: Boolean = false	健康状态标志
       #name @size: Long = chunks.map(_.limit().asInstanceOf[Long]).sum	当前缓冲大小
       操作集:
       def writeFully(channel: WritableByteChannel): Unit
       功能: 将缓冲块中内容写入到指定通道@channel
       for (bytes <- getChunks()) {
         val originalLimit = bytes.limit()
         while (bytes.hasRemaining) {
           // 如果获取的数据@bytes时处于堆上,NIO API写出时会将其拷贝到直接字节缓冲区,这个临时的字节缓冲区每		// 个线程都会被缓存,它的大小随着输入字节缓冲区的扩大没有限制的话,会导致本地内存泄漏,如果不对大量的	   //  直接字节缓冲区的内存资源进行释放,这里需要写出定长的字节数量,去形成直接字节缓冲区,从而避免内存		   // 泄漏
           val ioSize = Math.min(bytes.remaining(), bufferWriteChunkSize)
           bytes.limit(bytes.position() + ioSize) // 限定缓冲区大小,避免内存泄漏
           channel.write(bytes)
           bytes.limit(originalLimit)
         }
       }
       
       def toNetty: ChunkedByteBufferFileRegion
       功能: 转换为netty传输(包装@FileRegion 使其能够传输超过2GB数据)
       val= new ChunkedByteBufferFileRegion(this, bufferWriteChunkSize)
       
       def toArray: Array[Byte] 
       功能: 将缓冲区内容拷贝到数组中
       if (size >= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
         throw new UnsupportedOperationException(
           s"cannot call toArray because buffer size ($size bytes) exceeds maximum array size")
       }
       val byteChannel = new ByteArrayWritableChannel(size.toInt)
       writeFully(byteChannel)
       byteChannel.close()
       byteChannel.getData
       
       def toByteBuffer: ByteBuffer
       功能: 将字节缓冲块转化为普通的字节缓冲区
       val= if (chunks.length == 1) {
         chunks.head.duplicate()
       } else {
         ByteBuffer.wrap(toArray)
       }
       
       def toInputStream(dispose: Boolean = false): InputStream
       功能: 使用输入流读取字节缓冲块的数据
       输入参数: 
       dispose 为true时,会在流式读取之后调用去关闭内存映射文件(这个会返回这个缓冲区)
       val= new ChunkedByteBufferInputStream(this, dispose)
       
       def getChunks(): Array[ByteBuffer]
       功能: 获取字节缓冲区列表副本
       val= chunks.map(_.duplicate())
       
       def dispose(): Unit
       功能: 清除任意一个直接字节缓冲区/内存映射文件缓冲区
       参考@StorageUtils.dispose 获取更多信息
       if (!disposed) {
         chunks.foreach(StorageUtils.dispose)
         disposed = true
       }
       
       def copy(allocator: Int => ByteBuffer): ChunkedByteBuffer
       功能: 拷贝一份
       val= val copiedChunks = getChunks().map { chunk =>
         val newChunk = allocator(chunk.limit())
         newChunk.put(chunk)
         newChunk.flip()
         newChunk
       }
       new ChunkedByteBuffer(copiedChunks)
   }
   ```
   
   ```scala
   private[spark] object ChunkedByteBuffer {
       def fromManagedBuffer(data: ManagedBuffer): ChunkedByteBuffer
       功能: 从管理缓冲区@data 获取字节缓冲块
       val= data match {
         case f: FileSegmentManagedBuffer =>
           fromFile(f.getFile, f.getOffset, f.getLength)
         case e: EncryptedManagedBuffer =>
           e.blockData.toChunkedByteBuffer(ByteBuffer.allocate _)
         case other =>
           new ChunkedByteBuffer(other.nioByteBuffer())
       }
       
       def fromFile(file: File): ChunkedByteBuffer = fromFile(file, 0, file.length())
       功能: 从文件获取缓冲数据块
       
       def fromFile(file: File,offset: Long,length: Long): ChunkedByteBuffer
   	功能: 从文件中获取缓冲数据块
       val is = new FileInputStream(file)
       ByteStreams.skipFully(is, offset)
       val in = new LimitedInputStream(is, length)
       val chunkSize = math.min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH, length).toInt
       val out = new ChunkedByteBufferOutputStream(chunkSize, ByteBuffer.allocate _)
       Utils.tryWithSafeFinally {
         IOUtils.copy(in, out)
       } {
         in.close()
         out.close()
       }
       val= out.toChunkedByteBuffer
   }
   ```
   
   ```scala
   private[spark] class ChunkedByteBufferInputStream(
       var chunkedByteBuffer: ChunkedByteBuffer,dispose: Boolean)
   extends InputStream{
       功能: 数据块输入流
       构造器参数
       chunkedByteBuffer	 缓冲数据块
       dispose		为true 表示流结束时关闭内存映射内存
       属性:
       #name @chunks = chunkedByteBuffer.getChunks().filter(_.hasRemaining).iterator
       	非空缓冲区列表
       #name @currentChunk: ByteBuffer	当前缓冲区
       val= if (chunks.hasNext) chunks.next() else null
       
       操作集:
       def read(): Int
       功能: 读取字节缓冲区的一个字节,并移动缓冲区内读写指针
       if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
         currentChunk = chunks.next()
       }
       val= if (currentChunk != null && currentChunk.hasRemaining) {
         UnsignedBytes.toInt(currentChunk.get())
       } else {
         close()
         -1
       }
       
       def read(dest: Array[Byte], offset: Int, length: Int): Int
       功能: 读取范围内部数据
       if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
         currentChunk = chunks.next()
       }
       val= if (currentChunk != null && currentChunk.hasRemaining) {
         val amountToGet = math.min(currentChunk.remaining(), length)
         currentChunk.get(dest, offset, amountToGet)
         amountToGet
       } else {
         close()
         -1
       }
       
       def skip(bytes: Long): Long
       功能: 跳读指定字节数量的数据@bytes,对应到底层就是移动读写指针,返回实际跳读的字节数量
       val = if (currentChunk != null) {
         val amountToSkip = math.min(bytes, currentChunk.remaining).toInt
         currentChunk.position(currentChunk.position() + amountToSkip)
         if (currentChunk.remaining() == 0) {
           if (chunks.hasNext) {
             currentChunk = chunks.next()
           } else {
             close()
           }
         }
         amountToSkip
       } else {
         0L
       }
       
       def close(): Unit
       功能: 关流
       if (chunkedByteBuffer != null && dispose) {
         chunkedByteBuffer.dispose() // 取消文件映射内存
       }
       chunkedByteBuffer = null
       chunks = null
       currentChunk = null
   }
   ```
   
   
   
   #### ChunkedByteBufferFileRegion
   
   ```markdown
   介绍:
   	已netty的文件区域来暴露一个块状字节缓冲区@ChunkedByteBuffer,一个netty消息仅仅允许发送超过2GB的数据.因为netty不能发送超过2GB大小的缓冲区数据,但是它可以发送一个大的文件区域@FileRegion,尽管这些数据不是由一个文件返回的.
   ```
   
   ```scala
   private[io] class ChunkedByteBufferFileRegion(
       private val chunkedByteBuffer: ChunkedByteBuffer,
       private val ioChunkSize: Int) extends AbstractFileRegion {
       构造器参数:
       chunkedByteBuffer	块状字节缓冲区
       ioChunkSize	io块大小
       属性:
       #name @_transferred: Long = 0	转换字节数量
       #name @chunks = chunkedByteBuffer.getChunks()	字节缓冲块
       #name @size = chunks.foldLeft(0L) { _ + _.remaining() }	字节缓冲块大小
       #name @currentChunkIdx = 0	当前块编号
       操作集:
       def deallocate: Unit = {}
       功能: 解除分配
       
       def count(): Long = size
       功能: 获取字节缓冲区大小
       
       def position(): Long = 0
       功能: 获取返回文件数据的起始指针,不是当读写指针
       
       def transferred(): Long = _transferred
       功能: 获取传输字节量
       
       def transferTo(target: WritableByteChannel, position: Long): Long
       操作条件: 转换的字节指针移动完毕 
       assert(position == _transferred)
       功能: 获取需要转换的字节数量
       if (position == size) return 0L // 全部转换完毕,需要转换数量为0
       1. 设置起始参数
       var keepGoing = true
       var written = 0L
       var currentChunk = chunks(currentChunkIdx)
       2. 读取数据
       while (keepGoing) {
         while (currentChunk.hasRemaining && keepGoing) {
           val ioSize = Math.min(currentChunk.remaining(), ioChunkSize) // 获取当前块数据剩余量
           val originalLimit = currentChunk.limit() // 获取初始状态下,缓冲区上界
           currentChunk.limit(currentChunk.position() + ioSize) 
           val thisWriteSize = target.write(currentChunk) // 写出当前块数据
           currentChunk.limit(originalLimit) // 移动上界位置为@originalLimit(因为写了这么多)
           written += thisWriteSize // 更新写出字节量
           if (thisWriteSize < ioSize) {// 没有写够指定字节数,不允许停止写
             keepGoing = false
           }
         }
         if (keepGoing) { // 写完一个数据缓冲区,,但是仍旧没有写够指定字节量的处理方案
           currentChunkIdx += 1 // 移动块编号指针
           if (currentChunkIdx == chunks.size) { //跳出条件设置(没有更多的数据块了)
             keepGoing = false
           } else {
             currentChunk = chunks(currentChunkIdx)// 设置数据块内容
           }
         }
       }
   }
   ```
   
   #### ChunkedByteBufferOutputStream
   
   ```markdown
   介绍:
   	输出流,用户写数据到定长数据块(chunk)中
   ```
   
   ```scala
   private[spark] class ChunkedByteBufferOutputStream(chunkSize: Int,
       allocator: Int => ByteBuffer) extends OutputStream {
       构造器参数: 
       	chunkSize	块大小
       	allocator	分配函数
       属性:
       #name @toChunkedByteBufferWasCalled = false	是否使用了块状字节缓冲区
       #name @chunks = new ArrayBuffer[ByteBuffer]	字节缓冲块
       #name @lastChunkIndex = -1	最后一块的块号
       #name @position = chunkSize	写指针的位置(处于上次使用块中,如果等于块大小,则需要分配一个新的块并置0)
       #name @_size = 0	数据流大小(字节量)
       #name @closed: Boolean = false	是否关闭标志
       操作集:
       def size: Long = _size
       功能: 获取数据流大小
       
       def close(): Unit
       功能: 关闭输出流
       if (!closed) {
         super.close()
         closed = true
       }
       
       def write(b: Int): Unit
       功能: 写入b到数据块@chunk中,并移动写指针,更新写出大小
       require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
       allocateNewChunkIfNeeded()
       chunks(lastChunkIndex).put(b.toByte)
       position += 1
       _size += 1
       
       def write(bytes: Array[Byte], off: Int, len: Int): Unit 
       功能: 写出指定范围的字节(批量写入)
       require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
       var written = 0
       while (written < len) { // 批量写入
         allocateNewChunkIfNeeded()
         val thisBatch = math.min(chunkSize - position, len - written)
         chunks(lastChunkIndex).put(bytes, written + off, thisBatch)
         written += thisBatch
         position += thisBatch
       }
       _size += len
       
       @inline
       private def allocateNewChunkIfNeeded(): Unit
       功能: 有必要的情况下分配新的数据块
       if (position == chunkSize) {
         chunks += allocator(chunkSize)
         lastChunkIndex += 1
         position = 0
       }
       
       def toChunkedByteBuffer: ChunkedByteBuffer
       功能: 转化为块状缓冲区@ChunkedByteBuffer
       1. 状态断言
       require(closed, "cannot call toChunkedByteBuffer() unless close() has been called")
       require(!toChunkedByteBufferWasCalled, "toChunkedByteBuffer() can only be called once")
       2. 修改调用标记位
       toChunkedByteBufferWasCalled = true
       3. 获取块状字节缓冲区@ChunkedByteBuffer
       if (lastChunkIndex == -1) {
         new ChunkedByteBuffer(Array.empty[ByteBuffer])
       } else {
         val ret = new Array[ByteBuffer](chunks.size)
         for (i <- 0 until chunks.size - 1) {
           ret(i) = chunks(i)
           ret(i).flip()
         }
         if (position == chunkSize) {
           ret(lastChunkIndex) = chunks(lastChunkIndex)
           ret(lastChunkIndex).flip()
         } else {
           ret(lastChunkIndex) = allocator(position)
           chunks(lastChunkIndex).flip()
           ret(lastChunkIndex).put(chunks(lastChunkIndex))
           ret(lastChunkIndex).flip()
           StorageUtils.dispose(chunks(lastChunkIndex))
         }
         new ChunkedByteBuffer(ret)
       }
   }
   ```

#### logging

1.  [DriverLogger.scala](# DriverLogger)

2.  [FileAppender.scala](# FileAppender)

3.  [RollingFileAppender.scala](# RollingFileAppender)

4.  [RollingPolicy.scala](# RollingPolicy)

   ---

   #### DriverLogger

   ```scala
private[spark] class DriverLogger(conf: SparkConf) extends Logging {
       属性:
    #name @UPLOAD_CHUNK_SIZE = 1024 * 1024 上传块大小
       #name @UPLOAD_INTERVAL_IN_SECS	上传时间间隔
       #name @DEFAULT_LAYOUT = "%d{yy/MM/dd HH:mm:ss.SSS} %t %p %c{1}: %m%n"	默认格式
       #name @LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)
       	日志文件权限(默认770)
       #name @localLogFile #Type @String  本地日志文件路径
       val= FileUtils.getFile(
           Utils.getLocalDir(conf),
           DriverLogger.DRIVER_LOG_DIR,
           DriverLogger.DRIVER_LOG_FILE).getAbsolutePath()
   	#name @writer: Option[DfsAsyncWriter] = None 写出器
       初始化操作:
       addLogAppender()
       功能: 初始化日志添加器
       
       操作集:
       def addLogAppender(): Unit
       功能: 日志添加器初始化
       1. 获取日志添加器
       val appenders = LogManager.getRootLogger().getAllAppenders()
       2. 获取日志格式
       val layout = if (conf.contains(DRIVER_LOG_LAYOUT)) {
         new PatternLayout(conf.get(DRIVER_LOG_LAYOUT).get)
       } else if (appenders.hasMoreElements()) {
         appenders.nextElement().asInstanceOf[Appender].getLayout()
       } else {
         new PatternLayout(DEFAULT_LAYOUT)
       }
       3. 设置log4j日志添加器
       val fa = new Log4jFileAppender(layout, localLogFile)
       fa.setName(DriverLogger.APPENDER_NAME)
       LogManager.getRootLogger().addAppender(fa)
       
       def startSync(hadoopConf: Configuration): Unit
       功能: 同步启动
       try {
         val appId = Utils.sanitizeDirName(conf.getAppId)
         // 设置将本地文件到hdfs的写出器(异步写出)
         writer = Some(new DfsAsyncWriter(appId, hadoopConf))
       } catch {
         case e: Exception =>
           logError(s"Could not persist driver logs to dfs", e)
       }
       
       def stop(): Unit
       功能: 停止(停止所有写出器),最后删除本地日志文件
       try {
         val fa = LogManager.getRootLogger.getAppender(DriverLogger.APPENDER_NAME)
         LogManager.getRootLogger().removeAppender(DriverLogger.APPENDER_NAME)
         Utils.tryLogNonFatalError(fa.close())
         writer.foreach(_.closeWriter())
       } catch {
         case e: Exception =>
           logError(s"Error in persisting driver logs", e)
       } finally {
         Utils.tryLogNonFatalError {
           JavaUtils.deleteRecursively(FileUtils.getFile(localLogFile).getParentFile())
         }
       }
   }
   ```
   
   #subclass @DriverLogger.DfsAsyncWriter
   
   ```scala
   private[spark] class DfsAsyncWriter(appId: String, hadoopConf: Configuration) 
   extends Runnable with Logging {
       构造器属性:
       appId	应用ID
       hadoopConf	hadoop配置
       属性:
       #name @streamClosed = false	流关闭标志
       #name @inStream: InputStream = null	输入流
       #name @outputStream: FSDataOutputStream = null	文件系统输出流
       #name @tmpBuffer = new Array[Byte](UPLOAD_CHUNK_SIZE)	临时缓冲区
       #name @threadpool: ScheduledExecutorService = _	线程池
       初始化操作:
       init()
       功能: 初始化操作
       
       操作集:
       def init(): Unit
       功能: 初始化
       1. 获取DFS文件系统的文件目录
       val rootDir = conf.get(DRIVER_LOG_DFS_DIR).get
       2. 获取文件系统实例并校验
       val fileSystem: FileSystem = new Path(rootDir).getFileSystem(hadoopConf)
         if (!fileSystem.exists(new Path(rootDir))) {
           throw new RuntimeException(s"${rootDir} does not exist." +
             s" Please create this dir in order to persist driver logs")
         }
       3. 获取日志在dfs系统上的位置
       val dfsLogFile: String = FileUtils.getFile(rootDir, appId
           + DriverLogger.DRIVER_LOG_FILE_SUFFIX).getAbsolutePath()
       4. 设置写出器的写入@inStream 和写出@outputStream ,并设置文件的权限
       try {
           inStream = new BufferedInputStream(new FileInputStream(localLogFile))
           outputStream = SparkHadoopUtil.createFile(fileSystem, new Path(dfsLogFile),
             conf.get(DRIVER_LOG_ALLOW_EC))
           fileSystem.setPermission(new Path(dfsLogFile), LOG_FILE_PERMISSIONS)
         } catch {
           case e: Exception =>
             JavaUtils.closeQuietly(inStream)
             JavaUtils.closeQuietly(outputStream)
             throw e
         }
       5. 获取线程池对象,并对任务进行调度
       threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("dfsSyncThread")
       threadpool.scheduleWithFixedDelay(this, UPLOAD_INTERVAL_IN_SECS, UPLOAD_INTERVAL_IN_SECS,
           TimeUnit.SECONDS)
       
       def run(): Unit
       功能: 写出器运行过程
       // 读取本地日志文件,并将其写入到dfs系统上
       if (streamClosed) {
           return
         }
         try {
           var remaining = inStream.available()
           val hadData = remaining > 0
           while (remaining > 0) {
             val read = inStream.read(tmpBuffer, 0, math.min(remaining, UPLOAD_CHUNK_SIZE))
             outputStream.write(tmpBuffer, 0, read)
             remaining -= read
           }
           if (hadData) {
             outputStream match {
               case hdfsStream: HdfsDataOutputStream =>
                 hdfsStream.hsync(EnumSet.allOf(classOf[HdfsDataOutputStream.SyncFlag]))
               case other =>
                 other.hflush()
             }
           }
         } catch {
           case e: Exception => logError("Failed writing driver logs to dfs", e)
         }
       
       def close(): Unit 
       功能: 关闭异步写出
       // 修改状态位,输入输出流关闭
       if (streamClosed) {
           return
         }
         try {
           // Write all remaining bytes
           run()
         } finally {
           try {
             streamClosed = true
             inStream.close()
             outputStream.close()
           } catch {
             case e: Exception =>
               logError("Error in closing driver log input/output stream", e)
           }
         }
       
       def closeWriter(): Unit
       功能: 关闭写出器(关闭写出器线程)
       try {
           threadpool.execute(() => DfsAsyncWriter.this.close())
           threadpool.shutdown()
           threadpool.awaitTermination(1, TimeUnit.MINUTES)
         } catch {
           case e: Exception =>
             logError("Error in shutting down threadpool", e)
         }
   }
   ```
   
   ```scala
   private[spark] object DriverLogger extends Logging {
       属性:
       #name @DRIVER_LOG_DIR = "__driver_logs__"	驱动器日志目录
       #name @DRIVER_LOG_FILE = "driver.log"	驱动器日志文件
       #name @DRIVER_LOG_FILE_SUFFIX = "_" + DRIVER_LOG_FILE	驱动器文件前缀
       #name @APPENDER_NAME = "_DriverLogAppender"	日志添加器名称
       操作集:
       def apply(conf: SparkConf): Option[DriverLogger] 
       功能: 获取一个驱动器日志实例@DriverLogger
       val= if (conf.get(DRIVER_LOG_PERSISTTODFS) && Utils.isClientMode(conf)) {
         if (conf.contains(DRIVER_LOG_DFS_DIR)) {
           try {
             Some(new DriverLogger(conf))
           } catch {
             case e: Exception =>
               logError("Could not add driver logger", e)
               None
           }
         } else {
           logWarning(s"Driver logs are not persisted because" +
             s" ${DRIVER_LOG_DFS_DIR.key} is not configured")
           None
         }
       } else {
         None
       }
   }
   ```
   
   #### FileAppender
   
   ```scala
   private[spark] class FileAppender(inputStream: InputStream, file: File, bufferSize: Int = 8192)
   extends Logging {
       构造器参数:
       inputStream	输入流
       file	文件对象
       bufferSize	缓冲区大小(8 kb)
       属性:
       #name @outputStream: FileOutputStream = null volatile 文件输出流
       #name @markedForStop = false	volatile	停止标志
       #name @writingThread #Type @Thread	写出线程
       val= new Thread("File appending thread for " + file) {
           setDaemon(true)
           override def run(): Unit = {
             Utils.logUncaughtExceptions {
               appendStreamToFile()
             }
           }
         }
       操作集:
       def awaitTermination(): Unit
   	功能: 等待写出线程终止
       
       def stop(): Unit
       功能: 停止文件添加器
       markedForStop = true
       
       def appendStreamToFile(): Unit
       功能: 流式读取输入并将其写入文件中
       try {
         logDebug("Started appending thread")
         Utils.tryWithSafeFinally {
           openFile()
           val buf = new Array[Byte](bufferSize)
           var n = 0
           while (!markedForStop && n != -1) {
             try {
               n = inputStream.read(buf)
             } catch {
               case _: IOException if markedForStop => // 停止了什么都不做
             }
             if (n > 0) {
               appendToFile(buf, n)
             }
           }
         } {
           closeFile()
         }
       } catch {
         case e: Exception =>
           logError(s"Error writing stream to file $file", e)
       }
       
       def appendToFile(bytes: Array[Byte], len: Int): Unit
       功能: 添加字节数组@bytes 到输出流中
       1. 边界条件: 不存在输出流的状况
       if (outputStream == null) {
         openFile()
       }
       2. 写出指定内容@bytes
       outputStream.write(bytes, 0, len)
       
       def openFile(): Unit
       功能: 打开文件输出流
       outputStream = new FileOutputStream(file, true)
       
       def closeFile(): Unit
       功能: 关闭文件输出流
       outputStream.flush()
       outputStream.close()
   }
   ```
   
   ```scala
   private[spark] object FileAppender extends Logging {
       操作集:
       def apply(inputStream: InputStream, file: File, conf: SparkConf): FileAppender
       功能: 获取一个文件添加器@FileAppender 实例
       输入参数: 
       	inputStream	指定输入流
       	file	指定输出位置
       	conf	spark应用配置
       1. 获取日志滚动参数
       val rollingStrategy = conf.get(config.EXECUTOR_LOGS_ROLLING_STRATEGY) // 滚动策略 
       val rollingSizeBytes = conf.get(config.EXECUTOR_LOGS_ROLLING_MAX_SIZE)// 互动字节量
       val rollingInterval = conf.get(config.EXECUTOR_LOGS_ROLLING_TIME_INTERVAL) // 滚动时间间隔
       2. 根据指定的策略不同选用不同的创建方式
       rollingStrategy match {
         case "" =>
           new FileAppender(inputStream, file)
         case "time" =>
           createTimeBasedAppender()
         case "size" =>
           createSizeBasedAppender()
         case _ =>
           logWarning(
             s"Illegal strategy [$rollingStrategy] for rolling executor logs, " +
               s"rolling logs not enabled")
           new FileAppender(inputStream, file)
       }
       
       def createSizeBasedAppender(): FileAppender
       功能: 基于大小的文件添加器@FileAppender 
       rollingSizeBytes match {
           case IntParam(bytes) =>
             logInfo(s"Rolling executor logs enabled for $file with rolling every $bytes bytes")
             new RollingFileAppender(inputStream, file, new SizeBasedRollingPolicy(bytes), conf)
           case _ =>
             logWarning(
               s"Illegal size [$rollingSizeBytes] for rolling executor logs, 
               rolling logs not enabled")
             new FileAppender(inputStream, file)
         }
       
       def createTimeBasedAppender(): FileAppender
       功能: 创建时基文件添加器@FileAppender
       val validatedParams: Option[(Long, String)] = rollingInterval match {
           case "daily" =>
             logInfo(s"Rolling executor logs enabled for $file with daily rolling")
             Some((24 * 60 * 60 * 1000L, "--yyyy-MM-dd"))
           case "hourly" =>
             logInfo(s"Rolling executor logs enabled for $file with hourly rolling")
             Some((60 * 60 * 1000L, "--yyyy-MM-dd--HH"))
           case "minutely" =>
             logInfo(s"Rolling executor logs enabled for $file with rolling every minute")
             Some((60 * 1000L, "--yyyy-MM-dd--HH-mm"))
           case IntParam(seconds) =>
             logInfo(s"Rolling executor logs enabled for $file with rolling $seconds seconds")
             Some((seconds * 1000L, "--yyyy-MM-dd--HH-mm-ss"))
           case _ =>
             logWarning(s"Illegal interval for rolling executor logs [$rollingInterval], " +
                 s"rolling logs not enabled")
             None
         }
         validatedParams.map {
           case (interval, pattern) =>
             new RollingFileAppender(
               inputStream, file, new TimeBasedRollingPolicy(interval, pattern), conf)
         }.getOrElse {
           new FileAppender(inputStream, file)
         }
   }
   ```
   
   #### RollingFileAppender
   
   ```markdown
   介绍:
   	连续的从输入流中读取数据,将其写入到给定文件中.在给定的时间间隔滚动文件,滚动文件命名基于给定形式
   构造器给定参数:
   	inputStream	数据源输入流
   	activeFile	写入数据的目的文件
   	rollingPolicy	滚动策略
   	conf	spark配置
   	bufferSize	缓冲区大小
   ```
   
   ```scala
   private[spark] class RollingFileAppender(
       inputStream: InputStream,
       activeFile: File,
       val rollingPolicy: RollingPolicy,
       conf: SparkConf,
       bufferSize: Int = RollingFileAppender.DEFAULT_BUFFER_SIZE
     ) extends FileAppender(inputStream, activeFile, bufferSize) {
       属性:
       #name @maxRetainedFiles = conf.get(config.EXECUTOR_LOGS_ROLLING_MAX_RETAINED_FILES)
       	最大保留文件
       #name @enableCompression = conf.get(config.EXECUTOR_LOGS_ROLLING_ENABLE_COMPRESSION)
       	是否允许压缩
       操作集:
       def stop(): Unit = super.stop()
       功能: 停止添加器并将其移走
       try {
         closeFile()
         moveFile()
         openFile()
         if (maxRetainedFiles > 0) {
           deleteOldFiles()
         }
       } catch {
         case e: Exception =>
           logError(s"Error rolling over $activeFile", e)
       }
       
       def appendToFile(bytes: Array[Byte], len: Int): Unit
       功能: 添加数据到指定文件中@activeFile
       1. 进行必要的文件滚动
       if (rollingPolicy.shouldRollover(len)) {
         rollover()
         rollingPolicy.rolledOver()
       }
       2. 将数据写出到文件,并更新写出数量
       super.appendToFile(bytes, len)
       rollingPolicy.bytesWritten(len)
       
       def rollover(): Unit
       功能: 滚动文件,关闭输出流
       
       def rolloverFileExist(file: File): Boolean
       功能: 检查滚动文件是否存在
       val= file.exists || new File(file.getAbsolutePath +
            RollingFileAppender.GZIP_LOG_SUFFIX).exists
   	
       def rotateFile(activeFile: File, rolloverFile: File): Unit
       功能: 滚动日志,如果允许压缩则对其进行压缩
       if (enableCompression) {
         // 进行可能的压缩
         val gzFile = new File(rolloverFile.getAbsolutePath + RollingFileAppender.GZIP_LOG_SUFFIX)
         var gzOutputStream: GZIPOutputStream = null
         var inputStream: InputStream = null
         try {
           // 读取数据源数据,写出到指定文件中
           inputStream = new FileInputStream(activeFile)
           gzOutputStream = new GZIPOutputStream(new FileOutputStream(gzFile))
           IOUtils.copy(inputStream, gzOutputStream)
           inputStream.close()
           gzOutputStream.close()
           activeFile.delete()
         } finally {
           IOUtils.closeQuietly(inputStream)
           IOUtils.closeQuietly(gzOutputStream)
         }
       } else {
         // 非压缩情况下,直接滚动文件
         Files.move(activeFile, rolloverFile)
       }
       
       def deleteOldFiles(): Unit 
       功能: 删除旧文件,仅仅保留最近的一些文件
       1. 获取滚动文件
       val rolledoverFiles = activeFile.getParentFile.listFiles(new FileFilter {
           def accept(f: File): Boolean = {
             f.getName.startsWith(activeFile.getName) && f != activeFile
           }
       }).sorted
       2. 删除滚动文件中top X个文件
       val filesToBeDeleted = rolledoverFiles.take(
           math.max(0, rolledoverFiles.length - maxRetainedFiles))
         filesToBeDeleted.foreach { file =>
           logInfo(s"Deleting file executor log file ${file.getAbsolutePath}")
           file.delete()
         }
       
       def moveFile(): Unit
       功能: 将激活文件移动形成新的滚动文件
       1. 获取滚动前缀
       val rolloverSuffix = rollingPolicy.generateRolledOverFileSuffix()
       2. 获取滚动文件
       val rolloverFile = new File(
         activeFile.getParentFile, activeFile.getName + rolloverSuffix).getAbsoluteFile
       3. 允许的话滚动文件
       if (activeFile.exists) {
         if (!rolloverFileExist(rolloverFile)) {
           rotateFile(activeFile, rolloverFile)
           logInfo(s"Rolled over $activeFile to $rolloverFile")
         } else {
           var i = 0
           var altRolloverFile: File = null
           do {
             altRolloverFile = new File(activeFile.getParent,
               s"${activeFile.getName}$rolloverSuffix--$i").getAbsoluteFile
             i += 1
           } while (i < 10000 && rolloverFileExist(altRolloverFile))
           logWarning(s"Rollover file $rolloverFile already exists, " +
             s"rolled over $activeFile to file $altRolloverFile")
           rotateFile(activeFile, altRolloverFile)
         }
       } else {
         logWarning(s"File $activeFile does not exist")
       }
   }
   ```
   
   ```scala
   private[spark] object RollingFileAppender {
       属性:
       #name @DEFAULT_BUFFER_SIZE = 8192	默认缓冲区大小
       #name @GZIP_LOG_SUFFIX = ".gz"	gzip前缀
       操作集:
       def getSortedRolledOverFiles(directory: String, activeFileName: String): Seq[File]
       功能: 获取排序后的滚动日志文件
       val rolledOverFiles = new File(directory).getAbsoluteFile.listFiles.filter { file =>
         val fileName = file.getName
         fileName.startsWith(activeFileName) && fileName != activeFileName
       }.sorted
       val activeFile = {
         val file = new File(directory, activeFileName).getAbsoluteFile
         if (file.exists) Some(file) else None
       }
       val= rolledOverFiles.sortBy(_.getName.stripSuffix(GZIP_LOG_SUFFIX)) ++ activeFile
   }
   ```
   
   #### RollingPolicy
   
   ```scala
   private[spark] trait RollingPolicy {
     操作集:
     def shouldRollover(bytesToBeWritten: Long): Boolean
     功能: 确认是否需要滚动 
     
     def rolledOver(): Unit
     功能: 滚动日志文件
       
     def bytesWritten(bytes: Long): Unit
     功能: 写出指定内容
       
     def generateRolledOverFileSuffix(): String
     功能: 产生滚动日志前缀
   }
   ```
   
   ```scala
   private[spark] object TimeBasedRollingPolicy {
       属性:
       #name @MINIMUM_INTERVAL_SECONDS = 60L 最小时间间隔
   }
   ```
   
   ```scala
   private[spark] object SizeBasedRollingPolicy {
       #name @MINIMUM_SIZE_BYTES = RollingFileAppender.DEFAULT_BUFFER_SIZE * 10
       	最小字节量大小
   }
   ```
   
   ```scala
   private[spark] class SizeBasedRollingPolicy(
       var rolloverSizeBytes: Long,
       checkSizeConstraint: Boolean = true    
     ) extends RollingPolicy with Logging {
       介绍: 基于大小的滚动策略
       初始化操作:
       if (checkSizeConstraint && rolloverSizeBytes < MINIMUM_SIZE_BYTES) {
           logWarning(s"Rolling size [$rolloverSizeBytes bytes] is too small. " +
             s"Setting the size to the acceptable minimum of $MINIMUM_SIZE_BYTES bytes.")
           rolloverSizeBytes = MINIMUM_SIZE_BYTES
         }
       功能: 检测滚动字节大小是否符合要求
       
       属性:
       #name @formatter = new SimpleDateFormat("--yyyy-MM-dd--HH-mm-ss--SSSS", Locale.US)	格式器
       操作集:
       def shouldRollover(bytesToBeWritten: Long): Boolean
       功能: 确认是否需要滚动(下一组数据如果要超过大小范围则会滚动)
       val= bytesToBeWritten + bytesWrittenSinceRollover > rolloverSizeBytes
       
       def rolledOver(): Unit = bytesWrittenSinceRollover = 0
       功能: 发送滚动动作,重置计数器
       
       def bytesWritten(bytes: Long): Unit =bytesWrittenSinceRollover += bytes
       功能: 写入指定数量的字节数,计数器计数值变化
       
       def generateRolledOverFileSuffix(): String
       功能: 产生滚动前缀(时间类型前缀)
       val= formatter.format(Calendar.getInstance.getTime)
   }
   ```

#### random

1.  [Pseudorandom.scala](# Pseudorandom)

2.  [RandomSampler.scala](# RandomSampler)

3.  [SamplingUtils.scala](# SamplingUtils)

4.  [StratifiedSamplingUtils.scala](# StratifiedSamplingUtils)

5.  [XORShiftRandom.scala](# XORShiftRandom)

   ---

   #### Pseudorandom

   ```scala
   @DeveloperApi
   trait Pseudorandom {
       介绍: 伪随机类
       操作集:
       def setSeed(seed: Long): Unit
       功能: 设置伪随机的随机种子
   }
   ```

   #### RandomSampler

   ```markdown
   介绍:
   	随机采样器,可以改变采用类型,例如,如果我们想要添加权重用于分层采样器.仅仅需要使用连接到采样器的转换,而且不能使用在采样后.
   	参数介绍:
   	T	数据类型
   	U	采样数据类型
   ```

   ```scala
   @DeveloperApi
   trait RandomSampler[T, U] extends Pseudorandom with Cloneable with Serializable {
       操作集:
       def sample(items: Iterator[T]): Iterator[U]
       功能: 获取随机采样器
       val= items.filter(_ => sample > 0).asInstanceOf[Iterator[U]]
       
       def sample(): Int
       功能: 是否去采样下一个数据,返回需要多少次下个数据才会被采样.如果返回0,就没有被采样过.
       
       def clone: RandomSampler[T, U]
       功能: 获取一个随机采样器的副本(被禁用)
       val= throw new UnsupportedOperationException("clone() is not implemented.")
   }
   ```

   ```scala
   private[spark] object RandomSampler {
   	属性: 
       #name @defaultMaxGapSamplingFraction = 0.4	默认最大采集
       当采集部分小于这个值的时候,就会采取采集间隙优化策略.假定传统的伯努力采样更快,这个优化值会依赖于RNG.更有价值的RNG会使得这个优化值更大.决定值的最可靠方式就是通过实验获取一个新值.希望在大多数情况下使用 0.5作为初始猜想值.
       #name @rngEpsilon = 5e-11	随机点邻域大小
       默认的RNG采集点邻域大小.采集间距计算逻辑需要计算 log(x)值.x采样于RNG.为了防止获取到 log(0)而引发错误.就使用了正向邻域的下界限.一个优秀的取值是在或者通过@nextDouble() 返回的邻近最小正浮点数的值.
       #name @roundingEpsilon = 1e-6
       采样部分的参数是计算的结果,控制浮点指针.使用这个邻域的溢出因子去阻止假的警告.比如计算数的和用户获取
       1.000000001的采样值.
   }
   ```

   ```scala
   @DeveloperApi
   class BernoulliCellSampler[T](lb: Double, ub: Double, complement: Boolean = false) 
   extends RandomSampler[T, T]{
       介绍: 基于伯努利实验用于分割数据列表,伯努利单体采样器
       构造器参数:
       	lb	接受范围下界
       	ub	接受范围上界
       	complement	是否使用指定范围的实现,默认为false
       	T 数据类型
       属性:
       #name @rng: Random = new XORShiftRandom	随机值RNG
       参数断言:
       require(
       lb <= (ub + RandomSampler.roundingEpsilon),
       	s"Lower bound ($lb) must be <= upper bound ($ub)")
     require(
       lb >= (0.0 - RandomSampler.roundingEpsilon),
       	s"Lower bound ($lb) must be >= 0.0")
     require(
       ub <= (1.0 + RandomSampler.roundingEpsilon),
       	s"Upper bound ($ub) must be <= 1.0")
       
       操作集:
       def setSeed(seed: Long): Unit = rng.setSeed(seed)
   	功能: 设置随机值RNG的随机种子值
       
       def sample(): Int
       功能: 获取随机数值
       val= if (ub - lb <= 0.0) {// 非法界限 处理
         if (complement) 1 else 0
       } else { // 获取下个伯努利实现结果
         val x = rng.nextDouble()
         val n = if ((x >= lb) && (x < ub)) 1 else 0 // 确定下个随机值是否处于范围内
         if (complement) 1 - n else n //获取随机值
       }
       
       def cloneComplement(): BernoulliCellSampler[T]
       功能: 获取指定范围内采样器的实现
       val= new BernoulliCellSampler[T](lb, ub, !complement)
       
       def clone: BernoulliCellSampler[T] = new BernoulliCellSampler[T](lb, ub, complement)
       功能: 克隆一份伯努利采样器
   }
   ```

   ```scala
   @DeveloperApi
   class BernoulliSampler[T: ClassTag](fraction: Double) extends RandomSampler[T, T]{
       介绍: 基于伯努利实验的采样器
       构造器参数:
       	fraction	采样因子
       参数断言:
       require(
           fraction >= (0.0 - RandomSampler.roundingEpsilon)
             && fraction <= (1.0 + RandomSampler.roundingEpsilon),
           s"Sampling fraction ($fraction) must be on interval [0, 1]")
       采用因子在0的左邻域到1的右邻域之间
       属性:
       #name @rng: Random = RandomSampler.newDefaultRNG	随机值
       #name @val gapSampling: GapSampling =
       	new GapSampling(fraction, rng, RandomSampler.rngEpsilon)
       采样间隔
       操作集:
       def setSeed(seed: Long): Unit = rng.setSeed(seed)
       功能: 设置随机采用值的种子值
       
       def sample(): Int
       功能: 获取采样值
       val= if (fraction <= 0.0) {
         0
       } else if (fraction >= 1.0) {
         1
       } else if (fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
         gapSampling.sample() // 小于最大采样因子,则就是目前采样间隔的采样值
       } else {
         if (rng.nextDouble() <= fraction) {
           1
         } else {
           0
         }
       }
       
       def clone: BernoulliSampler[T] = new BernoulliSampler[T](fraction)
       功能: 获取一份伯努利采用副本
   }
   ```

   ```scala
   private[spark] class GapSampling(f: Double,rng: Random = RandomSampler.newDefaultRNG,
       epsilon: Double = RandomSampler.rngEpsilon) extends Serializable {
       介绍: 间隔采样
       构造器参数:
       	f	种子值
       	rng	采样器值
       	epsilon	邻域大小
       断言参数:
       require(f > 0.0  &&  f < 1.0, s"Sampling fraction ($f) must reside on open interval (0, 1)")
       require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")
       
       初始化操作:
       advance()
       功能: 获取第一个采样值作为构造器对象
       
       属性:
       #name @lnq = math.log1p(-f)	lnq值
       #name @countForDropping: Int = 0	弃用计数值
       
       def advance(): Unit
       功能: 决定元素值会不会被舍弃
       val u = math.max(rng.nextDouble(), epsilon)
       countForDropping = (math.log(u) / lnq).toInt
       
       def sample(): Int
       功能: 采样状态位,为1表示采样,为0表示不采用
       val= if (countForDropping > 0) {
         countForDropping -= 1
         0
       } else {
         advance()
         1
       }
   }
   ```

   ```scala
@DeveloperApi
   class PoissonSampler[T](fraction: Double,
       useGapSamplingIfPossible: Boolean) extends RandomSampler[T, T]{
       构造器参数:
       fraction	采样因子
       useGapSamplingIfPossible	是否可以使用间隔采样,为true时当泊松采样比率较低,则切换为等距离采样
       属性断言:
       require(
           fraction >= (0.0 - RandomSampler.roundingEpsilon),
           s"Sampling fraction ($fraction) must be >= 0")
       属性:
       #name @rng=PoissonDistribution(if (fraction > 0.0) fraction else 1.0)  泊松采样值
       #name @rngGap = RandomSampler.newDefaultRNG	等距离采样值
       #name @gapSamplingReplacement=
       	new GapSamplingReplacement(fraction, rngGap, RandomSampler.rngEpsilon)
   		等距离采样替代方案
       操作集:
       def setSeed(seed: Long): Unit
       功能: 设置种子值
       rng.reseedRandomGenerator(seed)
       rngGap.setSeed(seed)
       
       def sample(): Int
       功能: 获取采样值
       val= if (fraction <= 0.0) {
         0
       } else if (useGapSamplingIfPossible && // 采样因子足够小,则使用等距离采样
                  fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
         gapSamplingReplacement.sample()
       } else { // 泊松采样
         rng.sample()
       }
       
       def sample(items: Iterator[T]): Iterator[T]
       功能: 获取指定类型数据的采样值
       val= if (fraction <= 0.0) {
         Iterator.empty
       } else {
         val useGapSampling = useGapSamplingIfPossible &&
           fraction <= RandomSampler.defaultMaxGapSamplingFraction
         items.flatMap { item =>
           val count = if (useGapSampling) gapSamplingReplacement.sample() else rng.sample()
           if (count == 0) Iterator.empty else Iterator.fill(count)(item)
         }
       }
       
       def clone: PoissonSampler[T] = new PoissonSampler[T](fraction, useGapSamplingIfPossible)
       功能: 获取一份泊松采样器
   }
   ```
   
   ```scala
private[spark] class GapSamplingReplacement(val f: Double,
       val rng: Random = RandomSampler.newDefaultRNG,
       epsilon: Double = RandomSampler.rngEpsilon) extends Serializable {
       构造器属性:
       	f	采样因子
       	rng	采样随机值
       	epsilon	邻域大小
       属性断言:
       require(f > 0.0, s"Sampling fraction ($f) must be > 0")
       require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")
       
       属性:
       #name @q = math.exp(-f)	
       #name @countForDropping: Int = 0	抛弃采样值计数器
       
       初始化操作:
       advance()
       功能: 跳到第一个采样值
       
       操作集:
       def poissonGE1: Int
       功能: 获取泊松分布采样值
       val= {
   	   // 模拟标准泊松分布采样
           var pp = q + ((1.0 - q) * rng.nextDouble())
           var r = 1
           pp *= rng.nextDouble()
           while (pp > q) {
             r += 1
             pp *= rng.nextDouble()
           }
           r
         }
       
       def sample(): Int
       功能: 获取采样值
       val= if (countForDropping > 0) {
         countForDropping -= 1
         0
       } else {
         val r = poissonGE1
         advance()
         r
       }
       
       def advance(): Unit
       功能: 跳过不需要采样的点
   }
   ```
   
   #### SamplingUtils

   ```scala
private[spark] object SamplingUtils {
       
}
   ```

   ```scala
private[spark] object PoissonBounds {
       介绍: 泊松分布界限类
       操作集:
       def getLowerBound(s: Double): Double
       功能: 获取下界值,返回P[x>s]以至于这个概率值非常小,满足x ~ Pois(lambda)
       val= math.max(s - numStd(s) * math.sqrt(s), 1e-15)
       
       def getUpperBound(s: Double): Double
       功能: 获取上界限值,使得P[x<s]概率非常小,满足 x ~ Pois(lambda)
   	
       def numStd(s: Double): Double
       功能: 获取标准值
       val= if (s < 6.0) {
         12.0
       } else if (s < 16.0) {
         9.0
       } else {
         6.0
       }
   }
   ```
   
   ```scala
   private[spark] object BinomialBounds {
   	介绍: 伯努利界限类
       包含了一些使用函数,用于决定界限值,当没有采样替代时调整采样值去高置信度地保证精确的规模大小
   	属性:
       #name @minSamplingRate = 1e-10 最小采样间隔
       操作集:
       def getLowerBound(delta: Double, n: Long, fraction: Double): Double
       功能: 如果构建一个n次伯努利实验,成功概率为p,不太可能存在有超过fraction * n次成功次数.
       val gamma = - math.log(delta) / n * (2.0 / 3.0)
       fraction + gamma - math.sqrt(gamma * gamma + 3 * gamma * fraction)
       
       def getUpperBound(delta: Double, n: Long, fraction: Double): Double
       功能: 如果构建一个n次伯努利实验,成功概率为p,不太可能存在有少于fraction * n次成功次数.
   	val gamma = - math.log(delta) / n
       math.min(1,math.max(minSamplingRate, 
          fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction)))
   }
   ```
   
   ```scala
   private[spark] object SamplingUtils {
       def reservoirSampleAndCount[T: ClassTag](input: Iterator[T],k: Int,
         seed: Long = Random.nextLong()): (Array[T], Long)
   	功能: 存储采样值,并计数,返回的二元组为(采样值列表,计数值)
       输入参数:
       	input	输入迭代器
       	k	存入值的数量
       	seed	随机种子
       1. 存入输入到存储列表中
       val reservoir = new Array[T](k)
       var i = 0
       while (i < k && input.hasNext) {
         val item = input.next()
         reservoir(i) = item
         i += 1
       }
       2. 对存储列表赋值
       if (i < k) { // 输入规模小于k,只需要返回与输入数组相同数量的元素即可
         val trimReservoir = new Array[T](i)
         System.arraycopy(reservoir, 0, trimReservoir, 0, i)
         val= (trimReservoir, i)
       } else { // 输入规模大于k,通过采样将列表填满
         var l = i.toLong
         val rand = new XORShiftRandom(seed)
         while (input.hasNext) {
           val item = input.next()
           l += 1
           val replacementIndex = (rand.nextDouble() * l).toLong
           if (replacementIndex < k) {
             reservoir(replacementIndex.toInt) = item
           }
         }
       }
       val= (reservoir, l)
       
       def computeFractionForSampleSize(sampleSizeLowerBound: Int, total: Long,
         withReplacement: Boolean): Double
       功能: 计算采样因子
       输入参数:
       sampleSizeLowerBound 采样大小下限
       total 总计数量
       withReplacement	是否替代
       val= if (withReplacement) {
         PoissonBounds.getUpperBound(sampleSizeLowerBound) / total
       } else {
         val fraction = sampleSizeLowerBound.toDouble / total
         BinomialBounds.getUpperBound(1e-4, total, fraction)
       }
   }
   ```
   
   
   
   #### StratifiedSamplingUtils
   
   分层采样工具
   
   ```markdown
   介绍:
   	在pair RDD函数中@PairRDDFunctions 用作辅助函数和数据结构
   	本质上来说,当给定精确的采样大小时,我们需要通过RDD的计算去得到精确的容量值,用户每个层,去在大概率保证每层的精确采样.这个通过维护一个等待列表实现,大小为O(log(s)),s是一层中需要的采样大小
   	在简单随机抽样中，保证每个随机值都是统一的分布在[0.0,1.0]这个范围内.所有小于等于最小值的记录采样值都会被立即接受.立即接受的容量满足如下函数关系:
   	s - numAccepted = O(sqrt(s))
   	其中s为需要的采样规模大小,因此通过维护一个空间复杂度为O(sqrt(s)) 的等待列表,通过添加等待列表的部分内容到达立即接受集合中,实现创建指定大小的采样器.
   	通过对等待列表的值进行排序且获取处于位置为(s-numAccepted)的值作为容量精确值(等待列表容量).
   	注意到,当计算容量和实际采样值的时候,使用的是RNG的同一个种子,所以计算的容量保证了会生成需要的采样大小.
   ```
   
   ```scala
   private[spark] object StratifiedSamplingUtils extends Logging {
       介绍: 分层采用工具类
       操作集:
       def getAcceptanceResults[K, V](rdd: RDD[(K, V)],
         withReplacement: Boolean,
         fractions: Map[K, Double],
         counts: Option[Map[K, Long]],
         seed: Long): mutable.Map[K, AcceptanceResult]
       功能: 计算立即接受的采样值数量,并生成等待列表
       注: 这个只会在需要获取精确的采样规模时才会调用
       输入参数:
       	rdd	采样RDD(键值对RDD)
    	withReplacement	是否替换
       	fractions	采样因子
       	counts	采样计数表
       	seed	种子
       1. 获取结果转换函数
       val combOp = getCombOp[K]
       2. 获取分区迭代器
       val mappedPartitionRDD = rdd.mapPartitionsWithIndex { case (partition, iter) =>
         // 获取立即接收结果
         val zeroU: mutable.Map[K, AcceptanceResult] = new mutable.HashMap[K, AcceptanceResult]()
         val rng = new RandomDataGenerator()
         rng.reSeed(seed + partition)
         // 获取指定采样因子等待列表
      val seqOp = getSeqOp(withReplacement, fractions, rng, counts)
         // 连接立即接受表,和等待列表信息 --> 合并采样因子  
      Iterator(iter.aggregate(zeroU)(seqOp, combOp))
       }
    3. 使用RDD聚合,生成指定采样因子的采样集合
       val= mappedPartitionRDD.reduce(combOp)
       
       def getSeqOp[K, V](withReplacement: Boolean,
         fractions: Map[K, Double],
         rng: RandomDataGenerator,
         counts: Option[Map[K, Long]]):
       (mutable.Map[K, AcceptanceResult], (K, V)) => mutable.Map[K, AcceptanceResult]
       功能: 返回合并分区分层采样的函数
       输入参数:
       	withReplacement	是否替代
       	fractions	采集因子列表
       	rng	随机数据采集器
       	counts	采样计数器
       	(mutable.Map[K, AcceptanceResult], (K, V)) => mutable.Map[K, AcceptanceResult]	
       		分区分层采样函数
       1. 获取增量值
       val delta = 5e-5
       2. 根据接收结果和采用值@item 求转换函数
       (result: mutable.Map[K, AcceptanceResult], item: (K, V)) => {
         val key = item._1
         val fraction = fractions(key)
         if (!result.contains(key)) {
           result += (key -> new AcceptanceResult())
         }
         val acceptResult = result(key)
         if (withReplacement) {
           if (acceptResult.areBoundsEmpty) {
             val n = counts.get(key)
             val sampleSize = math.ceil(n * fraction).toLong
             val lmbd1 = PoissonBounds.getLowerBound(sampleSize)
             val lmbd2 = PoissonBounds.getUpperBound(sampleSize)
             acceptResult.acceptBound = lmbd1 / n
             acceptResult.waitListBound = (lmbd2 - lmbd1) / n
           }
           val acceptBound = acceptResult.acceptBound
           val copiesAccepted = if (acceptBound == 0.0) 0L else rng.nextPoisson(acceptBound)
           if (copiesAccepted > 0) {
             acceptResult.numAccepted += copiesAccepted
           }
           val copiesWaitlisted = rng.nextPoisson(acceptResult.waitListBound)
           if (copiesWaitlisted > 0) {
             acceptResult.waitList ++= ArrayBuffer.fill(copiesWaitlisted)(rng.nextUniform())
           }
         } else {
           acceptResult.acceptBound =
             BinomialBounds.getLowerBound(delta, acceptResult.numItems, fraction)
           acceptResult.waitListBound =
             BinomialBounds.getUpperBound(delta, acceptResult.numItems, fraction)
           val x = rng.nextUniform()
           if (x < acceptResult.acceptBound) {
             acceptResult.numAccepted += 1
           } else if (x < acceptResult.waitListBound) {
             acceptResult.waitList += x
           }
         }
         acceptResult.numItems += 1
         result
       }
       
       def getCombOp[K]: (mutable.Map[K, AcceptanceResult], mutable.Map[K, AcceptanceResult])
       功能: 聚合操作函数(两个分区@seqOp 转换为一个@seqOp)
       1. 对两个结果列表进行合并
       (result1: mutable.Map[K, AcceptanceResult], result2: mutable.Map[K, AcceptanceResult]) => {
         result1.keySet.union(result2.keySet).foreach { key =>
           val entry1 = result1.get(key)
           if (result2.contains(key)) { // 存在则res1合并到res2中
             result2(key).merge(entry1)
           } else {
             if (entry1.isDefined) { // 不存在则res2中添加一条记录
               result2 += (key -> entry1.get)
             }
           }
         }
         result2
       }
       
       def computeThresholdByKey[K](finalResult: Map[K, AcceptanceResult],
         fractions: Map[K, Double]): Map[K, Double]
       功能: 通过key计算容量
       给定方法@getCounts 的结果,决定产生接受数据的容量,用于产生精确的采样规模大小.
       为了计算方便,每层计算采样大小@sampleSize = math.ceil(size * samplingRate) ,并将采样大小与立即接受的数量做对比,这些采样数据会放置在当前层的等待列表中.
       在绝大多数情况下,numAccepted <= sampleSize <= (numAccepted + numWaitlisted).意味着我们需要对等待列表的数据元素进行排序,目的是找到值T,使得满足
       |{elements in the stratum whose associated values <= T}| = sampleSize
       注意到等待列表所有元素值都大于等于立即接受的上限值.所以T值范围内的等待列表使得所有元素都会一次被立即接受.
       输入参数: 
       	finalResult	最终接受结果列表
       	fractions	采用因子列表
       1. 获取key的容量列表
       val thresholdByKey = new mutable.HashMap[K, Double]()
       2. 精确获取指定采样规模的数据,置入立即接受组中
       for ((key, acceptResult) <- finalResult) {
         val sampleSize = math.ceil(acceptResult.numItems * fractions(key)).toLong
         if (acceptResult.numAccepted > sampleSize) {
           logWarning("Pre-accepted too many")
           // 采样数已经满足要求,所有采用总数val= acceptBound
           thresholdByKey += (key -> acceptResult.acceptBound)
         } else {
           // 计算等待列表中的元素数量
           val numWaitListAccepted = (sampleSize - acceptResult.numAccepted).toInt
           if (numWaitListAccepted >= acceptResult.waitList.size) {
             logWarning("WaitList too short")
             // 等待列表不足以填充需求的采样规模,情况比较少
             thresholdByKey += (key -> acceptResult.waitListBound)
           } else {
             // 足以填充,则将等待列表排序,并将需求的数量立即接收到结果集中
             thresholdByKey += (key -> acceptResult.waitList.sorted.apply(numWaitListAccepted))
           }
         }
       }
       3. 获取最终采样值列表
       val= thresholdByKey
       
       def getBernoulliSamplingFunction[K, V](rdd: RDD[(K, V)],
         fractions: Map[K, Double],
         exact: Boolean,
         seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] 
       功能: 伯努利采样处理函数
       输入参数:
       	fractions	采样因子列表
       	seed	种子值
       	exact	是否重新采样
       1. 获取采样key列表
       var samplingRateByKey = fractions
       2. 如果需要额外采样,则计算每层容量和重新采样
       if (exact) {
         val finalResult = getAcceptanceResults(rdd, false, fractions, None, seed)
         samplingRateByKey = computeThresholdByKey(finalResult, fractions)
       }
       3. 重新设置种子,并重新采样(但是采样结果与之前还是一致)
       (idx: Int, iter: Iterator[(K, V)]) => {
         val rng = new RandomDataGenerator()
         rng.reSeed(seed + idx)
         // 使用rng的这个调用形式,去获取与之前一样的采样数据列表
         iter.filter(t => rng.nextUniform() < samplingRateByKey(t._1))
       }
       
       def getPoissonSamplingFunction[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
         fractions: Map[K, Double],
         exact: Boolean,
         seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)]
       功能: 泊松采样函数
       每个分区使用可代替式的采样函数
       当需要准确的采样规模时,使用两个额外的RDD去决定准确的采样比例,这个采样比例具有高置信值保证采样大小.第一	个传入数据的计算每层RDD的数据数量.第二个传入参数去决定精确的采样比例.
       注意每个分区的采样函数有一个唯一的种子值.
       输入参数:
       	fractions	采样因子列表
       	exact	是否需要重新采样
       	seed	种子值
       分支1: 需要重新采样 exact=true
         val counts = Some(rdd.countByKey()) // 计算rdd中key的数量
         // 获取直接接受结果集,和容量列表
         val finalResult = getAcceptanceResults(rdd, true, fractions, counts, seed)
         val thresholdByKey = computeThresholdByKey(finalResult, fractions)
         (idx: Int, iter: Iterator[(K, V)]) => {
           val rng = new RandomDataGenerator()
           rng.reSeed(seed + idx)
           iter.flatMap { item =>
             val key = item._1 // 获取key
             val acceptBound = finalResult(key).acceptBound // 获取结果中接受值上限
             // 获取接受结果集采样值副本
             val copiesAccepted = if (acceptBound == 0) 0L else rng.nextPoisson(acceptBound)
             // 接受等待列表
             val copiesWaitlisted = rng.nextPoisson(finalResult(key).waitListBound)
             // 获取采样数量副本
             val copiesInSample = copiesAccepted +
               (0 until copiesWaitlisted).count(i => rng.nextUniform() < thresholdByKey(key))
             if (copiesInSample > 0) { // 对指定采样大小进行泊松采样,并返回数据
               Iterator.fill(copiesInSample.toInt)(item)
             } else {
               Iterator.empty
             }
           }
         }
       分支2: 不需要重新计算
       (idx: Int, iter: Iterator[(K, V)]) => {
           val rng = new RandomDataGenerator()
           rng.reSeed(seed + idx)
           iter.flatMap { item =>
             val count = rng.nextPoisson(fractions(item._1))
             if (count == 0) {
               Iterator.empty
             } else {
               Iterator.fill(count)(item)
             }
           }
         }
   }
   ```
   
   ```scala
   private class RandomDataGenerator {
       介绍: 随机数生成器,既可以生成统一型随机变量,也可以生成泊松随机变量
       #name @uniform = new XORShiftRandom() 统一随机变量
       #name @poissonCache = mutable.Map[Double, PoissonDistribution]()	泊松缓存列表
       #name @poissonSeed = 0L	泊松种子值
       操作集:
       def reSeed(seed: Long): Unit
       功能: 重新设置种子值
       
       def nextPoisson(mean: Double): Int
       功能: 获取泊松分布采样值
       val poisson = poissonCache.getOrElseUpdate(mean, {
           val newPoisson = new PoissonDistribution(mean)
           newPoisson.reseedRandomGenerator(poissonSeed)
           newPoisson
         })
       val= poisson.sample()
       
       def nextUniform(): Double
       功能: 获取统一性随机变量
       val= uniform.nextDouble()
   }
   ```
   
   ```scala
   private[random] class AcceptanceResult(var numItems: Long = 0L, var numAccepted: Long = 0L)
     extends Serializable{
   	介绍:
        属性:
        #name @waitList = new ArrayBuffer[Double]	等待列表
        #name @acceptBound: Double = Double.NaN 	直接接收值上限
        #name @waitListBound: Double = Double.NaN	等待列表上限
        操作集:
         def areBoundsEmpty: Boolean = acceptBound.isNaN || waitListBound.isNaN
         功能: 确定上限是否为空
         
         def merge(other: Option[AcceptanceResult]): Unit 
         功能: 设置等待列表,并更新直接接收结果集
         if (other.isDefined) {
             waitList ++= other.get.waitList
             numAccepted += other.get.numAccepted
             numItems += other.get.numItems
           }
   }
   ```
   
   #### XORShiftRandom
   
   ```scala
   private[spark] class XORShiftRandom(init: Long) extends JavaRandom(init) {
       构造器属性:
       	init	初始化值
       属性:
       #name @seed = XORShiftRandom.hashSeed(init)	种子
       操作集:
       def next(bits: Int): Int
       功能: 获取下一个随机值
       val= {
           var nextSeed = seed ^ (seed << 21)
           nextSeed ^= (nextSeed >>> 35)
           nextSeed ^= (nextSeed << 4)
           seed = nextSeed
           (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
         }
       
       def setSeed(s: Long): Unit
       功能: 设置种子信息
       seed = XORShiftRandom.hashSeed(s)
   }
   ```
   
   ```scala
   private[spark] object XORShiftRandom {
       介绍: 运行RNG的基准
       操作集:
       def hashSeed(seed: Long): Long
       功能: 对种子进行散列
       val bytes = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(seed).array()
       val lowBits = MurmurHash3.bytesHash(bytes, MurmurHash3.arraySeed)
       val highBits = MurmurHash3.bytesHash(bytes, lowBits)
       (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
   }
   ```
   
   #### 基础拓展
   
   1.  伯努利分布 :  [伯努利分布](https://zh.wikipedia.org/wiki/伯努利分布)
   2.  泊松分布: http://en.wikipedia.org/wiki/Poisson_distribution
   3.  XorShift随机采样 https://en.wikipedia.org/wiki/Xorshift
   4.   采样理论 http://jmlr.org/proceedings/papers/v28/meng13a.html
   5.   缓冲区设计不当的内存泄漏 http://www.evanjones.ca/java-bytebuffer-leak.html