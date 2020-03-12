1. [gBatchedWriteAheadLog.scala](BatchedWriteAheadLog)
2. [FileBasedWriteAheadLog.scala](FileBasedWriteAheadLog)
3. [FileBasedWriteAheadLogRandomReader.scala](FileBasedWriteAheadLogRandomReader)
4. [FileBasedWriteAheadLogReader.scala](FileBasedWriteAheadLogReader)
5. [FileBasedWriteAheadLogSegment.scala](FileBasedWriteAheadLogSegment)
6. [FileBasedWriteAheadLogWriter.scala](FileBasedWriteAheadLogWriter)
7. [HdfsUtils.scala](HdfsUtils)
8. [RateLimitedOutputStream.scala](RateLimitedOutputStream)
9. [RawTextHelper.scala](RawTextHelper)
10. [RawTextSender.scala](RawTextSender)
11. [RecurringTimer.scala](RecurringTimer)
12. [StateMap.scala](StateMap)
13. [WriteAheadLogUtils.scala](WriteAheadLogUtils)

---

#### BatchedWriteAheadLog

```markdown
介绍:
 	预先写日志的包装类,可以在写数据前批量的处理记录.在写的过程中处理聚合,使用读取方法的时候可以取消聚合.消费者必须要处理读取方法的解聚合的工作,此外@WriteAheadLogRecordHandle 在写出批量数据之后返回.写出批量记录之后,将时间`time`传送到@wrappedLog,会更新最新赔偿数据的时间戳信息,这个在获取其正确性是非常重要的.
 	考虑下面的实例:
 	在时间戳1,3,5,7接受记录,使用log-1位文件名称,一旦在时间戳3处接受清除请求,可以清除log-1文件,且将5和7视作数据丢失.
 	这就一位置合一假定在其他预先写日志中,也是相同的写入语义,物理实现是基于何种背景.当@write()方法返回的时候,数据会被写入到WAL中且是持续性的.为了利用批处理的优势,调用者可以使用多线程写入.每个线程都可以阻塞知道数据写入成功.所有预先写入接口的方法会被传递到包装的@WriteAheadLog中。
```

```scala
private[util] class BatchedWriteAheadLog(val wrappedLog: WriteAheadLog, conf: SparkConf)
extends WriteAheadLog with Logging {
    介绍: 批量预先写日志
    构造器参数:
    	wrappedLog	包装的预先写日志
    	cong	spark配置
    属性:
    #name @walWriteQueue = new LinkedBlockingQueue[Record]()	WAL写出队列
    #name @active: AtomicBoolean = new AtomicBoolean(true)	写出线程是否处于激活状态
    #name @buffer = new ArrayBuffer[Record]()	记录缓冲区
    #name @batchedWriterThread = startBatchedWriterThread()	批量写出线程
    操作集:
    def write(byteBuffer: ByteBuffer, time: Long): WriteAheadLogRecordHandle
    功能: 写入缓冲数据@byteBuffer到日志文件中,这个方法会添加缓冲数据到一个队列中,且一直会阻塞到需要写出的时候.
    1. 将数据置入队列中
    val promise = Promise[WriteAheadLogRecordHandle]()
    val putSuccessfully = synchronized {
      if (active.get()) {
        walWriteQueue.offer(Record(byteBuffer, time, promise))
        true
      } else {
        false
      }
    }
    2. 阻塞知直到数据可以写出
    if (putSuccessfully) {
      ThreadUtils.awaitResult(
        promise.future, WriteAheadLogUtils.getBatchingTimeout(conf).milliseconds)
    } else {
      throw new IllegalStateException("close() was called on 
      BatchedWriteAheadLog before " +
        s"write request with time $time could be fulfilled.")
    }
    
    def read(segment: WriteAheadLogRecordHandle): ByteBuffer 
    功能: 读取数据,由于结果缓冲不会接受解聚合的数据,所以不支持这个方法,这个方法用于测试,确保不会使用在生产状态下.
    throw new UnsupportedOperationException("read() is not supported for
    	BatchedWriteAheadLog " +"as the data may require de-aggregation.")
    
    def readAll(): JIterator[ByteBuffer]
    功能: 读取所有日志目录下存在的日志,输出的预先写日志@WriteAheadLog会被解聚合
    val= ppedLog.readAll().asScala.flatMap(deaggregate).asJava
    
    def clean(threshTime: Long, waitForCompletion: Boolean): Unit
    功能: 清除早于指定时间@threshTime 的日志,方法由父类@WriteAheadLog 处理
    val= wrappedLog.clean(threshTime, waitForCompletion)
    
    def close(): Unit
    功能: 关闭批量写出线程,使用失败填充@promise并关闭WAL
    1. 关闭批量处理线程
    logInfo(s"BatchedWriteAheadLog shutting down at 
    time: ${System.currentTimeMillis()}.")
    if (!active.getAndSet(false)) return
    batchedWriterThread.interrupt()
    batchedWriterThread.join()
    2. 情况WAL等待写入队列
    while (!walWriteQueue.isEmpty) {
      val Record(_, time, promise) = walWriteQueue.poll()
      promise.failure(new IllegalStateException("close() was called on
      BatchedWriteAheadLog " +
        s"before write request with time $time could be fulfilled."))
    }
    3. 关闭WAL
    wrappedLog.close()
    
    def startBatchedWriterThread(): Thread 
    功能: 启动批量写入器线程(线程的主要任务为写出记录@flushRecords())
    val thread = new Thread(() => {
      while (active.get()) {
        try {
          flushRecords()
        } catch {
          case NonFatal(e) =>
            logWarning("Encountered exception in Batched Writer Thread.", e)
        }
      }
      logInfo("BatchedWriteAheadLog Writer thread exiting.")
    }, "BatchedWriteAheadLog Writer")
    thread.setDaemon(true)
    thread.start()
    val= thread
    
    def flushRecords(): Unit
    功能: 写出缓冲区中的所有记录到WAL中
    1. 从等待队列中获取数据填充缓冲区
    try {
      buffer += walWriteQueue.take()
      val numBatched = walWriteQueue.drainTo(buffer.asJava) + 1
      logDebug(s"Received $numBatched records from queue")
    } catch {
      case _: InterruptedException =>
        logWarning("BatchedWriteAheadLog Writer queue interrupted.")
    }
    2. 将缓冲区的记录写入到wal中
    try {
      var segment: WriteAheadLogRecordHandle = null
      if (buffer.nonEmpty) {
        logDebug(s"Batched ${buffer.length} records for Write Ahead Log write")
        val sortedByTime = buffer.sortBy(_.time)
        val time = sortedByTime.last.time
        segment = wrappedLog.write(aggregate(sortedByTime), time)
      }
      buffer.foreach(_.promise.success(segment))
    } catch {
      case e: InterruptedException =>
        logWarning("BatchedWriteAheadLog Writer queue interrupted.", e)
        buffer.foreach(_.promise.failure(e))
      case NonFatal(e) =>
        logWarning(s"BatchedWriteAheadLog Writer failed to write $buffer", e)
        buffer.foreach(_.promise.failure(e))
    } finally {
      buffer.clear()
    }
    
    def getQueueLength(): Int = walWriteQueue.size()
    功能: 获取等待队列的大小,测试使用
}
```

```scala
private[util] object BatchedWriteAheadLog {
    介绍: 用于提供聚合/解聚合的方法
    样例类:
    case class Record(
        data: ByteBuffer, time: Long, promise: Promise[WriteAheadLogRecordHandle])
    介绍: 记录的包装类,WAL中记录的基本单位,带上时间戳,用于写出记录请求,且@promise会阻塞写出请求直到某个线程真正需要写出时才会释放.
    
    def aggregate(records: Seq[Record]): ByteBuffer
    功能: 聚合多个序列化的@ReceivedBlockTrackerLogEvents 到一个缓冲区中
    ByteBuffer.wrap(Utils.serialize[Array[Array[Byte]]](
      records.map(record => JavaUtils.bufferToArray(record.data)).toArray))
    
    def deaggregate(buffer: ByteBuffer): Array[ByteBuffer]
    功能: 对一个缓冲区的内容进行解聚合,流可能开始的情况下不会使用批量处理,但是重启之后可以使用,这个方法需要向后兼容.
    val prevPosition = buffer.position()
    try {
      Utils.deserialize[Array[Array[Byte]]](JavaUtils.bufferToArray(buffer)).map(ByteBuffer.wrap)
    } catch {
      case _: ClassCastException => // users may restart a stream with batching enabled
        // Restore `position` so that the user can read `buffer` later
        buffer.position(prevPosition)
        Array(buffer)
    }
}
```

#### FileBasedWriteAheadLog

```scala
介绍: 这个类管理WAL
- 周期性的写出记录到旋转日志目录文件中
- 恢复日志文件,且从失败中读取恢复的记录
- 清理旧的日志文件
使用@FileBasedWriteAheadLogWriter	写入
使用@FileBasedWriteAheadLogReader	读取
构造器参数:
logDirectory	旋转日志目录位置
hadoopConf	hadoop配置
```

```scala
private[streaming] class FileBasedWriteAheadLog(
    conf: SparkConf,
    logDirectory: String,
    hadoopConf: Configuration,
    rollingIntervalSecs: Int,
    maxFailures: Int,
    closeFileAfterWrite: Boolean
  ) extends WriteAheadLog with Logging {
    构造器参数:
    	conf	spark配置
    	logDirectory	旋转日志目录
    	hadoopConf	hadoop最大配置次数
    	rollingIntervalSecs	滚动周期
    	maxFailures	最大失败次数
    	closeFileAfterWrite	写出之后是否关闭文件
    属性:
    #name @pastLogs = new ArrayBuffer[LogInfo]	历史日志列表
    #name @callerName = getCallerName	调用者名称
    #name @threadpoolName	线程池名称
    val= "WriteAheadLogManager" + callerName.map(c => s" for $c").getOrElse("")
    #name @forkJoinPool = ThreadUtils.newForkJoinPool(threadpoolName, 20)
    	交叉连接线程池
    #name @executionContext = ExecutionContext.fromExecutorService(forkJoinPool)
    	执行内容
    #name @currentLogPath: Option[String] = None	当前日志路径
    #name @currentLogWriter: FileBasedWriteAheadLogWriter = null	WAL写出器
    #name @currentLogWriterStartTime: Long = -1L	日志写出起始时间
    #name @currentLogWriterStopTime: Long = -1L	日志写出结束时间
    初始化操作:
    initializeOrRecover()
    功能: 初始化或者从失败中恢复
    
    操作集:
    def write(byteBuffer: ByteBuffer, time: Long): FileBasedWriteAheadLogSegment
    功能: 将缓冲内容写出到日志文件中,这个方法同步的将数据写出到HDFS中,方法返回的时候,数据保证已经刷新到HDFS上,可以读取.
    synchronized {
        var fileSegment: FileBasedWriteAheadLogSegment = null
        var failures = 0
        var lastException: Exception = null
        var succeeded = false
       	// 写出缓冲区时间,并使用容错方式
        while (!succeeded && failures < maxFailures) {
          try {
            fileSegment = getLogWriter(time).write(byteBuffer)
            if (closeFileAfterWrite) {
              resetWriter()
            }
            succeeded = true
          } catch {
            case ex: Exception =>
              lastException = ex
              logWarning("Failed to write to write ahead log")
              resetWriter()
              failures += 1
          }
        }
        if (fileSegment == null) {
          logError(s"Failed to write to write ahead log after $failures failures")
          throw lastException
        }
        val= fileSegment
      }
    
    def read(segment: WriteAheadLogRecordHandle): ByteBuffer 
    功能: 从wal中读取数据,置于缓冲区中
    val fileSegment = segment.asInstanceOf[FileBasedWriteAheadLogSegment]
    var reader: FileBasedWriteAheadLogRandomReader = null
    var byteBuffer: ByteBuffer = null
    try {
      reader = new FileBasedWriteAheadLogRandomReader(fileSegment.path, hadoopConf)
      byteBuffer = reader.read(fileSegment)
    } finally {
      reader.close()
    }
    val= byteBuffer
    
    def readAll(): JIterator[ByteBuffer]
    功能: 读取日志目录下的所有存在的日志
    注意,当调用者初始化或者需要从过去的wal中恢复数据的时候,经常会被调用.如果使用管理器写出之后调用,那么会返回最新的一批数据,但是不会处理当前激活的日志文件.
    1. 确定需要读取的日志文件
    val logFilesToRead = pastLogs.map{ _.path} ++ currentLogPath
    logInfo("Reading from the logs:\n" + logFilesToRead.mkString("\n"))
    2. 定义读取文件的函数
    def readFile(file: String): Iterator[ByteBuffer] = {
      logDebug(s"Creating log reader with $file")
      val reader = new FileBasedWriteAheadLogReader(file, hadoopConf)
      CompletionIterator[ByteBuffer, Iterator[ByteBuffer]](reader, () => reader.close())
    }
    3. 读取日志文件中的数据
    if (!closeFileAfterWrite) {
      logFilesToRead.iterator.flatMap(readFile).asJava
    } else {
      seqToParIterator(executionContext, logFilesToRead, readFile).asJava
    }
    
    def clean(threshTime: Long, waitForCompletion: Boolean): Unit
    功能: 删除早于指定数据@threshTime 的日志文件
    注意到这个时间是基于日志文件的,这个时间基于本地的系统时间.如果多台机器之间需要协调,调用者必须要处理时间差的问题.@waitForCompletion如果为true,name必须要等到旧的日志删除才能返回.设置true的时候用于测试,否则删除文件将会是异步执行.
    1. 获取需要清除的日志
    val oldLogFiles = synchronized {
      val expiredLogs = pastLogs.filter { _.endTime < threshTime }
      pastLogs --= expiredLogs
      expiredLogs
    }
    logInfo(s"Attempting to clear ${oldLogFiles.size} old log files in $logDirectory " +
      s"older than $threshTime: ${oldLogFiles.map { _.path }.mkString("\n")}")
    2. 定义删除文件的方法
    def deleteFile(walInfo: LogInfo): Unit = {
      try {
        val path = new Path(walInfo.path)
        val fs = HdfsUtils.getFileSystemForPath(path, hadoopConf)
        fs.delete(path, true)
        logDebug(s"Cleared log file $walInfo")
      } catch {
        case ex: Exception =>
          logWarning(s"Error clearing write ahead log file $walInfo", ex)
      }
      logInfo(s"Cleared log files in $logDirectory older than $threshTime")
    }
    3. 删除需要删除的日志文件
    oldLogFiles.foreach { logInfo =>
      if (!executionContext.isShutdown) {
        try {
          val f = Future { deleteFile(logInfo) }(executionContext)
          if (waitForCompletion) {
            import scala.concurrent.duration._
            Await.ready(f, 1.second)
          }
        } catch {
          case e: RejectedExecutionException =>
            logWarning("Execution context shutdown before deleting 
            old WriteAheadLogs. " +
              "This would not affect recovery correctness.", e)
        }
      }
    }
    
    def close(): Unit 
    功能: 停止管理器,关闭打开的日志写出器
    if (!executionContext.isShutdown) {
      if (currentLogWriter != null) {
        currentLogWriter.close()
      }
      executionContext.shutdown()
    }
    logInfo("Stopped write ahead log manager")
    
    def getLogWriter(currentTime: Long): FileBasedWriteAheadLogWriter
    功能: 获取日志写出器,考虑到日志的滚动
    val= synchronized {
        if (currentLogWriter == null || currentTime > currentLogWriterStopTime) {
          resetWriter()
          currentLogPath.foreach {
            pastLogs += LogInfo(currentLogWriterStartTime, currentLogWriterStopTime, _)
          }
          currentLogWriterStartTime = currentTime
          currentLogWriterStopTime = currentTime + (rollingIntervalSecs * 1000L)
          val newLogPath = new Path(logDirectory,
            timeToLogFile(currentLogWriterStartTime, currentLogWriterStopTime))
          currentLogPath = Some(newLogPath.toString)
          currentLogWriter = new FileBasedWriteAheadLogWriter(
              currentLogPath.get, hadoopConf)
        }
        currentLogWriter
      }
    
    def initializeOrRecover(): Unit
    功能: 初始化日志目录,恢复目录中的日志
    1. 获取日志目录在文件系统的位置
    val logDirectoryPath = new Path(logDirectory)
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf)
    2. 更新历史日志
    try {
      // 调用@listStatus(file)返回文件状态列表,而非是列举所有子节点.
      if (fileSystem.getFileStatus(logDirectoryPath).isDirectory) {
        val logFileInfo = logFilesTologInfo(
          fileSystem.listStatus(logDirectoryPath).map { _.getPath })
        pastLogs.clear()
        pastLogs ++= logFileInfo
        logInfo(s"Recovered ${logFileInfo.size} write ahead log 
        files from $logDirectory")
        logDebug(s"Recovered files are:\n${logFileInfo.map(_.path).mkString("\n")}")
      }
    } catch {
      case _: FileNotFoundException =>
    }
    
    def resetWriter(): Unit
    功能: 重置写出器
    synchronized {
        if (currentLogWriter != null) {
          currentLogWriter.close()
          currentLogWriter = null
        }
      }
}
```

```scala
private[streaming] object FileBasedWriteAheadLog {
    属性:
    #name @logFileRegex = """log-(\d+)-(\d+)""".r	日志文件正则格式
    样例类:
    case class LogInfo(startTime: Long, endTime: Long, path: String)
    功能: 日志信息
    
    操作集:
    def timeToLogFile(startTime: Long, stopTime: Long): String
    功能: 获取日志文件名称为--> log+起始时间+结束时间
    val= s"log-$startTime-$stopTime"
    
    def getCallerName(): Option[String]
    功能: 获取调用者名称
    1. 获取黑名单列表
    val blacklist = Seq("WriteAheadLog", "Logging", "java.lang", "scala.")
    2. 获取调用者名称
    val= Thread.currentThread.getStackTrace()
      .map(_.getClassName)
      .find { c => !blacklist.exists(c.contains) }
      .flatMap(_.split("\\.").lastOption)
      .flatMap(_.split("\\$\\$").headOption)
    
    def logFilesTologInfo(files: Seq[Path]): Seq[LogInfo] 
    功能: 将文件序列转换为日志信息@LogInfo序列,并按照起始时间进行排序
    files.flatMap { file =>
      logFileRegex.findFirstIn(file.getName()) match {
        case Some(logFileRegex(startTimeStr, stopTimeStr)) =>
          val startTime = startTimeStr.toLong
          val stopTime = stopTimeStr.toLong
          Some(LogInfo(startTime, stopTime, file.toString))
        case None =>
          None
      }
    }.sortBy { _.startTime }
    
    def seqToParIterator[I, O](
      executionContext: ExecutionContext,
      source: Seq[I],
      handler: I => Iterator[O]): Iterator[O]
    功能: 
    从并行的集合类创建迭代器,任何时间保持n个对象在内存中.n是线程池的最大大小或者8.在使用
    @FileBasedWriteAheadLogReader并行恢复的情况下是非常重要的.不希望打开k个流(且这k个流式可以并行的)
    输入参数:
    	executionContext	执行线程池
    	source	资源列表(并行集合类)
    	handler	处理函数
    1. 获取处理组的大小
    val taskSupport = new ExecutionContextTaskSupport(executionContext)
    val groupSize = taskSupport.parallelismLevel.max(8)// max(8,max_parrel)
    2. 并行处理资源列表
    val= source.grouped(groupSize).flatMap { group =>
      val parallelCollection = new ParVector(group.toVector)
      parallelCollection.tasksupport = taskSupport
      parallelCollection.map(handler)
    }.flatten
}
```

#### FileBasedWriteAheadLogRandomReader

```scala
private[streaming] class FileBasedWriteAheadLogRandomReader(path: String, conf: Configuration) extends Closeable {
    介绍: 读取WAL的随机读取器.给定文件端信息,从日志文件中读取记录信息.
    构造器参数:
    	path	读取路径
    	conf	hadoop配置
    属性:
    #name @instream = HdfsUtils.getInputStream(path, conf)	输入流
    #name @closed = (instream == null)	随机读取器关闭标志
    操作集:
    def read(segment: FileBasedWriteAheadLogSegment): ByteBuffer
    功能: 读取文件段数据
    1. 断言读取器打开
    assertOpen()
    2. 寻找文件段起始偏移量,和文件长度
    instream.seek(segment.offset)
    val nextLength = instream.readInt()
    3. 检查文件状态
    HdfsUtils.checkState(nextLength == segment.length,
      s"Expected message length to be ${segment.length}, but was $nextLength")
    4. 读取时间到缓冲区中
    val buffer = new Array[Byte](nextLength)
    instream.readFully(buffer)
    ByteBuffer.wrap(buffer)
    
    def close(): Unit 
    功能: 关闭读取器
    synchronized {
        closed = true
        instream.close()
    }
    
    def assertOpen(): Unit 
    功能: 断言读取器打开
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Reader to 
    read from the file.")
}
```

#### FileBasedWriteAheadLogReader

```scala
private[streaming] class FileBasedWriteAheadLogReader(path: String, conf: Configuration)
extends Iterator[ByteBuffer] with Closeable with Logging {
    介绍: 基于文件的预先写日志读取器
    构造器参数:
    	path	文件路径
    	conf	hadoop配置
    属性:
    #name @instream = HdfsUtils.getInputStream(path, conf)	输入流
    #name @closed = (instream == null) 	读取器是否处于关闭状态
    #name @nextItem: Option[ByteBuffer] = None	当前读取内容
    操作集:
    def next(): ByteBuffer
    功能: 获取当前读取内容
    val data = nextItem.getOrElse {
      close()
      throw new IllegalStateException(
        "next called without calling hasNext or after hasNext returned false")
    }
    nextItem = None 
    val= data
    
    def hasNext: Boolean
    功能: 确定是否有下一条数据
    if (closed) {
      return false
    }
    if (nextItem.isDefined) { 
      true
    } else {
      try {
        val length = instream.readInt()
        val buffer = new Array[Byte](length)
        instream.readFully(buffer)
        nextItem = Some(ByteBuffer.wrap(buffer))
        logTrace("Read next item " + nextItem.get)
        true
      } catch {
        case e: EOFException =>
          logDebug("Error reading next item, EOF reached", e)
          close()
          false
        case e: IOException =>
          logWarning("Error while trying to read data. If the file was deleted, " +
            "this should be okay.", e)
          close()
          if (HdfsUtils.checkFileExists(path, conf)) {
            throw e
          } else {
            false
          }
        case e: Exception =>
          logWarning("Error while trying to read data from HDFS.", e)
          close()
          throw e
      }
    }
    
    def close(): Unit
    功能: 关闭读取器
    synchronized {
        if (!closed) {
            instream.close()
        }
        closed = true
    }
}
```

#### FileBasedWriteAheadLogSegment

```scala
private[streaming] case class FileBasedWriteAheadLogSegment(path: String, offset: Long, length: Int) extends WriteAheadLogRecordHandle
介绍: 基于文件的预先写日志文件段
构造器参数:
	path	文件路径
	offset	文件段起始偏移地址
	length	文件段长度
```

#### FileBasedWriteAheadLogWriter

```scala
private[streaming] class FileBasedWriteAheadLogWriter(path: String, hadoopConf: Configuration) extends Closeable {
    介绍: 基于文件的预先写日志写出器
    构造器参数:
    	path	文件路径
    	hadoopConf	hadoop配置
    属性:
    #name @stream = HdfsUtils.getOutputStream(path, hadoopConf)
    #name @nextOffset = stream.getPos()	文件的起始偏移地址
    #name @closed = false	是否处于关闭状态下
    操作集:
    def write(data: ByteBuffer): FileBasedWriteAheadLogSegment
    功能: 写出字节数组@data到文件段中@FileBasedWriteAheadLogSegment
    1. 断言处于开启状态
    assertOpen()
    2. 确保所有数据时可以检索到的
    data.rewind()
    3. 确定需要写出的文件段
    val lengthToWrite = data.remaining()
    val segment = new FileBasedWriteAheadLogSegment(path, nextOffset, lengthToWrite)
    4. 写出长度和数据内容
    stream.writeInt(lengthToWrite)
    Utils.writeByteBuffer(data, stream: OutputStream)
    flush()
    5. 移动到下一个文件段的起始偏移地址
    nextOffset = stream.getPos()
    val= segment
    
    def close(): Unit
    功能: 关闭写出器
    synchronized {
        closed = true
        stream.close()
    }
    
    def flush(): Unit
    功能: 刷新数据
    stream.hflush()
    stream.getWrappedStream.flush()
    
    def assertOpen(): Unit
    功能: 断言写出器打开
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new 
    Writer to write to file.")
}
```

#### HdfsUtils

```scala
private[streaming] object HdfsUtils {
    操作集:
    def getOutputStream(path: String, conf: Configuration): FSDataOutputStream
    功能: 获取输出流
    1. 确定dfs路径
    val dfsPath = new Path(path)
    val dfs = getFileSystemForPath(dfsPath, conf)
    2. 获取输出流,如果文件存在且可以支持,添加这个文件即可不需要创建
    val stream: FSDataOutputStream = {
      if (dfs.isFile(dfsPath)) {
        if (conf.getBoolean("dfs.support.append", true) ||
            conf.getBoolean("hdfs.append.support", false) ||
            dfs.isInstanceOf[RawLocalFileSystem]) {
          dfs.append(dfsPath)
        } else {
          throw new IllegalStateException("File exists and there is no append support!")
        }
      } else {
        SparkHadoopUtil.createFile(dfs, dfsPath, false)
      }
    }
    val= stream
    
    def getInputStream(path: String, conf: Configuration): FSDataInputStream
    功能: 获取输入流
    val dfsPath = new Path(path)
    val dfs = getFileSystemForPath(dfsPath, conf)
    try {
      dfs.open(dfsPath)
    } catch {
      case _: FileNotFoundException =>
        null
      case e: IOException =>
        if (!dfs.isFile(dfsPath)) null else throw e
    }
    
    def checkState(state: Boolean, errorMsg: => String): Unit
    功能: 检查状态
    if (!state) {
      throw new IllegalStateException(errorMsg)
    }
    
    def getFileSegmentLocations(
      path: String, offset: Long, length: Long, conf: Configuration): Array[String]
    功能: 获取文件段的位置列表
    val dfsPath = new Path(path)
    val dfs = getFileSystemForPath(dfsPath, conf)
    val fileStatus = dfs.getFileStatus(dfsPath)
    val blockLocs = Option(dfs.getFileBlockLocations(fileStatus, offset, length))
    val= blockLocs.map(_.flatMap(_.getHosts)).getOrElse(Array.empty)
    
    def getFileSystemForPath(path: Path, conf: Configuration): FileSystem
    功能: 获取路径的文件系统
    对于本地文件系统来说,返回行式文件系统,这样的调用会将数据刷新到磁盘上
    val fs = path.getFileSystem(conf)
    fs match {
      case localFs: LocalFileSystem => localFs.getRawFileSystem
      case _ => fs
    }
    
    def checkFileExists(path: String, conf: Configuration): Boolean
    功能: 检查文件的存在性
    val hdpPath = new Path(path)
    val fs = getFileSystemForPath(hdpPath, conf)
    val= fs.isFile(hdpPath)
}
```

#### RateLimitedOutputStream

```scala
private[streaming]
class RateLimitedOutputStream(out: OutputStream, desiredBytesPerSec: Int)
extends OutputStream with Logging {
    介绍: 比例限制输出流
    构造器参数:
    out	输出流
    desiredBytesPerSec	每个部分需要的字节数量
    属性: 
    #name @SYNC_INTERVAL = NANOSECONDS.convert(10, SECONDS)	同步周期
    #name @CHUNK_SIZE = 8192	数据块大小
    #name @lastSyncTime = System.nanoTime	上次同步时间
    #name @bytesWrittenSinceSync = 0L	同步写出字节量
    初始化操作:
    require(desiredBytesPerSec > 0)
    功能: 参数断言
    
    操作集:
    def write(b: Int): Unit 
    功能: 写出b个字节
    waitToWrite(1)
    out.write(b)
    
    def write(bytes: Array[Byte]): Unit 
    功能: 写出缓冲区内容
    write(bytes, 0, bytes.length)
    
    @tailrec
    override final def write(bytes: Array[Byte], offset: Int, length: Int): Unit
    功能: 写出缓冲区范围内内容
    val writeSize = math.min(length - offset, CHUNK_SIZE)
    if (writeSize > 0) {
      waitToWrite(writeSize)
      out.write(bytes, offset, writeSize)
      write(bytes, offset + writeSize, length)
    }
    
    def flush(): Unit = out.flush()
    功能: 刷新数据
    
    def close(): Unit = out.close()
    功能: 关闭输出流
    
    @tailrec
    def waitToWrite(numBytes: Int): Unit 
    功能: 等待写出@numBytes个字节
    1. 获取本地同步的当前时间
    val now = System.nanoTime
    val elapsedNanosecs = math.max(now - lastSyncTime, 1)
    2. 确定比例
    val rate = bytesWrittenSinceSync.toDouble * 1000000000 / elapsedNanosecs
    3. 写出数据
    if (rate < desiredBytesPerSec) {
        // 这种情况表示可以写入,仅仅更新变量并返回即可
      bytesWrittenSinceSync += numBytes
      if (now > lastSyncTime + SYNC_INTERVAL) {
        lastSyncTime = now
        bytesWrittenSinceSync = numBytes
      }
    } else {
        // 超出需要的比例,计算需要等待多少时间才能回到期待的比例
        1. 计算目标值
      val targetTimeInMillis = bytesWrittenSinceSync * 1000 / desiredBytesPerSec
      val elapsedTimeInMillis = NANOSECONDS.toMillis(elapsedNanosecs)
        2. 计算需要睡眠的时间
      val sleepTimeInMillis = targetTimeInMillis - elapsedTimeInMillis
      if (sleepTimeInMillis > 0) {
        logTrace("Natural rate is " + rate + " per second but desired rate is " +
          desiredBytesPerSec + ", sleeping for 
          " + sleepTimeInMillis + " ms to compensate.")
        Thread.sleep(sleepTimeInMillis)
      }
      waitToWrite(numBytes)
    }
}
```

#### RawTextHelper

```scala
private[streaming]
object RawTextHelper {
    介绍: 原始文本辅助器
    操作集:
    def splitAndCountPartitions(iter: Iterator[String]): Iterator[(String, Long)]
    功能: 分行和计算单词的数量(单行字符串的wordCount)
    参考逻辑:
    val map = new OpenHashMap[String, Long]
    var i = 0
    var j = 0
    while (iter.hasNext) {
      val s = iter.next()
      i = 0
      while (i < s.length) {
        j = i
        while (j < s.length && s.charAt(j) != ' ') {
          j += 1
        }
        if (j > i) {
          val w = s.substring(i, j)
          map.changeValue(w, 1L, _ + 1L)
        }
        i = j
        while (i < s.length && s.charAt(i) == ' ') {
          i += 1
        }
      }
      map.toIterator.map {
        case (k, v) => (k, v)
      }
    }
    val= map.toIterator.map{case (k, v) => (k, v)}
    
    def topK(data: Iterator[(String, Long)], k: Int): Iterator[(String, Long)] 
    功能: 再给定wordCount计算结果@data中计算topK
    val taken = new Array[(String, Long)](k)
    var i = 0
    var len = 0
    var value: (String, Long) = null
    var swap: (String, Long) = null
    var count = 0
    while(data.hasNext) {
      value = data.next()
      if (value != null) {
        count += 1
        if (len == 0) {
          taken(0) = value
          len = 1
        } else if (len < k || value._2 > taken(len - 1)._2) {
          if (len < k) {
            len += 1
          }
          taken(len - 1) = value
          i = len - 1
          while(i > 0 && taken(i - 1)._2 < taken(i)._2) {
            swap = taken(i)
            taken(i) = taken(i-1)
            taken(i - 1) = swap
            i -= 1
          }
        }
      }
    }
    val= taken.toIterator
    
    def add(v1: Long, v2: Long): Long = v1+v2
    def subtract(v1: Long, v2: Long): Long= v1-v2
    功能: 加减运算
    
    def max(v1: Long, v2: Long): Long = math.max(v1, v2)
    功能: 计算较大值
    
    def warmUp(sc: SparkContext): Unit
    功能: 在实际负载启动之前,通过运行任务使得JIT准备master和slave的spark上下文.
    for (i <- 0 to 1) {
      sc.parallelize(1 to 200000, 1000)
        .map(_ % 1331).map(_.toString)
        .mapPartitions(splitAndCountPartitions).reduceByKey(_ + _, 10)
        .count()
    }
}
```

#### RawTextSender

```scala
private[streaming]
object RawTextSender extends Logging {
    介绍: 未知文本发送器,使用socket按照一定频率发送经过kryo序列化的文本串.用于将数据喂给@RawInputDStream
    操作集:
    def main(args: Array[String]): Unit
    功能: 启动函数
    0. 参数校验
    if (args.length != 4) { // 参数顺序为 <port> <file> <blockSize> <bytesPerSec>
      System.err.println("Usage: RawTextSender <port> <file> <blockSize> <bytesPerSec>")
      System.exit(1)
    }
    1. 确定参数列表
    val Array(IntParam(port), file, IntParam(blockSize), IntParam(bytesPerSec)) = args
    2. 重复多次输入数据，直到填满缓冲区
    val lines = Source.fromFile(file).getLines().toArray
    val bufferStream = new ByteArrayOutputStream(blockSize + 1000)
    val ser = new KryoSerializer(new SparkConf()).newInstance()
    val serStream = ser.serializeStream(bufferStream)
    var i = 0
    while (bufferStream.size < blockSize) {
      serStream.writeObject(lines(i))
      i = (i + 1) % lines.length
    }
    val array = bufferStream.toByteArray
    3. 设置计数缓冲区
    val countBuf = ByteBuffer.wrap(new Array[Byte](4))
    countBuf.putInt(array.length)
    countBuf.flip()
    4. 使用socket发送数据(写出长度信息,和实际信息)
    val serverSocket = new ServerSocket(port)
    logInfo("Listening on port " + port)
    while (true) {
      val socket = serverSocket.accept()
      logInfo("Got a new connection")
      val out = new RateLimitedOutputStream(socket.getOutputStream, bytesPerSec)
      try {
        while (true) {
          out.write(countBuf.array)
          out.write(array)
        }
      } catch {
        case e: IOException =>
          logError("Client disconnected")
      } finally {
        socket.close()
      }
    }
}
```

#### RecurringTimer

```scala
private[streaming]
class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String)
extends Logging {
    介绍: 重现计时器
    构造器参数:
    	clock	时钟
    	period	周期
    	callback	回调函数
    	name	计时器名称
    属性:
    #name @thread	重现计时器名称
    val= new Thread("RecurringTimer - " + name) {
        setDaemon(true)
        override def run(): Unit = { loop }
      }
    #name @prevTime = -1L	volatile	上一个计时器时间
    #name @nextTime = -1L	volatile	当前计时器时间
    #name @stopped = false	volatile	停止标记
    操作集:
    def getStartTime(): Long 
    功能: 获取启动时间,是这个计时器的下一个周期,大于当前系统时间
    val= (math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
    
    def getRestartTime(originalStartTime: Long): Long 
    功能: 获取重启时间
    值为下一个周期开始时间假设间隔时间
    1. 计算间隔时间
    val gap = clock.getTimeMillis() - originalStartTime
    val= (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
    
    def start(startTime: Long): Long 
    功能: 在指定时间启动
    1. 设定当前计时器时间
    nextTime = startTime
    2. 启动计时器线程
    thread.start()
    logInfo("Started timer for " + name + " at time " + nextTime)
    val= nextTime
    
    def start(): Long 
    功能: 在最早可以启动的时间启动(下个周期起始时间)
    val= start(getStartTime())
    
    def stop(interruptTimer: Boolean): Long
    功能: 停止计时器,返回上传调用的时间
    1. 停止计时器(停止线程并修改状态)
    if (!stopped) {
      stopped = true
      if (interruptTimer) {
        thread.interrupt()
      }
      thread.join()
      logInfo("Stopped timer for " + name + " after time " + prevTime)
    }
    val= prevTime
    
    def triggerActionForNextInterval(): Unit
    功能: 触发下个时间间隔的动作
    1. 等待到下个周期开始
    clock.waitTillTime(nextTime)
    2. 触发动作
    callback(nextTime)
    3. 更新计时器时间(移动一个周期)
    prevTime = nextTime
    nextTime += period
    logDebug("Callback for " + name + " called at time " + prevTime)
    
    def loop(): Unit
    功能: 每个周期循环
    try {
      while (!stopped) {
        triggerActionForNextInterval()
      }
      triggerActionForNextInterval()
    } catch {
      case e: InterruptedException =>
    }
}
```

```scala
private[streaming]
object RecurringTimer extends Logging {
    def main(args: Array[String]): Unit 
    功能: 启动函数
    1. 设置结束时间指针和周期值
    var lastRecurTime = 0L
    val period = 1000
    2. 设置更新结束时间指针的函数
    def onRecur(time: Long): Unit = {
      val currentTime = System.currentTimeMillis()
      logInfo("" + currentTime + ": " + (currentTime - lastRecurTime))
      lastRecurTime = currentTime
    }
    3. 设置一个重现计时器并启动
    val timer = new  RecurringTimer(new SystemClock(), period, onRecur, "Test")
    timer.start()
    4. 执行30s,停止这个计时器
    Thread.sleep(30 * 1000)
    timer.stop(true)
}
```

#### StateMap

```scala
private[streaming] abstract class StateMap[K, S] extends Serializable {
    介绍: 状态映射表
    def get(key: K): Option[S]
    功能: 获取指定key的状态
    
    def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)]
    功能: 获取指定时间@threshUpdatedTime之后的所有key-> state映射信息
    
    def getAll(): Iterator[(K, S, Long)]
    功能: 获取映射表中所有的key--> state信息
    
    def put(key: K, state: S, updatedTime: Long): Unit
    功能: 添加或者更新信息
    
    def remove(key: K): Unit
    功能: 移除一个条目信息
    
    def copy(): StateMap[K, S]
    功能: 浅拷贝当前map,用于创建一个新的map.新map的更新不会影响这个map
    
    def toDebugString(): String = toString()
    功能: 显示debug信息
}
```

```scala
private[streaming] object StateMap {
    def empty[K, S]: StateMap[K, S] = new EmptyStateMap[K, S]
    功能: 获取空的map
    
    def create[K: ClassTag, S: ClassTag](conf: SparkConf): StateMap[K, S] 
    功能: 创建一个状态映射表
    val deltaChainThreshold = 
    	conf.getInt("spark.streaming.sessionByKey.deltaChainThreshold",
      	DELTA_CHAIN_LENGTH_THRESHOLD)
    val= new OpenHashMapBasedStateMap[K, S](deltaChainThreshold)
}
```

```scala
private[streaming] class EmptyStateMap[K, S] extends StateMap[K, S] {
	介绍: 空状态映射表
    def put(key: K, session: S, updateTime: Long): Unit
    功能: 存储数据,不支持
    throw new UnsupportedOperationException(
        "put() should not be called on an EmptyStateMap")
    
    def get(key: K): Option[S] = None
    功能: 获取数据
    def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)] = Iterator.empty
    def getAll(): Iterator[(K, S, Long)] = Iterator.empty
    def copy(): StateMap[K, S] = this
    def remove(key: K): Unit = { }
    def toDebugString(): String = ""
}
```

```scala
private[streaming] class OpenHashMapBasedStateMap[K, S](
    @transient @volatile var parentStateMap: StateMap[K, S],
    private var initialCapacity: Int = DEFAULT_INITIAL_CAPACITY,
    private var deltaChainThreshold: Int = DELTA_CHAIN_LENGTH_THRESHOLD
  )(implicit private var keyClassTag: ClassTag[K], private var stateClassTag: ClassTag[S])
  extends StateMap[K, S] with KryoSerializable {
      介绍: 基于openHashMap的状态映射表,特征(空值位于第一位,且只有一个)
      构造器参数:
          parentStateMap	父map
          initialCapacity	初始化容量
          deltaChainThreshold	增加的容量
          keyClassTag	key类型
          stateClassTag	状态类型
      构造器:
      def this(initialCapacity: Int, deltaChainThreshold: Int)
      (implicit keyClassTag: ClassTag[K], stateClassTag: ClassTag[S])
      val= this(
          new EmptyStateMap[K, S],
          initialCapacity = initialCapacity,
          deltaChainThreshold = deltaChainThreshold)
      
      def this(deltaChainThreshold: Int)
      (implicit keyClassTag: ClassTag[K], stateClassTag: ClassTag[S])
      val= this(
    	  initialCapacity = DEFAULT_INITIAL_CAPACITY, 
          deltaChainThreshold = deltaChainThreshold)
      
      def this()(implicit keyClassTag: ClassTag[K], stateClassTag: ClassTag[S])
      val= this(DELTA_CHAIN_LENGTH_THRESHOLD)
      初始化操作:
      require(initialCapacity >= 1, "Invalid initial capacity")
      require(deltaChainThreshold >= 1, "Invalid delta chain threshold")
      功能: 容量参数校验
      属性:
      #name @deltaMap = new OpenHashMap[K, StateInfo[S]](initialCapacity) 
      	transient volatile
      可增加的map(较父map@parentStateMap)
      操作集:
      def get(key: K): Option[S]
      功能: 获取会话数据
      val stateInfo = deltaMap(key)
      if (stateInfo != null) {
          if (!stateInfo.deleted) {
              Some(stateInfo.data)
          } else {
              None
          }
      } else {
          parentStateMap.get(key)
      }
      
      def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)]
      功能: 获取给定时间以前的信息
      1. 获取父map的数据
      val oldStates = parentStateMap.getByTime(threshUpdatedTime).filter { 
          case (key, value, _) =>
          !deltaMap.contains(key)
      }
      2. 获取本map的数据
      val updatedStates = deltaMap.iterator.filter { case (_, stateInfo) =>
          !stateInfo.deleted && stateInfo.updateTime < threshUpdatedTime
      }.map { case (key, stateInfo) =>
          (key, stateInfo.data, stateInfo.updateTime)
      }
      val= oldStates ++ updatedStates
		
      def getAll(): Iterator[(K, S, Long)]
      功能: 获取map中的所有数据
      1. 获取父map的数据
      val oldStates = parentStateMap.getAll().filter { case (key, _, _) =>
          !deltaMap.contains(key)
        }
      2. 获取当前map的数据
      val updatedStates = deltaMap.iterator.filter { ! _._2.deleted }.map { 
          case (key, stateInfo) =>
          (key, stateInfo.data, stateInfo.updateTime)
      }
      val= oldStates ++ updatedStates
      
      def put(key: K, state: S, updateTime: Long): Unit
      功能: 存储/更新数据
      val stateInfo = deltaMap(key)
      if (stateInfo != null) {
          stateInfo.update(state, updateTime)
      } else {
          deltaMap.update(key, new StateInfo(state, updateTime))
      }
      
      def remove(key: K): Unit
      功能: 移除指定key的状态
      val stateInfo = deltaMap(key)
      if (stateInfo != null) {
          stateInfo.markDeleted()
      } else {
          val newInfo = new StateInfo[S](deleted = true)
          deltaMap.update(key, newInfo)
      }
      
      def copy(): StateMap[K, S]
      功能: map的浅拷贝
      val= new OpenHashMapBasedStateMap[K, S](
          this, deltaChainThreshold = deltaChainThreshold)
      
      def shouldCompact: Boolean
      功能: 确定是否需要合并(是否增加的长度足够合并)
      val= deltaChainLength >= deltaChainThreshold
      
      def deltaChainLength: Int
      功能: 获取map增加的链长度
      val= parentStateMap match {
        case map: OpenHashMapBasedStateMap[_, _] => map.deltaChainLength + 1
        case _ => 0
      }
      
      def approxSize: Int
      功能: 获取map中key的近似大小(父+子map)
      val= deltaMap.size + {
        parentStateMap match {
          case s: OpenHashMapBasedStateMap[_, _] => s.approxSize
          case _ => 0
        }
      }
      
      def toDebugString(): String
      功能: 获取debug信息
      val tabs = if (deltaChainLength > 0) {
          ("    " * (deltaChainLength - 1)) + "+--- "
      } else ""
      val= parentStateMap.toDebugString() + "\n" + deltaMap.iterator.mkString(
          tabs, "\n" + tabs, "")
      
      def toString(): String
      功能: 信息显示
      val= s"[${System.identityHashCode(this)},
      	${System.identityHashCode(parentStateMap)}]"
	  
      def writeObject(outputStream: ObjectOutputStream): Unit
      功能: 写出信息(序列化)
      outputStream.defaultWriteObject()
      writeObjectInternal(outputStream)
      
      def readObject(inputStream: ObjectInputStream): Unit
      功能: 读取数据(反序列化)
      inputStream.defaultReadObject()
      readObjectInternal(inputStream)
      
      def write(kryo: Kryo, output: Output): Unit
      功能: 使用kryo序列化
      output.writeInt(initialCapacity)
      output.writeInt(deltaChainThreshold)
      kryo.writeClassAndObject(output, keyClassTag)
      kryo.writeClassAndObject(output, stateClassTag)
      writeObjectInternal(new KryoOutputObjectOutputBridge(kryo, output))
      
      def read(kryo: Kryo, input: Input): Unit 
      功能: 反序列化(kryo)
      initialCapacity = input.readInt()
      deltaChainThreshold = input.readInt()
      keyClassTag = kryo.readClassAndObject(input).asInstanceOf[ClassTag[K]]
      stateClassTag = kryo.readClassAndObject(input).asInstanceOf[ClassTag[S]]
      readObjectInternal(new KryoInputObjectInputBridge(kryo, input))
      
      def writeObjectInternal(outputStream: ObjectOutput): Unit
      功能: 写出内部数据
      1. 写出当前增量map中@deltaMap的数据
      outputStream.writeInt(deltaMap.size)
      val deltaMapIterator = deltaMap.iterator
      var deltaMapCount = 0
      while (deltaMapIterator.hasNext) {
          deltaMapCount += 1
          val (key, stateInfo) = deltaMapIterator.next()
          outputStream.writeObject(key)
          outputStream.writeObject(stateInfo)
      }
      assert(deltaMapCount == deltaMap.size)
      2. 写出父map中的数据,同时拷贝父map的数据,用于合并可以分配足够的map
      val doCompaction = shouldCompact
      val newParentSessionStore = if (doCompaction) {
          val initCapacity = if (approxSize > 0) approxSize else 64
          new OpenHashMapBasedStateMap[K, S](
              initialCapacity = initCapacity, deltaChainThreshold)
      } else { null }
      val iterOfActiveSessions = parentStateMap.getAll()
      var parentSessionCount = 0
   	  3. 首次写出数据大小数据,以便于@readObject可以分配足够的map空间
      outputStream.writeInt(approxSize)
      while(iterOfActiveSessions.hasNext) {
          parentSessionCount += 1
          val (key, state, updateTime) = iterOfActiveSessions.next()
          outputStream.writeObject(key)
          outputStream.writeObject(state)
          outputStream.writeLong(updateTime)
          if (doCompaction) {
              newParentSessionStore.deltaMap.update(
                  key, StateInfo(state, updateTime, deleted = false))
          }
      }
      4. 写出最终的限制标记,其中含有正确的记录写出数量
      val limiterObj = new LimitMarker(parentSessionCount)
      outputStream.writeObject(limiterObj)
      if (doCompaction) {
          parentStateMap = newParentSessionStore
      }
      
      def readObjectInternal(inputStream: ObjectInput): Unit
      功能: 反序列化map数据
      1. 读取增量map的数据@deltaMap
      val deltaMapSize = inputStream.readInt()
      deltaMap = if (deltaMapSize != 0) {
          new OpenHashMap[K, StateInfo[S]](deltaMapSize)
      } else {
          new OpenHashMap[K, StateInfo[S]](initialCapacity)
      }
      var deltaMapCount = 0
      while (deltaMapCount < deltaMapSize) {
          val key = inputStream.readObject().asInstanceOf[K]
          val sessionInfo = inputStream.readObject().asInstanceOf[StateInfo[S]]
          deltaMap.update(key, sessionInfo)
          deltaMapCount += 1
      }
      2. 读取父map的数据,保持读取记录直到达到限制标识符.首次读取记录数量的近似大小,用于分配map的空间.
      val parentStateMapSizeHint = inputStream.readInt()
      val newStateMapInitialCapacity = math.max(
          parentStateMapSizeHint, DEFAULT_INITIAL_CAPACITY)
      val newParentSessionStore = new OpenHashMapBasedStateMap[K, S](
          initialCapacity = newStateMapInitialCapacity, deltaChainThreshold)
      3. 读取数据,直到限制标识符
      var parentSessionLoopDone = false
      while(!parentSessionLoopDone) {
          val obj = inputStream.readObject()
          if (obj.isInstanceOf[LimitMarker]) {
              parentSessionLoopDone = true
              val expectedCount = obj.asInstanceOf[LimitMarker].num
              assert(expectedCount == newParentSessionStore.deltaMap.size)
          } else {
              val key = obj.asInstanceOf[K]
              val state = inputStream.readObject().asInstanceOf[S]
              val updateTime = inputStream.readLong()
              newParentSessionStore.deltaMap.update(
                  key, StateInfo(state, updateTime, deleted = false))
          }
      }
      parentStateMap = newParentSessionStore
  }
```

```scala
private[streaming] object OpenHashMapBasedStateMap {
    内部类:
    case class StateInfo[S](
      var data: S = null.asInstanceOf[S],
      var updateTime: Long = -1,
      var deleted: Boolean = false){
        介绍: 代表状态信息
        构造器参数:
        	data	状态数据信息
        	updateTime	更新时间
        	deleted	删除标记
        def markDeleted(): Unit = deleted = true	标记记录的删除
        def update(newData: S, newUpdateTime: Long): Unit
        功能: 更新数据的状态
        data = newData
        updateTime = newUpdateTime
        deleted = false
    }
    
    class LimitMarker(val num: Int) extends Serializable
    功能: 限制标识符,代表状态数据的结束
    
    属性:
    #name @DELTA_CHAIN_LENGTH_THRESHOLD = 20	增加的链长度容量
    #name @DEFAULT_INITIAL_CAPACITY=64	默认容量
}
```

#### WriteAheadLogUtils

```scala
private[streaming] object WriteAheadLogUtils extends Logging {
    结束: WAL工具类
    属性:
    #name @RECEIVER_WAL_ENABLE_CONF_KEY = "spark.streaming.receiver.writeAheadLog.enable"
    	接收器是否允许配置WAL(key)
    #name @RECEIVER_WAL_CLASS_CONF_KEY = "spark.streaming.receiver.writeAheadLog.class"
    	接收器WAL配置类(key)
    #name @RECEIVER_WAL_ROLLING_INTERVAL_CONF_KEY	接收器WAL滚动周期(key)
    val= "spark.streaming.receiver.writeAheadLog.rollingIntervalSecs"
    #name @RECEIVER_WAL_MAX_FAILURES_CONF_KEY	接收器WAL最大失败次数(key)
    #name @RECEIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY	接收器WAL是否写后关闭文件
    val= "spark.streaming.receiver.writeAheadLog.closeFileAfterWrite"
    #name @DRIVER_WAL_CLASS_CONF_KEY = "spark.streaming.driver.writeAheadLog.class"
    	接收器WAL类(key)
    #name @DRIVER_WAL_ROLLING_INTERVAL_CONF_KEY	驱动器WAL滚动周期(key)
    val= "spark.streaming.driver.writeAheadLog.rollingIntervalSecs"
    #name @DRIVER_WAL_MAX_FAILURES_CONF_KEY	驱动器WAL最大失败次数(key)
    val= "spark.streaming.driver.writeAheadLog.maxFailures"
    #name @DRIVER_WAL_BATCHING_CONF_KEY	驱动器WAL批次配置值(key)
    val= "spark.streaming.driver.writeAheadLog.allowBatching"
    #name @DRIVER_WAL_BATCHING_TIMEOUT_CONF_KEY	驱动器WAL批次超时时间(key)
    val= "spark.streaming.driver.writeAheadLog.batchingTimeout"
    #name @DRIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY	驱动器WAL是否写出之后关闭
    val= "spark.streaming.driver.writeAheadLog.closeFileAfterWrite"
    #name @DEFAULT_ROLLING_INTERVAL_SECS = 60	日志默认滚动周期
    #name @DEFAULT_MAX_FAILURES = 3	模式最大尝试次数
    
    操作集:
    def enableReceiverLog(conf: SparkConf): Boolean
    功能: 启动接收器日志
    val= conf.getBoolean(RECEIVER_WAL_ENABLE_CONF_KEY, false)
    
    def getRollingIntervalSecs(conf: SparkConf, isDriver: Boolean): Int
    功能: 获取日志滚动周期
    val= if (isDriver) {
      conf.getInt(DRIVER_WAL_ROLLING_INTERVAL_CONF_KEY, DEFAULT_ROLLING_INTERVAL_SECS)
    } else {
      conf.getInt(RECEIVER_WAL_ROLLING_INTERVAL_CONF_KEY, DEFAULT_ROLLING_INTERVAL_SECS)
    }
    
    def getMaxFailures(conf: SparkConf, isDriver: Boolean): Int 
    功能: 获取最大失败次数
    val= if (isDriver) {
      conf.getInt(DRIVER_WAL_MAX_FAILURES_CONF_KEY, DEFAULT_MAX_FAILURES)
    } else {
      conf.getInt(RECEIVER_WAL_MAX_FAILURES_CONF_KEY, DEFAULT_MAX_FAILURES)
    }
    
    def isBatchingEnabled(conf: SparkConf, isDriver: Boolean): Boolean
    功能: 确定是否允许批次处理
    val= isDriver && conf.getBoolean(DRIVER_WAL_BATCHING_CONF_KEY, defaultValue = true)
    
    def getBatchingTimeout(conf: SparkConf): Long 
    功能: 获取批次的超时时间
    val= conf.getLong(DRIVER_WAL_BATCHING_TIMEOUT_CONF_KEY, defaultValue = 5000)
    
    def shouldCloseFileAfterWrite(conf: SparkConf, isDriver: Boolean): Boolean
    功能: 确定写出知乎是否需要关闭文件
    val= if (isDriver) {
      conf.getBoolean(DRIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY, defaultValue = false)
    } else {
      conf.getBoolean(RECEIVER_WAL_CLOSE_AFTER_WRITE_CONF_KEY, defaultValue = false)
    }
    
    def createLogForDriver(
      sparkConf: SparkConf,
      fileWalLogDirectory: String,
      fileWalHadoopConf: Configuration
    ): WriteAheadLog
    功能: 创建驱动器的WAL
    输入参数:
        fileWalLogDirectory	文件WAL日志目录
        fileWalHadoopConf	文件WAL的hadoop配置
    createLog(true, sparkConf, fileWalLogDirectory, fileWalHadoopConf)
    
    def createLogForReceiver(
      sparkConf: SparkConf,
      fileWalLogDirectory: String,
      fileWalHadoopConf: Configuration
    ): WriteAheadLog
    功能: 创建接收器的WAL
    createLog(false, sparkConf, fileWalLogDirectory, fileWalHadoopConf)
    
    def createLog(
      isDriver: Boolean,
      sparkConf: SparkConf,
      fileWalLogDirectory: String,
      fileWalHadoopConf: Configuration
    ): WriteAheadLog
    功能: 基于指定的参数创建WAL,配置的key用于获取类的实例(使用@new CustomWAL(sparkConf, logDir)),否则使用@new CustomWAL(sparkConf) 如果两个都失败了.这个方法就会失败.如果没有配置就会创建默认的@FileBasedWriteAheadLog.
    输入参数:
    	isDriver 是否为驱动器
    	sparkConf	spark配置
    	fileWalLogDirectory	WAL目录位置
    	fileWalHadoopConf	WAL的hadoop配置
    1. 确定加载类名称
    val classNameOption = if (isDriver) {
      sparkConf.getOption(DRIVER_WAL_CLASS_CONF_KEY)
    } else {
      sparkConf.getOption(RECEIVER_WAL_CLASS_CONF_KEY)
    }
    2. 获取WAL实例
    val wal = classNameOption.map { className =>
      try {
        instantiateClass(Utils.classForName[WriteAheadLog](className), sparkConf)
      } catch {
        case NonFatal(e) =>
          throw new SparkException(s"Could not create a write ahead log 
          of class $className", e)
      }
    }.getOrElse {// 没有配置的默认值
      new FileBasedWriteAheadLog(sparkConf, fileWalLogDirectory, fileWalHadoopConf,
        getRollingIntervalSecs(sparkConf, isDriver), getMaxFailures(sparkConf, isDriver),
        shouldCloseFileAfterWrite(sparkConf, isDriver))
    }
    3. 创建日志文件
    val= if (isBatchingEnabled(sparkConf, isDriver)) {
      new BatchedWriteAheadLog(wal, sparkConf)
    } else {
      wal
    }
    
    def instantiateClass(cls: Class[_ <: WriteAheadLog], conf: SparkConf): WriteAheadLog
    功能: 实例化指定类@cls,使用单个参数构造器或者无参构造器
    val= try {
      cls.getConstructor(classOf[SparkConf]).newInstance(conf)
    } catch {
      case nsme: NoSuchMethodException =>
        cls.getConstructor().newInstance()
    }
}
```

#### 基础拓展

1.  预先写日志WAL

2.  交叉连接线程池

   