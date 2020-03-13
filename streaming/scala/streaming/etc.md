
1.  [CheckPoint.scala](CheckPoint)
2.  [ContextWaiter.scala](ContextWaiter)
3. [DStreamGraph.scala](DStreamGraph)
4. [Duration.scala](Duration)
5. [Interval.scala](Interval)
6. [State.scala](State)
7. [StateSpec.scala](# StateSpec)
8. [StreamingContext.scala](StreamingContext)
9. [StreamingSource.scala](StreamingSource)
10. [Time.scala](Time)

#### CheckPoint

```scala
private[streaming]
class Checkpoint(ssc: StreamingContext, val checkpointTime: Time)
extends Logging with Serializable {
    介绍: 检查点
    构造器参数:
    	ssc	streaming上下文
    	checkpointTime	检查点时间
    属性:
    #name @master = ssc.sc.master	master
    #name @framework = ssc.sc.appName	应用名称
    #name @jars = ssc.sc.jars	jar包
    #name @graph = ssc.graph	DStream图
    #name @checkpointDir = ssc.checkpointDir	检查点目录
    #name @checkpointDuration = ssc.checkpointDuration	检查点持续时间
    #name @pendingTimes = ssc.scheduler.getPendingTimes().toArray	待定计时器
    #name @sparkConfPairs = ssc.conf.getAll	spark配置信息
    操作集:
    def createSparkConf(): SparkConf 
    功能: 创建spark配置文件
    1. 获取需要加载的属性
    val propertiesToReload = List(
      "spark.yarn.app.id",
      "spark.yarn.app.attemptId",
      "spark.driver.host",
      "spark.driver.bindAddress",
      "spark.driver.port",
      "spark.master",
      "spark.ui.port",
      "spark.blockManager.port",
      "spark.kubernetes.driver.pod.name",
      "spark.kubernetes.executor.podNamePrefix",
      "spark.yarn.jars",
      "spark.yarn.keytab",
      "spark.yarn.principal",
      "spark.kerberos.keytab",
      "spark.kerberos.principal",
      UI_FILTERS.key,
      "spark.mesos.driver.frameworkId")
    2. 获取新建的spark配置
    val newSparkConf = new SparkConf(loadDefaults = false).setAll(sparkConfPairs)
      .remove("spark.driver.host")
      .remove("spark.driver.bindAddress")
      .remove("spark.driver.port")
      .remove("spark.ui.port")
      .remove("spark.blockManager.port")
      .remove("spark.kubernetes.driver.pod.name")
      .remove("spark.kubernetes.executor.podNamePrefix")
    3. 加载属性
    val newReloadConf = new SparkConf(loadDefaults = true)
    propertiesToReload.foreach { prop =>
      newReloadConf.getOption(prop).foreach { value =>
        newSparkConf.set(prop, value)
      }
    }
    4. 添加yarn代理过滤器,用于指定恢复的spark配置
    val filter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val filterPrefix = s"spark.$filter.param."
    newReloadConf.getAll.foreach { case (k, v) =>
      if (k.startsWith(filterPrefix) && k.length > filterPrefix.length) {
        newSparkConf.set(k, v)
      }
    }
    val= newSparkConf
    
    def validate(): Unit
    功能: 参数校验
    assert(master != null, "Checkpoint.master is null")
    assert(framework != null, "Checkpoint.framework is null")
    assert(graph != null, "Checkpoint.graph is null")
    assert(checkpointTime != null, "Checkpoint.checkpointTime is null")
    logInfo(s"Checkpoint for time $checkpointTime validated")
}
```

```scala
private[streaming]
object Checkpoint extends Logging {
    属性:
    #name @PREFIX = "checkpoint-"	前缀表达式
    #name @REGEX = (PREFIX + """([\d]+)([\w\.]*)""").r	正则表达式
    操作集:
    def checkpointFile(checkpointDir: String, checkpointTime: Time): Path 
    功能: 获取给定检查点时间@checkpointTime 的检查点文件
    val= new Path(checkpointDir, PREFIX + checkpointTime.milliseconds)
    
    def checkpointBackupFile(checkpointDir: String, checkpointTime: Time): Path
    功能: 获取给定检查点时间@checkpointTime 的检查点文件后备文件
    val=new Path(checkpointDir, PREFIX + checkpointTime.milliseconds + ".bk")
    
    def getCheckpointFiles(checkpointDir: String, fsOption: Option[FileSystem] = None)
    : Seq[Path]
    功能: 获取指定检查点目录下的检查点文件,按照时间顺序进行排序
    0. 给定路径排序函数
    def sortFunc(path1: Path, path2: Path): Boolean = {
      val (time1, bk1) = path1.getName match {
          case REGEX(x, y) => (x.toLong, !y.isEmpty) }
      val (time2, bk2) = path2.getName match { 
          case REGEX(x, y) => (x.toLong, !y.isEmpty) }
      (time1 < time2) || (time1 == time2 && bk1)
    }
    1. 获取检查点目录对应的文件系统位置
    val path = new Path(checkpointDir)
    val fs = fsOption.getOrElse(path.getFileSystem(SparkHadoopUtil.get.conf))
    2. 获取目录下的文件,并对文件进行排序
    try {
      val statuses = fs.listStatus(path)
      if (statuses != null) {
        val paths = statuses.filterNot(_.isDirectory).map(_.getPath)
        val filtered = paths.filter(p => REGEX.findFirstIn(p.getName).nonEmpty)
        filtered.sortWith(sortFunc)
      } else {
        logWarning(s"Listing $path returned null")
        Seq.empty
      }
    } catch {
      case _: FileNotFoundException =>
        logWarning(s"Checkpoint directory $path does not exist")
        Seq.empty
    }
    
    def serialize(checkpoint: Checkpoint, conf: SparkConf): Array[Byte]
    功能: 序列化检查点,在可能的情况下抛出异常
    val compressionCodec = CompressionCodec.createCodec(conf)
    val bos = new ByteArrayOutputStream()
    val zos = compressionCodec.compressedOutputStream(bos)
    val oos = new ObjectOutputStream(zos)
    Utils.tryWithSafeFinally {
      oos.writeObject(checkpoint)
    } {
      oos.close()
    }
    val= bos.toByteArray
    
    def deserialize(inputStream: InputStream, conf: SparkConf): Checkpoint
    功能: 反序列化,得到检查点信息
    val compressionCodec = CompressionCodec.createCodec(conf)
    var ois: ObjectInputStreamWithLoader = null
    Utils.tryWithSafeFinally {
      val zis = compressionCodec.compressedInputStream(inputStream)
      ois = new ObjectInputStreamWithLoader(zis,
        Thread.currentThread().getContextClassLoader)
      val cp = ois.readObject.asInstanceOf[Checkpoint]
      cp.validate()
      cp
    } {
      if (ois != null) {
        ois.close()
      }
    }
}
```

```scala
private[streaming]
class CheckpointWriter(
    jobGenerator: JobGenerator,
    conf: SparkConf,
    checkpointDir: String,
    hadoopConf: Configuration
  ) extends Logging {
    介绍: 检查点写出器,用于处理图检查点的写出到文件的工作
    属性:
    #name @MAX_ATTEMPTS = 3		最大请求数量
    #name @executor = new ThreadPoolExecutor(
        1, 1,0L, TimeUnit.MILLISECONDS,new ArrayBlockingQueue[Runnable](1000))
    执行器(单线程执行器,拒绝大量执行任务的涌入)
    #name @compressionCodec = CompressionCodec.createCodec(conf)	压缩形式
    #name @stopped = false	停止标志
    #name @fs: FileSystem = null	文件系统
    #name @latestCheckpointTime: Time = null	最新的检查点时间
    
    def write(checkpoint: Checkpoint, clearCheckpointDataLater: Boolean): Unit 
    功能: 写出检查点数据,可以选择之后是否清除检查点数据
    try {
      val bytes = Checkpoint.serialize(checkpoint, conf)
      executor.execute(new CheckpointWriteHandler(
        checkpoint.checkpointTime, bytes, clearCheckpointDataLater))
      logInfo(s"Submitted checkpoint of time ${checkpoint.checkpointTime} 
      to writer queue")
    } catch {
      case rej: RejectedExecutionException =>
        logError("Could not submit checkpoint task to the thread pool executor", rej)
    }
    
    def stop(): Unit
    功能: 停止写出器的功能
    synchronized {
        if (stopped) return
        executor.shutdown()
        val startTimeNs = System.nanoTime()
        val terminated = executor.awaitTermination(
            10, java.util.concurrent.TimeUnit.SECONDS)
        if (!terminated) {
          executor.shutdownNow()
        }
        logInfo(s"CheckpointWriter executor terminated? $terminated," +
          s" waited for ${TimeUnit.NANOSECONDS.toMillis(
          System.nanoTime() - startTimeNs)} ms.")
        stopped = true
      }
    
    内部类:
    class CheckpointWriteHandler(
      checkpointTime: Time,
      bytes: Array[Byte],
      clearCheckpointDataLater: Boolean) extends Runnable {
      	介绍: 检查点写出处理器
       	构造器参数:
        bytes	写出数据
        clearCheckpointDataLater 是否清楚检查点数据
        操作集:
        def run(): Unit 
        功能: 写出检查点数据
        1. 更新检查点时间
        if (latestCheckpointTime == null || latestCheckpointTime < checkpointTime) {
        	latestCheckpointTime = checkpointTime
      	}
        2. 创建临时目录
        var attempts = 0
        val startTimeNs = System.nanoTime()
        val tempFile = new Path(checkpointDir, "temp")
        3. 设置检查点目录
        // 在产生批量数据且完成一批数据时,会设置检查点.当批次进程时间大于批次间隔的时候,旧的检查点信息会
        // 在新的检查点之后运行.如果发生了这种事情,老的检查点信息确实有最新的信息,所以我们需要从中恢复
        // 它.因此,使用最新的检查点时间作为文件名称来更新它,由于这个机制,可以恢复最新的检查点文件.注意:
        // 这里只有一个写检查点的线程,所以不需要担心线程安全的事情.
        val checkpointFile = Checkpoint.checkpointFile(
            checkpointDir, latestCheckpointTime)
        val backupFile = Checkpoint.checkpointBackupFile(
            checkpointDir, latestCheckpointTime)
        4. 写出检查点数据(进行容错)
        while (attempts < MAX_ATTEMPTS && !stopped) {
        attempts += 1
        try {
          logInfo(s"Saving checkpoint for time $checkpointTime to 
          file '$checkpointFile'")
            // 获取文件写出位置
          if (fs == null) {
            fs = new Path(checkpointDir).getFileSystem(hadoopConf)
          }
            // 删除已经存在的临时文件
          fs.delete(tempFile, true) // just in case it exists
            // 创建临时文件并写出
          val fos = fs.create(tempFile)
          Utils.tryWithSafeFinally {
            fos.write(bytes)
          } {
            fos.close()
          }
            // 设置备份文件
          if (fs.exists(checkpointFile)) {
            fs.delete(backupFile, true) // just in case it exists
            if (!fs.rename(checkpointFile, backupFile)) {
              logWarning(s"Could not rename $checkpointFile to $backupFile")
            }
          }
          if (!fs.rename(tempFile, checkpointFile)) {
            logWarning(s"Could not rename $tempFile to $checkpointFile")
          }
            // 删除旧的检查点文件(10个以前的)
          val allCheckpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs))
          if (allCheckpointFiles.size > 10) {
            allCheckpointFiles.take(allCheckpointFiles.size - 10).foreach { file =>
              logInfo(s"Deleting $file")
              fs.delete(file, true)
            }
          }
          logInfo(s"Checkpoint for time $checkpointTime saved to 
          	file '$checkpointFile'" +
            s", took ${bytes.length} bytes and " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
            // 标记任务已经完成
          jobGenerator.onCheckpointCompletion(checkpointTime, clearCheckpointDataLater)
          return
        } catch {
          case ioe: IOException =>
            val msg = s"Error in attempt $attempts of writing 
            checkpoint to '$checkpointFile'"
            logWarning(msg, ioe)
            fs = null
        }
      }
    }
}
```

```scala
private[streaming]
object CheckpointReader extends Logging {
    介绍: 检查点阅读器
    操作集:
    def read(checkpointDir: String): Option[Checkpoint]
    功能: 读取检查点目录下的检查点数据
    读取给定检查点目录下的检查点文件,如果没有检查点文件,返回none,否则返回最近可用的检查点对象.如果全部不可用也返回None.
    val= read(
        checkpointDir, new SparkConf(), SparkHadoopUtil.get.conf, ignoreReadError = true)
    
    def read(
      checkpointDir: String,
      conf: SparkConf,
      hadoopConf: Configuration,
      ignoreReadError: Boolean = false): Option[Checkpoint]
    功能: 同上,但是如果ignoreReadError=false,则没有合法的检查点文件则会抛出异常
    0. 获取检查点目录在文件系统中的位置
    val checkpointPath = new Path(checkpointDir)
    val fs = checkpointPath.getFileSystem(hadoopConf)
    1. 尝试去寻找检查点文件
    val checkpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs)).reverse
    if (checkpointFiles.isEmpty) {
      return None
    }
    2. 尝试按照顺序读取检查点文件
    logInfo(s"Checkpoint files found: ${checkpointFiles.mkString(",")}")
    var readError: Exception = null
    checkpointFiles.foreach { file =>
      logInfo(s"Attempting to load checkpoint from file $file")
      try {
        val fis = fs.open(file)
        val cp = Checkpoint.deserialize(fis, conf)
        logInfo(s"Checkpoint successfully loaded from file $file")
        logInfo(s"Checkpoint was generated at time ${cp.checkpointTime}")
        return Some(cp)
      } catch {
        case e: Exception =>
          readError = e
          logWarning(s"Error reading checkpoint from file $file", e)
      }
    }
    3. 对于无合理的检查点文件做处理
    if (!ignoreReadError) {
      throw new SparkException(
        s"Failed to read checkpoint from directory $checkpointPath", readError)
    }
    val= None
}
```

```scala
private[streaming]
class ObjectInputStreamWithLoader(_inputStream: InputStream, loader: ClassLoader)
extends ObjectInputStream(_inputStream) {
    介绍: 使用类加载器的对象输入流
    构造器参数:
    _inputStream	输入流
    loader	类加载器
    操作集:
    def resolveClass(desc: ObjectStreamClass): Class[_]
    功能: 处理流式类@desc
    try {
      return Class.forName(desc.getName(), false, loader)
    } catch {
      case e: Exception =>
    }
    super.resolveClass(desc)
}
```

#### ContextWaiter

```scala
private[streaming] class ContextWaiter {
    属性:
    #name @lock = new ReentrantLock()	重入锁
    #name @condition = lock.newCondition()	条件信号
    #name @error: Throwable = null	错误信号
    #name @stopped: Boolean = false	停止标志
    操作集:
    def notifyError(e: Throwable): Unit
    功能: 错误提示
    lock.lock()
    try {
      error = e
      condition.signalAll()
    } finally {
      lock.unlock()
    }
    
    def notifyStop(): Unit
    功能: 提示停止
    lock.lock()
    try {
      stopped = true
      condition.signalAll()
    } finally {
      lock.unlock()
    }
    
    def waitForStopOrError(timeout: Long = -1): Boolean
    功能: 如果停止了则返回true,或者@notifyError调用的时候返回错误,等待超时则返回false
    1. 获取锁
    lock.lock()
    2.尝试获取状态
    try {
      if (timeout < 0) {
        while (!stopped && error == null) {
          condition.await()
        }
      } else {
        var nanos = TimeUnit.MILLISECONDS.toNanos(timeout)
        while (!stopped && error == null && nanos > 0) {
          nanos = condition.awaitNanos(nanos)
        }
      }
      if (error != null) throw error
      stopped
    } finally {
      lock.unlock()
    }
}
```

#### DStreamGraph

```scala
final private[streaming] class DStreamGraph extends Serializable with Logging {
    介绍: DSteam图
    属性:
    #name @inputStreams = new ArrayBuffer[InputDStream[_]]()	输入DStream列表
    #name @outputStreams = new ArrayBuffer[DStream[_]]()	输出DStream列表
    #name @inputStreamNameAndID: Seq[(String, Int)] = Nil	volatile
    	输入DStream名称和编号的二元组列表
    #name @rememberDuration: Duration = null	记忆周期
    #name @checkpointInProgress = false	是否开启进程内设置检查点
    #name @zeroTime: Time = null	零时间
    #name @startTime: Time = null	开始时间
    #name @batchDuration: Duration = null	批次间隔
    #name @numReceivers: Int = 0	volatile	Receiver(接收器)的数量
    操作集:
    def start(time: Time): Unit
    功能: 在时间@time 启动DStream图
    this.synchronized {
      require(zeroTime == null, "DStream graph computation already started")
      zeroTime = time
      startTime = time
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.remember(rememberDuration))
      outputStreams.foreach(_.validateAtStart())
      numReceivers = inputStreams.count(_.isInstanceOf[ReceiverInputDStream[_]])
      inputStreamNameAndID = inputStreams.map(is => (is.name, is.id)).toSeq
      // 启动每个参数向量表
      new ParVector(inputStreams.toVector).foreach(_.start())
    }
    
    def restart(time: Time): Unit
    功能: 在指定时间@time 重启DStream图
    this.synchronized { startTime = time }
    
    def stop(): Unit
    功能: 停止DStream图
    this.synchronized {
      // 停止参数向量表即可
      new ParVector(inputStreams.toVector).foreach(_.stop())
    }
    
    def setContext(ssc: StreamingContext): Unit
    功能: 设置streaming 上下文
    this.synchronized {
      outputStreams.foreach(_.setContext(ssc))
    }
    
    def setBatchDuration(duration: Duration): Unit
    功能: 设置批次间隔
    this.synchronized {
      require(batchDuration == null,
        s"Batch duration already set as $batchDuration. Cannot set it again.")
      batchDuration = duration
    }
    
    def remember(duration: Duration): Unit
    功能: 设置记忆周期
    this.synchronized {
      require(rememberDuration == null,
        s"Remember duration already set as $rememberDuration. Cannot set it again.")
      rememberDuration = duration
    }
    
    def addInputStream(inputStream: InputDStream[_]): Unit 
    功能: 添加指定输入DStream流到当前图中
    this.synchronized {
      inputStream.setGraph(this)
      inputStreams += inputStream
    }
    
    def addOutputStream(outputStream: DStream[_]): Unit
    功能: 添加指定输出DStream到当前图中
    this.synchronized {
      outputStream.setGraph(this)
      outputStreams += outputStream
    }
    
    def getInputStreams(): Array[InputDStream[_]] 
    功能: 获取输入DStream列表
    val= this.synchronized { inputStreams.toArray }
    
    def getOutputStreams(): Array[DStream[_]]
    功能: 获取输出DStream列表
    val= this.synchronized { outputStreams.toArray }
    
    def getReceiverInputStreams(): Array[ReceiverInputDStream[_]]
    功能: 获取接收器的输入DStream列表
    val= this.synchronized {
    inputStreams.filter(_.isInstanceOf[ReceiverInputDStream[_]])
      .map(_.asInstanceOf[ReceiverInputDStream[_]])
      .toArray
  	}
    
    def getNumReceivers: Int = numReceivers
    功能: 获取接收器的数量
    
    def getInputStreamNameAndID: Seq[(String, Int)] = inputStreamNameAndID
    功能: 获取输入DStream名称--> ID映射表
    
    def generateJobs(time: Time): Seq[Job] 
    功能: 生成指定时间@time 的job列表
    logDebug("Generating jobs for time " + time)
    val jobs = this.synchronized {
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        jobOption.foreach(_.setCallSite(outputStream.creationSite))
        jobOption
      }
    }
    logDebug("Generated " + jobs.length + " jobs for time " + time)
    val= jobs
    
    def clearMetadata(time: Time): Unit
    功能: 清理元数据
    logDebug("Clearing metadata for time " + time)
    this.synchronized {
      outputStreams.foreach(_.clearMetadata(time))
    }
    logDebug("Cleared old metadata for time " + time)
    
    def updateCheckpointData(time: Time): Unit 
    功能: 更新指定时间的检查点数据
    logInfo("Updating checkpoint data for time " + time)
    this.synchronized {
      outputStreams.foreach(_.updateCheckpointData(time))
    }
    logInfo("Updated checkpoint data for time " + time)
    
    def clearCheckpointData(time: Time): Unit
    功能: 清除检查点数据
    logInfo("Clearing checkpoint data for time " + time)
    this.synchronized {
      outputStreams.foreach(_.clearCheckpointData(time))
    }
    logInfo("Cleared checkpoint data for time " + time)
    
    def restoreCheckpointData(): Unit
    功能: 恢复检查点数据
    logInfo("Restoring checkpoint data")
    this.synchronized {
      outputStreams.foreach(_.restoreCheckpointData())
    }
    logInfo("Restored checkpoint data")
    
    def validate(): Unit
    功能: 参数校验
    this.synchronized {
      require(batchDuration != null, "Batch duration has not been set")
      require(getOutputStreams().nonEmpty, "No output operations registered, 
      so nothing to execute")
    }
    
    def getMaxInputStreamRememberDuration(): Duration
    功能: 获取最大输入流记忆时间
    val= inputStreams.map(_.rememberDuration).filter(_ != null).maxBy(_.milliseconds)
    
    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit
    功能: 序列化
    Utils.tryOrIOException {
        logDebug("DStreamGraph.writeObject used")
        this.synchronized {
          checkpointInProgress = true
          logDebug("Enabled checkpoint mode")
          oos.defaultWriteObject()
          checkpointInProgress = false
          logDebug("Disabled checkpoint mode")
    	}
    }
    
    @throws(classOf[IOException])
    private def readObject(ois: ObjectInputStream): Unit
    功能: 反序列化数据,但是不返回读取的数据
    Utils.tryOrIOException {
        logDebug("DStreamGraph.readObject used")
        this.synchronized {
          checkpointInProgress = true
          ois.defaultReadObject()
          checkpointInProgress = false
        }
    }
}
```

#### Duration

```scala
case class Duration (private val millis: Long) {
    介绍: 持续时间
    构造器参数:
    	millis	持续时间(ms)
    操作集:
    --- 
    scala版本
    
    def < (that: Duration): Boolean = (this.millis < that.millis)
   	def <= (that: Duration): Boolean = (this.millis <= that.millis)
    def > (that: Duration): Boolean = (this.millis > that.millis)
    def >= (that: Duration): Boolean = (this.millis >= that.millis)
    功能: 比指定持续时间@that 小/不大于/大/不小于
    
    def + (that: Duration): Duration = new Duration(millis + that.millis)
    def - (that: Duration): Duration = new Duration(millis - that.millis)
    def * (times: Int): Duration = new Duration(millis * times)
    def / (that: Duration): Double = millis.toDouble / that.millis.toDouble
    功能: 加减乘除指定持续时间@that(注意为了保证乘积的范围,只能使用int类型)
    
    ---
    java版本
    
    def less(that: Duration): Boolean = this < that
    def lessEq(that: Duration): Boolean = this <= that
    def greater(that: Duration): Boolean = this > that
    def greaterEq(that: Duration): Boolean = this >= that
    功能: 比指定持续时间@that 小/不大于/大/不小于
    
    def plus(that: Duration): Duration = this + that
    def minus(that: Duration): Duration = this - that
    def times(times: Int): Duration = this * times
    def div(that: Duration): Double = this / that
    功能: 加减乘除指定持续时间@that(注意为了保证乘积的范围,只能使用int类型)
    
    def isMultipleOf(that: Duration): Boolean = (this.millis % that.millis == 0)
    功能: 确定是否为指定持续时间@that的倍数
    
    def min(that: Duration): Duration = if (this < that) this else that
    def max(that: Duration): Duration = if (this > that) this else that
    功能: 获取持续时间的较小/大值
    
    def isZero: Boolean = (this.millis == 0)
    功能: 持续时间是否为0
    
    def toString: String = (millis.toString + " ms")
    def toFormattedString: String = millis.toString
    功能: 信息/格式化信息显示
    
    def milliseconds: Long = millis
    功能: 获取毫秒数
    
    def prettyPrint: String = Utils.msDurationToString(millis)
    功能: 打印时间信息
}
```

```scala
object Milliseconds {
   介绍: 时间单位/毫秒
  def apply(milliseconds: Long): Duration = new Duration(milliseconds)
}

object Seconds {
    介绍: 时间单位/秒
  def apply(seconds: Long): Duration = new Duration(seconds * 1000)
}

object Minutes {
    介绍: 时间单位/分
  def apply(minutes: Long): Duration = new Duration(minutes * 60000)
}

object Durations {
    介绍: 持续时间的描述
    def milliseconds(milliseconds: Long): Duration = Milliseconds(milliseconds)
    功能: 获取毫秒数
    
    def seconds(seconds: Long): Duration = Seconds(seconds)
    功能: 获取秒数
    
    def minutes(minutes: Long): Duration = Minutes(minutes)
    功能: 获取分钟数
}
```

#### Interval

```scala
private[streaming]
class Interval(val beginTime: Time, val endTime: Time) {
    介绍: 时间间隔
    构造器参数:
    	beginTime	起始时间
    	endTime	结束时间
    操作集:
    def duration(): Duration = endTime - beginTime
    功能: 获取持续时间
    
    def + (time: Duration): Interval = new Interval(beginTime + time, endTime + time)
    def - (time: Duration): Interval = new Interval(beginTime - time, endTime - time)
    功能: 时间间隔的加减
    
    def < (that: Interval): Boolean
    功能: 两个相等时间间隔的比较,结束时间靠后的获胜
    if (this.duration != that.duration) {
      throw new Exception("Comparing two intervals 
      with different durations [" + this + ", "
        + that + "]")
    }
    val= this.endTime < that.endTime
    
    def <= (that: Interval): Boolean = (this < that || this == that)
    def > (that: Interval): Boolean = !(this <= that)
    def >= (that: Interval): Boolean = !(this < that)
    功能: 两个相等时间间隔的比较
    
    def toString: String = "[" + beginTime + ", " + endTime + "]"
    功能: 信息显示
}
```

```scala
private[streaming]
object Interval {
    def currentInterval(duration: Duration): Interval
    功能: 获取以当前时间为起始时间,指定长度@duration的时间间隔
    val time = new Time(System.currentTimeMillis)
    val intervalBegin = time.floor(duration)
    val= new Interval(intervalBegin, intervalBegin + duration)
}
```

#### State

```markdown
结束:
 	这个抽象类用于获取和更新状态,这个状态用于@mapWithState 中的映射函数,用于类
 	@org.apache.spark.streaming.dstream.PairDStreamFunctions pair DStream(scala)或者
 	@org.apache.spark.streaming.dstream.PairDStreamFunctions pair DStream的操作,scala的示例使用这个状态。
 	{{{
 		// 状态的映射维护一个整数类型的状态,返回一个字符串.
 		def mappingFunction(
 		key: String, value: Option[Int], state: State[Int]): Option[String] ={
 			// 1. 检查状态的存在性
 			if (state.exists) {
             val existingState = state.get  // 获取存在的状态
             val shouldRemove = ...         // 决定是否移除
             if (shouldRemove) {
               state.remove()     // 移除状态
             } else {
               val newState = ...
               state.update(newState)    // 不移除状态则更新状态
             }
           } else {
             val initialState = ...
             state.update(initialState)  // 不存在状态则设置初始状态
          }
       }
 	}}}
 	java版本也可以按照上述逻辑设置.
 	类型声明:
 		S	状态的类型
```

```scala
@Experimental
sealed abstract class State[S] {
    操作集:
    def exists(): Boolean
    功能: 确认状态是否存在
    
    def get(): S
    功能: 在状态存在的状态下获取状态,
    
    def update(newState: S): Unit
    功能: 使用新值@newState更新状态,如果状态已经移除或者由于超时移除则状态无法更新
    
    def remove(): Unit
    功能: 移除状态
    
    def isTimingOut(): Boolean
    功能: 确定状态是否超时,且在当前批次之后通过系统移除,这个在@StatSpec 指定了超时时间之后可能会发生.且key不会接受新的数据,因为其超时了.
    
    @inline final def getOption(): Option[S] = if (exists) Some(get()) else None
    功能: 获取状态信息,形式为@scala.Option
    
    @inline final override def toString(): String
    功能: 信息显示
    val= getOption.map { _.toString }.getOrElse("<state not set>")
}
```

```scala
private[streaming] class StateImpl[S] extends State[S] {
    属性:
    #name @state: S = null.asInstanceOf[S]	状态
    #name @defined: Boolean = false	状态是否定义
    #name @timingOut: Boolean = false	状态是否超时
    #name @updated: Boolean = false	状态是否更新
    #name @removed: Boolean = false	状态是否移除
    操作集:
    def exists(): Boolean = defined
    功能: 确定状态是否存在
    
    def get(): S
    功能: 获取状态
    val= if (defined) {
      state
    } else {
      throw new NoSuchElementException("State is not set")
    }
    
    def update(newState: S): Unit
    功能: 更新状态
    require(!removed, "Cannot update the state after it has been removed")
    require(!timingOut, "Cannot update the state that is timing out")
    state = newState
    defined = true
    updated = true
    
    def isTimingOut(): Boolean = timingOut
    功能: 确定是否超时
    
    def remove(): Unit 
    功能: 移除状态
    require(!timingOut, "Cannot remove the state that is timing out")
    require(!removed, "Cannot remove the state that has already been removed")
    defined = false
    updated = false
    removed = true
    
    def isRemoved(): Boolean= removed
    功能: 确定状态是否移除
    
    def isUpdated(): Boolean = updated
    功能: 确定状态是否更新
    
    def wrapTimingOutState(newState: S): Unit
    功能: 包装超时状态,更新内部数据并标记给定状态为超时,这个方法运行当前对象重用
    this.state = newState
    defined = true
    timingOut = true
    removed = false
    updated = false
    
    def wrap(optionalState: Option[S]): Unit
    功能: 更新内部数据,并标记当前对象的状态,当前对象可重用
    optionalState match {
      case Some(newState) =>
        this.state = newState
        defined = true
      case None =>
        this.state = null.asInstanceOf[S]
        defined = false
    }
    timingOut = false
    removed = false
    updated = false
}
```

#### StateSpec

```markdown
介绍:
 	这个抽象类代表所有的DStream转换,@mapWithState操作的相应的类,使用工程方法创建此类.
 	scala示例
 	{{{
 		// 映射函数维护整数的状态并返回字符串
 		def mappingFunction(key: String, value: Option[Int], state: State[Int])
 			: Option[String]= {
 				// 使用state.exists(), state.get(), state.update() and state.remove() 管理					状态,返回必要的字符串
 			}
 		val spec = StateSpec.function(mappingFunction).numPartitions(10)
 		val mapWithStateDStream = keyValueDStream.mapWithState
 			[StateType, MappedType](spec)
 	}}}
 	类型声明:
 		KeyType	状态key类型
 		ValueType	状态value类型
 		StateType	状态数据类型
 		MappedType	映射元素类型
```

```scala
@Experimental
sealed abstract class StateSpec[KeyType, ValueType, StateType, MappedType] extends Serializable {
    操作集:
    def initialState(rdd: RDD[(KeyType, StateType)]): this.type
    功能: 初始化状态,设置包含初始化的RDD,这个会被@mapWithState 使用
    
    def initialState(javaPairRDD: JavaPairRDD[KeyType, StateType]): this.type
    功能: 同上,java版本的实现
    
    def numPartitions(numPartitions: Int): this.type
    功能: 设置分区数量,使用hash分区
    
    def partitioner(partitioner: Partitioner): this.type
    功能: 设置分区器
    
    def timeout(idleDuration: Duration): this.type
    功能: 设置空载持续时间(超时时间).在状态移除之后(空载)的持续时间,key和状态会被视作空载(如果在给定持续时间内接受不到数据).映射函数会调用最后一次(用于空载状态),使之移除
}
```

```markdown
介绍:
 创建对象用于指定DStream转换@mapWithState 的参数.需要提供scala和java两种实现.
```

```scala
@Experimental
object StateSpec {
    操作集:
    def function[KeyType, ValueType, StateType, MappedType](
      mappingFunction: (
          Time, KeyType, Option[ValueType], State[StateType]) => Option[MappedType]
    ): StateSpec[KeyType, ValueType, StateType, MappedType]
    功能: 创建@StateSpec,用于设置@mapWithState操作的参数
    ClosureCleaner.clean(mappingFunction, checkSerializable = true)
    val= new StateSpecImpl(mappingFunction)
    
    def function[KeyType, ValueType, StateType, MappedType](
      mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
    ): StateSpec[KeyType, ValueType, StateType, MappedType]
    功能: 同上
    ClosureCleaner.clean(mappingFunction, checkSerializable = true)
    val wrappedFunction =
      (time: Time, key: KeyType, value: Option[ValueType], state: State[StateType]) => {
        Some(mappingFunction(key, value, state))
      }
    val= new StateSpecImpl(wrappedFunction)
    
    def function[KeyType, ValueType, StateType, MappedType](mappingFunction:
      JFunction4[Time, KeyType, Optional[ValueType], State[StateType],
                 Optional[MappedType]]):
    StateSpec[KeyType, ValueType, StateType, MappedType] 
    功能: 同上
    val wrappedFunc = (
        time: Time, k: KeyType, v: Option[ValueType], s: State[StateType]) => {
      val t = mappingFunction.call(time, k, JavaUtils.optionToOptional(v), s)
      if (t.isPresent) {
        Some(t.get)
      } else {
        None
      }
    }
    val= StateSpec.function(wrappedFunc)
    
    def function[KeyType, ValueType, StateType, MappedType](
      mappingFunction: JFunction3[KeyType, Optional[ValueType], State[StateType], MappedType]):StateSpec[KeyType, ValueType, StateType, MappedType]
    功能: 同上
    val wrappedFunc = (k: KeyType, v: Option[ValueType], s: State[StateType]) => {
      mappingFunction.call(k, JavaUtils.optionToOptional(v), s)
    }
    val= StateSpec.function(wrappedFunc)
}
```

```scala
private[streaming]
case class StateSpecImpl[K, V, S, T](
    function: (Time, K, Option[V], State[S]) => Option[T]) extends StateSpec[K, V, S, T] {
    介绍: @StateSpec 接口的内部实现
    属性:
    #name @partitioner: Partitioner = null	分区器
    #name @initialStateRDD: RDD[(K, S)] = null	初始化状态RDD
    #name @timeoutInterval: Duration = null	超时时间间隔
    初始化操作:
    require(function != null)
    功能: 转换函数校验
    
    操作集:
    def initialState(rdd: RDD[(K, S)]): this.type 
    功能: 初始化状态
    this.initialStateRDD = rdd
    val= this
    
    def initialState(javaPairRDD: JavaPairRDD[K, S]): this.type
    功能: 同上
    this.initialStateRDD = javaPairRDD.rdd
    val= this
    
    def numPartitions(numPartitions: Int): this.type
    功能: 设置分区数量
    this.partitioner(new HashPartitioner(numPartitions))
    val= this
    
    def partitioner(partitioner: Partitioner): this.type
    功能: 设置分区器
    this.partitioner = partitioner
    val= this
    
    def timeout(interval: Duration): this.type
    功能: 设置空载超时时间
    this.timeoutInterval = interval
    val= this
    
    def getFunction(): (Time, K, Option[V], State[S]) => Option[T] = function
    功能: 获取转换函数
    
    def getInitialStateRDD(): Option[RDD[(K, S)]] = Option(initialStateRDD)
    功能: 获取初始化状态RDD
    
    def getPartitioner(): Option[Partitioner] = Option(partitioner)
    功能: 获取分区器
    
    def getTimeoutInterval(): Option[Duration] = Option(timeoutInterval)
    功能: 获取超时时间间隔
}
```

#### StreamingContext

```markdown
介绍:
 	streaming上下文,主要的spark streaming功能性的键值对配置,提供用于创建@D各类输入资源的DStream 的方法.可以通过spark master地址和应用名称创建,也可以从sparkConf中创建,或者从spark上下文中创建@SparkContext.相应的spark上下文需要使用`context.sparkContext`创建,在创建转换的DStream之后,streaming计算可以使用`context.start()`和`context.stop()`进行启动和停止.`context.awaitTermination()`允许当前线程瞪大上下文的停止.
```

```scala
class StreamingContext private[streaming] (
    _sc: SparkContext,
    _cp: Checkpoint,
    _batchDur: Duration
  ) extends Logging {
    构造器属性:
    	_sc	spark上下文
    	_cp	检查点
    	_batch	批次持续时间
    构造器:
    def this(sparkContext: SparkContext, batchDuration: Duration) = {
        this(sparkContext, null, batchDuration)
      }
    
    def this(conf: SparkConf, batchDuration: Duration) = {
        this(StreamingContext.createNewSparkContext(conf), null, batchDuration)
      }
    
    def this(
      master: String,
      appName: String,
      batchDuration: Duration,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map())={
        this(
            StreamingContext.createNewSparkContext(
                master, appName, sparkHome, jars, environment),
         null, batchDuration)
    }
    
    def this(path: String, hadoopConf: Configuration) =
   	 this(null, CheckpointReader.read(path, new SparkConf(), hadoopConf).orNull, null)
    
    def this(path: String) = this(path, SparkHadoopUtil.get.conf)
    
    def this(path: String, sparkContext: SparkContext) = {
    this(
      sparkContext,
      CheckpointReader.read(
          path, sparkContext.conf, sparkContext.hadoopConfiguration).orNull,
      null)
    }
    初始化操作:
    require(_sc != null || _cp != null,
    "Spark Streaming cannot be initialized with both 
    SparkContext and checkpoint as null")
    功能: 参数校验
    
    if (sc.conf.get("spark.master") == "local" || 
        sc.conf.get("spark.master") == "local[1]") {
        logWarning("spark.master should be set as local[n], n > 1 in local 
        mode if you have receivers" +
          " to get data, otherwise Spark jobs will not get resources to 
          process the received data.")
      }
    功能: 建议master本地的核心数量(需要大于1)
    
    属性:
    #name @isCheckpointPresent: Boolean = _cp != null	当前检查点是否存在
    #name @sc: SparkContext	spark上下文
    val= if (_sc != null) {
      _sc
    } else if (isCheckpointPresent) {
      SparkContext.getOrCreate(_cp.createSparkConf())
    } else {
      throw new SparkException("Cannot create StreamingContext without a SparkContext")
    }
    #name @conf = sc.conf	spark配置
    #name @env = sc.env	spark环境
    #name @graph: DStreamGraph	DStream图
    val= if (isCheckpointPresent) {
      _cp.graph.setContext(this)
      _cp.graph.restoreCheckpointData()
      _cp.graph
    } else {
      require(_batchDur != null, "Batch duration for StreamingContext cannot be null")
      val newGraph = new DStreamGraph()
      newGraph.setBatchDuration(_batchDur)
      newGraph
    }
    #name @nextInputStreamId = new AtomicInteger(0)	当前输入流(DStream)编号
    #name @checkpointDir: String	检查点目录
    val= if (isCheckpointPresent) {
      sc.setCheckpointDir(_cp.checkpointDir)
      _cp.checkpointDir
    } else {
      null
    }
    #name @checkpointDuration: Duration	检查点持续时间
    val= if (isCheckpointPresent) _cp.checkpointDuration else graph.batchDuration
    #name @scheduler = new JobScheduler(this)	job调度器
    #name @waiter = new ContextWaiter	上下文等待器
    #name @progressListener = new StreamingJobProgressListener(this) streamingJob进程监听器
    #name @uiTab: Option[StreamingTab]	sparkWebUI表单
    val= sparkContext.ui match {
      case Some(ui) => Some(new StreamingTab(this, ui))
      case None => None
    }
    #name @streamingSource = new StreamingSource(this)	streaming资源
    #name @state: StreamingContextState = INITIALIZED	streaming上下文状态
    #name @startSite = new AtomicReference[CallSite](null)	起始位置
    #name @savedProperties = new AtomicReference[Properties](new Properties) 保存的配置
    	本地线程属性的副本,这些属性会在任务提交之后设置
    #name @shutdownHookRef: AnyRef = _	关闭位置引用
    初始化操作:
    conf.getOption("spark.streaming.checkpoint.directory").foreach(checkpoint)
    功能: 设置检查点目录
    
    操作集:
    def getStartSite(): CallSite = startSite.get()
    功能: 获取起始调用位置
    
    def sparkContext: SparkContext = sc
    功能: 获取spark上下文
    
    def remember(duration: Duration): Unit
    功能: 设置每个DStream中RDD的记忆时间为@duration
    graph.remember(duration)
    
    def checkpoint(directory: String): Unit
    功能: 设置检查点DStream操作,用于驱动器的默认容错措施
    if (directory != null) {
      val path = new Path(directory)
      val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
      fs.mkdirs(path)
      val fullPath = fs.getFileStatus(path).getPath().toString
      sc.setCheckpointDir(fullPath)
      checkpointDir = fullPath
    } else {
      checkpointDir = null
    }
    
    def isCheckpointingEnabled: Boolean= checkpointDir != null
    功能: 确定是否允许检查点配置
    
    def initialCheckpoint: Checkpoint =if (isCheckpointPresent) _cp else null
    功能: 初始化检查点
    
    def getNewInputStreamId() = nextInputStreamId.getAndIncrement()
    功能: 获取新的DStreamID编号
    
    def withScope[U](body: => U): U = sparkContext.withScope(body)
    功能: 执行代码块@body,以便于DStream可以在body中设置运行逻辑
    
    def withNamedScope[U](name: String)(body: => U): U
    功能: 执行代码块@body,以便于DStream可以在body中设置运行逻辑,更多信息参考@doCompute
    val= RDDOperationScope.withScope(
        sc, name, allowNesting = false, ignoreParent = false)(body)
    
    def receiverStream[T: ClassTag](receiver: Receiver[T]): ReceiverInputDStream[T]
    功能: 获取接收器的输入流@ReceiverInputDStream
    val= withNamedScope("receiver stream") {
      new PluggableInputDStream[T](this, receiver)
    }
    
    def socketTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] 
    功能: 由TCP资源(主机+端口号)创建输入流,数据接受TCP socket,且接受数据转换为UTF-8编码.
    val= withNamedScope("socket text stream") {
        socketStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel)
      }
    
    def socketStream[T: ClassTag](
      hostname: String,
      port: Int,
      converter: (InputStream) => Iterator[T],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[T]
    功能: 获取socket流
    val= new SocketInputDStream[T](this, hostname, port, converter, storageLevel)
    
    def rawSocketStream[T: ClassTag](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[T]
    功能: 由网络资源(主机+端口号)创建输入流,数据按照序列化数据块接受,可以直接转化为数据块管理器,不需要反序列化,这是接受数据最高效的方式.
    val= new RawInputDStream[T](this, hostname, port, storageLevel)
    
    def fileStream[
    K: ClassTag,
    V: ClassTag,
    F <: NewInputFormat[K, V]: ClassTag
  	] (directory: String): InputDStream[(K, V)]
    功能: 创建一个监视hadoop文件系统的输入流,用于监视新文件,和使用kv类型读取数据,输入类型必须写入到监视的目录中,以.开头的文件会被忽视.
    输入类型:
    	K	HDFS文件key类型
    	V	HDFS文件value类型
    	F	HDFS输入文件类型
    val= new FileInputDStream[K, V, F](this, directory)
    
    def fileStream[
    K: ClassTag,
    V: ClassTag,
    F <: NewInputFormat[K, V]: ClassTag
 	 ] (directory: String, filter: Path => Boolean, newFilesOnly: Boolean): InputDStream[(K, V)]
    功能:同上
    输入属性:
    directory	监视新文件的HDFS目录
    filter	过滤路径的函数
    newFilesOnly	是否只传递新文件,且忽视目录中已存在的文件
    val= new FileInputDStream[K, V, F](this, directory, filter, newFilesOnly)
    
    def fileStream[
    K: ClassTag,
    V: ClassTag,
    F <: NewInputFormat[K, V]: ClassTag
  	] (directory: String,
     filter: Path => Boolean,
     newFilesOnly: Boolean,
     conf: Configuration): InputDStream[(K, V)]
    功能: 同上
    输入参数:
    	conf	hadoop配置
    val= new FileInputDStream[K, V, F](
        this, directory, filter, newFilesOnly, Option(conf))
    
    def textFileStream(directory: String): DStream[String] 
    功能: 创建监视hadoop文件系统的输入流,主要用于新文件,使用text文件读取(key->LongWritable,value->Text,输入类型-> TextInputFormat).文件必须写入到监视的文件系统中.以.开头的文件会被忽视.必须使用UTF-8编码.
    val= fileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
    
    def binaryRecordsStream(
      directory: String,
      recordLength: Int): DStream[Array[Byte]]
    功能: 创建监视hadoop文件系统的输入流,主用用于新文件,使用二进制文件读取,假定每个记录定长,每个记录都产生字节数组.文件必须要写入到监视目录中,通过将其移动到文件系统的其他位置的方式.以.开头的文件会被忽略.
    val= withNamedScope("binary records stream") {
        val conf = _sc.hadoopConfiguration
        conf.setInt(FixedLengthBinaryInputFormat.RECORD_LENGTH_PROPERTY, recordLength)
        val br = fileStream[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](
          directory, FileInputDStream.defaultFilter: Path => 
            Boolean, newFilesOnly = true, conf)
        br.map { case (k, v) =>
          val bytes = v.copyBytes()
          require(bytes.length == recordLength, "Byte array does not 
          have correct length. " +
            s"${bytes.length} did not equal recordLength: $recordLength")
          bytes
        }
      }
    
    def queueStream[T: ClassTag](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean = true
    ): InputDStream[T] 
    功能: 创建指定RDD队列@queue 的输入流,每个批次中,可以有一个或者多个返回
    val= queueStream(queue, oneAtATime, sc.makeRDD(Seq.empty[T], 1))
    
    def queueStream[T: ClassTag](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean,
      defaultRDD: RDD[T]
    ): InputDStream[T]
    功能: 同上
    val= new QueueInputDStream(this, queue, oneAtATime, defaultRDD)
    
    def union[T: ClassTag](streams: Seq[DStream[T]]): DStream[T]
    功能: 创建多个DStream的联合DStream
    val=new UnionDStream[T](streams.toArray)
    
    def transform[T: ClassTag](
      dstreams: Seq[DStream[_]],
      transformFunc: (Seq[RDD[_]], Time) => RDD[T]
    ): DStream[T]
    功能: 使用转换函数@transformFunc 转换指定的@dstreams列表
    val= new TransformedDStream[T](dstreams, sparkContext.clean(transformFunc))
    
    def addStreamingListener(streamingListener: StreamingListener): Unit
    功能: 添加流式监听器,用于接收流式系统事件
    scheduler.listenerBus.addListener(streamingListener)
    
    def removeStreamingListener(streamingListener: StreamingListener): Unit
    功能: 移除指定流式监听器
    scheduler.listenerBus.removeListener(streamingListener)
    
    def validate(): Unit 
    功能: 参数校验
    1. DStream图校验
    assert(graph != null, "Graph is null")
    graph.validate()
    2. 检查点校验
    require(
      !isCheckpointingEnabled || checkpointDuration != null,
      "Checkpoint directory has been set, but the graph checkpointing interval has " +
        "not been set. Please use StreamingContext.checkpoint() to set the interval."
    )
    if (isCheckpointingEnabled) {
      val checkpoint = new Checkpoint(this, Time(0))
      try {
        Checkpoint.serialize(checkpoint, conf)
      } catch {
        case e: NotSerializableException =>
          throw new NotSerializableException(
            "DStream checkpointing has been enabled but the DStreams 
            with their functions " +
              "are not serializable\n" +
              SerializationDebugger.improveException(checkpoint, e).getMessage()
          )
      }
    }
    3. 提示动态分配信息
    if (Utils.isDynamicAllocationEnabled(sc.conf) ||
        ExecutorAllocationManager.isDynamicAllocationEnabled(conf)) {
      logWarning("Dynamic Allocation is enabled for this application. " +
        "Enabling Dynamic allocation for Spark Streaming applications 
        can cause data loss if " +
        "Write Ahead Log is not enabled for non-replayable sources. " +
        "See the programming guide for details on how to enable the Write Ahead Log.")
    }
    
    @DeveloperApi
    def getState(): StreamingContextState
    功能: 获取streaming上下文状态信息
    val= synchronized { state }
    
    def awaitTermination(): Unit 
    功能: 等待执行结束
    waiter.waitForStopOrError()
    
    def awaitTerminationOrTimeout(timeout: Long): Boolean 
    功能: 等待执行结束或者超过指定时间@timeout
    val= waiter.waitForStopOrError(timeout)
    
    def stop(
      stopSparkContext: Boolean =
        conf.getBoolean("spark.streaming.stopSparkContextByDefault", true)
     ): Unit
    功能: 立刻停止streaming的执行,默认状态下@stopSparkContext没有指定,底层的sparkContext也会被关闭,可以使用@spark.streaming.stopSparkContextByDefault配置实现行为.
    stop(stopSparkContext, false)
    
    def start(): Unit
    功能: 启动stream的执行,只有@INITIALIZED 状态才需要启动
    synchronized {
        state match {
          case INITIALIZED =>
            startSite.set(DStream.getCreationSite())
            StreamingContext.ACTIVATION_LOCK.synchronized {
              StreamingContext.assertNoOtherContextIsActive()
              try {
                validate()
                registerProgressListener()
                ThreadUtils.runInNewThread("streaming-start") {
                  sparkContext.setCallSite(startSite.get)
                  sparkContext.clearJobGroup()
                  sparkContext.setLocalProperty(
                      SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
                savedProperties.set(Utils.cloneProperties(
                    sparkContext.localProperties.get()))
                  scheduler.start()
                }
                state = StreamingContextState.ACTIVE
                scheduler.listenerBus.post(
                  StreamingListenerStreamingStarted(System.currentTimeMillis()))
              } catch {
                case NonFatal(e) =>
                  logError("Error starting the context, marking it as stopped", e)
                  scheduler.stop(false)
                  state = StreamingContextState.STOPPED
                  throw e
              }
              StreamingContext.setActiveContext(this)
            }
            logDebug("Adding shutdown hook") // force eager creation of logger
            shutdownHookRef = ShutdownHookManager.addShutdownHook(
              StreamingContext.SHUTDOWN_HOOK_PRIORITY)(() => stopOnShutdown())
            assert(env.metricsSystem != null)
            env.metricsSystem.registerSource(streamingSource)
            uiTab.foreach(_.attach())
            logInfo("StreamingContext started")
          case ACTIVE =>
            logWarning("StreamingContext has already been started")
          case STOPPED =>
            throw new IllegalStateException("StreamingContext has already been stopped")
        }
      }
    
    def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit
    功能: 停止stream,可以选择@stopSparkContext使用关闭底层的sparkContext,和选择是否选择接受完当前批次所有数据再关闭@stopGracefully
    1. 校验线程监听器
    var shutdownHookRefToRemove: AnyRef = null
    if (LiveListenerBus.withinListenerThread.value) {
      throw new SparkException(s"Cannot stop StreamingContext 
      within listener bus thread.")
    }
    2. 关闭stream,只有ACTIVE状态下才能关闭应用
    synchronized {
      state match {
        case INITIALIZED =>
          logWarning("StreamingContext has not been started yet")
          state = STOPPED
        case STOPPED =>
          logWarning("StreamingContext has already been stopped")
          state = STOPPED
        case ACTIVE =>
          Utils.tryLogNonFatalError {
            scheduler.stop(stopGracefully)
          }
          Utils.tryLogNonFatalError {
            env.metricsSystem.removeSource(streamingSource)
          }
          Utils.tryLogNonFatalError {
            uiTab.foreach(_.detach())
          }
          Utils.tryLogNonFatalError {
            unregisterProgressListener()
          }
          StreamingContext.setActiveContext(null)
          Utils.tryLogNonFatalError {
            waiter.notifyStop()
          }
          if (shutdownHookRef != null) {
            shutdownHookRefToRemove = shutdownHookRef
            shutdownHookRef = null
          }
          logInfo("StreamingContext stopped successfully")
          state = STOPPED
      }
    }
    3. 移除当前关闭点
    if (shutdownHookRefToRemove != null) {
      ShutdownHookManager.removeShutdownHook(shutdownHookRefToRemove)
    }
    4. 停止底层sparkContext
    if (stopSparkContext) sc.stop()
    
    def stopOnShutdown(): Unit
    功能: 关闭时停止stream
    val stopGracefully = conf.getBoolean("spark.streaming.stopGracefullyOnShutdown"
                                         , false)
    logInfo(s"Invoking stop(stopGracefully=$stopGracefully) from shutdown hook")
    stop(stopSparkContext = false, stopGracefully = stopGracefully)
    
    def registerProgressListener(): Unit
    功能: 注册进程监视器
    addStreamingListener(progressListener)
    sc.addSparkListener(progressListener)
    sc.ui.foreach(_.setStreamingJobProgressListener(progressListener))
    
    def unregisterProgressListener(): Unit 
    功能: 解除进程监视器的注册
    removeStreamingListener(progressListener)
    sc.removeSparkListener(progressListener)
    sc.ui.foreach(_.clearStreamingJobProgressListener())
}
```

```scala
object StreamingContext extends Logging {
    介绍: 包含相关的实用方法
    属性:
    #name @ACTIVATION_LOCK = new Object()	激活锁
    #name @SHUTDOWN_HOOK_PRIORITY 	关闭点优先级
    val= ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY + 1
    #name @activeContext = new AtomicReference[StreamingContext](null)	激活上下文
    操作集:
    def assertNoOtherContextIsActive(): Unit 
    功能: 没有其他上下文激活断言
    ACTIVATION_LOCK.synchronized {
      if (activeContext.get() != null) {
        throw new IllegalStateException(
          "Only one StreamingContext may be started in this JVM. " +
            "Currently running StreamingContext was started at" +
            activeContext.get.getStartSite().longForm)
      }
    }
    
    def setActiveContext(ssc: StreamingContext): Unit
    功能: 设置激活上下文
    ACTIVATION_LOCK.synchronized {
      activeContext.set(ssc)
    }
    
    def getActive(): Option[StreamingContext]
    功能: 获取激活上下文,如果有一个,激活意味着启动但是不会停止
    val= ACTIVATION_LOCK.synchronized {
      Option(activeContext.get())
    }
    
    def getActiveOrCreate(creatingFunc: () => StreamingContext): StreamingContext
    功能: 获取激活的streamingContext,没有则会创建@creatingFunc
    ACTIVATION_LOCK.synchronized {
      getActive().getOrElse { creatingFunc() }
    }
    
    def getActiveOrCreate(
      checkpointPath: String,
      creatingFunc: () => StreamingContext,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
    ): StreamingContext
    功能:同上,当检查点数据不存在的时候,使用创建函数创建上下文
    val= ACTIVATION_LOCK.synchronized {
      getActive().getOrElse { getOrCreate(
          checkpointPath, creatingFunc, hadoopConf, createOnError) }
    }
    
    def getOrCreate(
      checkpointPath: String,
      creatingFunc: () => StreamingContext,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
    ): StreamingContext 
    功能: 同上
    1. 读取检查点配置
    val= val checkpointOption = CheckpointReader.read(
      checkpointPath, new SparkConf(), hadoopConf, createOnError)
    2. 获取上下文
    val= checkpointOption.map(
        new StreamingContext(null, _, null)).getOrElse(creatingFunc())
    
    def jarOfClass(cls: Class[_]): Option[String] = SparkContext.jarOfClass(cls)
    功能: 获取指定类的jar
    
    def createNewSparkContext(conf: SparkConf): SparkContext
    功能: 由指定spark配置创建spark上下文
    val= new SparkContext(conf)
    
    def createNewSparkContext(
      master: String,
      appName: String,
      sparkHome: String,
      jars: Seq[String],
      environment: Map[String, String]
    ): SparkContext
    功能: 获取spark上下文
    val conf = SparkContext.updatedConf(
      new SparkConf(), master, appName, sparkHome, jars, environment)
    val= new SparkContext(conf)
    
    def rddToFileName[T](prefix: String, suffix: String, time: Time): String
    功能: 获取文件名称为前缀+时间+后缀
    var result = time.milliseconds.toString
    if (prefix != null && prefix.length > 0) {
      result = s"$prefix-$result"
    }
    if (suffix != null && suffix.length > 0) {
      result = s"$result.$suffix"
    }
    val= result
}
```

```scala
private class StreamingContextPythonHelper {
    def tryRecoverFromCheckpoint(checkpointPath: String): Option[StreamingContext] 
    功能: python创建streaming上下文的方法
    val checkpointOption = CheckpointReader.read(
      checkpointPath, new SparkConf(), SparkHadoopUtil.get.conf, ignoreReadError = false)
    val= checkpointOption.map(new StreamingContext(null, _, null))
}
```

#### StreamingSource

```scala
private[streaming] class StreamingSource(ssc: StreamingContext) extends Source {
    介绍: streaming资源类
    构造器参数:
    	ssc	streaming上下文
    属性:
    #name @metricRegistry = new MetricRegistry	度量注册器
    #name @sourceName = "%s.StreamingMetrics".format(ssc.sparkContext.appName)	资源名称
    #name @streamingListener = ssc.progressListener	streaming监听器
    操作集:
    def registerGauge[T](name: String, f: StreamingJobProgressListener => T,
      defaultValue: T): Unit
    功能: 注册参数 name --> f/defaultValue
    registerGaugeWithOption[T](name,
      (l: StreamingJobProgressListener) => Option(f(streamingListener)), defaultValue)
    
    def registerGaugeWithOption[T](
      name: String,
      f: StreamingJobProgressListener => Option[T],
      defaultValue: T): Unit
    功能: 同上
    metricRegistry.register(MetricRegistry.name("streaming", name), new Gauge[T] {
      override def getValue: T = f(streamingListener).getOrElse(defaultValue)
    })
    
    初始化操作:
    registerGauge("receivers", _.numReceivers, 0)
    功能: 注册网络接收器数量
    
    registerGauge("totalCompletedBatches", _.numTotalCompletedBatches, 0L)
    功能: 注册完成的批次数量
    
    registerGauge("totalReceivedRecords", _.numTotalReceivedRecords, 0L)
    功能: 注册接受的记录数量
    
    registerGauge("totalProcessedRecords", _.numTotalProcessedRecords, 0L)
    功能: 注册进行中的记录数量
    
    registerGauge("unprocessedBatches", _.numUnprocessedBatches, 0L)
    功能: 注册未进行的记录数量
    
    registerGauge("waitingBatches", _.waitingBatches.size, 0L)
    功能: 注册等待的批次数量
    
    registerGauge("runningBatches", _.runningBatches.size, 0L)
    功能: 注册运行的批次数量
    
    registerGauge("retainedCompletedBatches", _.retainedCompletedBatches.size, 0L)
    功能: 注册剩余需要完成的数量
    
    registerGaugeWithOption("lastCompletedBatch_submissionTime",
        _.lastCompletedBatch.map(_.submissionTime), -1L)
    registerGaugeWithOption("lastCompletedBatch_processingStartTime",
        _.lastCompletedBatch.flatMap(_.processingStartTime), -1L)
    registerGaugeWithOption("lastCompletedBatch_processingEndTime",
        _.lastCompletedBatch.flatMap(_.processingEndTime), -1L)
    功能: 注册上次完成的提交时间/进程开始时间/进程结束时间
    
    registerGaugeWithOption("lastCompletedBatch_processingDelay",
        _.lastCompletedBatch.flatMap(_.processingDelay), -1L)
    registerGaugeWithOption("lastCompletedBatch_schedulingDelay",
        _.lastCompletedBatch.flatMap(_.schedulingDelay), -1L)
    registerGaugeWithOption("lastCompletedBatch_totalDelay",
        _.lastCompletedBatch.flatMap(_.totalDelay), -1L)
    功能: 注册上次完成批次的进程延时,调度眼熟,总计延时
    
    registerGaugeWithOption("lastReceivedBatch_submissionTime",
        _.lastReceivedBatch.map(_.submissionTime), -1L)
    registerGaugeWithOption("lastReceivedBatch_processingStartTime",
        _.lastReceivedBatch.flatMap(_.processingStartTime), -1L)
    registerGaugeWithOption("lastReceivedBatch_processingEndTime",
        _.lastReceivedBatch.flatMap(_.processingEndTime), -1L)
    功能: 注册上个接收批次的提交时间/进程开始时间/进程结束时间
    
    registerGauge("lastReceivedBatch_records", _.lastReceivedBatchRecords.values.sum, 0L)
    功能: 注册上批次接收的记录数量
}
```

#### Time

```markdown
介绍:
	这个类代表绝对时间,使用毫秒计数,可以按照不同的时间区域进行转换.起始时间为January 1, 1970 UTC.
```

```scala
case class Time(private val millis: Long) {
    def milliseconds: Long = millis
    功能: 获取毫秒数
    
    def < (that: Time): Boolean = (this.millis < that.millis)
    def <= (that: Time): Boolean = (this.millis <= that.millis)
    def > (that: Time): Boolean = (this.millis > that.millis)
    def >= (that: Time): Boolean = (this.millis >= that.millis)
    def less(that: Time): Boolean = this < that
    def lessEq(that: Time): Boolean = this <= that
    def greater(that: Time): Boolean = this > that
    def greaterEq(that: Time): Boolean = this >= that
    功能: 时间比较运算
    
    def + (that: Duration): Time = new Time(millis + that.milliseconds)
    def - (that: Time): Duration = new Duration(millis - that.millis)
    def - (that: Duration): Time = new Time(millis - that.milliseconds)
    def plus(that: Duration): Time = this + that
    def minus(that: Time): Duration = this - that
    def minus(that: Duration): Time = this - 
    功能: 时间加减运算
    
    def floor(that: Duration): Time
    功能: 时间向下取整
    val t = that.milliseconds
    val= new Time((this.millis / t) * t)
   
    def floor(that: Duration, zeroTime: Time): Time
    功能: 同上
    val t = that.milliseconds
    val= new Time(((
        this.millis - zeroTime.milliseconds) / t) * t + zeroTime.milliseconds)
    
    def min(that: Time): Time = if (this < that) this else that
    def max(that: Time): Time = if (this > that) this else that
    功能: 时间的较小/较大值
    
    def isMultipleOf(that: Duration): Boolean 
    功能: 判断时间倍数关系
    val= (this.millis % that.milliseconds == 0)
    
    def until(that: Time, interval: Duration): Seq[Time]
    功能: 均匀获取时间点
    val= (this.milliseconds) until 
    	(that.milliseconds) by (interval.milliseconds) map (new Time(_))
    
    def to(that: Time, interval: Duration): Seq[Time] 
    功能: 时间均匀取点
    val= (this.milliseconds) to (
        that.milliseconds) by (interval.milliseconds) map (new Time(_))
    
    def toString: String = (millis.toString + " ms")
    功能: 信息显示                                                    
}
```

```scala
object Time {
  implicit val ordering = Ordering.by((time: Time) => time.millis)
    数据排序规则
}
```

#### 基础拓展

1.  scala中to和until的区别