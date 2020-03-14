1. [ConstantInputDStream.scala](# ConstantInputDStream)
2. [DStream.scala](# DStream)
3. [DStreamCheckpointData.scala](DStreamCheckpointData)
4. [FileInputDStream.scala](# FileInputDStream)
5. [FilteredDStream.scala](# FilteredDStream)
6. [FlatMappedDStream.scala](# FlatMappedDStream)
7. [FlatMapValuedDStream.scala](# FlatMapValuedDStream)
8. [ForEachRDD.scala](# ForEachRDD)
9. [GlommedDStream.scala](# GlommedDStream)
10. [InputDStream.scala](# InputDStream)
11. [MapPartitionedDStream.scala](# MapPartitionedDStream)
12. [MappedDStream.scala](# MappedDStream)

---

#### ConstantInputDStream

```scala
class ConstantInputDStream[T: ClassTag](_ssc: StreamingContext, rdd: RDD[T])
extends InputDStream[T](_ssc) {
    介绍: 输入流,每次执行总是返回同一个RDD,用于测试
    构造器参数:
    	_ssc	streaming上下文
    	rdd	RDD
    初始化操作:
    require(rdd != null,
            "parameter rdd null is illegal, which will lead to NPE in 
            the following transformation")
    功能: RDD校验
    操作集:
    def start(): Unit = {}
    def stop(): Unit = {}
    功能: 常数输入流的起始/停止
    
    def compute(validTime: Time): Option[RDD[T]]
    功能: 计算指定时间的RDD(为常数)
    val= Some(rdd)
}
```

#### DStream

```markdown
介绍:
 	离散流,spark streaming的基本抽象,是连续RDD序列(同一个类型的RDD),代表着连续流类型的数据(参考spark core中对RDD的描述).离散流既可以使用@StreamingContext从存活的RDD中创建(例如,TCP socket,Kafka).或者可以从已经存在的离散流中创建,使用`map`,`window`或者`reduceByKeyAndWindow`创建.当spark streaming程序创建的时候,每个离散流周期性的产生RDD,或者从父离散流中转换RDD.
    这个类中包含离散流的基本操作,比如`map`,`filter`,和`windows`.此外@PairDStreamFunctions中包含kv对的离散流的操作,比如`groupByKeyAndWindow`和`join`.这个操作可以在任何离散流的kv对中自动获取.
 	离散流内部使用下述基本属性描述:
 	1. 当前离散流所依赖的离散流
 	2. 离散流所生成的时间间隔
 	3. 每个时间间隔用于生成RDD的函数
```

```scala
abstract class DStream[T: ClassTag] (
    @transient private[streaming] var ssc: StreamingContext
  ) extends Serializable with Logging {
    
}
```

#### DStreamCheckpointData

```scala
private[streaming]
class DStreamCheckpointData[T: ClassTag](dstream: DStream[T])
extends Serializable with Logging {
    介绍: 离散流检查点数据
    构造器参数:
    	dstream	离散流
    属性:
    #name @data = new HashMap[Time, AnyRef]()	时间映射的数据表
    #name @timeToCheckpointFile = new HashMap[Time, String]	时间->检查点文件映射表
    #name @timeToOldestCheckpointFileTime = new HashMap[Time, Time]	
    	时间->最初检查点文件时间映射
    操作集:
    def currentCheckpointFiles = data.asInstanceOf[HashMap[Time, String]]
    功能: 获取当前检查点文件的时间--> 检查点文件映射
    
    def update(time: Time): Unit
    功能: 更新离散流的检查点数据,每次检查点初始化的时候,这个方法就会被调用.默认实现记录了离散流生成的RDD对应的检查点文件.
    1. 从生成的RDD中获取检查点RDD
    val checkpointFiles = dstream.generatedRDDs.filter(_._2.getCheckpointFile.isDefined)
                                       .map(x => (x._1, x._2.getCheckpointFile.get))
    logDebug("Current checkpoint files:\n" + checkpointFiles.toSeq.mkString("\n"))
    2. 添加检查点文件,这些文件的数据需要被序列化
    if (!checkpointFiles.isEmpty) {
      currentCheckpointFiles.clear()
      currentCheckpointFiles ++= checkpointFiles
        // 添加当前检查点文件到所有检查点文件映射表中
      timeToCheckpointFile ++= currentCheckpointFiles
        // 记录当前状态的最早检查点RDD时间
      timeToOldestCheckpointFileTime(time) =
        currentCheckpointFiles.keys.min(Time.ordering)
    }
    
    def cleanup(time: Time): Unit 
    功能: 清除旧有的检查点数据,在检查点的时间写入到检查点目录中
    timeToOldestCheckpointFileTime.remove(time) match {
      case Some(lastCheckpointFileTime) =>
        val filesToDelete = timeToCheckpointFile.filter(_._1 < lastCheckpointFileTime)
        logDebug("Files to delete:\n" + filesToDelete.mkString(","))
        var fileSystem: FileSystem = null
        filesToDelete.foreach {
          case (time, file) =>
            try {
              val path = new Path(file)
              if (fileSystem == null) {
                fileSystem =
                  path.getFileSystem(dstream.ssc.sparkContext.hadoopConfiguration)
              }
              if (fileSystem.delete(path, true)) {
                logInfo("Deleted checkpoint file '" + file + "' for time " + time)
              } else {
                logWarning(s"Error deleting old checkpoint file '$file' for time $time")
              }
              timeToCheckpointFile -= time
            } catch {
              case e: Exception =>
                logWarning("Error deleting old checkpoint file '
                " + file + "' for time " + time, e)
                fileSystem = null
            }
        }
      case None =>
        logDebug("Nothing to delete")
    }
    
    def restore(): Unit
    功能: 恢复检查点数据,每次当离散流图从图检查点文件中恢复的时候就会被调用,默认实现了检查点目录中的RDD恢复功能.
    // 从检查点数据中创建RDD,并添加到离散流中(=> 检查点数据的恢复)
    currentCheckpointFiles.foreach {
      case(time, file) =>
        logInfo("Restoring checkpointed RDD for time " + time + " 
        from file '" + file + "'")
        dstream.generatedRDDs += (
            (time, dstream.context.sparkContext.checkpointFile[T](file)))
    }
    
    def toString: String 
    功能: 信息显示
    val= "[\n" + currentCheckpointFiles.size + " checkpoint files \n" +
      currentCheckpointFiles.mkString("\n") + "\n]"
    
    @throws(classOf[IOException])
    private def writeObject(oos: ObjectOutputStream): Unit
    功能: 序列化
    Utils.tryOrIOException {
        logDebug(this.getClass().getSimpleName + ".writeObject used")
        if (dstream.context.graph != null) {
          dstream.context.graph.synchronized {
            if (dstream.context.graph.checkpointInProgress) {
              oos.defaultWriteObject()
            } else {
              val msg = "Object of " + this.getClass.getName + " is being serialized " +
                " possibly as a part of closure of an RDD operation. This is because " +
                " the DStream object is being referred to from within the closure. " +
                " Please rewrite the RDD operation inside this DStream to avoid this. " +
                " This has been enforced to avoid bloating of Spark tasks " +
                " with unnecessary objects."
              throw new java.io.NotSerializableException(msg)
            }
          }
        } else {
          throw new java.io.NotSerializableException(
            "Graph is unexpectedly null when DStream is being serialized.")
        }
    }
    
    @throws(classOf[IOException])
    private def readObject(ois: ObjectInputStream): Unit
    功能: 反序列化
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    timeToOldestCheckpointFileTime = new HashMap[Time, Time]
    timeToCheckpointFile = new HashMap[Time, String]
}
```

#### FileInputDStream

#### FilteredDStream

```scala
private[streaming]
class FilteredDStream[T: ClassTag](
    parent: DStream[T],
    filterFunc: T => Boolean
) extends DStream[T](parent.ssc) {
    介绍: 带有过滤功能的离散流
    构造器:
        parent	离散流
        filterFunc	过滤函数
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期(等于父离散流滑动周期)
    
    def compute(validTime: Time): Option[RDD[T]] 
    功能: 计算指定时间离散流中的RDD
    val= parent.getOrCompute(validTime).map(_.filter(filterFunc))
}
```

#### FlatMappedDStream

```scala
private[streaming]
class FlatMappedDStream[T: ClassTag, U: ClassTag](
    parent: DStream[T],
    flatMapFunc: T => TraversableOnce[U]
) extends DStream[U](parent.ssc) {
    介绍: 带有flatmap功能的离散流
    构造器参数:
    parent	父级离散流
    flatMapFunc	flatMap处理函数(拆解函数)
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]] 
    功能: 计算指定时间离散流中的RDD
    val= parent.getOrCompute(validTime).map(_.flatMap(flatMapFunc))
}
```

#### FlatMapValuedDStream

```scala
private[streaming]
class FlatMapValuedDStream[K: ClassTag, V: ClassTag, U: ClassTag](
    parent: DStream[(K, V)],
    flatMapValueFunc: V => TraversableOnce[U]
) extends DStream[(K, U)](parent.ssc) {
    介绍: 带有flatMapValue功能的离散流
    构造器参数:
    	parent	父级离散流
    	flatMapValueFunc	flatMapValue处理函数
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[(K, U)]]
    功能: 计算指定时间离散流中的RDD
}
```

#### ForEachDStream

```scala
private[streaming]
class ForEachDStream[T: ClassTag] (
    parent: DStream[T],
    foreachFunc: (RDD[T], Time) => Unit,
    displayInnerRDDOps: Boolean
) extends DStream[Unit](parent.ssc) {
    构造器参数:
    parent	父级离散流
    foreachFunc	foreach函数
    displayInnerRDDOps	是否RDD的具体操作位置会被汇报给webUI
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[Unit]] = None
    功能: 计算指定时间离散流的RDD
    
    def generateJob(time: Time): Option[Job] 
    功能: 生成指定时间的job
    val= parent.getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => createRDDWithLocalProperties(time, displayInnerRDDOps) {
          foreachFunc(rdd, time)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
}
```

#### GlommedDStream

```scala
private[streaming]
class GlommedDStream[T: ClassTag](parent: DStream[T]){
    介绍: 无功能的离散流
    构造器参数:
    parent	父级离散流
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[Array[T]]]
    功能: 计算指定时间离散流的RDD
    val= parent.getOrCompute(validTime).map(_.glom())
}
```

#### InputDStream

#### MapPartitionedDStream

```scala
private[streaming]
class MapPartitionedDStream[T: ClassTag, U: ClassTag](
    parent: DStream[T],
    mapPartFunc: Iterator[T] => Iterator[U],
    preservePartitioning: Boolean
) extends DStream[U](parent.ssc) {
    介绍: 带有mapPartition功能的离散流
    构造器参数:
    parent	父级RDD
    mapPartFunc	mapPartition函数
    preservePartitioning	是否保留分区
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]] 
    功能: 计算指定时间离散流的RDD
    val=parent.getOrCompute(validTime).map(
        _.mapPartitions[U](mapPartFunc, preservePartitioning))
}
```

#### MappedDStream

```scala
private[streaming]
class MappedDStream[T: ClassTag, U: ClassTag] (
    parent: DStream[T],
    mapFunc: T => U
) extends DStream[U](parent.ssc) {
    介绍: 带有map功能的离散流
    构造器参数:
    parent	父级离散流
    mapFunc	map映射函数
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]]
    功能: 计算指定时间离散流的RDD
    val= parent.getOrCompute(validTime).map(_.map[U](mapFunc))
}
```

#### MapValuedDStream

```scala
private[streaming]
class MapValuedDStream[K: ClassTag, V: ClassTag, U: ClassTag](
    parent: DStream[(K, V)],
    mapValueFunc: V => U
) extends DStream[(K, U)](parent.ssc) {
    介绍: 带有mapValue功能的离散流
    构造器参数:
    parent	父级离散流
    mapValueFunc	mapValue
    操作集:
    def dependencies: List[DStream[_]] = List(parent)
    功能: 获取依赖列表
    
    def slideDuration: Duration = parent.slideDuration
    功能: 获取滑动周期
    
    def compute(validTime: Time): Option[RDD[U]]
    功能: 计算指定时间离散流的RDD
    val= parent.getOrCompute(validTime).map(_.mapValues[U](mapValueFunc))
}
```

#### MapWithStateDStream

#### PairDStreamFunctions

#### PluggableInputDStream

```scala
private[streaming]
class PluggableInputDStream[T: ClassTag](
  _ssc: StreamingContext,
  receiver: Receiver[T]) extends ReceiverInputDStream[T](_ssc) {
    介绍: 可插入离散输入流
    操作集:
    def getReceiver(): Receiver[T] = receiver
    功能: 获取接收器
}
```

#### QueueInputDStream

```scala
private[streaming]
class QueueInputDStream[T: ClassTag](
    ssc: StreamingContext,
    val queue: Queue[RDD[T]],
    oneAtATime: Boolean,
    defaultRDD: RDD[T]
) extends InputDStream[T](ssc) {
    介绍: 队列输入离散流
    
```

