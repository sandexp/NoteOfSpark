1. [BatchedWriteAheadLog.scala](BatchedWriteAheadLog)
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

#### FileBasedWriteAheadLog

#### FileBasedWriteAheadLogRandomReader

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

#### RateLimitedOutputStream

#### RawTextHelper

#### RawTextSender

#### RecurringTimer

#### StateMap

#### WriteAheadLogUtils