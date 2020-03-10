#### **status**

---

##### org.apache.spark.status.api.v1.streaming.BatchStatus

```java
public enum BatchStatus {
    介绍: 批次状态
    COMPLETED	完成
    QUEUED	处于等待队列中
    PROCESSING	进行中
    操作集:
    public static BatchStatus fromString(String str) {
        return EnumUtil.parseIgnoreCase(BatchStatus.class, str);
    }
    功能: 获取批次状态
}
```

##### org.apache.spark.streaming.util.WriteAheadLog

```markdown
这个抽象类代表预先写日志,用于spark streaming去保存接受的数据(通过receiver).且将元数据联系到可靠的存储器上,以至于可以从驱动器的失败中恢复.可以参考spark文档获取更多的信息,主要是关于如何在自定义插入预先写日志.
```

```java
@org.apache.spark.annotation.DeveloperApi
public abstract class WriteAheadLog {
   public abstract WriteAheadLogRecordHandle write(ByteBuffer record, long time);
   功能: 
   写出记录到日志,并返回记录处理器@WriteAheadLogRecordHandle,包含了需要写回的重要信息.其中时间是用来索引记录的,以便可以在之后清除,主要这个抽象类的实现必须保证写出的数据的持久性和可读性(使用记录处理器).通过这个函数返回的时间来确保上述的性质.
   
   public abstract ByteBuffer read(WriteAheadLogRecordHandle handle);
   功能: 基于指定的记录处理器@WriteAheadLogRecordHandle 读取写出的记录
   
   public abstract Iterator<ByteBuffer> readAll();
   功能: 读取所有写出但是没有被清理的迭代器信息,并返回
   
   public abstract void clean(long threshTime, boolean waitForCompletion);
   功能: 清理所有早于推测时间@threshTime 的记录,可以等待到任务完成时删除
   
   public abstract void close();
   功能: 关闭日志并释放资源,必须具有幂等性
}
```

##### org.apache.spark.streaming.util.WriteAheadLogRecordHandle

```markdown
介绍:
	这个抽象类代表写出记录的处理策略,必须包含所有需要读取的记录,且可以返回@WriteAheadLog的实现类.
```

```java
@org.apache.spark.annotation.DeveloperApi
public abstract class WriteAheadLogRecordHandle implements java.io.Serializable {
}
```

##### org.apache.spark.streaming.StreamingContextState

```scala
@DeveloperApi
public enum StreamingContextState {
    介绍: 表示streamingContext的状态
    INITIALIZED,	
    初始化状态,上下文创建了,但是还没有开始,上下文中可以创建@DSreams,转换和输出操作
    
    ACTIVE,
    激活状态,上下文已经开始,但是还没有停止
    
    STOPPED
    停止状态,已经被停止但是不能再使用了
}
```

