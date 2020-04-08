## **Spark-Storage**

---

@TimeTrackingOutputStream

1. 使用输出流输出数据

2. 计算输出流写出所需要的时间

```java
数据元素:
	private final ShuffleWriteMetricsReporter writeMetrics
		这是一个scala的接口,假设了所有的方法都是单线程调用的,因此写它的实现类的时候不需要同步.它的主要功能是报告洗牌操作@shuffle的度量值
		接口中的方法都是私有化的，但是都有对spark都有额外的可见性。重写的时候依旧有这个属性。
		比如说:
			private[spark] def incBytesWritten(v: Long): Unit
	private final OutputStream outputStream
		用于输出操作
操作集:
	初始化操作(构造器)void write(int b)
        
     public TimeTrackingOutputStream(
      ShuffleWriteMetricsReporter writeMetrics, OutputStream outputStream)
      	初始化指定的度量器，以及输出流
      1. 写操作
      + void write(int b)
		单字节写入，同时计算写入时间
	 + void write(byte[] b)
	 	多个字节写入，同时计算写入时间
	 + void write(byte[] b, int off, int len)
	 	多个字节带偏移量的写入，计算写入时间
	2. 刷新
		刷新写入数据，并计算写入时间
	3. 关流
		关流，并计算关流时间
```

3. **拓展**

   ```markdown
   1. scala的权限修饰符
   2. scala 累加器的设计
   ```

   