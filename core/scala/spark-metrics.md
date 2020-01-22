## **spark-metrics**

---

1.  [sink](# sink)
2.  [source](# source)
3.  [ExecutorMetricType.scala](# ExecutorMetricType)
4.  [MetricsConfig.scala](# MetricsConfig)
5.  [MetricsSystem.scala](# MetricsSystem)

---

#### sink

1.  [ConsoleSink.scala](# ConsoleSink)

2.  [CsvSink.scala](# CsvSink)

3.  [GraphiteSink.scala](# GraphiteSink)

4.  [JmxSink.scala]($ JmxSink)

5.  [MetricsServlet.scala](# MetricsServlet)

6.  [PrometheusServlet.scala](# PrometheusServlet)

7.  [Sink.scala](# Sink)

8.  [Slf4jSink.scala](# Slf4jSink)

9.  [StatsdReporter.scala](# StatsdReporter)

10.  [StatsdSink.scala](# StatsdSink)

    ---

    #### ConsoleSink

    ```markdown
private[spark] class ConsoleSink(val property: Properties, val registry: MetricRegistry,
        securityMgr: SecurityManager){
	关系: father --> Sink
        构造器属性:
    	property	属性
        	registry	度量注册器
    	securityMgr	安全管理器
        属性:
    #name @CONSOLE_DEFAULT_PERIOD=10 		控制台默认周期
        #name @CONSOLE_DEFAULT_UNIT="SECONDS"	 控制台测度单位
    #name @CONSOLE_KEY_PERIOD="period"		控制台周期的key值
        #name @CONSOLE_KEY_UNIT	 控制台测度单元的key值
    #name @pollPeriod	测试周期
        	val=  Option(property.getProperty(CONSOLE_KEY_PERIOD)) match{
    		case Some(s) => s.toInt
        		case None => CONSOLE_DEFAULT_PERIOD
        	}
        #name @pollUnit 	测试单元(单位)
        	val= Option(property.getProperty(CONSOLE_KEY_UNIT)) match{
        		case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
        		case None => TimeUnit.valueOf(CONSOLE_DEFAULT_UNIT)
        	}
        #name @reporter #type @ConsoleReporter	控制台汇报器
    		val= ConsoleReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MILLISECONDS)
          			.convertRatesTo(TimeUnit.SECONDS).build()
    	初始化操作:
    	MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)
    	功能: 度量系统检测周期和单位设置的是否符合要求
    	
    	操作集:
    	def start(): Unit ={ reporter.start(pollPeriod, pollUnit) }
    	功能: 开始测试
    	
    	def stop(): Unit={reporter.stop()}
    	功能: 停止测度
    	
    	def report(): Unit = { reporter.report() }
    	功能: 汇报器汇报
    }
    ```
    
    #### CsvSink
    
    ```markdown
    private[spark] class CsvSink(val property: Properties, val registry: MetricRegistry,
        securityMgr: SecurityManager){
    	关系: father --> Sink
        构造器属性:
        	property	属性
        	registry	度量注册器
        	securityMgr	安全管理器
        属性:
        #name @CSV_KEY_PERIOD="period"	周期key值
        #name @CSV_KEY_UNIT = "unit"	单位key值
        #name @CSV_KEY_DIR = "directory"	目录key值
        #name @CSV_DEFAULT_PERIOD = 10	周期默认值
        #name @CSV_DEFAULT_UNIT = "SECONDS"	单位默认值
        #Name @CSV_DEFAULT_DIR = "/tmp/"	csv默认目录
        #name @pollPeriod 	测试周期
        	val= Option(property.getProperty(CSV_KEY_PERIOD)) match {
        		case Some(s) => s.toInt
        		case None => CSV_DEFAULT_PERIOD}
        #name @pollUnit #type @TimeUnit
        	val= Option(property.getProperty(CSV_KEY_UNIT)) match {
        		case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
        		case None => TimeUnit.valueOf(CSV_DEFAULT_UNIT) }
        #name @pollDir 
        	val= Option(property.getProperty(CSV_KEY_DIR)) match {
        		case Some(s) => s
        		case None => CSV_DEFAULT_DIR
      		}
      	#name @reporter #type @CsvReporter
      	val= CsvReporter.forRegistry(registry).formatFor(Locale.US)
          .convertDurationsTo(TimeUnit.MILLISECONDS).convertRatesTo(TimeUnit.SECONDS)
          .build(new File(pollDir))
    	
    	操作集:
    	def start(): Unit
    	功能: 开始以pollPeriod为周期的测试
    	
    	def stop(): Unit
    	功能: 停止测试
    	
    	def report(): Unit
    	功能: 汇报器汇报
    }
    ```
    
    #### GraphiteSink
    
    ```markdown
    private[spark] class GraphiteSink(val property: Properties, val registry: MetricRegistry,
        securityMgr: SecurityManager){
    	关系: father --> Sink
        构造器属性:
        	property	属性
        	registry	度量注册器
        	securityMgr	安全管理器
        属性:
        #name @GRAPHITE_DEFAULT_PERIOD=10
        #name @GRAPHITE_DEFAULT_UNIT = "SECONDS"
        #name @GRAPHITE_DEFAULT_PREFIX=""
        #name @GRAPHITE_KEY_HOST = "host"
        #name @GRAPHITE_KEY_PORT = "port"
        #name @GRAPHITE_KEY_PERIOD = "period"
        #name @GRAPHITE_KEY_UNIT = "unit"
        #name @GRAPHITE_KEY_PREFIX = "prefix"
        #name @GRAPHITE_KEY_PROTOCOL = "protocol"
        #name @GRAPHITE_KEY_REGEX = "regex"
        #name @host = propertyToOption(GRAPHITE_KEY_HOST).get	主机名称
        #name @port=propertyToOption(GRAPHITE_KEY_PORT).get.toInt	端口名称
        #name @pollPeriod	测试周期
       		val= propertyToOption(GRAPHITE_KEY_PERIOD) match {
                case Some(s) => s.toInt
                case None => GRAPHITE_DEFAULT_PERIOD }	
        #name @pollUnit #type @TimeUnit	测试时间单位
        	val= propertyToOption(GRAPHITE_KEY_UNIT) match {
                case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
                case None => TimeUnit.valueOf(GRAPHITE_DEFAULT_UNIT) }
        #name @prefix=propertyToOption(GRAPHITE_KEY_PREFIX).getOrElse(GRAPHITE_DEFAULT_PREFIX) 
        	前缀
        #name @graphite	协议
        	val= propertyToOption(GRAPHITE_KEY_PROTOCOL).map(_.toLowerCase(Locale.ROOT)) match {
            	case Some("udp") => new GraphiteUDP(host, port)
        		case Some("tcp") | None => new Graphite(host, port)
        		case Some(p) => throw new Exception(s"Invalid Graphite protocol: $p")
      		}
      	#name @filter	过滤器
      		val= propertyToOption(GRAPHITE_KEY_REGEX) match {
                case Some(pattern) => new MetricFilter() {
                  override def matches(name: String, metric: Metric): Boolean = {
                    pattern.r.findFirstMatchIn(name).isDefined
                  }
                }
                case None => MetricFilter.ALL
              }
        #name @reporter #type @GraphiteReporter	汇报器
       		val= GraphiteReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MILLISECONDS)
          		.convertRatesTo(TimeUnit.SECONDS).prefixedWith(prefix).filter(filter).build(graphite)
    	初始化操作
    	MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)
    	功能: 度量系统检测测试周期是否合法
    	
    	操作集:
    	def start(): Unit ={ reporter.start(pollPeriod, pollUnit) }
    	def stop(): Unit = { reporter.stop() }
    	def report(): Unit = { reporter.report() }
    }
    ```
    
    
    
    #### JmxSink
    
    ```markdown
    private[spark] class JmxSink(val property: Properties, val registry: MetricRegistry,
        securityMgr: SecurityManager){
    	关系: father --> 	Sink
        构造器属性:
            property	属性
            registry	度量注册器
            securityMgr	安全管理器
        属性:
        #name @reporter #type @JmxReporter	java扩展程序汇报器
        	val= JmxReporter.forRegistry(registry).build()
        操作集:
        def start(): Unit ={ reporter.start() }
        功能: 开始测试
        
        def stop(): Unit = { reporter.stop() }
        功能: 停止测试
        
        def report(): Unit = { }
        功能: 汇报(空实现)
    }
    ```
    
    #### MetricsServlet
    
    ```markdown
    private[spark] class MetricsServlet(val property: Properties,val registry: MetricRegistry,
    	securityMgr: SecurityManager){
    	关系: father --> Sink
        构造器属性:
        	property	属性
        	registry	度量注册器
        	securityMgr	安全管理器
        属性:
        #name @SERVLET_KEY_PATH	服务器路径的key值
        #name @SERVLET_KEY_SAMPLE	服务器sample的key值
        #name @SERVLET_DEFAULT_SAMPLE=false	服务器默认示例值
        #name @servletPath=property.getProperty(SERVLET_KEY_PATH)	服务器路径
        #name @servletShowSample	服务器显示示例
    			val=Option(property.getProperty(SERVLET_KEY_SAMPLE)).map(_.toBoolean)
    				.getOrElse(SERVLET_DEFAULT_SAMPLE)
    	#name @mapper	映射器
    	val=new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
        	TimeUnit.MILLISECONDS, servletShowSample))
    	操作集:
    	def start(): Unit = { }
    	def stop(): Unit = { }
    	def report(): Unit = { }
    	
    	def getHandlers(conf: SparkConf): Array[ServletContextHandler]
    	功能: 获取服务端上下文处理列表
    	val= Array[ServletContextHandler](
          createServletHandler(servletPath,
            new ServletParams(request => getMetricsSnapshot(request), "text/json"), conf))
            
         def getMetricsSnapshot(request: HttpServletRequest): String
         功能: 获取度量器副本
         val= mapper.writeValueAsString(registry)
    }
    ```
    
    #### PrometheusServlet
    
    ```markdown
    private[spark] class PrometheusServlet(val property: Properties,val registry: MetricRegistry,
        securityMgr: SecurityManager){
    	关系: father --> Sink
        属性:
        #name @SERVLET_KEY_PATH = "path"	服务端key值
        #name @servletPath=property.getProperty(SERVLET_KEY_PATH)	服务端地址
        操作集:
        def normalizeKey(key: String): String
        功能: key的格式化
        val= s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
        
        def getHandlers(conf: SparkConf): Array[ServletContextHandler] 
        功能: 获取服务端处理器列表
        val= Array[ServletContextHandler](createServletHandler(servletPath,
        	new ServletParams(request => getMetricsSnapshot(request), "text/plain"), conf)
        
        def getMetricsSnapshot(request: HttpServletRequest): String
        功能: 获取度量器副本
        1. 设置记录副本
        val sb = new StringBuilder()
        2. 记录估值信息
        registry.getGauges.asScala.foreach { case (k, v) =>
          if (!v.getValue.isInstanceOf[String]) {
            sb.append(s"${normalizeKey(k)}Value ${v.getValue}\n")
          }}
        3. 记录计数信息
        registry.getCounters.asScala.foreach { case (k, v) =>
          sb.append(s"${normalizeKey(k)}Count ${v.getCount}\n") }
    	4. 获取统计直方图(Histogram)信息
    	registry.getHistograms.asScala.foreach { case (k, h) =>
              val snapshot = h.getSnapshot
              val prefix = normalizeKey(k)
              sb.append(s"${prefix}Count ${h.getCount}\n")
              sb.append(s"${prefix}Max ${snapshot.getMax}\n")
              sb.append(s"${prefix}Mean ${snapshot.getMean}\n")
              sb.append(s"${prefix}Min ${snapshot.getMin}\n")
              sb.append(s"${prefix}50thPercentile ${snapshot.getMedian}\n")
              sb.append(s"${prefix}75thPercentile ${snapshot.get75thPercentile}\n")
              sb.append(s"${prefix}95thPercentile ${snapshot.get95thPercentile}\n")
              sb.append(s"${prefix}98thPercentile ${snapshot.get98thPercentile}\n")
              sb.append(s"${prefix}99thPercentile ${snapshot.get99thPercentile}\n")
              sb.append(s"${prefix}999thPercentile ${snapshot.get999thPercentile}\n")
              sb.append(s"${prefix}StdDev ${snapshot.getStdDev}\n") }
         5. 记录度量频率信息
         registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
              val prefix = normalizeKey(kv.getKey)
              val meter = kv.getValue
              sb.append(s"${prefix}Count ${meter.getCount}\n")
              sb.append(s"${prefix}MeanRate ${meter.getMeanRate}\n")
              sb.append(s"${prefix}OneMinuteRate ${meter.getOneMinuteRate}\n")
              sb.append(s"${prefix}FiveMinuteRate ${meter.getFiveMinuteRate}\n")
              sb.append(s"${prefix}FifteenMinuteRate ${meter.getFifteenMinuteRate}\n")}
         6. 记录定时器信息
         registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
              val prefix = normalizeKey(kv.getKey)
              val timer = kv.getValue
              val snapshot = timer.getSnapshot
              sb.append(s"${prefix}Count ${timer.getCount}\n")
              sb.append(s"${prefix}Max ${snapshot.getMax}\n")
              sb.append(s"${prefix}Mean ${snapshot.getMax}\n")
              sb.append(s"${prefix}Min ${snapshot.getMin}\n")
              sb.append(s"${prefix}50thPercentile ${snapshot.getMedian}\n")
              sb.append(s"${prefix}75thPercentile ${snapshot.get75thPercentile}\n")
              sb.append(s"${prefix}95thPercentile ${snapshot.get95thPercentile}\n")
              sb.append(s"${prefix}98thPercentile ${snapshot.get98thPercentile}\n")
              sb.append(s"${prefix}99thPercentile ${snapshot.get99thPercentile}\n")
              sb.append(s"${prefix}999thPercentile ${snapshot.get999thPercentile}\n")
              sb.append(s"${prefix}StdDev ${snapshot.getStdDev}\n")
              sb.append(s"${prefix}FifteenMinuteRate ${timer.getFifteenMinuteRate}\n")
              sb.append(s"${prefix}FiveMinuteRate ${timer.getFiveMinuteRate}\n")
              sb.append(s"${prefix}OneMinuteRate ${timer.getOneMinuteRate}\n")
              sb.append(s"${prefix}MeanRate ${timer.getMeanRate}\n") }
         
         def start(): Unit = { } 
         def stop(): Unit = { }
         def report(): Unit = { }
    }
    ```
    
    
    
    #### Sink
    
    ```markdown
    private[spark] trait Sink {
    	操作集:
    	def start(): Unit	功能: 开始
      	def stop(): Unit	功能: 停止
      	def report(): Unit	功能: 汇报
    }
    ```
    
    #### Slf4jSink
    
    ```markdown
    private[spark] class Slf4jSink(val property: Properties,val registry: MetricRegistry,
        securityMgr: SecurityManager){
    	关系: father --> Sink
        构造器属性:
        	property	属性
        	registry	度量注册器
        	securityMgr	安全管理器
        属性:
        	#name @SLF4J_DEFAULT_PERIOD = 10
        	#name @SLF4J_DEFAULT_UNIT = "SECONDS"
        	#name @SLF4J_KEY_PERIOD = "period"
        	#name @SLF4J_KEY_UNIT = "unit"
        	#name @pollPeriod	测试周期
        		val= Option(property.getProperty(SLF4J_KEY_PERIOD)) match {
        			case Some(s) => s.toInt
        			case None => SLF4J_DEFAULT_PERIOD }
        	#name @pollUnit #type @TimeUnit 时间单位
        		val= Option(property.getProperty(SLF4J_KEY_UNIT)) match {
        			case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
        			case None => TimeUnit.valueOf(SLF4J_DEFAULT_UNIT) }
        	#name @reporter #type @Slf4jReporter
        		val= Slf4jReporter.forRegistry(registry)..convertDurationsTo(TimeUnit.MILLISECONDS)
        			.convertRatesTo(TimeUnit.SECONDS).build()
        初始化操作:
        MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)
        功能: 度量系统检测测试周期的合法性
        
        操作集:
        def start(): Unit = { reporter.start(pollPeriod, pollUnit) }
        def stop(): Unit = { reporter.stop() }
        def report(): Unit = { reporter.report() }
    }
    ```
    
    
    
    #### StatsdReporter
    
    ```markdown
    private[spark] object StatsdMetricType{
    	属性:
    	#name @COUNTER = "c"	计数器
    	#name @GAUGE = "g"	估值
    	#name @TIMER="ms"	定时器
    	#name @Set = "s"	集合
    }
    ```
    
    ```markdown
    private[spark] class StatsdReporter(registry: MetricRegistry,host: String = "127.0.0.1",
        port: Int = 8125,prefix: String = "",filter: MetricFilter = MetricFilter.ALL,
        rateUnit: TimeUnit = TimeUnit.SECONDS,durationUnit: TimeUnit = TimeUnit.MILLISECONDS){
    	关系: 
    	father --> ScheduledReporter(registry, "statsd-reporter", filter, rateUnit, durationUnit)    
    	sibling --> Logging
    	属性:
    	#name @address = new InetSocketAddress(host, port)	地址
    	#name whitespace="[\\s]+".r	空格
    	操作集:
    	def reportGauge(name: String, gauge: Gauge[_])(implicit socket: DatagramSocket): Unit
    	功能: 汇报估值情况
    		formatAny(gauge.getValue).foreach(v => send(fullName(name), v, GAUGE))
    	
    	def reportCounter(name: String, counter: Counter)(implicit socket: DatagramSocket): Unit
    	功能: 汇报计数情况
    		send(fullName(name), format(counter.getCount), COUNTER)
    	
    	def reportHistogram(name: String, histogram: Histogram)
    		(implicit socket: DatagramSocket): Unit
    	功能: 通过socket@socket汇报统计直方图@histogram
    	    val snapshot = histogram.getSnapshot
            send(fullName(name, "count"), format(histogram.getCount), GAUGE)
            send(fullName(name, "max"), format(snapshot.getMax), TIMER)
            send(fullName(name, "mean"), format(snapshot.getMean), TIMER)
            send(fullName(name, "min"), format(snapshot.getMin), TIMER)
            send(fullName(name, "stddev"), format(snapshot.getStdDev), TIMER)
            send(fullName(name, "p50"), format(snapshot.getMedian), TIMER)
            send(fullName(name, "p75"), format(snapshot.get75thPercentile), TIMER)
            send(fullName(name, "p95"), format(snapshot.get95thPercentile), TIMER)
            send(fullName(name, "p98"), format(snapshot.get98thPercentile), TIMER)
            send(fullName(name, "p99"), format(snapshot.get99thPercentile), TIMER)
            send(fullName(name, "p999"), format(snapshot.get999thPercentile), TIMER)
        
        def reportMetered(name: String, meter: Metered)(implicit socket: DatagramSocket): Unit 
        功能: 汇报移动频率参数@meter #class @Meterd
            send(fullName(name, "count"), format(meter.getCount), GAUGE)
        	send(fullName(name, "m1_rate"), format(convertRate(meter.getOneMinuteRate)), TIMER)
        	send(fullName(name, "m5_rate"), format(convertRate(meter.getFiveMinuteRate)), TIMER)
        	send(fullName(name, "m15_rate"), format(convertRate(meter.getFifteenMinuteRate)), TIMER)
        	send(fullName(name, "mean_rate"), format(convertRate(meter.getMeanRate)), TIMER)
        
        def reportTimer(name: String, timer: Timer)(implicit socket: DatagramSocket): Unit
        功能: 汇报计数器统计参数#class @Timer
            val snapshot = timer.getSnapshot
            send(fullName(name, "max"), format(convertDuration(snapshot.getMax)), TIMER)
            send(fullName(name, "mean"), format(convertDuration(snapshot.getMean)), TIMER)
            send(fullName(name, "min"), format(convertDuration(snapshot.getMin)), TIMER)
            send(fullName(name, "stddev"), format(convertDuration(snapshot.getStdDev)), TIMER)
            send(fullName(name, "p50"), format(convertDuration(snapshot.getMedian)), TIMER)
            send(fullName(name, "p75"), format(convertDuration(snapshot.get75thPercentile)), TIMER)
            send(fullName(name, "p95"), format(convertDuration(snapshot.get95thPercentile)), TIMER)
            send(fullName(name, "p98"), format(convertDuration(snapshot.get98thPercentile)), TIMER)
            send(fullName(name, "p99"), format(convertDuration(snapshot.get99thPercentile)), TIMER)
            send(fullName(name, "p999"), format(convertDuration(snapshot.get999thPercentile)), TIMER)
            reportMetered(name, timer)
       
       	def fullName(names: String*): String = MetricRegistry.name(prefix, names : _*)
       	功能: 获取全名信息
       	
       	def sanitize(s: String): String = whitespace.replaceAllIn(s, "-")
    	功能: 替换所有空格
    	
    	def format(v: Any): String = formatAny(v).getOrElse("")
    	功能: 格式化v
    	
    	def formatAny(v: Any): Option[String] 
    	功能： 对v进行格式化
    	val= v match {
    		case f: Float => Some("%2.2f".format(f))
          	case d: Double => Some("%2.2f".format(d))
          	case b: BigDecimal => Some("%2.2f".format(b))
          	case n: Number => Some(v.toString)
          	case _ => None
    	}
    	
    	def send(name: String, value: String, metricType: String)
    		(implicit socket: DatagramSocket): Unit
    	功能:通过@socket 发送度量信息
            val bytes = sanitize(s"$name:$value|$metricType").getBytes(UTF_8)
            val packet = new DatagramPacket(bytes, bytes.length, address)
            socket.send(packet)
            
        def report(gauges: SortedMap[String, Gauge[_]],counters: SortedMap[String, Counter],
          histograms: SortedMap[String, Histogram],meters: SortedMap[String, Meter],
          timers: SortedMap[String, Timer]): Unit
        功能: 汇报度量信息
    }
    ```
    
    
    
    #### StatsdSink
    
    ```markdown
    private[spark] object StatsdSink{
    	属性:
    	#name @STATSD_KEY_HOST="host"	规定host的key
    	#name @STATSD_KEY_PORT="port"	规定port的key
    	#name @STATSD_KEY_PERIOD="period" 规定period的key
    	#name @STATSD_KEY_UNIT="unit"	规定unit的key
    	#name @STATSD_KEY_PREFIX = "prefix"	规定prefix的key
    	#name @STATSD_DEFAULT_HOST="127.0.0.1"	规定默认host
    	#name @STATSD_DEFAULT_PORT = "8125"		规定默认端口
    	#name @STATSD_DEFAULT_PERIOD=10		规定默认period
    	#name @STATSD_DEFAULT_UNIT= "SECONDS"	规定默认单位
    	#name @STATSD_DEFAULT_PREFIX=""		规定默认前缀
    }
    ```
    
    ```markdown
    private[spark] class StatsdSink(val property: Properties,val registry: MetricRegistry,
        securityMgr: SecurityManager){
    	关系: father --> Sink
        	sibling --> Logging
        构造器属性:
        	property	属性
        	registry	度量注册器
        	securityMgr	安全管理器
        属性:
        #name @host=property.getProperty(STATSD_KEY_HOST, STATSD_DEFAULT_HOST)	主机名称
        #name @port=property.getProperty(STATSD_KEY_PORT, STATSD_DEFAULT_PORT).toInt	端口号
        #name @pollPeriod=property.getProperty(STATSD_KEY_PERIOD, STATSD_DEFAULT_PERIOD).toInt
        	测试周期
        #name @pollUnit		测试单元
        	val= TimeUnit.valueOf(
          		property.getProperty(STATSD_KEY_UNIT, STATSD_DEFAULT_UNIT).toUpperCase(Locale.ROOT))
    	#name @prefix	前缀
    		val= property.getProperty(STATSD_KEY_PREFIX, STATSD_DEFAULT_PREFIX)
    	#name @reporter=new StatsdReporter(registry, host, port, prefix)	状态汇报器
    	
    	初始化操作:
    	MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)
    	功能: 度量系统检测最小测试周期是否合法
    	
    	操作集:
    	def start(): Unit
    	功能: 汇报器开始以pollPeriod为周期，unit为单元的测试
    		reporter.start(pollPeriod, pollUnit)
    	
    	def stop(): Unit
    	功能: 关闭测试
    		reporter.stop()
    	
    	def report(): Unit = reporter.report()
    	功能: 汇报
    }
    ```
    
    

---

#### source

1. [AccumulatorSource.scala](# AccumulatorSource)

2. [JVMCPUSource.scala](# JVMCPUSource)

3. [JvmSource.scala](# JvmSource)

4. [Source.scala](# Source)

5. [StaticSources.scala](# StaticSources)

   ---

   #### AccumulatorSource

   ```markdown
介绍:
   	累加资源是一种会报告当前累加器value值的spark度量资源。
   	需要将累加器的范围严格的限定在@LongAccumulator和@DoubleAccumulator，这些累加器是当前建立在数字统计的累加器。且去除了@CollectionAccumulator，原因它是一个value的列表很难去汇报给度量系统。
   ```
   
   ```markdown
   private[spark] class AccumulatorSource{
   	关系: father --> Source
   	属性:
   	#name @registry=new MetricRegistry private	度量登记器
   	操作集:
   	def sourceName: String = "AccumulatorSource"
   	功能: 获取资源名称
   	
   	def metricRegistry: MetricRegistry = registry
   	功能: 获取度量登记器
   	
   	def register[T](accumulators: Map[String, AccumulatorV2[_, T]]): Unit 
   	功能: 注册累加器的value值
   	    accumulators.foreach {
         		case (name, accumulator) =>
                   val gauge = new Gauge[T] {
                  override def getValue: T = accumulator.value
           	}
        	registry.register(MetricRegistry.name(name), gauge)
       	}
}
   ```
   
   ```markdown
   @Experimental
   class LongAccumulatorSource extends AccumulatorSource
   ```
   
   ```markdown
   @Experimental
   class DoubleAccumulatorSource extends AccumulatorSource
   ```
   
   ```markdown
   @Experimental
   object LongAccumulatorSource {
   	介绍: 这是给@LongAccumulators 的度量资源。累加器只能在驱动器上有效。所有度量数据由驱动器汇报。
   	操作集:
   ```

	def register(sc: SparkContext, accumulators: Map[String, LongAccumulator]): Unit 
   	功能: 注册累加器到度量系统中
	val source = new LongAccumulatorSource
       source.register(accumulators)
    sc.env.metricsSystem.registerSource(source)
   }
   ```
   
   ```markdown
   @Experimental
   object DoubleAccumulatorSource {
   	def register(sc: SparkContext, accumulators: Map[String, DoubleAccumulator]): Unit
   	功能: 注册累加器
   	val source = new DoubleAccumulatorSource
     source.register(accumulators)
        sc.env.metricsSystem.registerSource(source)
}
   ```

   #### JVMCPUSource

   ```markdown
   private[spark] class JVMCPUSource{
	关系: father --> Source
   	属性:
   	#name @sourceName="JVMCPU" 	资源名称
   	#name @metricRegistry=new MetricRegistry()	资源登记器
       初始化操作:
         metricRegistry.register(MetricRegistry.name("jvmCpuTime"), new Gauge[Long] {
           val mBean: MBeanServer = ManagementFactory.getPlatformMBeanServer
           val name = new ObjectName("java.lang", "type", "OperatingSystem")
           override def getValue: Long = {
             try {
               mBean.getAttribute(name, "ProcessCpuTime").asInstanceOf[Long]
             } catch {
               case NonFatal(_) => -1L
             }
           }
         })    
     功能: 登记CPU及其使用量
   }
   ```

   

   #### JvmSource

   ```markdown
   private[spark] class JvmSource {
   	关系: father --> Source
   	属性:
   	#name @sourceName="jvm"		资源名称
   	#name @metricRegistry = new MetricRegistry()	度量注册器
   	初始化操作:
   	metricRegistry.registerAll(new GarbageCollectorMetricSet)
      	功能: 注册垃圾回收器度量集合
      	
      	metricRegistry.registerAll(new MemoryUsageGaugeSet)
   	功能: 注册内存使用计量集合
   	
   	metricRegistry.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer))
   	功能: 注册缓冲池度量集合
   }
   ```

   #### Source

   ```markdown
   private[spark] trait Source {
   	操作集:
   	def sourceName: String
   	功能: 获取资源名称
   	def metricRegistry: MetricRegistry
   	功能: 获取度量注册器
   }
   ```

   #### StaticSources

```markdown
   private[spark] object StaticSources {
   	属性:
   	#name @allSources=Seq(CodegenMetrics, HiveCatalogMetrics)	所有资源列表
   }
```

   ```markdown
   object CodegenMetrics { 
   	关系: father -->soruce
   	介绍: 编译度量器
   	属性:
   	#name @METRIC_SOURCE_CODE_SIZE #type @Histogram	代码生成器编译出来源码的长度统计
   		val= metricRegistry.histogram(MetricRegistry.name("sourceCodeSize"))
   	#name @METRIC_COMPILATION_TIME  #type @Histogram	编译代码时间统计
   		val= metricRegistry.histogram(MetricRegistry.name("compilationTime"))
   	#name @METRIC_GENERATED_CLASS_BYTECODE_SIZE  #type @Histogram	代码生成器生成每个类的字节长度
   		val= metricRegistry.histogram(MetricRegistry.name("generatedClassSize"))
   	#name @METRIC_GENERATED_METHOD_BYTECODE_SIZE  #type @Histogram 每个方法生成的字节长度统计
   		val= metricRegistry.histogram(MetricRegistry.name("generatedMethodSize"))
   }
   ```

   ```markdown
   object HiveCatalogMetrics{
   	关系: father -->soruce
   	介绍: hive 外部目录度量
   	属性:
   	#name @sourceName: String = "HiveExternalCatalog"	资源名称
   	#name @metricRegistry=new MetricRegistry()	度量登记器
   	#name @METRIC_PARTITIONS_FETCHED #type @Counter	分区总数
   		val=metricRegistry.counter(MetricRegistry.name("partitionsFetched"))
   	#name @METRIC_FILES_DISCOVERED #type @Counter	找到的文件数量
   		val=metricRegistry.counter(MetricRegistry.name("filesDiscovered"))
   	#name @METRIC_FILE_CACHE_HITS #type @Counter	文件缓存命中数量
   		val=metricRegistry.counter(MetricRegistry.name("fileCacheHits"))
   	#name @METRIC_HIVE_CLIENT_CALLS #type @Counter	Hive客户端调用数量
       	val= metricRegistry.counter(MetricRegistry.name("hiveClientCalls"))
       #name @METRIC_PARALLEL_LISTING_JOB_COUNT #type @Counter	spark并行任务数量
       	val=metricRegistry.counter(MetricRegistry.name("parallelListingJobCount"))
   	操作集:
   	def reset(): Unit
   	功能: 所有计数值清零
           METRIC_PARTITIONS_FETCHED.dec(METRIC_PARTITIONS_FETCHED.getCount())
           METRIC_FILES_DISCOVERED.dec(METRIC_FILES_DISCOVERED.getCount())
           METRIC_FILE_CACHE_HITS.dec(METRIC_FILE_CACHE_HITS.getCount())
           METRIC_HIVE_CLIENT_CALLS.dec(METRIC_HIVE_CLIENT_CALLS.getCount())
           METRIC_PARALLEL_LISTING_JOB_COUNT.dec(METRIC_PARALLEL_LISTING_JOB_COUNT.getCount())
   	
   	def incrementFetchedPartitions(n: Int): Unit = METRIC_PARTITIONS_FETCHED.inc(n)
   	功能: 增加分区数量
   	
   	def incrementFilesDiscovered(n: Int): Unit = METRIC_FILES_DISCOVERED.inc(n)
     	功能: 发现文件数+=n
     	
     	def incrementFileCacheHits(n: Int): Unit = METRIC_FILE_CACHE_HITS.inc(n)
     	功能: 文件缓存命中数量+=n
     	
     	def incrementHiveClientCalls(n: Int): Unit = METRIC_HIVE_CLIENT_CALLS.inc(n)
     	功能: 客户端调用数量+=n
     	
     	def incrementParallelListingJobCount(n: Int): Unit = METRIC_PARALLEL_LISTING_JOB_COUNT.inc(n)	 功能: 并行任务数量+=n	
   }
   ```

   

---
#### ExecutorMetricType

```markdown
sealed trait ExecutorMetricType {
	介绍: 执行器度量器类型，存储在@ExecutorMetrics的执行器等级的度量器
	操作集:
	def getMetricValues(memoryManager: MemoryManager): Array[Long]
	功能: 获取度量器值
	
	def names: Seq[String]
	功能: 获取名称列表
}
```

```markdown
sealed trait SingleValueExecutorMetricType{
	关系: father --> ExecutorMetricType
	介绍: 单值执行度量器类型
	操作集:
	def names:seq[String]
	功能: 获取名称列表，使用获取.分割的最后一个
	val= Seq(getClass().getName().stripSuffix("$").split("""\.""").last)
	
	def getMetricValues(memoryManager: MemoryManager): Array[Long]
	功能: 获取指定内存管理器@memoryManager的度量值列表
		val metrics = new Array[Long](1)
    	metrics(0) = getMetricValue(memoryManager)
		val=metrics
	
	def getMetricValue(memoryManager: MemoryManager): Long
	功能: 获取指定内存管理器的度量值
}
```

```markdown
private[spark] abstract class MemoryManagerExecutorMetricType(f: MemoryManager => Long){
	关系: father --> SingleValueExecutorMetricType
	构造器:
		f 	计算内存管理器对应的度量值函数
	操作集:
	def getMetricValue(memoryManager: MemoryManager): Long 
	功能: 获取指定内存管理器@memoryManager的度量值
	val= f(memoryManager)
}
```

```markdown
private[spark] abstract class MBeanExecutorMetricType(mBeanName: String){
	关系: father --> SingleValueExecutorMetricType
	属性:
	#name @bean= ManagementFactory.newPlatformMXBeanProxy(
    	ManagementFactory.getPlatformMBeanServer,
    	new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])
	操作集:
	def getMetricValue(memoryManager: MemoryManager): Long
	功能: 获取指定内存管理器的度量值
	val=bean.getMemoryUsed
}
```

```markdown
case object JVMHeapMemory {
	关系: father --> SingleValueExecutorMetricType
	操作集:
	def getMetricValue(memoryManager: MemoryManager): Long
	功能: 获取指定内存管理器的度量值
	val= ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
}
case object JVMOffHeapMemory{
	关系: father --> SingleValueExecutorMetricType
	操作集:
	def getMetricValue(memoryManager: MemoryManager): Long
	功能: 获取指定内存管理器的度量值
	val= ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
}
```

```markdown
case object ProcessTreeMetrics{
	介绍: 进程树
    关系: father --> ExecutorMetricType
    属性: 
    #name @names 名称列表
    val=Seq("ProcessTreeJVMVMemory","ProcessTreeJVMRSSMemory","ProcessTreePythonVMemory",
    	"ProcessTreePythonRSSMemory","ProcessTreeOtherVMemory","ProcessTreeOtherRSSMemory")
	
	操作集:
	def getMetricValues(memoryManager: MemoryManager): Array[Long]
	功能: 获取指定内存管理器的度量值
        val allMetrics = ProcfsMetricsGetter.pTreeInfo.computeAllMetrics()
        val processTreeMetrics = new Array[Long](names.length)
        processTreeMetrics(0) = allMetrics.jvmVmemTotal
        processTreeMetrics(1) = allMetrics.jvmRSSTotal
        processTreeMetrics(2) = allMetrics.pythonVmemTotal
        processTreeMetrics(3) = allMetrics.pythonRSSTotal
        processTreeMetrics(4) = allMetrics.otherVmemTotal
        processTreeMetrics(5) = allMetrics.otherRSSTotal
       val=processTreeMetrics
}
```

```markdown

case object OnHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.onHeapExecutionMemoryUsed)
介绍: 堆模式下执行器内存度量

case object OffHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.offHeapExecutionMemoryUsed)
介绍: 非堆模式下执行器内存度量

case object OnHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.onHeapStorageMemoryUsed)
介绍: 堆模式下存储器内存度量

case object OffHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.offHeapStorageMemoryUsed)
介绍: 非堆模式下存储器内存度量

case object OnHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.onHeapExecutionMemoryUsed + m.onHeapStorageMemoryUsed))
介绍: 堆模式下联合内存度量

case object OffHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.offHeapExecutionMemoryUsed + m.offHeapStorageMemoryUsed))
介绍: 非堆模式下联合内存的度量

case object DirectPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=direct")
介绍: 直接内存池度量

case object MappedPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=mapped")
 介绍: 映射内存池度量
```
```markdown
case object GarbageCollectionMetrics{
	关系: father --> ExecutorMetricType
		sibling --> Logging
	介绍: GC度量器
	属性:
	#name @nonBuiltInCollectors=Nil	#type @Seq[String]
		未建立收集器列表
	#name @names=Seq("MinorGCCount","MinorGCTime","MajorGCCount","MajorGCTime")	名称列表
	#name @YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS	年轻代内建垃圾收集器列表
		val=  Seq("Copy","PS Scavenge","ParNew","G1 Young Generation")
	#name @OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS	老年代内建垃圾收集器列表
		val= Seq("MarkSweepCompact","PS MarkSweep","ConcurrentMarkSweep","G1 Old Generation")
	#name @youngGenerationGarbageCollector	#type @lazy Seq[String] 年轻代垃圾收集器
		val= SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS)
	#name @oldGenerationGarbageCollector #type @lazy Seq[String]	老年代垃圾收集器
		val= SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS)
	操作集:
	def getMetricValues(memoryManager: MemoryManager): Array[Long]
	功能: 获取指定内存管理器的度量值列表
	0. 确定数组大小
		val gcMetrics = new Array[Long](names.length)
		(minorCount次要计数值，minorTime次要计时，majorCount主要计数值，majorTime主要计时)
	1. 获取年轻代度量参数
		if (youngGenerationGarbageCollector.contains(mxBean.getName)) {
            gcMetrics(0) = mxBean.getCollectionCount
            gcMetrics(1) = mxBean.getCollectionTime }
    2. 获取老年代度量参数
    	if (oldGenerationGarbageCollector.contains(mxBean.getName)) {
        	gcMetrics(2) = mxBean.getCollectionCount
        	gcMetrics(3) = mxBean.getCollectionTime}
    3. 其他没有内建的情况
    	if (!nonBuiltInCollectors.contains(mxBean.getName)) {
    		nonBuiltInCollectors = mxBean.getName +: nonBuiltInCollectors }
    val= gcMetrics
}
```
```markdown
private[spark] object ExecutorMetricType {
	属性:
	#name @metricGetters #type @IndexedSeq 		度量获取列表
	val= IndexedSeq(
            JVMHeapMemory,
            JVMOffHeapMemory,
            OnHeapExecutionMemory,
            OffHeapExecutionMemory,
            OnHeapStorageMemory,
            OffHeapStorageMemory,
            OnHeapUnifiedMemory,
            OffHeapUnifiedMemory,
            DirectPoolMemory,
            MappedPoolMemory,
            ProcessTreeMetrics,
            GarbageCollectionMetrics )
    
    #name @(metricToOffset, numMetrics)	度量信息
    val= {
        var numberOfMetrics = 0
        val definedMetricsAndOffset = mutable.LinkedHashMap.empty[String, Int]
        metricGetters.foreach { m =>
          (0 until m.names.length).foreach { idx =>
            definedMetricsAndOffset += (m.names(idx) -> (idx + numberOfMetrics))
          }
          numberOfMetrics += m.names.length }
        (definedMetricsAndOffset, numberOfMetrics)
    }
}
```
#### MetricsConfig

```markdown
private[spark] class MetricsConfig(conf: SparkConf){
	关系: father --> Logging
	构造器属性:
	#name @DEFAULT_PREFIX="*" 	默认前缀
	#name @INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r		实例的正则表达式
	#name @DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"		默认度量配置文件名称
	#name @properties = new Properties()	属性
	#name @perInstanceSubProperties=null #type @mutable.HashMap[String, Properties]	每个实例子属性列表
	操作集:
	def setDefaultProperties(prop: Properties): Unit 
	功能: 设置默认属性
		prop.setProperty("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
    	prop.setProperty("*.sink.servlet.path", "/metrics/json")
    	prop.setProperty("master.sink.servlet.path", "/metrics/master/json")
    	prop.setProperty("applications.sink.servlet.path", "/metrics/applications/json")
	
    def initialize(): Unit 
    功能: 初始化
    1. 设置默认属性
    	setDefaultProperties(properties)
    2. 加载文件中的属性
    	loadPropertiesFromFile(conf.get(METRICS_CONF))
	3. 设置前缀为指定值的属性
		val prefix = "spark.metrics.conf."
    	conf.getAll.foreach {
      	case (k, v) if k.startsWith(prefix) =>
        	properties.setProperty(k.substring(prefix.length()), v)
      	case _ =>
    	}
    4. 获取每个实例的子属性列表 并设置到子属性列表中
    perInstanceSubProperties = subProperties(properties, INSTANCE_REGEX)
	if (perInstanceSubProperties.contains(DEFAULT_PREFIX)) {
      val defaultSubProperties = perInstanceSubProperties(DEFAULT_PREFIX).asScala
      for ((instance, prop) <- perInstanceSubProperties if (instance != DEFAULT_PREFIX);
           (k, v) <- defaultSubProperties if (prop.get(k) == null)) {
        prop.put(k, v)
      }
    }
    
    def getInstance(inst: String): Properties 
    功能: 获取指定串@inst对应的属性
    val= {perInstanceSubProperties.get(inst) match {
      case Some(s) => s
      case None => perInstanceSubProperties.getOrElse(DEFAULT_PREFIX, new Properties) }
    
    def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] 
    功能: 获取指定属性prop下符合正则表达式@regex对应的子属性表
    val subProperties = new mutable.HashMap[String, Properties]
    prop.asScala.foreach { kv =>
      	if (regex.findPrefixOf(kv._1.toString).isDefined) {
        	val regex(prefix, suffix) = kv._1.toString
        	subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2.toString)
      	}
    }
    
    private[this] def loadPropertiesFromFile(path: Option[String]): Unit
    功能: 加载指定路径@path对应的属性
    1. 读取路径中的内容
    var is: InputStream = null
    is = path match {
        case Some(f) => new FileInputStream(f)
        case None => Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME)
    }
    2. 加载属性值
    if (is != null) {properties.load(is)}
}
```

#### MetricsSystem

```markdown
介绍:
	spark的度量系统，由指定的实例创建。联合sink,source周期性的测试source源，并将度量数据写入到sink目的地。
	实例指出了谁使用度量系统问题，在spark中，有多种身份，比如说master，worker，executor，client driver。这些角色会创建度量系统的监视。所以这里的角色就指的是它们。
	资源(source)是度量数据收集的地方。度量系统中存在如下两种资源:
		1. spark内部资源比如:MasterSource,WorkerSource等等，这些会收集spark组件的内部状态，这些资源会被加	载到特定的度量系统中
		2. 普通资源，比如说JVMSource这样会收集低级别状态的资源，由配置决定，并通过反射加载
	sink指定数据输出到哪，多个sink可以共存，且度量数据可以刷写到这些sink中。
	度量信息配置格式:
		[instance].[sink|source].[name].[options] = xxxx
		instance : "master", "worker", "executor", "driver", "applications"
		sink|source	: 种类
		name : 名称
		options	: 代表source/sink 属性
```
```markdown
private[spark] class MetricsSystem private (val instance: String,conf: SparkConf,
    securityMgr: SecurityManager){
 	关系: father --> Logging
 	属性:
 	#name @metricsConfig = new MetricsConfig(conf)	度量配置
 	#name @sinks = new mutable.ArrayBuffer[Sink]	sink列表
 	#name @sources = new mutable.ArrayBuffer[Source]	source列表
 	#name @registry = new MetricRegistry()	注册度量器
 	#name @running: Boolean = false	运行状态标记
 	#name @metricsServlet: Option[MetricsServlet] = None	度量服务端
 	#name @prometheusServlet: Option[PrometheusServlet] = None 	prometheus服务端
 	
 	初始化操作:
 	metricsConfig.initialize()
 	功能: 配置初始化
 	
 	操作集:
 	def getServletHandlers: Array[ServletContextHandler] 
 	功能: 获取服务端处理器列表
 	操作条件: 度量系统运行中@running=true(两种服务端的并集)
 	val= metricsServlet.map(_.getHandlers(conf)).getOrElse(Array()) ++
 		prometheusServlet.map(_.getHandlers(conf)).getOrElse(Array())
 		
 	def report(): Unit = {sinks.foreach(_.report())}
 	功能: sink 信息汇报
 	
 	def stop(): Unit
	功能: 停止度量系统
        if (running) {
            sinks.foreach(_.stop)	// 停止sink
            registry.removeMatching((_: String, _: Metric) => true) // 移除所有度量器
        } 
        running = false
    
    def start(registerStaticSources: Boolean = true): Unit
    功能: 度量系统不在运行
        running = true
        if (registerStaticSources) {
          StaticSources.allSources.foreach(registerSource)
          registerSources()
        }
        registerSinks()
        sinks.foreach(_.start) // 开启每一个sink
    
    def getSourcesByName(sourceName: String): Seq[Source]
    功能: 获取资源名称
    val= sources.filter(_.sourceName == sourceName)
    
    def registerSource(source: Source): Unit
    功能: 登记资源@source
    	sources += source
        val regName = buildRegistryName(source)
      	registry.register(regName, source.metricRegistry)
      	
    def removeSource(source: Source): Unit
    功能: 移除资源@source
        sources -= source
        val regName = buildRegistryName(source)
        registry.removeMatching((name: String, _: Metric) => name.startsWith(regName))
 	
 	def registerSources(): Unit 
 	功能: 注册资源
 	1. 获取资源列表
 		val instConfig = metricsConfig.getInstance(instance)
    	val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)
 	2. 注册资源
 	sourceConfigs.foreach { kv =>
 		val classPath = kv._2.getProperty("class")
        val source = Utils.classForName[Source](classPath).getConstructor().newInstance()
        registerSource(source) }
    
    def buildRegistryName(source: Source): String 
    功能: 给每个资源@source建立一个唯一的标识
    	格式<app ID>.<executor ID (or "driver")>.<source name>
    	如果两个ID都不可用直接使用 <source name>.
    
    def registerSinks(): Unit
    功能: 注册sinks
    1. 获取sink列表
        val instConfig = metricsConfig.getInstance(instance)
        val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)
 	2. 注册sinks
 	```scala
      sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          if (kv._1 == "servlet") {
            val servlet = Utils.classForName[MetricsServlet](classPath)
              .getConstructor(
                classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
              .newInstance(kv._2, registry, securityMgr)
            metricsServlet = Some(servlet)
          } else if (kv._1 == "prometheusServlet") {
            val servlet = Utils.classForName[PrometheusServlet](classPath)
              .getConstructor(
                classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
              .newInstance(kv._2, registry, securityMgr)
            prometheusServlet = Some(servlet)
          } else {
            val sink = Utils.classForName[Sink](classPath)
              .getConstructor(
                classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
              .newInstance(kv._2, registry, securityMgr)
            sinks += sink
          }
        } catch {
          case e: Exception =>
            logError("Sink class " + classPath + " cannot be instantiated")
            throw e
        }
      }
    ```
 }
```
```markdown
private[spark] object MetricsSystem{
	属性:
	#name @SINK_REGEX= "^sink\\.(.+)\\.(.+)".r	sink正则
	#name @SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r	source正则
	#name @MINIMAL_POLL_UNIT = TimeUnit.SECONDS	最小测试单位
	#name @MINIMAL_POLL_PERIOD=1	最小测试周期值
	操作集:
	def createMetricsSystem(instance: String, conf: SparkConf, securityMgr: SecurityManager)
		: MetricsSystem
	功能: 创建一个度量系统
	val= new MetricsSystem(instance, conf, securityMgr)
	
	def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int): Unit
	功能: 检查周期值设置的是否合理
	
}
```

```markdown
private[spark] object MetricsSystemInstances{
	介绍: 度量系统实例
	属性:
	#name @MASTER="master"	Spark独立的master进程
	#name @APPLICATIONS = "applications"	master组件
	#name @WORKER = "worker"	spark独立的worker进程
	#name @EXECUTOR = "executor"	spark执行器
	#name @DRIVER = "driver"	spark执行器内容
	#name @SHUFFLE_SERVICE = "shuffleService"	spark shuffle服务
	#name @APPLICATION_MASTER="applicationMaster"	yarn模式下任务管理器@ApplicationMaster
	#Name @MESOS_CLUSTER="mesos_cluster"	Mesos集群模式下调度器
}
```

#### 基础拓展

1.  关键字 sealed