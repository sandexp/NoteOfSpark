## **spark-resources**

---

1.  [ExecutorResourceRequest.scala](# ExecutorResourceRequest)
2.  [ExecutorResourceRequests](# ExecutorResourceRequests)
3.  [ResourceAllocator.scala](# ResourceAllocator)
4.  [ResourceInformation.scala](# ResourceInformation)
5.  [ResourceProfile.scala](# ResourceProfile)
6.  [ResourceUtils.scala](# ResourceUtils)
7.  [TaskResourceRequest.scala](# TaskResourceRequest)
8.  [TaskResourceRequests.scala](# TaskResourceRequests)

---

#### ExecutorResourceRequest

#### ExecutorResourceRequests

#### ResourceAllocator

```markdown
介绍:
	这个特征用于帮助执行器/工作者(worker)分配资源，注意需要使用在单线程中
```



#### ResourceInformation

```markdown
介绍:
	用于容纳与资源相关的信息，资源可以是GPU,FGPA等等。地址数组是指定的资源，取决于用户中断地址。
	例如GPU,那么地址就是GPU的目录。(用于索引资源的下标参数)
```
```markdown
@Evolving
class ResourceInformation(val name: String,val addresses: Array[String]){
	关系: father --> Serializable
	构造器属性:
		name	资源名称
		addresses	描述资源地址的参数列表
	操作集:
		def toString: String 
		功能: 显示属性值
		
		def toJson(): JValue = ResourceInformationJson(name, addresses).toJValue
		功能: 将数据转化为json形式
		
		def equals(obj: Any): Boolean
		功能: 判断对象相等逻辑
		true when that.getClass == this.getClass && that.name == name
			&& that.addresses.toSeq == addresses.toSeq
		
		def hashCode(): Int = Seq(name, addresses.toSeq).hashCode()
		功能: 计算当前对象所对应的hash值
}
```
```markdown
private case class ResourceInformationJson(name: String, addresses: Seq[String]){
	介绍: 简单的对@ResourceInformation进行json序列化
	构造器属性:
		name	资源名称
		addresses	资源地址列表
	操作集:
	def toJValue: JValue = {Extraction.decompose(this)(DefaultFormats)}
	功能: 将@ResourceInformation 序列化为json
	
	def toResourceInformation: ResourceInformation = {new ResourceInformation(
		name, addresses.toArray)}
	功能: 根据name和addresses转化为@ResourceInformation
}
```
```markdown
private[spark] object ResourceInformation{
	属性:
	#name @exampleJson	示例json对象
	
	操作集:
	def parseJson(json: String): ResourceInformation 
	功能: 将json串转化为@ResourceInformation
	
	def parseJson(json: JValue): ResourceInformation
	功能: 将json对象转化为@ResourceInformation
}
```



#### ResourceProfile

```markdown
介绍:
	资源描述类，与RDD紧密相连，一个资源描述类@ResourceProfile允许用户指定对于处于一个stage中的RDD的执行器(executors)和任务(task)需求。在不同stage下允许用户改变资源需求。
	这个类在初始开发的状态下时私有的，一旦起到作用，即表现为公有的。
```

```markdown
@Evolving
private[spark] class ResourceProfile(){
	关系: father --> Serializable
	属性:
	#name @_id=ResourceProfile.getNextProfileId		资源描述ID
	#name @_taskResources=new mutable.HashMap[String, TaskResourceRequest]() 	任务资源列表
	#name @_executorResources=new mutable.HashMap[String, ExecutorResourceRequest]()	执行器资源列表
	操作集:
	def id: Int = _id
	功能: 获取资源描述ID
	
	def taskResources: Map[String, TaskResourceRequest] = _taskResources.toMap
	功能: 获取任务资源列表@_taskResources
	
	def executorResources: Map[String, ExecutorResourceRequest] = _executorResources.toMap
	功能: 获取执行器资源列表@executorResources
	
	def taskResourcesJMap: JMap[String, TaskResourceRequest] = _taskResources.asJava
	功能: 获取java Map形式的任务资源列表@_taskResources
	
	def executorResourcesJMap: JMap[String, ExecutorResourceRequest] = _executorResources.asJava
	功能: 获取javaMap形式的执行器资源列表@_executorResources
	
	def reset(): Unit 
	功能: 重置，清空资源列表
		_taskResources.clear()
    	_executorResources.clear()
	
	def require(requests: ExecutorResourceRequests): this.type
	功能: 申请执行器资源@request
	val = {
		_executorResources ++= requests.requests
    	this
	}
	
	def require(requests: TaskResourceRequests): this.type 
	功能: 申请任务资源@requests
	val = {
		_taskResources ++= requests.requests
    	this
	}
	
	def toString(): String
	功能: 属性显示
}
```

```markdown
private[spark] object ResourceProfile{
	关系: father --> Logging
	属性:
	#name @UNKNOWN_RESOURCE_PROFILE_ID=-1	未知资源编号
	#name @DEFAULT_RESOURCE_PROFILE_ID		默认资源编号
	#name @CPUS="cpus"	CPU资源标识
	#name @CORES="cores"	使用核心数量标识
	#name @MEMORY="memory"	内存使用量标识
	#name @OVERHEAD_MEM="memoryOverhead"	常驻内存使用量标志
	#name @PYSPARK_MEM="pyspark.memory"		pyspark使用内存量
	#name @nextProfileId=new AtomicInteger(0) lazy	下一个资源编号Id 
	#name @defaultProfileRefnew AtomicReference[ResourceProfile](new ResourceProfile())
		默认参考描述(默认资源描述使用应用级别配置，常见默认描述可理解获得ID 0，初始化获取时进行)
	操作集:
	def getNextProfileId: Int = nextProfileId.getAndIncrement()
	功能: 获取下一个资源描述ID
	
	def resetDefaultProfile(conf: SparkConf): Unit = getOrCreateDefaultProfile(conf).reset()
	功能: 重置默认资源描述(常用于测试)
	
	getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile 
	功能: 获取默认资源描述
	1. 获取参考资源描述
		val defaultProf = defaultProfileRef.get()
	2. 检查默认参考描述是否初始化
		+ 没有初始化
			defaultProf.executorResources == Map.empty
			1. 再获取一个参考描述
			val prof = defaultProfileRef.get()
			2. 将资源描述放置到资源列表中
			if (prof.executorResources == Map.empty) {
          		addDefaultTaskResources(prof, conf)
          		addDefaultExecutorResources(prof, conf)
        	}
		+ 已经初始化
		val=defaultProf
	
	def addDefaultTaskResources(rprof: ResourceProfile, conf: SparkConf): Unit 
	功能: 添加@rprof到默认任务资源列表中
	1. 设置任务资源请求中每个任务的cpu数量
	    val cpusPerTask = conf.get(CPUS_PER_TASK)
    	val treqs = new TaskResourceRequests().cpus(cpusPerTask)
	2. 为任务申请资源列表
		val taskReq = ResourceUtils.parseResourceRequirements(conf, SPARK_TASK_PREFIX)
         taskReq.foreach { req =>
          val name = s"${RESOURCE_PREFIX}.${req.resourceName}"
          treqs.resource(name, req.amount)  // 设置资源属性列表
         }
    	rprof.require(treqs)
    	
    def addDefaultExecutorResources(rprof: ResourceProfile, conf: SparkConf): Unit 
    功能： 添加@rprof到默认执行器资源列表中
    1. 设置执行器资源的核心数量和内存使用量
    	val ereqs = new ExecutorResourceRequests()
        ereqs.cores(conf.get(EXECUTOR_CORES))
        ereqs.memory(conf.get(EXECUTOR_MEMORY).toString)
	2. 执行器申请资源
		val execReq = ResourceUtils.parseAllResourceRequests(conf, SPARK_EXECUTOR_PREFIX)
    	execReq.foreach { req =>
          val name = s"${RESOURCE_PREFIX}.${req.id.resourceName}"
          ereqs.resource(name, req.amount, req.discoveryScript.getOrElse(""),
            req.vendor.getOrElse(""))
    	}
   		rprof.require(ereqs)
}
```

#### ResourceUtils

#### TaskResourceRequest

```markdown
任务资源请求，用于以编程方式连接@ResourceProfile到指定需要RDD的资源。这个会反应到stage层级上。
使用@TaskResourceRequests作为遍历API.
这个api是私有的，直到其他块就位了，它就会变成共有的。
```

```markdown
private[spark] class TaskResourceRequest(val resourceName: String, val amount: Double){
	关系: father --> Serializable
	操作条件: amount <= 0.5 || amount % 1 == 0
	初始化操作:
	+ 未分配CPU或者资源 抛出异常@IllegalArgumentException	
	if (!resourceName.equals(ResourceProfile.CPUS) && !resourceName.startsWith(RESOURCE_DOT)){
		throw new IllegalArgumentException(s"Task resource not allowed: $resourceName")
	}
	通过这个检测的进程可以进入到运行状态
}
```

#### TaskResourceRequests

```markdown
介绍:
	处理一系列任务资源请求，用于连接@ResourceProfile使用程序去指定RDD所需要的资源。这个会体现在storage级别上。这个API是私有的，知道所有资源准备完毕，才会变为共有。
```

```markdown
private[spark] class TaskResourceRequests(){
	关系: father --> Serializable
	属性:
	#name @_taskResources=new mutable.HashMap[String, TaskResourceRequest]()	任务资源
	操作集:
	def toString: String
	功能: 属性信息显示
	
	def requests: Map[String, TaskResourceRequest] = _taskResources.toMap
	功能: 获取任务资源列表(键值对形式)
	
	def cpus(amount: Int): this.type
	功能: 指定每个任务的cpu数量
	val= {
		val t = new TaskResourceRequest(CPUS, amount)	// 发起一个请求,CPU资源,数量为amout
		_taskResources(CPUS) = t	// 设置任务资源管理器的CPU属性
		this
	}
	
	def resource(rName: String, amount: Double): this.type
	功能: 申请名称为rNaame的资源
	val= {
		val t = new TaskResourceRequest(rName, amount) // 申请amount个资源类型为rName的
		_taskResources(rName) = t // 注册到任务资源管理列表
		this
	}
	
	def addRequest(treq: TaskResourceRequest): this.type 
	功能: 添加一个请求(向任务资源列表中再添加一个元素)
	val= {
		_taskResources(treq.resourceName) = treq
    	this
  	}
}
```

#### 基础拓展

1.  注解@Evolving