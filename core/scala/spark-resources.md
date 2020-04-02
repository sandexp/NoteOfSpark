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

```markdown
介绍:
	执行器(excutor)资源请求,用于连接@ResourceProfile资源描述，程序指定RDD需要的资源，并且会应用到stage级别上。
	这个类用于指定什么资源会分配给执行器，以及spark如何去找到这些资源的特定描述。对于不同的资源类型，不是都需要所有的参数。提供的资源名称时支持常用的spark配置(将前缀溢出的形式)。比如说常驻内存，在这个api中称作@memoryOverhead 它的全称为@spark.executor.memoryOverhead 移除前缀的结果。像GPU的资源，在这里就称作resource.gpu(spark.executor.resource.gpu.*).资源的数量(amount),发现脚本(discoveryScript),以及提供参数(vendor parameters)参数，用户可以使用spark.executor.resource.{resourceName}.{amount, discoveryScript, vendor}指出。
	比如说一个用户想要分配yarn的CPU资源。那么,用户必须指出资源名称(resource.gpu),每个执行器上的GPU数量,指定发现脚本(discovery script).以便于执行器启动时，能够找到GPU地址(yarn不会告诉spark程序gpu地址在哪),供应参数(vendor)由于是给Kubernetes的属性，所以不会使用。
	详细请参照配置文档和集群特性文档。
```

```scala
private[spark] class ExecutorResourceRequest(val resourceName: String,val amount: Long,
    val discoveryScript: String = "",val vendor: String = ""){
	关系: father --> Serializable
    构造器属性:
    	resourceName	资源名称
    	amount			数量
    	discoveryScript	查询脚本描述
    	vendor			供应参数
    属性:
    #name @allowedExecutorResources #type @mutable.HashSet	可行的执行器资源
    	可行的spark内部资源，诸如GPS,FGPA等用户资源也是允许的。
    初始化校验:
    !allowedExecutorResources.contains(resourceName) && !resourceName.startsWith(RESOURCE_DOT) ?
    throw IllegalArgumentException : Nop
}
```

#### ExecutorResourceRequests

```scala
private[spark] class ExecutorResourceRequests(){
	关系: father --> Serializable
	介绍: 一组执行器资源请求,用于连接@ResourceProfile对RDD进行资源指定，会反映到stage层级上。
	属性:
	#name @_executorResources #type @mutable.HashMap	资源管理列表
	操作集:
	def requests: Map[String, ExecutorResourceRequest] = _executorResources.toMap
	功能: 请求执行器资源列表
	
	def memory(amount: String): this.type
	功能: 指定堆内存,指定的值会转化为MB
	输入参数: amount 申请内存大小
	1. 获取内存实际大小
		val amountMiB = JavaUtils.byteStringAsMb(amount)
	2. 发起资源申请
		val rr = new ExecutorResourceRequest(MEMORY, amountMiB)
	3. 将申请到的资源注册到资源管理列表@_executorResources中
		_executorResources(MEMORY) = rr
	
	 def memoryOverhead(amount: String): this.type
	 功能: 指定常驻内存
	 1. 获取内存实际大小
		val amountMiB = JavaUtils.byteStringAsMb(amount)
	 2. 发起资源申请
		val rr = new ExecutorResourceRequest(OVERHEAD_MEM, amountMiB)
	 3. 将申请到的资源注册到资源管理列表@_executorResources中
		_executorResources(OVERHEAD_MEM) = rr
		
	def pysparkMemory(amount: String): this.type
	功能: 指定pyspark内存
	val amountMiB = JavaUtils.byteStringAsMb(amount)
    val rr = new ExecutorResourceRequest(PYSPARK_MEM, amountMiB)
    _executorResources(PYSPARK_MEM) = rr
	
	def cores(amount: Int): this.type 
	功能: 申请并注册执行器核心数
	val t = new ExecutorResourceRequest(CORES, amount)
    _executorResources(CORES) = t
	
	def resource(resourceName: String,amount: Long,discoveryScript: String = "",
      	vendor: String = ""): this.type
     功能: 用于给指定用户使用(GPU,FGPA),支持去前缀的spark配置
     输入参数:
     	resourceName	资源名称
     	amount			资源数量
     	discoveryScript	资源描述(按照描述来查找资源)(一些不告诉spark资源地址的集群管理器来说这个是必须项)
     	vendor			一些集群管理器所必须要的供应参数
    val eReq = new ExecutorResourceRequest(resourceName, amount, discoveryScript, vendor)
    _executorResources(resourceName) = eReq
    
    def addRequest(ereq: ExecutorResourceRequest): this.type
    功能:注册新的请求到资源管理列表中
    _executorResources(ereq.resourceName) = ereq
	
	def toString: String
	功能: 属性显示
}
```

#### ResourceAllocator

```markdown
介绍:
	这个特征用于帮助执行器/工作者(worker)分配资源，注意需要使用在单线程中
```

```scala
trait ResourceAllocator {
	属性:
	#name @addressAvailabilityMap (lazy)  
	操作集:
	def resourceName: String
	功能: 获取资源名称
	
	def resourceAddresses: Seq[String]
	功能: 获取资源地址列表
	
	def slotsPerAddress: Int
	功能: 获取每个地址的槽数(地址所对应的可使用地址数量)
		当value>0时表示地址可用，当value=0意味着地址全部被分配完毕，不可以再分配了。所以可以得到一个地址可以	 被分配@slotsPerAddress次。
		这里使用@OpenHashMap可以达到更好的效果(快速hashMap,5倍于普通hashMap)
	val= mutable.HashMap(resourceAddresses.map(_ -> slotsPerAddress): _*)
	
	def availableAddrs: Seq[String]
	功能: 获取可以资源地址信息
	val= addressAvailabilityMap.flatMap { case (addr, available) =>(0 until available).map(_ => addr)
    	}.toSeq
	
	private[spark] def assignedAddrs: Seq[String]
	功能: 当前已经分配的资源地址
	val= addressAvailabilityMap.flatMap { case (addr, available) =>
      	(0 until slotsPerAddress - available).map(_ => addr)}.toSeq
      
    def acquire(addrs: Seq[String]): Unit 
    功能: 请求资源列表，这些地址必须是可用的
    对需要申请的资源列表中的资源地址做出如下处理:
    1. 资源列表中没有这个类型的资源
    	!addressAvailabilityMap.contains(address) ? throw @SparkException : Nop
    2. 存在有此类资源，但是资源可使用量为0
    	throw SparkException
    3. 存在有此类资源，但是资源可使用量大于0
    	从资源列表中获取一个资源地址，并更新剩余可用资源量
    	isAvailable > 0 ? addressAvailabilityMap(address) = addressAvailabilityMap(address) - 1 : Nop
    
    def release(addrs: Seq[String]): Unit 
    功能: 释放资源列表,其中资源列表中的地址必须要是已经分配过的。当任务结束的时候，会将资源释放掉。
    对于需要释放的资源列表中的每一个地址做如下处理:
    1. 资源列表@addressAvailabilityMap 没有这个类型的资源
    	throw SparkException
    2. 可用资源在最大允许分配返回内
    	isAvailable < slotsPerAddress ? 
    	addressAvailabilityMap(address) = addressAvailabilityMap(address) + 1 :
    	throw SparkException(企图释放了没有分配的地址)
}
```

#### ResourceInformation

```markdown
介绍:
	用于容纳与资源相关的信息，资源可以是GPU,FGPA等等。地址数组是指定的资源，取决于用户中断地址。
	例如GPU,那么地址就是GPU的目录。(用于索引资源的下标参数)
```
```scala
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
```scala
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
```scala
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

```scala
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

```scala
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
	#name @nextProfileId=new AtomicInteger(0) lazy	当前资源编号Id 
	#name @defaultProfileRefnew AtomicReference[ResourceProfile](new ResourceProfile())
		默认参考描述(默认资源描述使用应用级别配置，常见默认描述可理解获得ID 0，初始化获取时进行)
	操作集:
	def getNextProfileId: Int = nextProfileId.getAndIncrement()
	功能: 获取当前资源描述ID
	
	def resetDefaultProfile(conf: SparkConf): Unit = getOrCreateDefaultProfile(conf).reset()
	功能: 重置默认资源描述(常用于测试)
	
	def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile 
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

```scala
private[spark] case class ResourceID(componentName: String, resourceName: String){
	介绍: 这个类是用于标识资源的，保证资源id不会重复
	构造器参数:
		componentName	组件名称(spark.driver / spark.executor / spark.task)
		resourceName	资源名称(gpu/fpga)
	操作集:
		def confPrefix: String = s"$componentName.${ResourceUtils.RESOURCE_PREFIX}.$resourceName."
		功能: 获取截取的前缀信息
		
		def amountConf: String = s"$confPrefix${ResourceUtils.AMOUNT}"
		功能: 获取资源数量信息
		
		def discoveryScriptConf: String = s"$confPrefix${ResourceUtils.DISCOVERY_SCRIPT}"
		功能: 获取查询脚本信息
		
		def vendorConf: String = s"$confPrefix${ResourceUtils.VENDOR}"
		功能: 获取供应参数信息
}
```

```scala
private[spark] case class ResourceRequest(id: ResourceID,amount: Int,discoveryScript: Option[String],
    vendor: Option[String]){
	介绍: 代表执行器层次的资源请求，这个类用于资源发现(使用查找脚本@discoveryScript)或者是通过转化配置信息得到的上下文对象@context
    构造器属性:
    	id		资源编号
    	amount	资源数量(整数类型)
    	discoveryScript	查找脚本描述
 		vendor	供应参数   	
}
```

```scala
private[spark] case class ResourceRequirement(resourceName: String,amount: Int,numParts: Int = 1){
	介绍: 表示一个组件的资源需求(这里的组件时执行器，驱动器，任务)
	例如: 
	+ spark.task.resource.[resourceName].amount = 4
		可以看成amount = 4, and numParts = 1
    + spark.task.resource.[resourceName].amount = 0.25
    	可以看成amount = 1, and numParts = 4
    	
    构造器参数:
    	resourceName 资源名称
    	amount	资源数量
    	numParts	参与均分的消费者数量(>=1 int)
}
```

```scala
private[spark] case class ResourceAllocation(id: ResourceID, addresses: Seq[String]){
	介绍: 代表指定资源的已分配资源地址，集群管理器使用json序列化的方式将这个案例类去将地址传给执行器/驱动器
	操作集:
	def toResourceInformation: ResourceInformation
	功能: 获取资源描述信息
	val= new ResourceInformation(id.resourceName, addresses.toArray)
}
```

```scala
private[spark] object ResourceUtils{
	关系: father --> Logging
	属性:
	#name @DISCOVERY_SCRIPT="discoveryScript"	查找脚本
	#name @VENDOR="vendor"	供应参数
	#name @AMOUNT="amount"	资源数量
	// 已知资源类型
	#name @GPU="gpu"	GPU
	#name @FPGA="fgpa"	FGPA
	#name @RESOURCE_PREFIX="resource"	资源前缀
	#name @RESOURCE_DOT=s"$RESOURCE_PREFIX."	资源点
	
	操作集:
	def parseResourceRequest(sparkConf: SparkConf, resourceId: ResourceID): ResourceRequest 
	功能: 转换资源请求
	输入参数: 
		sparkConf	应用程序配置集
		resourceId	资源编号
	1. 获取资源配置列表@settings
		val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
	2. 获取指定资源对应的资源数量,查找脚本,供应参数
		val amount = settings.getOrElse(AMOUNT,throw SparkException).toInt
		val discoveryScript = settings.get(DISCOVERY_SCRIPT)
		val vendor = settings.get(VENDOR)
	3. 根据已知的参数，发起资源请求并返回
		ResourceRequest(resourceId, amount, discoveryScript, vendor)
	
	def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID]
	功能: 获取资源id列表(为RESOURCE_DOT中点包围的数据)
	Array arr= sparkConf.getAllWithPrefix(s"$componentName.$RESOURCE_DOT")
	val= arr.map { case (key, _) =>key.substring(0, key.indexOf('.'))}
		.toSet.toSeq.map(name => ResourceID(componentName, name))
	
	def parseAllResourceRequests(sparkConf: SparkConf,componentName: String): Seq[ResourceRequest]
	功能: 转换所有资源请求(对资源列表中每个元素发起资源请求@parseResourceRequest)
	输入参数:
		sparkConf	应用程序配置集
		componentName	组件名称
	val= listResourceIds(sparkConf, componentName).map { id =>parseResourceRequest(sparkConf, id)}
	
	def resourcesMeetRequirements(
      resourcesFree: Map[String, Int],resourceRequirements: Seq[ResourceRequirement]): Boolean
     功能: 确认剩余资源量是否足以满足需求
     输入参数:
     	resourcesFree	剩余资源列表
     	resourceRequirements	资源需求
     val= resourceRequirements.forall {
     	req => resourcesFree.getOrElse(req.resourceName, 0) >= req.amount}
     
     def withResourcesJson[T](resourcesFile: String)(extract: String => Seq[T]): Seq[T]
     功能: 将json形式给出的资源信息转换为列表形式
     输入参数:
     	resourcesFile: String	资源文件名称(JSON 单个数据元素)
     	extract: String => Seq[T]	json转列表函数
     val= extract(json) OrElse	case NonFatal(e) throw SparkException
     
     def parseAllocatedFromJsonFile(resourcesFile: String): Seq[ResourceAllocation]
     功能: 将json文件的给出的资源转换为列表
     val= {
     	withResourcesJson[ResourceAllocation](resourcesFile) { json =>
     	implicit val formats = DefaultFormats
      	parse(json).extract[Seq[ResourceAllocation]]
     }
	
	def parseAllocatedOrDiscoverResources(sparkConf: SparkConf,componentName: String,
      resourcesFileOpt: Option[String]): Seq[ResourceAllocation]
	功能: 分配/发现资源
	输入参数:
		sparkConf	应用程序配置集
		componentName	组件名称
		resourcesFileOpt	资源文件配置
	1. 获取资源列表中名称为@componentName
		val allocated = resourcesFileOpt.toSeq.flatMap(parseAllocatedFromJsonFile)
			.filter(_.id.componentName == componentName)
	2. 获取列表中同个组件其他资源id
		val otherResourceIds = listResourceIds(sparkConf, componentName).diff(allocated.map(_.id))
	3. 对这个组件的资源进行资源分配工作
		allocated ++ otherResourceIds.map { id =>
			val request = parseResourceRequest(sparkConf, id)
      		ResourceAllocation(id, discoverResource(request).addresses)}
      		
    def parseResourceRequirements(sparkConf: SparkConf, componentName:String:Seq[ResourceRequirement]
    功能: 转换资源需求
    操作逻辑:
    对当前组件下所有资源id进行如下处理:
    1. 获取配置信息，主要需要资源需求量
      val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
      val amountDouble = settings.getOrElse(AMOUNT,throw new SparkException).toDouble
    2. 获取每个资源消费者应该获取的资源数量
    val= !componentName.equalsIgnoreCase(SPARK_TASK_PREFIX)?Nop:
    	(amountDouble % 1 != 0)? throw SparkException : amountDouble <= 0.5? 
        Math.floor(1.0 / amountDouble).toInt : 1
    3. 发起对于单个资源消费者的资源请求
    ResourceRequirement(resourceId.resourceName, amount, parts)
    
    def assertResourceAllocationMeetsRequest(allocation: ResourceAllocation,
      request: ResourceRequest): Unit 
    功能: 假设检定资源满足分配需求(相当于java中的assert)
    
    def assertAllResourceAllocationsMeetRequests(allocations: Seq[ResourceAllocation],
      requests: Seq[ResourceRequest]): Unit
    功能: 假设检定所有资源满足分配需求
    val allocated = allocations.map(x => x.id -> x).toMap// 获取已分配资源列表
    requests.foreach(r => assertResourceAllocationMeetsRequest(allocated(r.id), r))
	
	def logResourceInfo(componentName: String, resources: Map[String, ResourceInformation])
	功能: 显示所有资源信息
	
	def getOrDiscoverAllResources(sparkConf: SparkConf,componentName: String,
      resourcesFileOpt: Option[String]): Map[String, ResourceInformation]
	功能: 获取/查找所有资源(合计资源=已分配+未分配)
	介绍: 获取输入组件属性的所有资源信息，并通过查询脚本获取剩余的资源，返回一个资源列表@Map
	输入参数:
		sparkConf	应用程序配置集
		componentName	组件名称
		resourcesFileOpt	资源文件
	1. 获取当前组件以及分配的资源列表信息
	val requests = parseAllResourceRequests(sparkConf, componentName)
	2. 获取当前组件总共需要分配资源列表
	val allocations = parseAllocatedOrDiscoverResources(sparkConf, componentName, resourcesFileOpt)
	3. 获取资源信息列表,并返回
	@assertAllResourceAllocationsMeetRequests(allocations, requests)
    val resourceInfoMap = allocations.map(a => (a.id.resourceName, a.toResourceInformation)).toMap
	
	def discoverResource(resourceRequest: ResourceRequest): ResourceInformation
	功能: 查找资源
	输入参数:
		resourceRequest	请求资源
	操作逻辑:
	1. 获取资源基本信息
		资源名称		val resourceName = resourceRequest.id.resourceName
		资源的查找脚本		val script = resourceRequest.discoveryScript
	2. 获取查找脚本
		分为三种情况
		+ 查找脚本为空(!script.nonEmpty)
		throw SparkException
		+ 脚本对应的文件不存在(!scriptFile.exists())
		throw SparkException
		+ 其他情况
		val output = executeAndGetOutput(Seq(script.get), new File("."))
        ResourceInformation.parseJson(output)
   	3. 校验当前结果的合法性
   		+ !result.name.equals(resourceName)
   		throw SparkException
   		+ result.name.equals(resourceName)
   		val=result
}
```



#### TaskResourceRequest

```markdown
任务资源请求，用于以编程方式连接@ResourceProfile到指定需要RDD的资源。这个会反应到stage层级上。
使用@TaskResourceRequests作为遍历API.
这个api是私有的，直到其他块就位了，它就会变成共有的。
```

```scala
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

```scala
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

1. 注解@Evolving

2.  scala关于json的处理

   #trait @JsonMethods 

   #class @ExtractableJsonAstNode(jv: JValue)