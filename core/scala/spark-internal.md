

## *spark-internal**

---

1.  [config](# config)
2.  [io](# io)
3.  [plugin](# plugin)
4.  [Logging.scala](# Logging)

---

#### config

1.  [package.scala](# package)
2.  [ConfigBuilder.scala](# ConfigBuilder)
3.  [ConfigEntry.scala](# ConfigEntry)
4.  [ConfigProvider.scala](# ConfigProvider)
5.  [ConfigReader.scala](# ConfigReader)
6.  [Deploy.scala](# Deploy)
7.  [History.scala](# History)
8.  [Kryo.scala](# Kryo)
9.  [Network.scala](# Network)
10.  [Python.scala](# Python)
11.  [R.scala](# R)
12.  [Status.scala](# Status)
13.  [Streaming.scala](# Streaming)
14.  [Tests.scala](# Tests)
15.  [UI.scala](# UI)
16.  [Worker.scala](# Worker)

---

#### package

| 参数名称                        | 参数类型  | 参数值                                                       | 文档注释                                                     |
| ------------------------------- | --------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| SPARK_DRIVER_PREFIX             | String    | spark.driver                                                 | diver前缀                                                    |
| SPARK_EXECUTOR_PREFIX           | String    | spark.executor                                               | excutor前缀                                                  |
| SPARK_TASK_PREFIX               | String    | spark.task                                                   | task前缀                                                     |
| LISTENER_BUS_EVENT_QUEUE_PREFIX | String    | spark.scheduler.listenerbus.eventqueue                       | 监听事件队列前缀                                             |
| SPARK_RESOURCES_COORDINATE      | Boolean   | spark.resources.coordinate.enable=true                       | 是否自动的协调worker和driver之间的资源.如果配置为false,用于需要配置不同的资源给运行在一台机器上的workers和drivers. |
| SPARK_RESOURCES_DIR             | String    | spark.resources.dir = stringConf.createOptional              | 在脱机模式下用于协调资源的目录,默认情况下是SPARK_HOME,需要确保在一台机器上workers和divers使用的是一个目录.在workers和drivers存在时注意不要去删除掉这个目录,很有可能引发资源冲突 |
| DRIVER_RESOURCES_FILE           | String    | spark.driver.resourcesFile=stringConf.createOptional         | 这是一个包含分配给driver的资源的文件.文件需要格式化为json格式的@ResourceAllocation数组,仅仅使用在脱机模式下. |
| DRIVER_CLASS_PATH               | String    | ConfigBuilder(SparkLauncher.DRIVER_EXTRA_CLASSPATH).stringConf.createOptional | Driver类路径(DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath") |
| DRIVER_JAVA_OPTIONS             | String    | ConfigBuilder(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)  .withPrepended(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS)  .stringConf  .createOptional |                                                              |
| DRIVER_LIBRARY_PATH             | String    | ConfigBuilder(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH).stringConf.createOptional | driver 库路径                                                |
| DRIVER_USER_CLASS_PATH_FIRST    | String    | ConfigBuilder("spark.driver.userClassPathFirst").booleanConf.createWithDefault(false) | driver用户首选类路径                                         |
| DRIVER_CORES                    | int       | spark.driver.cores=1                                         | driver进程的核心数量,仅仅使用在集群模式下                    |
| DRIVER_MEMORY                   | bytesConf | ConfigBuilder(SparkLauncher.DRIVER_MEMORY).bytesConf(ByteUnit.MiB)..createWithDefaultString("1g") | 使用在driver进程中的的内存数量,在不指定的模式下单位为MB.     |
| DRIVER_MEMORY_OVERHEAD          | bytesConf | ConfigBuilder("spark.driver.memoryOverhead").bytesConf(ByteUnit.MiB).createOptional | 集群模式下每个驱动器分配的非堆模式内存.除非指定,单位为MB     |
| DRIVER_LOG_DFS_DIR              | String    | ConfigBuilder("spark.driver.log.dfsDir").stringConf.createOptional | 驱动器DFS日志目录                                            |
| DRIVER_LOG_LAYOUT               | String    | ConfigBuilder("spark.driver.log.layout")  .stringConf.createOptional | 驱动器日志布局                                               |
| DRIVER_LOG_PERSISTTODFS         | Boolean   | ConfigBuilder("spark.driver.log.persistToDfs.enabled")  .booleanConf  .createWithDefault(false) | 驱动器日志是否持久化到DFS上                                  |
| DRIVER_LOG_ALLOW_EC             | Boolean   | ConfigBuilder("spark.driver.log.allowErasureCoding")  .booleanConf  .createWithDefault(false) | driver侧是否支持擦除式编程                                   |

#### ConfigBuilder

```markdown
private object ConfigHelpers {
	功能: 辅助配置
	操作集:
	def toNumber[T](s: String, converter: String => T, key: String, configType: String): T
	功能: 将字符串类型转化为数字类型T
	val= converter(s.trim)
	
	def toBoolean(s: String, key: String): Boolean
	功能: 转化为boolean类型
	val= s.trim.toBoolean
	
	def stringToSeq[T](str: String, converter: String => T): Seq[T]
	功能: 字符串转序列
	val= Utils.stringToSeq(str).map(converter)
	
	def seqToString[T](v: Seq[T], stringConverter: T => String): String 
	功能: 序列转串
	val= v.map(stringConverter).mkString(",")
	
	def timeFromString(str: String, unit: TimeUnit): Long = JavaUtils.timeStringAs(str, unit)
	功能: 获取串对应的时间值(long型)
	
	def timeToString(v: Long, unit: TimeUnit): String = TimeUnit.MILLISECONDS.convert(v, unit) + "ms"
	功能: 时间转串
	
	def byteFromString(str: String, unit: ByteUnit): Long
	功能: 串转字节
	val (input, multiplier) =
      if (str.length() > 0 && str.charAt(0) == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
	
	def byteToString(v: Long, unit: ByteUnit): String = unit.convertTo(v, ByteUnit.BYTE) + "b"
	功能: 字节转串
	
	def regexFromString(str: String, key: String): Regex
	功能: 串转正则表达式
	try str.r catch {
		case e: PatternSyntaxException =>
			throw IllegalArgumentException
	}
}
```

```markdown
private[spark] class TypedConfigBuilder[T] (val parent: ConfigBuilder,val converter: String => T,
  	val stringConverter: T => String){
 	介绍: 类型安全的配置组件，提供了转换输入数据以及创建最终配置键值对的方法。
    	返回@ConfigEntry配置键值对的方法，必须要能够可以使用在sparkConf下。
    构造器:
    	parent	上级配置组件器
    	converter	转换函数(String --> T)
    	stringConverter	串转换函数(T--> String)
    操作集:
    def transform(fn: T => T): TypedConfigBuilder[T]
    功能: 转换为类型安全的配置组件@TypedConfigBuilder，转换函数为指定@fn
    val= new TypedConfigBuilder(parent, s => fn(converter(s)), stringConverter)
    
    def checkValue(validator: T => Boolean, errorMsg: String): TypedConfigBuilder[T]
    功能: 检查用户给出的value值是否匹配,返回类型安全的配置组件@TypedConfigBuilder
 	输入参数: validator	验证函数
 			errorMsg	错误信息
 	val= transform { v =>
      		if (!validator(v)) throw new IllegalArgumentException(errorMsg)
      		v
    	}
    
    def checkValues(validValues: Set[T]): TypedConfigBuilder[T]
    功能: 检查用户提供的值是否满足预设集合
    val= transform { v =>
      if (!validValues.contains(v)) 
        throw new IllegalArgumentException
      v
    }
    
    def toSequence: TypedConfigBuilder[Seq[T]]
    功能: 转换配置键值对成底层类型序列@TypedConfigBuilder[Seq[T]]
    val= new TypedConfigBuilder(parent, stringToSeq(_, converter), seqToString(_, stringConverter))
    
    def createOptional: OptionalConfigEntry[T] 
    功能: 创建一个配置键值对@OptionalConfigEntry[T]
 	1. 获取键值对信息
 	val entry = new OptionalConfigEntry[T](parent.key, parent._prependedKey,
      	parent._prependSeparator, parent._alternatives, converter, stringConverter, parent._doc,
      	parent._public)
 	2. 创建获取的键值对
 	parent._onCreate.foreach(_(entry))
 	val= entry
 	
 	def createWithDefault(default: T): ConfigEntry[T]
 	功能: 使用默认值@default创建配置键值对@ConfigEntry
 	default.isInstanceOf[String] ?
 		createWithDefaultString(default.asInstanceOf[String]) :
 		{	
 			// 获取默认值对应的字符串
 			val transformedDefault = converter(stringConverter(default))
             // 获取字符串对应的键值对
             val entry = new ConfigEntryWithDefault[T](parent.key, parent._prependedKey,
                parent._prependSeparator, parent._alternatives, transformedDefault, converter,
                stringConverter, parent._doc, parent._public)
 			val= entry
 		}
 	
 	def createWithDefaultString(default: String): ConfigEntry[T]
 	功能: 根据默认的串创建配置键值对@ConfigEntry
 	val entry = new ConfigEntryWithDefaultString[T](parent.key, parent._prependedKey,
      parent._prependSeparator, parent._alternatives, default, converter, stringConverter,
      parent._doc, parent._public)
    parent._onCreate.foreach(_(entry)) // 创建配置键值对
    val= entry
    
    def createWithDefaultFunction(defaultFunc: () => T): ConfigEntry[T]
    功能: 根据默认函数@defaultFunc 创建配置键值对
    val entry = new ConfigEntryWithDefaultFunction[T](parent.key, parent._prependedKey,
      parent._prependSeparator, parent._alternatives, defaultFunc, converter, stringConverter,
      parent._doc, parent._public)
    parent._onCreate.foreach(_ (entry))
    val= entry
 }
```

```markdown
private[spark] case class ConfigBuilder(key: String) {
	介绍: spark配置的基础组件器@ConfigBuilder
	属性:
	#name @_prependedKey=None #type @Option[String] 	预先添加的key值
	#name @_prependSeparator="" #type @String	预先添加的分离字符串
	#name @_public = true	是否为公用标志
	#name @_doc="" 	说明文档
	#name @_onCreate=None #type @Option[ConfigEntry[_] => Unit]	创建函数
	#name @_alternatives=List.empty[String] 可代替选择列表
	
	操作集:
	def internal(): ConfigBuilder
	功能: 禁止外部访问
        _public = false
        val=this
	
	def doc(s: String): ConfigBuilder
	功能: 设置文档内容
        _doc = s
        val=this
	
	def onCreate(callback: ConfigEntry[_] => Unit): ConfigBuilder 
	功能: 配置键值对最终实例化时，注册回调函数@callback，当前用于SQLConf用于追踪SQL配置键值对
        _onCreate = Option(callback)
        this
	
	def withPrepended(key: String, separator: String = " "): ConfigBuilder
	功能: 预读@key与分割字符串@separator
        _prependedKey = Option(key)
        _prependSeparator = separator
        this
	
	def withAlternative(key: String): ConfigBuilder
	功能: 可代替列表中添加key值
        _alternatives = _alternatives :+ key
        this
	
	def intConf: TypedConfigBuilder[Int]
	功能: int型配置
        checkPrependConfig
        val= new TypedConfigBuilder(this, toNumber(_, _.toInt, key, "int"))
	
	def longConf: TypedConfigBuilder[Long]
	功能: long 型配置
        checkPrependConfig
        new TypedConfigBuilder(this, toNumber(_, _.toLong, key, "long"))
	
	def doubleConf: TypedConfigBuilder[Double]
	功能: double 型配置
	val= new TypedConfigBuilder(this, toNumber(_, _.toDouble, key, "double"))
	
	 def timeConf(unit: TimeUnit): TypedConfigBuilder[Long]
	 功能: time 型配置
         checkPrependConfig
         val= new TypedConfigBuilder(this, timeFromString(_, unit), timeToString(_, unit))
	
	def stringConf: TypedConfigBuilder[Long] 
	功能: string 配置
	val= new TypedConfigBuilder(this, v => v)
	
	def bytesConf(unit: ByteUnit): TypedConfigBuilder[Long]
	功能: byte型 配置
	checkPrependConfig
     val= new TypedConfigBuilder(this, byteFromString(_, unit), byteToString(_, unit))
	
	def regexConf: TypedConfigBuilder[Regex]
	功能: 正则配置
	checkPrependConfig
     val= new TypedConfigBuilder(this, regexFromString(_, this.key), _.toString)
	
	def fallbackConf[T](fallback: ConfigEntry[T]): ConfigEntry[T]
	功能: 回退配置
	输入参数: fallback 回退配置键值对(恢复到fallback的配置)
	val entry = new FallbackConfigEntry(key, _prependedKey, _prependSeparator, _alternatives, _doc,
      _public, fallback)
    _onCreate.foreach(_(entry))
    val= entry
	
	def checkPrependConfig:Unit
	功能: 检查预置配置
		if (_prependedKey.isDefined)
			throw new IllegalArgumentException
}	
```



#### ConfigEntry

```markdown
介绍:
	这个entry(键值对信息)包含配置的元数据信息。
	当设置变量替代一个配置值的时候。仅仅前缀为spark.的才会被考虑在默认命名空间内。对于已知的spark配置键值(使用ConfigBuilder配置的键值)，当其存在时，也会考虑默认值。
	变量扩展也使用在有默认值的键值对信息中，这些信息含有一个string格式的默认值。
```

```markdown
private[spark] abstract class ConfigEntry[T] (val key: String,val prependedKey: Option[String],
    val prependSeparator: String,val alternatives: List[String],val valueConverter: String => T,
    val stringConverter: T => String,val doc: String,val isPublic: Boolean)
    构造器属性:
    	key	配置的key值
    	prependedKey	预置key值
    	prependSeparator	预置分离字符串
    	valueConverter	value转换函数(String --> T)
    	stringConverter	string转换函数
    	doc	配置文档
    	isPublic	是否对用于公开
 	
 	def defaultValueString: String
 	功能: 获取默认value的串信息
 	
 	def readString(reader: ConfigReader): Option[String]
 	功能: 获取配置阅读器@reader的
 	1. 获取预置信息中的value列表
 	val values = Seq(
      prependedKey.flatMap(reader.get(_)),
      alternatives.foldLeft(reader.get(key))((res, nextKey) => res.orElse(reader.get(nextKey)))
    ).flatten
    2. 返回value列表信息
    val= values.nonEmpty ? Some(values.mkString(prependSeparator)) : None
    
    def readFrom(reader: ConfigReader): T
    功能: 获取阅读器@reader的值
    
    def defaultValue: Option[T] = None
    功能: 获取默认值
    
    def toString: String
    功能: 显示信息值
 }
```

```markdown
private class ConfigEntryWithDefault[T] (
    key: String,
    prependedKey: Option[String],
    prependSeparator: String,
    alternatives: List[String],
    _defaultValue: T,
    valueConverter: String => T,
    stringConverter: T => String,
    doc: String,
    isPublic: Boolean){
	介绍: 默认状态下获取配置键值对信息
    关系: father --> ConfigEntry(key,prependedKey,prependSeparator,alternatives,valueConverter,
    		stringConverter,doc,isPublic)
	操作集:
	def defaultValue: Option[T] = Some(_defaultValue)
	功能: 获取默认value值
	
	def defaultValueString: String = stringConverter(_defaultValue)
	功能: 获取默认value值的串表达形式
	
	def readFrom(reader: ConfigReader): T
	功能: 获取阅读器的默认value值，类型为T
	readString(reader).map(valueConverter).getOrElse(_defaultValue)
}
```

```markdown
private class ConfigEntryWithDefaultString[T] (key: String,prependedKey: Option[String],
    prependSeparator: String,alternatives: List[String],_defaultValue: String,
    valueConverter: String => T,stringConverter: T => String,
    doc: String,isPublic: Boolean){
	关系: father -->  ConfigEntry(key,prependedKey,prependSeparator,alternatives,valueConverter,
    		stringConverter,doc,isPublic)
    介绍: 默认串情景下配置键值对的处理方式
    操作集:
    def defaultValue: Option[T] = Some(valueConverter(_defaultValue))
    功能: 获取默认值
    
    def defaultValueString: String = _defaultValue
    功能: 获取默认值的串表达形式
    
    def readFrom(reader: ConfigReader): T
    功能: 获取阅读器默认value值
    val value = readString(reader).getOrElse(reader.substitute(_defaultValue))// String
    val= valueConverter(value)
}
```

```markdown
private class ConfigEntryWithDefaultFunction[T] (key: String,prependedKey: Option[String],
    prependSeparator: String,alternatives: List[String],_defaultFunction: () => T,
    valueConverter: String => T,stringConverter: T => String,
    doc: String,isPublic: Boolean){
	关系: father --> ConfigEntry(key,prependedKey,prependSeparator,alternatives,valueConverter,
    		stringConverter,doc,isPublic)
     操作集:
     def defaultValue: Option[T] = Some(_defaultFunction())
     功能: 获取默认value值
     
     def defaultValueString: String = stringConverter(_defaultFunction())
     功能: 获取默认value值的串表达方式
     
     def readFrom(reader: ConfigReader): T
     功能: 获取阅读器默认value值
     val= readString(reader).map(valueConverter).getOrElse(_defaultFunction())
}
```

```markdown
private[spark] class OptionalConfigEntry[T] (key: String,prependedKey: Option[String],
    prependSeparator: String,alternatives: List[String],
    val rawValueConverter: String => T,val rawStringConverter: T => String,
    doc: String,isPublic: Boolean){
	关系: father -->  ConfigEntry[Option[T]](key,prependedKey,prependSeparator,alternatives,
    	s => Some(rawValueConverter(s)),v => v.map(rawStringConverter).orNull,doc,isPublic)
     介绍: 可以包含空的配置键值对
	操作集:
	def defaultValueString: String = ConfigEntry.UNDEFINED
	功能: 获取默认value的串表达形式
	
	def readFrom(reader: ConfigReader): Option[T]
	功能: 获取阅读器的默认值
	val=readString(reader).map(rawValueConverter)
}
```

```markdown
private[spark] class FallbackConfigEntry[T] (key: String,prependedKey: Option[String],
    prependSeparator: String,alternatives: List[String],
    doc: String,isPublic: Boolean,val fallback: ConfigEntry[T]))
	关系: father --> ConfigEntry[T](key,prependedKey,prependSeparator,alternatives,
    		fallback.valueConverter,fallback.stringConverter,doc,isPublic)
	介绍: 为回退而设置的配置键值对信息
	操作集:
	def defaultValueString: String = s"<value of ${fallback.key}>"
	功能: 获取默认value的串表达形式
	
	def readFrom(reader: ConfigReader): T
	功能: 获取1默认值的value值
	val= readString(reader).map(valueConverter).getOrElse(fallback.readFrom(reader))
}
```

```markdown
private[spark] object ConfigEntry {
	属性:
    #name @UNDEFINED = "<undefined>" 	未定义信息
    #name @knownConfigs = new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]()
    	已知配置列表
    操作集:
    def registerEntry(entry: ConfigEntry[_]): Unit 
	功能: 注册键值对
	val existing = knownConfigs.putIfAbsent(entry.key, entry)
    require(existing == null, s"Config entry ${entry.key} already registered!")
	
	def findEntry(key: String): ConfigEntry[_] = knownConfigs.get(key)
	功能: 查找key对应的value串表达信息
}
```



#### ConfigProvider

```markdown
private[spark] trait ConfigProvider {
	介绍: 提供配置的查询
	操作集:
	def get(key: String): Option[String]
	功能: 获取配置key对应的value值
}
```

```markdown
private[spark] class EnvProvider{
	关系: father --> ConfigProvider
	介绍: 提供环境的查询
	操作集:
	def get(key: String): Option[String] = sys.env.get(key)
	功能: 获取系统环境参数
}
```

```markdown
private[spark] class SystemProvider{
	关系: father --> ConfigProvider
	介绍: 获取系统属性
	def get(key: String): Option[String] = sys.props.get(key)
	功能: 获取系统属性值
}
```

```markdown
private[spark] class MapProvider(conf: JMap[String, String]) {
	关系: father --> ConfigProvider
	
	def get(key: String): Option[String] = Option(conf.get(key))
	功能: 获取key的value值
}
```

```markdown
private[spark] class SparkConfigProvider(conf: JMap[String, String]) {
	关系: father --> ConfigProvider
	def get(key: String): Option[String]
	功能: 获取spark配置属性值
	val=key.startsWith("spark.") ? 
	Option(conf.get(key)).orElse(SparkConf.getDeprecatedConfig(key, conf)): None
}
```

#### ConfigReader

```markdown
介绍:
	这是一个辅助类,用于读取配置键值对和进行变量的替换.
	如果配置值包含变量,且形如"${prefix:variableName}",参考会根据前缀代替属性值.默认情况下,处理如下属性:
	1. 没有前缀,使用默认配置提供器
	2. 系统: 查找系统属性的值
	3. 环境变量: 查找环境变量的值
	不同前缀受制于#class @ConfigProvider,用于读取数据源中的配置值,系统和环境变量都能够被重写.
	如果参考不能被解决问题,原始字符串会被保留.
```

```markdown
private[spark] class ConfigReader(conf: ConfigProvider) {
	属性:
	#name @bindings=new HashMap[String, ConfigProvider]() 绑定端口
	操作集:
	def bind(prefix: String, provider: ConfigProvider): ConfigReader
	功能: 绑定端口到一个@ConfigProvider，方法是线程不安全的
		bindings(prefix) = provider
	
	def bind(prefix: String, values: JMap[String, String]): ConfigReader 
	功能: 绑定前缀到map中
		bind(prefix, new MapProvider(values))
	
	def bindEnv(provider: ConfigProvider): ConfigReader = bind("env", provider)
	功能: 绑定环境变量参数
	
	def bindSystem(provider: ConfigProvider): ConfigReader = bind("system", provider)
	功能: 绑定系统变量
	
	def get(key: String): Option[String] = conf.get(key).map(substitute)
	功能: 获取配置
	读取默认提供器的key对应的value值,并应用到各种各样的替代物
	
	def substitute(input: String): String = substitute(input, Set())
	功能: 对指定的输入串@input 进行替换@substitute(String,Set<String>)
	0. 判空 if(input==null) val=input else jump 1; 
	1. 替换满足正则表达式的子串
	ConfigReader.REF_RE.replaceAllIn(input, { m =>
        val prefix = m.group(1)
        val name = m.group(2)
        val ref = if (prefix == null) name else s"$prefix:$name"
        require(!usedRefs.contains(ref), s"Circular reference in $input: $ref")

        val replacement = bindings.get(prefix)
          .flatMap(getOrDefault(_, name))
          .map { v => substitute(v, usedRefs + ref) }
          .getOrElse(m.matched)
        Regex.quoteReplacement(replacement)
     })
	
	def getOrDefault(conf: ConfigProvider, key: String): Option[String]
	功能: 获取配置@ConfigProvider key对应的属性值,如果找不到值且@ConfigEntry有默认值,则返回这个默认值
	conf.get(key).orElse {
      ConfigEntry.findEntry(key) match {
        case e: @ConfigEntryWithDefault[_] => Option(e.defaultValueString)
        // 采取默认串
        case e: @ConfigEntryWithDefaultString[_] => Option(e.defaultValueString)
        // 采取默认函数
        case e: @ConfigEntryWithDefaultFunction[_] => Option(e.defaultValueString)
        // 采取回退参数	
        case e: @FallbackConfigEntry[_] => getOrDefault(conf, e.fallback.key)
        // 其他类型 -->None
        case _ => None
      }
    }
    
}
```

#### Deploy

```markdown
介绍:	spark 部署参数
```

| 参数                  | 属性值                                                       | 介绍                                                         |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| RECOVERY_MODE         | ConfigBuilder("spark.deploy.recoveryMode")  .stringConf  .createWithDefault("NONE") | 恢复模式                                                     |
| RECOVERY_MODE_FACTORY | ConfigBuilder("spark.deploy.recoveryMode.factory")  .stringConf  .createWithDefault("") | 恢复模式工厂                                                 |
| RECOVERY_DIRECTORY    | ConfigBuilder("spark.deploy.recoveryDirectory")  .stringConf  .createWithDefault("") | 恢复目录                                                     |
| ZOOKEEPER_URL         | ConfigBuilder("spark.deploy.zookeeper.url").stringConf.createOptional | Zookeeper地址: 当恢复模式设置为zookeeper时,这个属性用于设置zookeeper的连接地址 |
| ZOOKEEPER_DIRECTORY   | ConfigBuilder("spark.deploy.zookeeper.dir").stringConf.createOptional | Zookeeper目录                                                |
| RETAINED_APPLICATIONS | ConfigBuilder("spark.deploy.retainedApplications")  .intConf  .createWithDefault(200) | 保留应用程序数目,默认200                                     |
| RETAINED_DRIVERS      | RETAINED_DRIVERS = ConfigBuilder("spark.deploy.retainedDrivers")  .intConf  .createWithDefault(200) | 保留Driver数量,默认200                                       |
| REAPER_ITERATIONS     | ConfigBuilder("spark.dead.worker.persistence")  .intConf  .createWithDefault(15) | 获取迭代器数量,默认15                                        |
| MAX_EXECUTOR_RETRIES  | ConfigBuilder("spark.deploy.maxExecutorRetries")  .intConf  .createWithDefault(10) | 执行器最大尝试次数,默认10                                    |
| DEFAULT_CORES         | ConfigBuilder("spark.deploy.defaultCores")  .intConf  .createWithDefault(Int.MaxValue) | 默认核心数量                                                 |
| SPREAD_OUT_APPS       | ConfigBuilder("spark.deploy.spreadOut")  .booleanConf  .createWithDefault(true) | 是否溢出属性                                                 |

#### History

| 属性 | 参数值 | 介绍     |
| ---- | ------ | ---------- |
| DEFAULT_LOG_DIR | "file:/tmp/spark-events" | 默认日志目录 |
| SAFEMODE_CHECK_INTERVAL_S | ConfigBuilder("spark.history.fs.safemodeCheck.interval")  .timeConf(TimeUnit.SECONDS)  .createWithDefaultString("5s") | 安全模式检查时间间隔/s,默认5s |
| UPDATE_INTERVAL_S | ConfigBuilder("spark.history.fs.update.interval")  .timeConf(TimeUnit.SECONDS)  .createWithDefaultString("10s") | 更新时间间隔10s |
| CLEANER_ENABLED | ConfigBuilder("spark.history.fs.cleaner.enabled")  .booleanConf  .createWithDefault(false) | 清除标记 |
| CLEANER_INTERVAL_S | ConfigBuilder("spark.history.fs.cleaner.interval")  .timeConf(TimeUnit.SECONDS)  .createWithDefaultString("1d") | 清除事件间隔,默认1d |
| MAX_LOG_AGE_S | ConfigBuilder("spark.history.fs.cleaner.maxAge")  .timeConf(TimeUnit.SECONDS)  .createWithDefaultString("7d") | 最大日志生存时间,默认7d |
| MAX_LOG_NUM | ConfigBuilder("spark.history.fs.cleaner.maxNum").intConf  .createWithDefault(Int.MaxValue) | 最大日志数量,默认为Int最大值 |
| LOCAL_STORE_DIR | ConfigBuilder("spark.history.store.path")  stringConf  .createOptional | 本地存储目录,应用历史信息缓存的地方,默认没有设置,意味着所有的历史信息会存储在内存中 |
| MAX_LOCAL_DISK_USAGE | ConfigBuilder("spark.history.store.maxDiskUsage")  .bytesConf(ByteUnit.BYTE)  .createWithDefaultString("10g") | 本地磁盘最大使用量 |
| HISTORY_SERVER_UI_PORT | ConfigBuilder("spark.history.ui.port") .intConf  .createWithDefault(18080) | spark history服务器的web端口,默认18080 |
| FAST_IN_PROGRESS_PARSING | ConfigBuilder("spark.history.fs.inProgressOptimization.enabled")  .booleanConf  .createWithDefault(true) | 快速转换成in-process的日志.但是会遗留下重命名失败的文件对应的应用 |
| END_EVENT_REPARSE_CHUNK_SIZE | ConfigBuilder("spark.history.fs.endEventReparseChunkSize")  .bytesConf(ByteUnit.BYTE)  .createWithDefaultString("1m") | 在日志文件末尾有多少字节需要转化为结束事件.主要用于加速应用生成,通过跳过不必要的日志文件块.可以将参数设置为0,去关闭这个设置 |
| DRIVER_LOG_CLEANER_ENABLED | ConfigBuilder("spark.history.fs.driverlog.cleaner.enabled")  .fallbackConf(CLEANER_ENABLED) | 驱动器日志生成标志 |
| DRIVER_LOG_CLEANER_INTERVAL | ConfigBuilder("spark.history.fs.driverlog.cleaner.interval")  .fallbackConf(CLEANER_INTERVAL_S) | 驱动器日志清除时间间隔 |
| MAX_DRIVER_LOG_AGE_S | ConfigBuilder("spark.history.fs.driverlog.cleaner.maxAge")  .fallbackConf(MAX_LOG_AGE_S) | driver日志最大生成时间 |
| HISTORY_SERVER_UI_ACLS_ENABLE | ConfigBuilder("spark.history.ui.acls.enable")  .booleanConf  .createWithDefault(false) | history服务器web页面可否请求标志,默认false |
| HISTORY_SERVER_UI_ADMIN_ACLS | ConfigBuilder("spark.history.ui.admin.acls")  .stringConf  .toSequence  .createWithDefault(Nil) | history服务器Web管理请求 |
| HISTORY_SERVER_UI_ADMIN_ACLS | ConfigBuilder("spark.history.ui.admin.acls.groups")  .stringConf  .toSequence  .createWithDefault(Nil) | history服务器Web管理请求组 |
| NUM_REPLAY_THREADS | ConfigBuilder("spark.history.fs.numReplayThreads")  .intConf  .createWithDefaultFunction(() => Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt) | 重新运行线程数量 |
| RETAINED_APPLICATIONS | ConfigBuilder("spark.history.retainedApplications")  .intConf  .createWithDefault(50) | 剩余应用数量,默认50 |
| PROVIDER | ConfigBuilder("spark.history.provider")  .stringConf  .createOptional |  |
| KERBEROS_ENABLED | ConfigBuilder("spark.history.kerberos.enabled")  .booleanConf  .createWithDefault(false) | kerberos安全系统使能标记.默认false |
| KERBEROS_PRINCIPAL | ConfigBuilder("spark.history.kerberos.principal")  .stringConf  .createOptional | Kerberos系统准则 |
| KERBEROS_KEYTAB | ConfigBuilder("spark.history.kerberos.keytab")  .stringConf  .createOptional | Kerberos密钥表 |
| CUSTOM_EXECUTOR_LOG_URL | ConfigBuilder("spark.history.custom.executor.log.url")  .stringConf  .createOptional | 用于执行器日志地址: 指定用户spark执行器日志,支持外部日志服务,而不是使用集群管理器在history服务器中的地址.如果样式在集群中会发生变化的化,spark支持多个路径变量.请检查你的集群模式下样式是否能够被支持.该项配置对于一个已经运行的程序配置无效. |
| APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP | ConfigBuilder("spark.history.custom.executor.log.url.applyIncompleteApplication")  .booleanConf  .createWithDefault(true) | 使用去应用用户执行器上的日志地址,进对于未运行的应用程序设置才有效. |

#### Kryo

kryo注册与序列化参数的设置

| 属性                            | 参数值                                                       | 介绍                                        |
| ------------------------------- | ------------------------------------------------------------ | ------------------------------------------- |
| KRYO_REGISTRATION_REQUIRED      | ConfigBuilder("spark.kryo.registrationRequired")  .booleanConf  .createWithDefault(false) | 是否需要登记Kryo,默认false                  |
| KRYO_USER_REGISTRATORS          | ConfigBuilder("spark.kryo.registrator")  .stringConf  .createOptional | Kryo用户注册                                |
| KRYO_CLASSES_TO_REGISTER        | ConfigBuilder("spark.kryo.classesToRegister")  .stringConf  .toSequence  .createWithDefault(Nil) | 需要Kryo注册列表                            |
| KRYO_USE_UNSAFE                 | ConfigBuilder("spark.kryo.pool")  .booleanConf  .createWithDefault(true) | kryo是否采用不安全的方式,默认不采用,为false |
| KRYO_USE_POOL                   | ConfigBuilder("spark.kryo.pool")  .booleanConf  .createWithDefault(true) | 是否使用kryo池技术,默认使用                 |
| KRYO_REFERENCE_TRACKING         | ConfigBuilder("spark.kryo.referenceTracking")  .booleanConf  .createWithDefault(true) | 是否采用kryo追踪.默认采用,为true            |
| KRYO_SERIALIZER_BUFFER_SIZE     | ConfigBuilder("spark.kryoserializer.buffer")  .bytesConf(ByteUnit.KiB)  .createWithDefaultString("64k") | Kryo序列化缓冲大小.默认64K                  |
| KRYO_SERIALIZER_MAX_BUFFER_SIZE | ConfigBuilder("spark.kryoserializer.buffer.max")  .bytesConf(ByteUnit.MiB)  .createWithDefaultString("64m") | Kryo最大序列化缓冲大小,默认64M              |

#### Network

网络传输相关参数设置

| 属性                                      | 参数值                                                       | 介绍                                |
| ----------------------------------------- | ------------------------------------------------------------ | ----------------------------------- |
| NETWORK_CRYPTO_SASL_FALLBACK              | ConfigBuilder("spark.network.crypto.saslFallback") <br /> .booleanConf <br /> .createWithDefault(true) | 是否留有网络加密传输的备用          |
| NETWORK_CRYPTO_ENABLED                    | ConfigBuilder("spark.network.crypto.enabled") <br /> .booleanConf  .createWithDefault(false) | 是否允许网加密,默认false            |
| NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION | ConfigBuilder("spark.network.remoteReadNioBufferConversion")  .booleanConf  .createWithDefault(false) | 是否允许远端读取NIO缓冲区,默认false |
| NETWORK_TIMEOUT                           | ConfigBuilder("spark.network.timeout") <br /> .timeConf(TimeUnit.SECONDS)<br />  .createWithDefaultString("120s") | 网络传输延时上限                    |
| NETWORK_TIMEOUT_INTERVAL                  | ConfigBuilder("spark.network.timeoutInterval") <br /> .timeConf(TimeUnit.MILLISECONDS)  <br />.createWithDefaultString(<br />STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL.defaultValueString) | 网络超时间隔                        |
| RPC_ASK_TIMEOUT                           | ConfigBuilder("spark.rpc.askTimeout") <br /> .stringConf .createOptional | RPC访问时间阈值                     |
| RPC_CONNECT_THREADS                       | ConfigBuilder("spark.rpc.connect.threads") <br /> .intConf  .createWithDefault(64) | RPC连接线程数量，默认64             |
| RPC_IO_NUM_CONNECTIONS_PER_PEER           | ConfigBuilder("spark.rpc.io.numConnectionsPerPeer") <br /> .intConf  .createWithDefault(1) | RPC单节点io连接数量，默认1个        |
| RPC_IO_THREADS                            | ConfigBuilder("spark.rpc.io.threads") <br /> .intConf  .createOptional | RPC  io线程数量                     |
| RPC_LOOKUP_TIMEOUT                        | ConfigBuilder("spark.rpc.lookupTimeout")<br />  .stringConf  .createOptional | RPC查找时间上限                     |
| RPC_MESSAGE_MAX_SIZE                      | ConfigBuilder("spark.rpc.message.maxSize") <br /> .intConf  .createWithDefault(128) | RPC携带最大信息量                   |
| RPC_NETTY_DISPATCHER_NUM_THREADS          | ConfigBuilder("spark.rpc.netty.dispatcher.numThreads")<br />  .intConf  .createOptional | RPC netty分发线程数量               |
| RPC_NUM_RETRIES                           | ConfigBuilder("spark.rpc.numRetries")  <br />.intConf  .createWithDefault(3) | RPC重试次数，默认3次                |
| RPC_RETRY_WAIT                            | ConfigBuilder("spark.rpc.retry.wait")  <br />.timeConf(TimeUnit.MILLISECONDS) <br /> .createWithDefaultString("3s") | RPC重试等待时间，默认3s             |

#### Python

python使用的一些参数设定

| 属性 | 参数值 | 介绍 |
| ------------------- | ------ | ---- |
| PYTHON_WORKER_REUSE | ConfigBuilder("spark.python.worker.reuse")  .booleanConf  .createWithDefault(true) | Python中worker是否可以重新使用，默认true |
| PYTHON_TASK_KILL_TIMEOUT | ConfigBuilder("spark.python.task.killTimeout")  .timeConf(TimeUnit.MILLISECONDS)  .createWithDefaultString("2s") | python kill任务的时间阈值，默认2s |
| PYTHON_USE_DAEMON | ConfigBuilder("spark.python.use.daemon")  .booleanConf  .createWithDefault(true) | 是否使用守护线程，默认使用 |
| PYTHON_DAEMON_MODULE | ConfigBuilder("spark.python.daemon.module")  .stringConf  .createOptional | python 守护线程模块 |
| PYTHON_WORKER_MODULE | ConfigBuilder("spark.python.worker.module")  .stringConf  .createOptional | python worker模块 |
| PYSPARK_EXECUTOR_MEMORY | ConfigBuilder("spark.executor.pyspark.memory")  .bytesConf(ByteUnit.MiB)  .createOptional | python执行器大小，以MB计数 |



#### R

R参数的设置

| 属性                         | 参数值                                                       | 介绍                           |
| ---------------------------- | ------------------------------------------------------------ | ------------------------------ |
| R_BACKEND_CONNECTION_TIMEOUT | ConfigBuilder("spark.r.backendConnectionTimeout")  .intConf  .createWithDefault(6000) | 后台连接时间上限，默认为6000ms |
| R_NUM_BACKEND_THREADS        | ConfigBuilder("spark.r.numRBackendThreads")  .intConf  .createWithDefault(2) | 后端线程数量,默认为2           |
| R_HEARTBEAT_INTERVAL         | ConfigBuilder("spark.r.heartBeatInterval")  .intConf  .createWithDefault(100) | 心跳间隔时间,默认100ms         |
| SPARKR_COMMAND               | ConfigBuilder("spark.sparkr.r.command")  .stringConf  .createWithDefault("Rscript") | spark指令(R语言)               |
| R_COMMAND                    | ConfigBuilder("spark.r.command")  .stringConf  .createOptional | R指令                          |



#### Status

状态参数

| 属性                                      | 参数值                                                       | 介绍                                    |
| ----------------------------------------- | ------------------------------------------------------------ | --------------------------------------- |
| ASYNC_TRACKING_ENABLED                    | ConfigBuilder("spark.appStateStore.asyncTracking.enable")  .booleanConf  .createWithDefault(true) | 是否允许异步追踪，默认状态下为true      |
| LIVE_ENTITY_UPDATE_PERIOD                 | ConfigBuilder("spark.ui.liveUpdate.period")  .timeConf(TimeUnit.NANOSECONDS)  .createWithDefaultString("100ms") | 存活实例更新周期，默认100ms             |
| LIVE_ENTITY_UPDATE_MIN<br />_FLUSH_PERIOD | ConfigBuilder("spark.ui.liveUpdate.minFlushPeriod")   .timeConf(TimeUnit.NANOSECONDS)  .createWithDefaultString("1s") | 存活实例更新的最小刷新周期，默认1s      |
| MAX_RETAINED_JOBS                         | ConfigBuilder("spark.ui.retainedJobs")  .intConf  .createWithDefault(1000) | 最大存留job数量，默认1000               |
| MAX_RETAINED_STAGES                       | ConfigBuilder("spark.ui.retainedStages")  .intConf  .createWithDefault(1000) | 最大存在stage数量，默认1000             |
| MAX_RETAINED_TASKS<br />_PER_STAGE        | ConfigBuilder("spark.ui.retainedTasks")  .intConf  .createWithDefault(100000) | 每个stage最大保留任务，默认10000        |
| MAX_RETAINED_DEAD_EXECUTORS               | ConfigBuilder("spark.ui.retainedDeadExecutors")  .intConf  .createWithDefault(100) | 处于dead状态的最大保留执行器，默认值100 |
| MAX_RETAINED_ROOT_NODES                   | ConfigBuilder("spark.ui.dagGraph.retainedRootRDDs")  .intConf  .createWithDefault(Int.MaxValue) | 最大保留根节点                          |
| METRICS_APP_STATUS_SOURCE_<br />ENABLED   | ConfigBuilder("spark.metrics.appStatusSource.enabled")  .booleanConf  .createWithDefault(false) | 使能度量状态位                          |

#### Streaming

streaming 参数配置

| 属性                                              | 参数值                                                       | 介绍                                      |
| ------------------------------------------------- | ------------------------------------------------------------ | ----------------------------------------- |
| STREAMING_DYN_<br />ALLOCATION_ENABLED            | ConfigBuilder("spark.streaming.dynamicAllocation.enabled")  .booleanConf.createWithDefault(false) | 运行streaming动态分配标志位，默认false    |
| STREAMING_DYN_<br />ALLOCATION_TESTING            | ConfigBuilder("spark.streaming.dynamicAllocation.testing")  .booleanConf  .createWithDefault(false) | streaming 动态分配是否需要测试，默认false |
| STREAMING_DYN_<br />ALLOCATION_MIN_EXECUTORS      | ConfigBuilder("spark.streaming.dynamicAllocation.minExecutors")  .intConf  .checkValue(_ > 0, "The min executor number of streaming dynamic " +    "allocation must be positive.")  .createOptional | 动态分配最小执行器数量                    |
| STREAMING_DYN_<br />ALLOCATION_MAX_EXECUTORS      | ConfigBuilder("spark.streaming.dynamicAllocation.maxExecutors")  .intConf  .checkValue(_ > 0, "The max executor number of streaming dynamic " +    "allocation must be positive.")  .createWithDefault(Int.MaxValue) | 动态分配最大执行器数量                    |
| STREAMING_DYN_<br />ALLOCATION_SCALING_INTERVAL   | ConfigBuilder("spark.streaming.dynamicAllocation.scalingInterval")  .timeConf(TimeUnit.SECONDS)  .checkValue(_ > 0, "The scaling interval of streaming dynamic " +    "allocation must be positive.")  .createWithDefault(60) | 动态分配扫描时间间隔，默认60s             |
| STREAMING_DYN_<br />ALLOCATION_SCALING_UP_RATIO   | ConfigBuilder("spark.streaming.dynamicAllocation.scalingUpRatio")  .doubleConf  .checkValue(_ > 0, "The scaling up ratio of streaming dynamic " +    "allocation must be positive.")  .createWithDefault(0.9) | 按比例放大比例。默认0.9                   |
| STREAMING_DYN_<br />ALLOCATION_SCALING_DOWN_RATIO | ConfigBuilder("spark.streaming.dynamicAllocation.scalingDownRatio")  .doubleConf  .checkValue(_ > 0, "The scaling down ratio of streaming dynamic " +    "allocation must be positive.")  .createWithDefault(0.3) | 按比例缩小比例。默认0.3                   |
#### Tests

测试相关属性

| 属性                                     | 参数值                                                       | 介绍                       |
| ---------------------------------------- | ------------------------------------------------------------ | -------------------------- |
| TEST_USE_<br />COMPRESSED_<br />OOPS_KEY | "spark.test.useCompressedOops"                               | 测试使用压缩Oops           |
| TEST_MEMORY                              | ConfigBuilder("spark.testing.memory")  .longConf  .createWithDefault(Runtime.getRuntime.maxMemory) | 测试内存量                 |
| TEST_<br />SCHEDULE_INTERVAL             | ConfigBuilder("spark.testing.dynamicAllocation.scheduleInterval")  .longConf  .createWithDefault(100) | 测试调度时间间隔，默认100  |
| IS_TESTING                               | ConfigBuilder("spark.testing")  .booleanConf  .createOptional | 是否测试                   |
| TEST_NO_STAGE_RETRY                      | ConfigBuilder("spark.test.noStageRetry")  .booleanConf  .createWithDefault(false) | 没有stage的重试，默认false |
| TEST_RESERVED_<br />MEMORY               | ConfigBuilder("spark.testing.reservedMemory")  .longConf  .createOptional | 测试保留内存               |
| TEST_N_HOSTS                             | ConfigBuilder("spark.testing.nHosts")  .intConf  .createWithDefault(5) | 测试主机数量，默认5个      |
| TEST_N_<br />EXECUTORS_HOST              | ConfigBuilder("spark.testing.nExecutorsPerHost")  .intConf  .createWithDefault(4) | 测试n个执行器主机，默认4个 |
| TEST_N_<br />CORES_EXECUTOR              | ConfigBuilder("spark.testing.nCoresPerExecutor")  .intConf  .createWithDefault(2) | 测试n个执行器核心，默认2个 |

#### UI

UI组件参数设置

| 属性                                      | 参数值                                                       | 介绍                                                         |
| ----------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| UI_SHOW_<br />CONSOLE_PROGRESS            | ConfigBuilder("spark.ui.showConsoleProgress")<br />.booleanConf  .createWithDefault(false) | 是否在控制台显示进程Bar,默认false                            |
| UI_CONSOLE_PROGRESS_<br />UPDATE_INTERVAL | ConfigBuilder("spark.ui.consoleProgress.update.interval")  .timeConf(TimeUnit.MILLISECONDS)  .createWithDefault(200) | 控制台更新时间间隔，默认200ms                                |
| UI_ENABLED                                | ConfigBuilder("spark.ui.enabled").booleanConf  .createWithDefault(true) | 是否允许WEBUI默认true                                        |
| UI_PORT                                   | ConfigBuilder("spark.ui.port").intConf  .createWithDefault(4040) | WEB端口，默认4040                                            |
| UI_FILTERS                                | ConfigBuilder("spark.ui.filters")<br />.stringConf  .toSequence  .createWithDefault(Nil) | WEB过滤器，列表中的元素会应用到WEB  UI中                     |
| UI_ALLOW_FRAMING_FROM                     | ConfigBuilder("spark.ui.allowFramingFrom")  .stringConf  .createOptional | WEB允许的架构                                                |
| UI_REVERSE_PROXY                          | ConfigBuilder("spark.ui.reverseProxy").booleanConf  .createWithDefault(false) | WEB UI反向代理，默认false                                    |
| UI_REVERSE_PROXY_URL                      | ConfigBuilder("spark.ui.reverseProxyUrl").stringConf  .createOptional | 反向代理地址                                                 |
| UI_KILL_ENABLED                           | ConfigBuilder("spark.ui.killEnabled") .booleanConf  .createWithDefault(true) | 是否允许再WEB UI中删除job或者stage                           |
| UI_THREAD_DUMPS_ENABLED                   | ConfigBuilder("spark.ui.threadDumpsEnabled")  .booleanConf  .createWithDefault(true) | 是否允许线程倾倒，默认true                                   |
| UI_PROMETHEUS_ENABLED                     | ConfigBuilder("spark.ui.prometheus.enabled")  .internal()  .booleanConf  .createWithDefault(false) | 是否允许prometheus，默认tfalse，目的是暴露执行器度量器，当然需要配置conf/metrics.properties |
| UI_X_XSS_PROTECTION                       | ConfigBuilder("spark.ui.xXssProtection").stringConf  .createWithDefaultString("1; mode=block") | XSS攻击防护策略的响应头                                      |
| UI_X_CONTENT_TYPE_OPTIONS                 | ConfigBuilder("spark.ui.xContentTypeOptions.enabled")  .booleanConf  .createWithDefault(true) | 默认为true，设置X-Content-Type-Options HTTP 请求头为 nosniff |
| UI_STRICT_<br />TRANSPORT_SECURITY        | ConfigBuilder("spark.ui.strictTransportSecurity")  .stringConf  .createOptional | HTTP 严格安全传输请求头                                      |
| UI_REQUEST_HEADER_SIZE                    | ConfigBuilder("spark.ui.requestHeaderSize")<br />.bytesConf(ByteUnit.BYTE)  .createWithDefaultString("8k") | HTTP 请求头大小，单位字节                                    |
| UI_TIMELINE_TASKS_MAXIMUM                 | ConfigBuilder("spark.ui.timeline.tasks.maximum")  .intConf.createWithDefault(1000) | 时间轴任务最大值，默认1000                                   |
| ACLS_ENABLE                               | ACLS_ENABLE = ConfigBuilder("spark.acls.enable")  .booleanConf  .createWithDefault(false) | 是否支持访问控制列表，默认false                              |
| UI_VIEW_ACLS                              | ConfigBuilder("spark.ui.view.acls")  .stringConf  .toSequence  .createWithDefault(Nil) | 视图控制列表                                                 |
| UI_VIEW_ACLS_GROUPS                       | ConfigBuilder("spark.ui.view.acls.groups")  .stringConf  .toSequence  .createWithDefault(Nil) | 视图控制列表组                                               |
| ADMIN_ACLS                                | ConfigBuilder("spark.admin.acls")  .stringConf  .toSequence  .createWithDefault(Nil) | 管理控制列表                                                 |
| ADMIN_ACLS_GROUPS                         | ConfigBuilder("spark.admin.acls.groups")  .stringConf  .toSequence  .createWithDefault(Nil) | 管理控制列表组                                               |
| MODIFY_ACLS                               | ConfigBuilder("spark.modify.acls")  .stringConf  .toSequence  .createWithDefault(Nil) | 修改控制列表                                                 |
| MODIFY_ACLS_GROUPS                        | ConfigBuilder("spark.modify.acls.groups")  .stringConf  .toSequence  .createWithDefault(Nil) | 修改控制列表组                                               |
| USER_GROUPS_MAPPING                       | ConfigBuilder("spark.user.groups.mapping")  .stringConf  .createWithDefault("<br />org.apache.spark.security.ShellBasedGroupsMappingProvider") | 指定用户组映射                                               |
| PROXY_REDIRECT_URI                        | ConfigBuilder("spark.ui.proxyRedirectUri").stringConf  .createOptional | 代理重定向资源标识符<br />HTTP重定向时代理地址               |
| CUSTOM_EXECUTOR_LOG_URL                   | ConfigBuilder("spark.ui.custom.executor.log.url")<br />.stringConf.createOptional | 用户执行器日志地址                                           |

#### Worker

worker信息配置

| 属性                                                     | 参数值                                                       | 介绍                                                         |
| -------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| SPARK_WORKER<br />_PREFIX                                | "spark.worker"                                               | spark worker前缀                                             |
| SPARK_WORKER_<br />RESOURCE_FILE                         | ConfigBuilder("spark.worker.resourcesFile")<br />.internal().stringConf.createOptional | spark worker的资源文件<br />包含分配给worker的资源，文件需要格式化为@ResourceAllocation的json数组。仅仅使用在脱机模式下。 |
| WORKER_TIMEOUT                                           | ConfigBuilder("spark.worker.timeout")  .longConf  .createWithDefault(60) | worker的时间上限，默认60                                     |
| WORKER_DRIVER_<br />TERMINATE_TIMEOUT                    | ConfigBuilder("spark.worker.driverTerminateTimeout")  .timeConf(TimeUnit.MILLISECONDS)  .createWithDefaultString("10s") | worker-driver终端时间上限。默认10s                           |
| WORKER_CLEANUP_<br />ENABLED                             | ConfigBuilder("spark.worker.cleanup.enabled")  .booleanConf  .createWithDefault(false) | worker是否能够情况。默认false                                |
| WORKER_CLEANUP_<br />INTERVAL                            | ConfigBuilder("spark.worker.cleanup.interval")  .longConf  .createWithDefault(60 * 30) | worker时间间隔，默认60*30                                    |
| APP_DATA_<br />RETENTION                                 | ConfigBuilder("spark.worker.cleanup.appDataTtl")  .longConf  .createWithDefault(7 * 24 * 3600) | 数据滞留时间。默认7* 24* 60                                  |
| PREFER_CONFIGURED<br />_MASTER_ADDRESS                   | ConfigBuilder("spark.worker.preferConfiguredMasterAddress")  .booleanConf  .createWithDefault(false) | 是否倾向于获得配置的master地址                               |
| WORKER_UI_PORT                                           | ConfigBuilder("spark.worker.ui.port")  .intConf  .createOptional | Worker WEB端口                                               |
| WORKER_UI_<br />RETAINED_EXECUTORS                       | ConfigBuilder("spark.worker.ui.retainedExecutors")  .intConf  .createWithDefault(1000) | worker保留执行器数量，默认1000                               |
| WORKER_UI_<br />RETAINED_DRIVERS                         | ConfigBuilder("spark.worker.ui.retainedDrivers")  .intConf  .createWithDefault(1000) | worker保留驱动器实例，默认1000                               |
| UNCOMPRESSED_LOG<br />_FILE_LENGTH_<br />CACHE_SIZE_CONF | ConfigBuilder("spark.worker.ui.compressedLogFileLengthCacheSize").i<br />ntConf.createWithDefault(100) | 不经压缩日志文件缓存大小配置。默认为100                      |

---

#### io

1.  [FileCommitProtocol.scala](# FileCommitProtocol)
2.  [HadoopMapRedCommitProtocol.scala](# HadoopMapRedCommitProtocol)
3.  [HadoopMapReduceCommitProtocol.scala](# HadoopMapReduceCommitProtocol)
4.  [HadoopWriteConfigUtil.scala](# HadoopWriteConfigUtil)
5.  [SparkHadoopWriter.scala](# SparkHadoopWriter)
6.  [SparkHadoopWriterUtils.scala](# SparkHadoopWriterUtils)

---

#### FileCommitProtocol

```markdown
介绍 :
文件提交协议，这是一个定义单个spark job如何提交输出的方式的接口。注意如下三点:
1. 实现必须要是可序列化的，因为提交者@commiter实例初始化在driver上，会在executor上供任务使用。
2. 实现时构造器需要包含2-3个参数。
	(jobId: String, path: String)  
	或者
	(jobId: String, path: String, dynamicPartitionOverwrite: Boolean)
3. 提交者不应当在多个spark job 中重新使用
合适的调用顺序为:
1. driver端调用设置job @setupJob()
2. 作为每个任务的执行，执行器调用@setupTask() 和 @commitTask()去提交任务(如果失败，则使用@abortTask()放弃提交)
3. 当所有必要的任务全部成功完成，驱动器提交job。如果job执行失败(有过多的失败task)，那么这个job会调用@abortJob,弃用这个job。
```
```markdown
abstract class FileCommitProtocol{
	关系 : father --> Logging
	操作集:
	def setupJob(jobContext: JobContext): Unit
	功能: 建立job工作，必须在驱动器上调用，且需要在其他方法调用前使用
	
	def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit
	功能: 写成功之后提交job工作，必须在驱动器上调用
	
	def abortJob(jobContext: JobContext): Unit
	功能: 写失败放弃工作job,必须在驱动器上调用，调用函数尽最大努力，原因是驱动器在弃用job之前可能会被kill或宕		掉。
	
	def setupTask(taskContext: TaskAttemptContext): Unit
	功能: 在一个job中建立一个任务，必须要在其他任务相关方法调用之前使用
	
	def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String
	功能: 新建任务临时文件
	注意添加文件的提交协议，且获取应当使用的全路径，当运行任务时必须在执行器上调用。
	注意返回的临时文件可能含有一个专有(arbitrary)文件,提交协议只会保证文件提交到job提交的参数位置。全路径包含	如下几个部分:
	1. 基本路径
	2. 基本路径下的子文件目录，用于指定分区
	3. 文件前缀，使用唯一的带有task id的job Id
	4. 桶(bucket)id
	5. 特点的文件名扩展(e.g. ".snappy.parquet")
	目录蚕食指定2,"ext" 参数指定4和5.其他类型有实现决定。
	
	def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, 
		ext: String): String
	功能: 类似于@newTaskTempFile，但是允许文件提交到绝对路径中，依赖于实现，但是会对添加文件这种方式的可靠性	降低。重点： 如果任务需要写出多个文件到相同目录中,添加独特标志内容到'ext'中。这些都是调用者需要做的工作。这	个文件提交协议只保证不同任务写出的文件不会冲突。
	
	def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage
	功能: 写出成功之后提交任务，必须要在运行任务中且在执行器中提交
	
	def abortTask(taskContext: TaskAttemptContext): Unit
	功能: 写出失败放弃提交任务，必须在运行任务中且在执行器中提交
		调用时尽最大努力，因为可能执行器宕机或者被kill掉
	
	def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean
	功能: 指定需要删除的文件，且带有job的提交默认实现立即删除文件。
	val= fs.delete(path, recursive)
	
	def onTaskCommit(taskCommit: TaskCommitMessage): Unit
	功能: 任务提交之后调用驱动器，这个可以在job完成之前获取task提交信息。如果整个job成功，这些任务提交信息会		被传递给@commitJob()
}
```
```markdown
object FileCommitProtocol{
	关系: father --> Logging
	内部类:
	class TaskCommitMessage(val obj: Any) extends Serializable
		任务提交信息
	object EmptyTaskCommitMessage extends TaskCommitMessage(null)
		空任务提交信息
	操作集:
	def instantiate(className: String,jobId: String,outputPath: String,
      dynamicPartitionOverwrite: Boolean = false): FileCommitProtocol 
	功能: 使用给定类型初始化文件传输协议
	1. 获取指定类名@className对应的实例
	val clazz = Utils.classForName[FileCommitProtocol](className)
	+ 尝试构造器(jobId: String, outputPath: String,dynamicPartitionOverwrite: Boolean)
	val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[String], classOf[Boolean])
	ctor.newInstance(jobId, outputPath, dynamicPartitionOverwrite.asInstanceOf[java.lang.Boolean])
	+ 获取构造器1失败，获取构造器2  (jobId: string, outputPath: String)
	val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[String])
    ctor.newInstance(jobId, outputPath)
}
```

#### HadoopMapRedCommitProtocol

```markdown
介绍:
	Hadoop MapReduce 提交协议
	这是一种文件传输协议的实现，由底层hadoop 输出提交器实现。
	与hadoop 输出提交器@OutputCommitter不同的是，这个实现是可序列化的。
```

```markdown
class HadoopMapRedCommitProtocol(jobId: String, path: String){
	关系 : father --> HadoopMapReduceCommitProtocol(jobId, path)
	操作集:
	def setupCommitter(context: NewTaskAttemptContext): OutputCommitter 
	功能: 建立提交器
	val config = context.getConfiguration.asInstanceOf[JobConf]
    val committer = config.getOutputCommitter
    val= committer
}
```

#### HadoopMapReduceCommitProtocol

```markdown
介绍:
	文件传输协议，由底层hadoop 输出提交器返回@OutputCommitter，与输出提交器@OutputCommitter不同的是，实现是可以序列化的。
```

```markdown
class HadoopMapReduceCommitProtocol(jobId: String,path: String,
    dynamicPartitionOverwrite: Boolean = false){
	关系: father --> FileCommitProtocol
    sibling --> Serializable/Logging
    构造器属性:
    	jobId	job/stage ID
    	path	job提交路径，为null时nop
    	dynamicPartitionOverwrite 为true，spark则会在运行时覆盖分区目录，首次写文件时在stage目录下携带有分		区路径，比如说/path/to/staging/a=1/b=1/xxx.parquet。 当提交工作时，首先清除目标路径相应分区目录,例		如/path/to/destination/a=1/b=1，然后移动stage目录到相应分区目录且在目标路径下。
    属性:
    	#name @committer=_ #type @OutputCommitter transient		输出提交器
    		由于hadoop输出提交器没有序列化，所以需要添加注解@transient，让它序列化
    	#name @hasValidPath=Try { new Path(path) }.isSuccess	是否是合法路径
    		检查文件是否被提交到了合法路径上
    	#name @addedAbsPathFiles=null #type @mutable.Map[String, String]	已添加的绝对路径
    		本任务绝对定位的输出路径，这些输出不是由hadoop 输出提交器管理@OutputCommitter，所以必须要移动这			些最终地址到任务提交中。临时输出文件与最终需求的映射由这个维护。
    	#name @partitionPaths=null #type @mutable.Set[String]	分区长度
    		使用默认路径定位分区这种默认路径含有由任务写入的新文件,例如a=1/b=2文件.在这些分区底下的文件需要保		存在stage 目录，且再最后移动到目的目录下。这些操作进行在@dynamicPartitionOverwrite设置为true的情况			下。
	操作集:
	def stagingDir = new Path(path, ".spark-staging-" + jobId)
	功能: 获取写job的stage目录，spark使用这个处理绝对路径的输出，或者通过设置@dynamicPartitionOverwrite
	=true将数据写出到分区目录下。
	
	def setupCommitter(context: TaskAttemptContext): OutputCommitter
	功能: 建立对指定任务上下文@context的输出提交器
	1. 获取输出类型并格式化
	val format = context.getOutputFormatClass.getConstructor().newInstance()
	format match {
      case c: Configurable => c.setConf(context.getConfiguration)
      case _ => ()
    }    
    2. 获取提交器
    format.getOutputCommitter(context)
    
    def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String
	功能: 常见任务临时文件
    输入参数:
    	taskContext	任务上下文管理器
    	dir	目录名称
    	ext	???
    1. 获取文件名称
    val filename = getFilename(taskContext, ext)
    2. 获取stage的目录
    val stagingDir: Path = committer match {
    	//  可以动态覆盖分区内容，则stage目录为本类的stage目录
      case _ if dynamicPartitionOverwrite => 
        assert(dir.isDefined,
          "The dataset to be written must be partitioned when dynamicPartitionOverwrite is true.")
        partitionPaths += dir.get
        this.stagingDir
		//匹配文件输出提交器，路径就是自己的工作路径
      case f: FileOutputCommitter =>
        new Path(Option(f.getWorkPath).map(_.toString).getOrElse(path))
      case _ => new Path(path)
    }
    3. 获取目录的名称
    dirmap { d =>new Path(new Path(stagingDir, d), filename).toString}
    .getOrElse{ new Path(stagingDir, filename).toString}
    
    def getFilename(taskContext: TaskAttemptContext, ext: String): String
    功能: 获取文件名
    文件名称形如part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet 注意到%05d不会清楚分	片数量，所以当任务数量超过100000个任务时，文件名称仍就完好且不会溢出。
    1. 获取分片id
    part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    2. 返回文件名称
    val= f"part-$split%05d-$jobId$ext"
    
    def setupJob(jobContext: JobContext): Unit 
    功能: 建立job
    1. 设置id(jobId 工作id,taskId任务id,taskAttemptId任务请求id)
	val jobId = SparkHadoopWriterUtils.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)
    2. 设置相关配置信息
    jobContext.getConfiguration.set("mapreduce.job.id", jobId.toString)
    jobContext.getConfiguration.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    jobContext.getConfiguration.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    jobContext.getConfiguration.setBoolean("mapreduce.task.ismap", true)
    jobContext.getConfiguration.setInt("mapreduce.task.partition", 0)
    3. 建立job
    val taskAttemptContext = new TaskAttemptContextImpl(jobContext.getConfiguration, taskAttemptId)
    committer = setupCommitter(taskAttemptContext)
    committer.setupJob(jobContext)
    
    def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit
    功能: 提交job
    1. 提交job
    committer.commitJob(jobContext)
    2. 操作条件 
    	路径合法 hasValidPath=true
 		hasValidPath ? Jmp 3 : Nop
    3. 获取绝对路径信息/所有分区长度
    	val (allAbsPathFiles, allPartitionPaths) =
        	taskCommits.map(_.obj.asInstanceOf[(Map[String, String], Set[String])]).unzip
	4. 获取需要移动的列表信息
	    val fs = stagingDir.getFileSystem(jobContext.getConfiguration)
		val filesToMove = allAbsPathFiles.foldLeft(Map[String, String]())(_ ++ _)
	5. 删除需要移除列表的文件
		if (dynamicPartitionOverwrite) {
        	val absPartitionPaths = filesToMove.values.map(new Path(_).getParent).toSet
        	absPartitionPaths.foreach(fs.delete(_, true))}
    6. 重命名文件src->dst
    	for ((src, dst) <- filesToMove) fs.rename(new Path(src), new Path(dst))
	7. 创建最终目录文件
	val partitionPaths = allPartitionPaths.foldLeft(Set[String]())(_ ++ _) // 获取分区长度列表
	for (part <- partitionPaths) {
		val finalPartPath = new Path(path, part)
		// 删除最终路径文件
         if (!fs.delete(finalPartPath, true) && !fs.exists(finalPartPath.getParent)) {
         	// 常见最终路径的上级文件
         	fs.mkdirs(finalPartPath.getParent)
         }
         // 重命名stagingDir下文件为最终路径
         fs.rename(new Path(stagingDir, part), finalPartPath)
	}
	8. 删除stageDir目录，下的文件
	fs.delete(stagingDir, true)
	
	def abortJob(jobContext: JobContext): Unit
	功能: 放弃提交job
	1. commiter放弃提交任务
	committer.abortJob(jobContext, JobStatus.State.FAILED)
	2. 删除stageDir目录
	if (hasValidPath) {
        val fs = stagingDir.getFileSystem(jobContext.getConfiguration)
        fs.delete(stagingDir, true)}
    
    def setupTask(taskContext: TaskAttemptContext): Unit 
    功能: 建立task任务
    1. 创建commiter
    committer = setupCommitter(taskContext)
    2. 创建任务
    committer.setupTask(taskContext)
    3. 获取已添加的路径文件/分区路径列表
    addedAbsPathFiles = mutable.Map[String, String]()
    partitionPaths = mutable.Set[String]()
    
    def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage
    功能: 提交task
    1. 获取任务请求id
    val attemptId = taskContext.getTaskAttemptID
    2. 提交任务
    SparkHadoopMapRedUtil.commitTask(
      committer, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    3. 获取任务提交信息
    val= new TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
    
    def abortTask(taskContext: TaskAttemptContext): Unit 
    功能: 抛弃任务task
    1. 放弃任务
    committer.abortTask(taskContext)
    2. 尽最大努力去清除其他stage 文件
    for ((src, _) <- addedAbsPathFiles) {
        val tmp = new Path(src)
        tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, false)}
}
```

#### HadoopWriteConfigUtil

```markdown
介绍:
	创建输出格式/提交器/写出器的接口，用于保存RDD的过程中，使用Hadoop的输出方式@OutputFormat(可以兼容新旧Hadoop API)
	注意:
	1. 当错误的hadoop API使用时会抛出异常
	2. 实现类必须要是可序列化的，因为实例初始化在driver端，且用于executor端的任务中
	3. 实现需要有确定一个的构造器参数
```

```markdown
abstract class HadoopWriteConfigUtil[K, V: ClassTag] {
	关系: father --> Serializable
	操作集:
	def createJobContext(jobTrackerId: String, jobId: Int): JobContext
	功能: 创建job的上下文实例
	
	def createTaskAttemptContext(jobTrackerId:String,jobId:Int,splitId:Int,
      taskAttemptId: Int): TaskAttemptContext
    功能: 创建任务上下文对象
    
    def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol
    功能: 创建提交器
    
    def initWriter(taskContext: TaskAttemptContext, splitId: Int): Unit
    功能: 初始化写出器
    
    def write(pair: (K, V)): Unit
    功能: 写出键值对
    
    def closeWriter(taskContext: TaskAttemptContext): Unit
    功能: 关闭写出器
    
    def initOutputFormat(jobContext: JobContext): Unit
    功能: 初始化输出器
    
    def assertConf(jobContext: JobContext, conf: SparkConf): Unit
    功能: 配置断言
}
```

#### SparkHadoopWriter

```markdown
介绍: 使用hadoop @OutputFormat 保存RDD的辅助类
```

```markdown
private[spark] object SparkHadoopWriter{
	关系: father --> Logging
	操作集:
	def write[K, V: ClassTag](rdd: RDD[(K, V)],config: HadoopWriteConfigUtil[K, V]): Unit 
	功能: 这个操作的工作流为:
		1. 驱动器侧建立，准备数据源和hadoop配置用于写需要解决的job
		2. 处理包含一个或者多个执行器任务的写工作，每个都会在RDD分区中写出所有列
		3. 如果任务中没有抛出异常，提交任务，否则抛弃任务。如果提交过程中抛出异常，也会抛弃这个task
		4. 如果所有task都提交了，则提交job，否则抛弃job。如果异常在job提交过程中发生，抛弃这个job。
	1. 获取RDD信息
	val sparkContext = rdd.context
    val commitJobId = rdd.id
    2. 创建一个job
    val jobTrackerId = createJobTrackerID(new Date()) // 获取job追踪id
    val jobContext = config.createJobContext(jobTrackerId, commitJobId) //获取job上下文对象
    config.initOutputFormat(jobContext)
    3. 配置信息断言检测
    config.assertConf(jobContext, rdd.conf)
    4. 创建job信息
    val committer = config.createCommitter(commitJobId)
    committer.setupJob(jobContext)
    5. 作为hadoop @OutputFormat 写出RDD分区
      // 启动job任务
      val ret = sparkContext.runJob(rdd, (context: TaskContext, iter: Iterator[(K, V)]) => {
        // 创建唯一的请求id,主要根据stage,task请求编号。假定不会超过short最大值。
        val attemptId = (context.stageAttemptNumber << 16) | context.attemptNumber
        // 写RDD分区到单个spark任务中
        executeTask(
          context = context,
          config = config,
          jobTrackerId = jobTrackerId,
          commitJobId = commitJobId,
          sparkPartitionId = context.partitionId,
          sparkAttemptNumber = attemptId,
          committer = committer,
          iterator = iter)
      })
    6. 提交任务
    	committer.commitJob(jobContext, ret)
    7. 任务中途失败
    	committer.abortJob(jobContext)
    	throw SparkException
	
	def executeTask[K, V: ClassTag](context: TaskContext,config: HadoopWriteConfigUtil[K, V],
      jobTrackerId: String,commitJobId: Int,sparkPartitionId: Int,sparkAttemptNumber: Int,
      committer: FileCommitProtocol,iterator: Iterator[(K, V)]): TaskCommitMessage
	功能: 写出RDD分区到单个spark任务task中
	输入参数:
		context	任务上下文
		config	配置工具信息
		jobTrackerId	job追踪信息
		commitJobId		提交job的id
		sparkPartitionId	spark分区id
		sparkAttemptNumber	spark请求编号
		committer	文件提交协议
		iterator	迭代器
	1. 创建task
    val taskContext = config.createTaskAttemptContext(
      jobTrackerId, commitJobId, sparkPartitionId, sparkAttemptNumber)
    committer.setupTask(taskContext)
	2. 初始化写出器
	config.initWriter(taskContext, sparkPartitionId)
    var recordsWritten = 0L
    3. 初始化hadoop输出度量器
    val (outputMetrics, callback) = initHadoopOutputMetrics(context)
    4. 写出rdd分区中的所有行
    val ret = Utils.tryWithSafeFinallyAndFailureCallbacks {
        // 写出记录，并更新度量值
        while (iterator.hasNext) {
          val pair = iterator.next()
          config.write(pair)
          maybeUpdateOutputMetrics(outputMetrics, callback, recordsWritten)
          recordsWritten += 1 }
        config.closeWriter(taskContext)
        committer.commitTask(taskContext)
      }(catchBlock = { // 如果图中存在错误，释放资源，并放弃提交任务
        try 
          config.closeWriter(taskContext)
        finally
          committer.abortTask(taskContext)})
    5. 更新度量器信息
    outputMetrics.setBytesWritten(callback())
    outputMetrics.setRecordsWritten(recordsWritten)
}
```

```markdown
private[spark] class HadoopMapRedWriteConfigUtil[K, V: ClassTag] (conf: SerializableJobConf){
	关系: father --> HadoopWriteConfigUtil[K, V]
		sibling --> Logging
	构造器属性:
		conf	序列化job配置
	属性:
		#name @outputFormat=null #type @Class[_ <: OutputFormat[K, V]] 	输出形式
		#name @writer=null #type @RecordWriter[K, V]	记录写出器
	操作集:
	def getConf: JobConf = conf.value
	功能: 获取配置信息
	
	def createJobContext(jobTrackerId: String, jobId: Int): NewJobContext
	功能: 获取job上下文
	1. 获取job请求id
	val jobAttemptId = new SerializableWritable(new JobID(jobTrackerId, jobId))
	2. 获取job上下文对象
	val= new JobContextImpl(getConf, jobAttemptId.value)
	
	def createTaskAttemptContext(jobTrackerId: String,jobId: Int,
      splitId: Int,taskAttemptId: Int): NewTaskAttemptContext
	功能: 获取任务请求上下文
	1. 更新job配置conf
	HadoopRDD.addLocalConfiguration(jobTrackerId, jobId, splitId, taskAttemptId, conf.value)
	2. 获取请求id
	val attemptId = new TaskAttemptID(jobTrackerId, jobId, TaskType.MAP, splitId, taskAttemptId)
    3. 获取任务上下文
    val= new TaskAttemptContextImpl(getConf, attemptId)
	
	def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol
	功能: 创建提交器
	1. 更新job conf
	HadoopRDD.addLocalConfiguration("", 0, 0, 0, getConf)
	2. 创建提交协议
	FileCommitProtocol.instantiate(
      className = classOf[HadoopMapRedCommitProtocol].getName,
      jobId = jobId.toString,outputPath = getConf.get("mapred.output.dir")
    ).asInstanceOf[HadoopMapReduceCommitProtocol]
	
	def initWriter(taskContext: NewTaskAttemptContext, splitId: Int): Unit
	功能: 初始化写出器
	1. 获取输出名称
        val numfmt = NumberFormat.getInstance(Locale.US)
        numfmt.setMinimumIntegerDigits(5)
        numfmt.setGroupingUsed(false)
        val outputName = "part-" + numfmt.format(splitId)
	2. 获取文件系统fs
        val path = FileOutputFormat.getOutputPath(getConf)
        val fs: FileSystem = {
          if (path != null) 
            path.getFileSystem(getConf)
          else
            FileSystem.get(getConf)
        }
     3. 获取输出器，并对其进行断言检测
         writer = getConf.getOutputFormat
          .getRecordWriter(fs, getConf, outputName, Reporter.NULL)
          .asInstanceOf[RecordWriter[K, V]]
        require(writer != null, "Unable to obtain RecordWriter")
	
	def write(pair: (K, V)): Unit 
	功能: 写出键值对信息
		writer.write(pair._1, pair._2)
	
	def closeWriter(taskContext: NewTaskAttemptContext): Unit
	功能: 关闭写出器
        if (writer != null) 
          writer.close(Reporter.NULL)
	
	def initOutputFormat(jobContext: NewJobContext): Unit
	功能: 初始化输出格式
        if (outputFormat == null) {
          outputFormat = getConf.getOutputFormat.getClass
            .asInstanceOf[Class[_ <: OutputFormat[K, V]]] }
	
	def getOutputFormat(): OutputFormat[K, V]
	功能: 获取输出格式
	val= outputFormat.getConstructor().newInstance()
	
	def assertConf(jobContext: NewJobContext, conf: SparkConf): Unit
	功能: 断言配置
}
```

```markdown
private[spark] class HadoopMapReduceWriteConfigUtil[K,V: ClassTag] (conf: SerializableConfiguration){
	关系: father --> HadoopWriteConfigUtil[K, V]
		sibling --> Logging
	属性:
		#name @outputFormat=null #type @Class[_ <: NewOutputFormat[K, V]]
			输出格式
		#name @writer=null #type @NewRecordWriter[K, V]
			写出器
	操作集:
	def getConf: Configuration = conf.value
	功能: 获取配置值
	
	def createJobContext(jobTrackerId: String, jobId: Int): NewJobContext
	功能: 创建job上下文
		val jobAttemptId = new NewTaskAttemptID(jobTrackerId, jobId, TaskType.MAP, 0, 0)
    	new NewTaskAttemptContextImpl(getConf, jobAttemptId)
	
	def createTaskAttemptContext(jobTrackerId: String,jobId: Int,splitId: Int,
      taskAttemptId: Int): NewTaskAttemptContext
     功能: 创建任务请求上下文
         val attemptId = new NewTaskAttemptID(
          jobTrackerId, jobId, TaskType.REDUCE, splitId, taskAttemptId)
        new NewTaskAttemptContextImpl(getConf, attemptId)
	
	def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol
	功能: 创建提交器(MR提交协议)
        FileCommitProtocol.instantiate(
          className = classOf[HadoopMapReduceCommitProtocol].getName,
          jobId = jobId.toString,
          outputPath = getConf.get("mapreduce.output.fileoutputformat.outputdir")
        ).asInstanceOf[HadoopMapReduceCommitProtocol]
	
	def initWriter(taskContext: NewTaskAttemptContext, splitId: Int): Unit
	功能: 初始化写出器
	1. 获取任务输出格式
	val taskFormat = getOutputFormat()
    taskFormat match {
      case c: Configurable => c.setConf(getConf)
      case _ => ()}
    2. 获取写出器，并断言检测
    writer = taskFormat.getRecordWriter(taskContext)
      .asInstanceOf[NewRecordWriter[K, V]]
    require(writer != null, "Unable to obtain RecordWriter")
	
	def write(pair: (K, V)): Unit
    功能: 写出键值对
        require(writer != null, "Must call createWriter before write.")
        writer.write(pair._1, pair._2)

	def closeWriter(taskContext: NewTaskAttemptContext): Unit
	功能: 关闭写出器
	if (writer != null) {
      writer.close(taskContext)
      writer = null}
     
     def initOutputFormat(jobContext: NewJobContext): Unit
     功能: 初始化输出格式
     if (outputFormat == null) {
      	outputFormat = jobContext.getOutputFormatClass
        .asInstanceOf[Class[_ <: NewOutputFormat[K, V]]]}
     
     def getOutputFormat(): NewOutputFormat[K, V]
     功能: 获取输出形式
     require(outputFormat != null, "Must call initOutputFormat first.")
     outputFormat.getConstructor().newInstance()
	
	def assertConf(jobContext: NewJobContext, conf: SparkConf): Unit
	功能: 配置断言
}
```



#### SparkHadoopWriterUtils

```markdown
介绍:
	辅助对象，用于提供通用工具，用于保存RDD期间使用Hadoop输出@OutputFormat
```

```markdown
private[spark] object SparkHadoopWriterUtils{
	属性:
	#name @RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES=256	写出字节度量更新数量(默认256)
	#name @disableOutputSpecValidation=new DynamicVariable[Boolean](false)
		允许@spark.hadoop.validateOutputSpecs按情况检查失效情况，参照SPARK-4835去寻找更多的细节。
	操作集:
	def createJobID(time: Date, id: Int): JobID
	功能: 创建任务ID
	1. 根据指定时间获取job 追踪ID
	val jobtrackerID = @createJobTrackerID(time)
	2. 获取jobID
	val= new JobID(jobtrackerID, id)
	
	def createJobTrackerID(time: Date): String 
	功能: 获取job追踪器ID
	val= new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(time)
		job追踪器格式: yyyyMMddHHmmss
	
	def createPathFromString(path: String, conf: JobConf): Path
	功能: 创建指定@path的路径
	操作条件: 输出串非空@path!=null，指定文件系统非空@outputPath.getFileSystem(conf)!=null
        val outputPath = new Path(path)
        val fs = outputPath.getFileSystem(conf)
		outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
	
	def isOutputSpecValidationEnabled(conf: SparkConf): Boolean
	功能: 输出是否允许批准
	1. 确认批准是否失效
	val validationDisabled = disableOutputSpecValidation.value
	2. 是否允许配置
	val enabledInConf = conf.getBoolean("spark.hadoop.validateOutputSpecs", true)
	val= enabledInConf && !validationDisabled
	
	def initHadoopOutputMetrics(context: TasgkContext): (OutputMetrics, () => Long)
	功能: 初始化Hadoop输出度量器
	1. 获取写出字节数量
	val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
	val= (context.taskMetrics().outputMetrics, bytesWrittenCallback)
	
	def maybeUpdateOutputMetrics(outputMetrics: OutputMetrics,callback: () => Long,
      recordsWritten: Long): Unit 
     功能: 更新输出度量器
     1. 更新条件 recordsWritten % RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0
     + 更新操作
      outputMetrics.setBytesWritten(callback())	// 更新已经写出的字节数量
      outputMetrics.setRecordsWritten(recordsWritten) // 更新已经写出的记录数量
}
```

---

#### plugin

1. [PluginContainer.scala](# PluginContainer)

2. [PluginContextImpl.scala](# PluginContextImpl)
   
3. [PluginEndpoint.scala](# PluginEndpoint)

   ---

   #### PluginContainer

   ```markdown
   sealed abstract class PluginContainer{
   	操作集:
   	def shutdown(): Unit
   	功能: 关闭插件容器
   	def registerMetrics(appId: String): Unit
   	功能: 注册度量参数
   }
   ```
   
   ```markdown
   private class DriverPluginContainer(sc: SparkContext, plugins: Seq[SparkPlugin]){
   	关系: father --> PluginContainer
   	sibling --> Logging
   	介绍: 驱动器插件容器
   	构造器属性:
   		sc	应用程序配置集
   		plugins	插件序列
   	属性:
   	#name @driverPlugins #type @ Seq[(String, DriverPlugin, PluginContextImpl)]	驱动器插件
   	plugins.flatMap { p =>
   		val driverPlugin = p.driverPlugin()
   		if (driverPlugin != null) {
   			val name = p.getClass().getName()
   			val ctx = new PluginContextImpl(name, sc.env.rpcEnv, sc.env.metricsSystem,
               	sc.conf,sc.env.executorId)
   			// 获取并初始化额外信息
   			val extraConf = driverPlugin.init(sc, ctx)
   			if (extraConf != null) {
                   extraConf.asScala.foreach { case (k, v) =>
             		sc.conf.set(s"${PluginContainer.EXTRA_CONF_PREFIX}$name.$k", v)
           	}
               // 注册插件信息
         		Some((p.getClass().getName(), driverPlugin, ctx))
         	}else
         		None
        
        初始化操作:
        if (driverPlugins.nonEmpty) {
       	val pluginsByName = driverPlugins.map { case (name, plugin, _) => (name, plugin) }.toMap
       	sc.env.rpcEnv.setupEndpoint(classOf[PluginEndpoint].getName(),
         	new PluginEndpoint(pluginsByName, sc.env.rpcEnv))
     	}
     	功能: 获取插件列表对应的插件末端点数据信息@PluginEndpoint
       
       操作集:
       def registerMetrics(appId: String): Unit
       功能: 登记度量器
       	driverPlugins.foreach { case (_, plugin, ctx) =>
        	 	plugin.registerMetrics(appId, ctx)
         		ctx.registerMetrics()
       	}
       
       def shutdown(): Unit
       功能: 关闭
       driverPlugins.foreach { case (name, plugin, _) =>
       	plugin.shutdown() : Throwable}
   }
   ```
   
   ```markdown
   private class ExecutorPluginContainer(env: SparkEnv, plugins: Seq[SparkPlugin]){
   	关系: father --> PluginContainer
   		sibling --> Logging
   	构造器属性:
   		env		spark环境变量
   		plugins	插件列表
   	属性:
   	#name @executorPlugins	#type @Seq[(String, ExecutorPlugin)]	执行器插件列表
   	1. 获取其余配置信息
   	val allExtraConf = env.conf.getAllWithPrefix(PluginContainer.EXTRA_CONF_PREFIX)
   	2. 注册执行器插件信息
   	plugins.flatMap { p =>
         val executorPlugin = p.executorPlugin()
         if (executorPlugin != null) {
           val name = p.getClass().getName()
           val prefix = name + "."
           val extraConf = allExtraConf
             .filter { case (k, v) => k.startsWith(prefix) }
             .map { case (k, v) => k.substring(prefix.length()) -> v }
             .toMap
             .asJava
           val ctx = new PluginContextImpl(name, env.rpcEnv, env.metricsSystem, env.conf,
             env.executorId)
   		// 初始化配置信息
           executorPlugin.init(ctx, extraConf)
           ctx.registerMetrics()
           Some(p.getClass().getName() -> executorPlugin)
        }else
        	None
        	
       def registerMetrics(appId: String): Unit
       	throw IllegalStateException ("Should not be called for the executor container.")
       
       def shutdown(): Unit
       功能: 关闭
       executorPlugins.foreach { case (name, plugin) =>
       	plugin.shutdown() : Throwable }
   }
   ```
   
   ```markdown
   object PluginContainer {
   	属性: 
   	#name @EXTRA_CONF_PREFIX="spark.plugins.internal.conf."	配置前缀
   	操作集：
   	def apply(sc: SparkContext): Option[PluginContainer] = PluginContainer(Left(sc))
   	功能: 根据指定的spark上下文@sc获取一个插件容器
   	
   	def apply(env: SparkEnv): Option[PluginContainer] = PluginContainer(Right(env))
   	功能： 根据spark环境变量@env获取一个插件容器
   	
   	def apply(ctx: Either[SparkContext, SparkEnv]): Option[PluginContainer]
   	功能: 根据任意一种情况，获取插件容器
   	val conf = ctx.fold(_.conf, _.conf)
       val plugins = Utils.loadExtensions(classOf[SparkPlugin], conf.get(PLUGINS).distinct, conf)
           if (plugins.nonEmpty) {
             ctx match {
               case Left(sc) => Some(new DriverPluginContainer(sc, plugins))
               case Right(env) => Some(new ExecutorPluginContainer(env, plugins))
             }
           } else {
             None
           }
   }
   ```
   
   
   
   #### PluginContextImpl
   
   ```markdown
   private class PluginContextImpl(pluginName: String,rpcEnv: RpcEnv,metricsSystem: MetricsSystem,
       override val conf: SparkConf,override val executorID: String){
   	关系: father --> PluginContext
       	sibling --> Logging
       构造器属性:
       	pluginName	插件名称
       	rpcEnv		rpc环境
       	metricsSystem	度量系统
       	conf		应用程序配置集
       	executorID	执行器ID
       属性:
       	#name @registry=new MetricRegistry()	度量登记器
       	#name @driverEndpoint	driver末端点
       	val=RpcUtils.makeDriverRef(classOf[PluginEndpoint].getName(), conf, rpcEnv) : null
       操作集:
       def metricRegistry(): MetricRegistry = registry
       功能: 获取度量登记器
       
       def send(message: AnyRef): Unit
       功能: 发送信息
       操作条件: 存在有driver末端点 driverEndpoint == null
       driverEndpoint.send(PluginMessage(pluginName, message))
       
       def ask(message: AnyRef): AnyRef
       功能: 请求获取信息
       操作条件: 存在有driver末端点
       val= driverEndpoint.askSync[AnyRef](PluginMessage(pluginName, message))
       
       def registerMetrics(): Unit 
       功能: 注册度量器
       操作条件： 度量器非空
       val= !registry.getMetrics().isEmpty() ? 
       	{val src = new PluginMetricsSource(s"plugin.$pluginName", registry)
       	metricsSystem.registerSource(src)} : Nop
       	
       内部类:
       class PluginMetricsSource(override val sourceName: String,
         	override val metricRegistry: MetricRegistry)
   	关系: father --> Source
   }
   ```
   #### PluginEndpoint
   
   ```markdown
   private class PluginEndpoint(plugins: Map[String, DriverPlugin],override val rpcEnv: RpcEnv){
   	关系: father --> IsolatedRpcEndpoint
   		sibling --> Logging
   	构造器属性: 
   		plugins	参数列表
   		rpcEnv	rpc环境
   	操作集:
   	def receive: PartialFunction[Any, Unit]
   	功能: 接受局部函数
   	1. 匹配插件信息
   	case PluginMessage(pluginName, message) =>
   		plugins.get(pluginName) match {
   			case Some(plugin) =>{ 
   				val reply = plugin.receive(message)
   				val= reply
   			}
   			case None => throw IllegalArgumentException
   		}
   	
   	def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit]
   	功能: 很久指定RPC调用上下文环境@context获取局部函数
   	val= case PluginMessage(pluginName, message) =>
   			plugins.get(pluginName) match {
   				case Some(plugin) =>
   					context.reply(plugin.receive(message))
   				case None
   					throw IllegalArgumentException
   }
   ```
   
   

#### Logging

```markdown
介绍:
	对于希望对数据做日志记录的类来说的一个实用特征。创建一个slf4j的日记记录器，允许日志信息使用懒加载的估算值,并登记到不同等级的日志中去。
```

```markdown
trait Logging {
	属性:
	#name @log_=null #type @Logger
		使得日志属性变成@transient,以至于带有Logging的对象能够被序列化，且使用在其他的机器上
	操作集:
	def logName: String={this.getClass.getName.stripSuffix("$")}
	功能: 获取本对象的记录器名称
	
	def log: Logger
	功能: 获取或者创造本类对象的记录器
	1. 获取log_ 日志管理器
    	(log_==null)? 
		{initializeLogIfNecessary(false)
		log_ = LoggerFactory.getLogger(logName)}:Nop
	2. val=log_
	
	def logInfo(msg: => String): Unit = if (log.isInfoEnabled) log.info(msg)
	功能: 记录info级别的日志信息
	
	def logDebug(msg: => String): Unit = if (log.isDebugEnabled) log.debug(msg)
	功能: 记录debug级别的日志信息
	
	def logTrace(msg: => String): Unit = if (log.isTraceEnabled) log.trace(msg)
	功能：记录trace级别日志信息
	
	def logWarning(msg: => String): Unit = if (log.isWarnEnabled) log.warn(msg)
	功能: 记录warning级别的日志
	
	def logError(msg: => String): Unit = if (log.isErrorEnabled) log.error(msg)
	功能: 记录error级别的日志
	
	def logInfo(msg: => String, throwable: Throwable): Unit
	功能: 允许接收异常的info级别日志记录
	
	def logDebug(msg: => String, throwable: Throwable): Unit
	功能: 允许接收异常的debug级别日志记录
	
	def logTrace(msg: => String, throwable: Throwable): Unit
	功能: 允许接收异常的trace级别日志记录
	
	def logWarning(msg: => String, throwable: Throwable): Unit
	功能: 允许接收异常的warning级别日志记录
	
	def logError(msg: => String, throwable: Throwable): Unit
	功能: 允许接收异常的error级别日志
	
	def isTraceEnabled(): Boolean = { log.isTraceEnabled }
	功能: 确定是否可以追踪
	
	def initializeLogIfNecessary(isInterpreter: Boolean): Unit
	功能: 如果有必要的话，对日志文件进行初始化
		initializeLogIfNecessary(isInterpreter, silent = false)
	
	def initializeLogIfNecessary(isInterpreter: Boolean,
      silent: Boolean = false): Boolean
    功能: 初始化日志
    操作条件: 日志系统没有初始化
    val= 
    if (!Logging.initialized) {
    	Logging.initLock.synchronized {
    		if (!Logging.initialized) { // 双重检索式保证线程安全
    			initializeLogging(isInterpreter, silent)
          		return true
          	}
         }
     }
     false
     
	def initializeForcefully(isInterpreter: Boolean, silent: Boolean): Unit 
	功能: 强力初始化
		initializeLogging(isInterpreter, silent)
	
	def initializeLogging(isInterpreter: Boolean, silent: Boolean): Unit
	功能: 初始化日志
	操作条件: 日志类型为log4j类型日志
	0. Logging.isLog4j12() ? Jump 1 : Nop
	1. 确认日志管理器是否初始化完毕
	val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
	!log4j12Initialized? Jump 2 : Nop
	2. 获取默认参数
	Logging.defaultSparkLog4jConfig = true // 开启soark log4j配置
	val defaultLogProps = "org/apache/spark/log4j-defaults.properties" // 默认日志属性
	Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
		case Some(url) => PropertyConfigurator.configure(url)
		if (!silent)
			System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
		case None =>
			System.err.println(s"Spark was unable to load $defaultLogProps")
	}
	3. 获取根日志管理器
	val rootLogger = LogManager.getRootLogger()
    if (Logging.defaultRootLevel == null) 
        Logging.defaultRootLevel = rootLogger.getLevel()
	
	4. 中断处理
	isInterpreter ? JUMP 5 :Nop
	5. 获取响应日志和响应等级
	val replLogger = LogManager.getLogger(logName)
    val replLevel = Option(replLogger.getLevel()).getOrElse(Level.WARN)
	6. 更新控制台添加器(spark-shell)容量为响应等级@replLevel
	if (replLevel != rootLogger.getEffectiveLevel()) {
		if (!silent) {
            System.err.printf("Setting default log level to \"%s\".\n", replLevel)
            System.err.println("To adjust logging level use sc.setLogLevel(newLevel). " +
              "For SparkR, use setLogLevel(newLevel).")
         }
         Logging.sparkShellThresholdLevel = replLevel
         rootLogger.getAllAppenders().asScala.foreach {
            case ca: ConsoleAppender =>
              ca.addFilter(new SparkShellLoggingFilter())
            case _ => // no-op
         }
	}
	7. 完成初始化
	Logging.initialized = true
	val= log
}
```

```markdown
private[spark] object Logging {
	属性:
	#name @initialized=false #type @Boolean volatile	日志管理器初始化状态
    #name @defaultRootLevel=null #type @Level volatile 	默认根目录等级
    #name @defaultSparkLog4jConfig=false #type @Boolean volatile 默认spark Log4j配置状态位
	#name @sparkShellThresholdLevel=null #type @Level volatile 默认spark shell 容量等级
	#name @initLock = new Object() 初始化锁
	初始化操作:
	try{
		// 通过反射用来处理用户移除slf4j-to-jul桥，用于定位其JUL log位置
        val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
        bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
        val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
        if (!installed) bridgeClass.getMethod("install").invoke(null)
	}catch {
		case e: ClassNotFoundException => // Nop
	}
	
	操作集：
	def isLog4j12(): Boolean
	功能: 分辨是否为log4j 1.2绑定
	val=binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    
    def uninitialize(): Unit 
    功能: 标记日志系统当前处于未初始化状态,尽最大努力重置日志系统到达其初始状态，以便于下个类触发时，运行在初始		状态下。
	// 重置defaultSparkLog4jConfig,sparkShellThresholdLevel属性
	if (isLog4j12()) {
      if (defaultSparkLog4jConfig) {
        defaultSparkLog4jConfig = false
        LogManager.resetConfiguration()
      } else {
        val rootLogger = LogManager.getRootLogger()
        rootLogger.setLevel(defaultRootLevel)
        sparkShellThresholdLevel = null
      }
    }
    // 修改标志
    this.initialized = false
}
```

```markdown
private class SparkShellLoggingFilter{
	关系: father --> Filter
	def decide(loggingEvent: LoggingEvent): Int
	功能: 决定接受还是质疑日志事件
	介绍: 如果spark-shell 容量等级@sparkShellThresholdLevel没有定义，那么过滤器不会进行操作
	如果日志等级不等于根目录等级(root level),那么这个日志事件被允许。否则则决定于日志是来自于日志是否来自于
    root或者用户配置
    if (Logging.sparkShellThresholdLevel == null) {
      Filter.NEUTRAL // 接受sparkshell容量未定义状态
    } else if (loggingEvent.getLevel.isGreaterOrEqual(Logging.sparkShellThresholdLevel)) {
      Filter.NEUTRAL // 接受等级是否满足要求
    } else {
      var logger = loggingEvent.getLogger()
      while (logger.getParent() != null) {
        if (logger.getLevel != null || logger.getAllAppenders.hasMoreElements) {
          return Filter.NEUTRAL // 向root获取日志信息，则接受
        }
        logger = logger.getParent()
      }
      Filter.DENY  // 不在root一系列的树型结构中，质疑
    }
}
```



基础拓展

1.  scala apply方法
2.  注解@transient
3.   #class @OutputCommitter