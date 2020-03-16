1. [collection](# collection)
2. [BytecodeUtils.scala](# BytecodeUtils)
3. [GraphGenerators.scala](# GraphGenerators)
4. [PeriodicGraphCheckpointer.scala](# PeriodicGraphCheckpointer)

---

#### collection

##### GraphXPrimitiveKeyOpenHashMap

```markdown
介绍:
	快速hashmap,没有空值,hashmap支持插入和更新,但是不支持删除,速度比普通的hashmap快,但是使用了较少的空间.
```

```scala
private[graphx]
class GraphXPrimitiveKeyOpenHashMap[@specialized(Long, Int) K: ClassTag,
                              @specialized(Long, Int, Double) V: ClassTag](
    val keySet: OpenHashSet[K], var _values: Array[V])
extends Iterable[(K, V)] with Serializable {
    构造器参数:
    	K	key类型
    	V	value类型
    	keySet	底层openHashSet(用于存放key的位置,返回位置@pos给value查找)
    	_values	value列表(用于存放value)
    属性:
    #name @grow	hashmap扩容策略(改变容量,将当前value值表备份)
    val= (newCapacity: Int) => {
        _oldValues = _values
        _values = new Array[V](newCapacity)
    }
    #name @move 扩容后数据转移策略
    val= (oldPos: Int, newPos: Int) => {
        _values(newPos) = _oldValues(oldPos)
    }
    
    构造器:
    def this(initialCapacity: Int) =
    this(new OpenHashSet[K](initialCapacity), new Array[V](initialCapacity))
    功能: 指定快速hashmap的初始容量
    
    def this() = this(64)
    功能: 初始容量为64
    
    def this(keySet: OpenHashSet[K]) = this(keySet, new Array[V](keySet.capacity))
    功能: 指定keyset初始化
    
    初始化操作:
    require(classTag[K] == classTag[Long] || classTag[K] == classTag[Int])
    功能: key类型校验
    
    属性:
    #name @_oldValues: Array[V] = null	旧值列表
    操作集:
    def size: Int = keySet.size
    功能: 获取hashmap的大小
    
    def apply(k: K): V 
    功能: 获取指定key的value
    1. 查找key在底层set中的位置
    val pos = keySet.getPos(k)
    2. 查找位置所在的值
    val= _values(pos)
    
    def getOrElse(k: K, elseValue: V): V 
    功能: 查找指定key的value,查找不到则返回@elseValue
    val pos = keySet.getPos(k)
    val= if (pos >= 0) _values(pos) else elseValue
    
    def update(k: K, v: V): Unit
    功能: 更新指定key的value值
    1. 查找位置@pos
    val pos = keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
    2. 更新
    _values(pos) = v
    3. 进行可能的rehash
    keySet.rehashIfNeeded(k, grow, move)
    4. 置空旧值
    _oldValues = null
    
    def setMerge(k: K, v: V, mergeF: (V, V) => V): Unit 
    功能: 设置value,用于合并
    输入参数:
    mergeF	合并函数
    1. 获取value列表的位置
    val pos = keySet.addWithoutResize(k)
    val ind = pos & OpenHashSet.POSITION_MASK
    2. 进行可能的值合并(当前值@ind不是首次添加)
    if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) { // if first add
      _values(ind) = v
    } else {
      _values(ind) = mergeF(_values(ind), v)
    }
    3. 进行可能的rehash
    keySet.rehashIfNeeded(k, grow, move)
    _oldValues = null
    
    def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V 
    功能: 改变value,如果key不存在于hashmap中则新建条目的value为@defaultValue,否则使用@mergeValue合并
    val pos = keySet.addWithoutResize(k)
    if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
      val newValue = defaultValue
      _values(pos & OpenHashSet.POSITION_MASK) = newValue
      keySet.rehashIfNeeded(k, grow, move)
      newValue
    } else {
      _values(pos) = mergeValue(_values(pos))
      _values(pos)
    }
    
    def iterator: Iterator[(K, V)]
    功能: 获取迭代器
    val= new Iterator[(K, V)] {
        var pos = 0
        var nextPair: (K, V) = computeNextPair()
        def computeNextPair(): (K, V) = {
            pos = keySet.nextPos(pos)
            if (pos >= 0) {
                val ret = (keySet.getValue(pos), _values(pos))
                pos += 1
                ret
            } else {
                null
            }
        }
        def hasNext: Boolean = nextPair != null
        def next(): (K, V) = {
            val pair = nextPair
            nextPair = computeNextPair()
            pair
        }
    }
}
```

#### BytecodeUtils

```scala
private[graphx] object BytecodeUtils {
    介绍: 字节码工具
    def invokedMethod(
        closure: AnyRef, targetClass: Class[_], targetMethod: String): Boolean
    功能: 调用方法,测试是否给定闭包@closure 是否在指定类@targetClass 中调用了指定方法@targetMethod
    if (_invokedMethod(closure.getClass, "apply", targetClass, targetMethod)) {
      true
    } else {
        // 查询关闭这个闭包的闭包,查询不到则说明不存在这个方法
      for (f <- closure.getClass.getDeclaredFields
           if f.getType.getName.startsWith("scala.Function")) {
        f.setAccessible(true)
        if (invokedMethod(f.get(closure), targetClass, targetMethod)) {
          return true
        }
      }
      false
    }
    
    def _invokedMethod(cls: Class[_], method: String,
      targetClass: Class[_], targetMethod: String): Boolean
    功能: 同上
    1. 设置调用栈
    val seen = new HashSet[(Class[_], String)]
    var stack = List[(Class[_], String)]((cls, method))
    2. 将调用栈中的每个元素置入集合@seen,如果与目标类和目标方法匹配则返回,否则一直进行到栈空位置,返回false
    while (stack.nonEmpty) {
      val c = stack.head._1
      val m = stack.head._2
      stack = stack.tail
      seen.add((c, m))
      val finder = new MethodInvocationFinder(c.getName, m)
      getClassReader(c).accept(finder, 0)
      for (classMethod <- finder.methodsInvoked) {
        if (classMethod._1 == targetClass && classMethod._2 == targetMethod) {
          return true
        } else if (!seen.contains(classMethod)) {
          stack = classMethod :: stack
        }
      }
    }
    false
    
    def getClassReader(cls: Class[_]): ClassReader
    功能: 从指定jar中获取指定类的asm格式读取器
    1. 在授权给读取器之前,将数据拷贝,否则可能运行在打开的文件之外
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    if (resourceStream == null) return new ClassReader(resourceStream)
    val baos = new ByteArrayOutputStream(128)
    Utils.copyStream(resourceStream, baos, true) // 将内容拷贝走
    val= new ClassReader(new ByteArrayInputStream(baos.toByteArray))
    
    def skipClass(className: String): Boolean 
    功能: 确定是否跳过指定类@className
    val c = className
    val= c.startsWith("java/") || c.startsWith("scala/") || c.startsWith("javax/")
    
    内部类:
    private class MethodInvocationFinder(className: String, methodName: String)
    extends ClassVisitor(ASM7) {
        介绍: 方法调用查找器
        查找指定类指定方法的调用,在运行访问器之后.
        例如:MethodInvocationFinder("spark/graph/Foo", "test"),它的方法调用变量会包含由@Foo.test()直接调用的方法集.接口的调用不会作为结果返回,因为不能决定字节码实际的调用方法.
        属性:
        #name @methodsInvoked = new HashSet[(Class[_], String)]	调用方法集
        操作集:
        def visitMethod(access: Int, name: String, desc: String,
                             sig: String, exceptions: Array[String]): MethodVisitor 
        功能: 访问指定方法,并返回方法访问器
        val= if (name == methodName) {
            new MethodVisitor(ASM7) {
                override def visitMethodInsn(
                    op: Int, owner: String, name: String, 
                    desc: String, itf: Boolean): Unit = {
                    if (op == INVOKEVIRTUAL || 
                        op == INVOKESPECIAL || op == INVOKESTATIC) {
                        if (!skipClass(owner)) {
                            methodsInvoked.add((
                                Utils.classForName(owner.replace("/", ".")), name))
                        }
                    }
                }
            }
        } else {
            null
        }
    }
}
```

#### GraphGenerators

```scala
object GraphGenerators extends Logging {
    属性:
    #name @RMATa = 0.45
    #name @RMATb = 0.15
    #name @RMATd = 0.25
    #name @RMATc = 0.15
    操作集:
    def logNormalGraph(
      sc: SparkContext, numVertices: Int, numEParts: Int = 0, mu: Double = 4.0,
      sigma: Double = 1.3, seed: Long = -1): Graph[Long, Int]
    功能: 生成图,顶点的出度分布呈对数正态分布.@mu和@sigma的参数值可以从@Pregel论文中获取,如果种子值为-1(默认值),则会选择随机种子,否则使用用户指定的种子.
    输入参数:
    	sc	spark上下文
    	numVertices	生成图的顶点数量
    	numEParts	分区数量(可以选择配置)
    	mu	(默认为4.0)出度分布的平均值
    	sigma	(默认值1.3)出度分布标准差
    	seed	(默认-1)种子(-1代表随机选取)
    返回一个图对象.
    1. 获取估算的分区数量
    val evalNumEParts = if (numEParts == 0) sc.defaultParallelism else numEParts
    2. 生成随机种子
    val seedRand = if (seed == -1) new Random() else new Random(seed)
    val seed1 = seedRand.nextInt()
    val seed2 = seedRand.nextInt()
    3. 获取顶点RDD信息
    val vertices: RDD[(VertexId, Long)] = 
    	sc.parallelize(0 until numVertices, evalNumEParts).map {
            src => (src, sampleLogNormal(mu, sigma, numVertices, seed = (seed1 ^ src)))
        }
    4. 获取边列表信息(随机)
    val edges = vertices.flatMap { case (src, degree) =>
      generateRandomEdges(src.toInt, degree.toInt, numVertices, seed = (seed2 ^ src))
    }
    val= Graph(vertices, edges, 0)
    
    def generateRandomEdges(
      src: Int, numEdges: Int, maxVertexId: Int, seed: Long = -1): Array[Edge[Int]] 
    功能: 生成随机边列表
    1. 确定随机值
    val rand = if (seed == -1) new Random() else new Random(seed)
    2. 填充指定长度的边
    val= Array.fill(numEdges) { Edge[Int](src, rand.nextInt(maxVertexId), 1) }
    
    private[spark] def sampleLogNormal(
      mu: Double, sigma: Double, maxVal: Int, seed: Long = -1): Int
    功能: 对数正态采样
    使用公式x=exp(m+s*Z),其中m,s是平均数和标准差,且Z~N(0,1).在这个函数中,m=e^(mu+sigma^2/2),且
    s=sqrt(e^(sigma^2)-1)(e^(2*mu+sigma^2)))
    输入参数:
    mu	正态分布的平均值
    sigma	正态分布的标准差
    maxVal	采样值的上限值
    seed	种子值
    1. 求m和s
    val rand = if (seed == -1) new Random() else new Random(seed)
    val sigmaSq = sigma * sigma
    val m = math.exp(mu + sigmaSq / 2.0)
    val s = math.sqrt(math.expm1(sigmaSq) * math.exp(2*mu + sigmaSq))
    2. 求取z分布
    var X: Double = maxVal
    while (X >= maxVal) {
      val Z = rand.nextGaussian()
      X = math.exp(mu + sigma*Z)
    }
    3. 确定采样值(x=exp(m+s*Z))
    val= math.floor(X).toInt
    
    def rmatGraph(
        sc: SparkContext, requestedNumVertices: Int, numEdges: Int): Graph[Int, Int] 
    功能: 使用R-MAT模型随机生成图
    1. 确定顶点数量和边数量的上限
    使N=请求的顶点数量@requestedNumVertices,顶点数量=ceil(log2[N]).这样就可以保证在所有的迭代层中,4个象限的大小是相等的.
    val numVertices = math.round(
      math.pow(2.0, math.ceil(math.log(requestedNumVertices) / math.log(2.0)))).toInt
    val numEdgesUpperBound =
      math.pow(2.0, 2 * ((math.log(numVertices) / math.log(2.0)) - 1)).toInt
    2. 边数校验
    if (numEdgesUpperBound < numEdges) {
      throw new IllegalArgumentException(
        s"numEdges must be <= $numEdgesUpperBound but was $numEdges")
    }
    3. 添加边
    var edges = mutable.Set.empty[Edge[Int]]
    while (edges.size < numEdges) {
      if (edges.size % 100 == 0) {
        logDebug(edges.size + " edges")
      }
      edges += addEdge(numVertices)
    }
    4. 生成图
    val= outDegreeFromEdges(sc.parallelize(edges.toList))
    
    def outDegreeFromEdges[ED: ClassTag](edges: RDD[Edge[ED]]): Graph[Int, ED]
    功能: 根据边确定顶点的出度
    val vertices = edges.flatMap { edge => List((edge.srcId, 1)) }
      .reduceByKey(_ + _)
      .map{ case (vid, degree) => (vid, degree) }
    val= Graph(vertices, edges, 0)
    
    def addEdge(numVertices: Int): Edge[Int]
    功能: 添加边
    输入参数: numVertices	需要添加到图中的边数
    val v = math.round(numVertices.toFloat/2.0).toInt
    val (src, dst) = chooseCell(v, v, v)
    Edge[Int](src, dst, 1)
    
    def chooseCell(x: Int, y: Int, t: Int): (Int, Int) 
    功能: 这个方法迭代的将邻接矩阵划分为4个部分,直到划分的单个单元,这个就匹配了R-MAT论文中的命名规范,邻接矩阵的形式类似与此.
    * <pre>
    *
    *          dst ->
    * (x,y) ***************  _
    *       |      |      |  |
    *       |  a   |  b   |  |
    *  src  |      |      |  |
    *   |   ***************  | T
    *  \|/  |      |      |  |
    *       |   c  |   d  |  |
    *       |      |      |  |
    *       ***************  -
    * </pre>
    这个就代表邻接矩阵的子象限,子象限将会被迭代划分,(x,y)代表子象限的左上角.且T代表边的长度.
    选择完毕下一级的子象限之后,获取如下结果:
    {{{
        quad = a, x'=x, y'=y, T'=T/2
        quad = b, x'=x+T/2, y'=y, T'=T/2
        quad = c, x'=x, y'=y+T/2, T'=T/2
        quad = d, x'=x+T/2, y'=y+T/2, T'=T/2
    }}}
    使用代码表示如下:
    val= if (t <= 1) {
      (x, y)
    } else {
      val newT = math.round(t.toFloat/2.0).toInt
      pickQuadrant(RMATa, RMATb, RMATc, RMATd) match {
        case 0 => chooseCell(x, y, newT)
        case 1 => chooseCell(x + newT, y, newT)
        case 2 => chooseCell(x, y + newT, newT)
        case 3 => chooseCell(x + newT, y + newT, newT)
      }
    }
    
    def pickQuadrant(a: Double, b: Double, c: Double, d: Double): Int
    功能: 确定指定数值a,b,c,d所在的象限
    if (a + b + c + d != 1.0) {
      throw new IllegalArgumentException("R-MAT probability parameters 
      sum to " + (a + b + c + d)
        + ", should sum to 1.0")
    }
    val rand = new Random()
    val result = rand.nextDouble()
    result match {
      case x if x < a => 0 // 0 corresponds to quadrant a
      case x if (x >= a && x < a + b) => 1 // 1 corresponds to b
      case x if (x >= a + b && x < a + b + c) => 2 // 2 corresponds to c
      case _ => 3 // 3 corresponds to d
    }
    
    def gridGraph(sc: SparkContext, rows: Int, cols: Int): Graph[(Int, Int), Double] 
    功能: 创建行列网格图,基于这个创建row+1,和col+1的邻接元素
    输入参数:
    	rows	行号
    	cols	列号
    1. 定义行列转换为函数
    def sub2ind(r: Int, c: Int): VertexId = r * cols + c
    2. 获取顶点列表
    val vertices: RDD[(VertexId, (Int, Int))] = sc.parallelize(0 until rows).flatMap {
        r =>
      (0 until cols).map( c => (sub2ind(r, c), (r, c)) )
    }
    3. 基于顶点创建边
    val edges: RDD[Edge[Double]] =
      vertices.flatMap{ case (vid, (r, c)) =>
        (if (r + 1 < rows) { Seq( (sub2ind(r, c), sub2ind(r + 1, c))) } 
         else { Seq.empty }) ++
        (if (c + 1 < cols) { Seq( (sub2ind(r, c), sub2ind(r, c + 1))) } 
         else { Seq.empty })
      }.map{ case (src, dst) => Edge(src, dst, 1.0) }
    val= Graph(vertices, edges)
    
    def starGraph(sc: SparkContext, nverts: Int): Graph[Int, Int]
    功能: 使用以顶点0为中心节点,创建启动图
    val edges: RDD[(VertexId, VertexId)] = sc.
    	parallelize(1 until nverts).map(vid => (vid, 0))
    Graph.fromEdgeTuples(edges, 1)
}
```

#### PeriodicGraphCheckpointer

```markdown
介绍:
	这个类帮助持久化和对图进行检查点操作.特别地,自动的处理持久化和检查点操作,也可以解除持久化和移除检查点文件.
	图在实体化之前,用户在创建新的图的时候可以调用@update()方法,在更新@PeriodicGraphCheckpointer之后,用户需要对图进行实体化,保证持久化和检查点操作实际发生.
	当更新@update()调用的时候,进行如下操作
	- 持久化新的图(如果还没有持久化),将持久化完成的图放到队列当中
	- 解除队列中图的持久化,直到队列中至多只含有三个图的时候
	- 如果使用检查点,且到达了检查点的周
	对新的图进行检查点操作,将进行检查点的图放到队列中
	移除旧的检查点	
	
	注意:
	- 这个类不能被拷贝(拷贝会引起已经设置检查点的图冲突)
	- 这个类在检查点设置完毕之后移除检查点文件,但是旧的图引用仍然保留isCheckpointed = true
	
	示例:
	{{{
		// 检查点周期为2
        val (graph1, graph2, graph3, ...) = ...
        val cp = new PeriodicGraphCheckpointer(2, sc)
        cp.updateGraph(graph1)
        graph1.vertices.count(); graph1.edges.count()
        // 持久化g1
        cp.updateGraph(graph2)
 		graph2.vertices.count(); graph2.edges.count()
 		// 持久化g1,g2,g2设置检查点
 		cp.updateGraph(graph3)
 		graph3.vertices.count(); graph3.edges.count()
 		// 持久化g1,g2,g3,g2设置检查点
 		cp.updateGraph(graph4)
 		graph4.vertices.count(); graph4.edges.count()
 		// 持久化g2,g3,g4.g4设置检查点
 		cp.updateGraph(graph5)
 		graph5.vertices.count(); graph5.edges.count()
 		// 持久化g3,g4,g5,g4设置检查点
	}}}
	
	构造器参数:
	checkpointInterval	检查点周期
	VD	顶点类型
	ED	边类型
```

```scala
private[spark] class PeriodicGraphCheckpointer[VD, ED](
    checkpointInterval: Int,
    sc: SparkContext)
extends PeriodicCheckpointer[Graph[VD, ED]](checkpointInterval, sc) {
    操作集:
    def checkpoint(data: Graph[VD, ED]): Unit = data.checkpoint()
    功能: 设置检查点
    
    def isCheckpointed(data: Graph[VD, ED]): Boolean = data.isCheckpointed
    功能: 确定是否设置了检查点
    
    def persist(data: Graph[VD, ED]): Unit
    功能: 持久化图数据
    1. 持久化顶点信息
    if (data.vertices.getStorageLevel == StorageLevel.NONE) {
      data.vertices.cache()
    }
    2.持久化边信息
    if (data.edges.getStorageLevel == StorageLevel.NONE) {
      data.edges.cache()
    }
    
    def unpersist(data: Graph[VD, ED]): Unit
    功能: 解除图的持久化
    val= data.unpersist()
    
    def getCheckpointFiles(data: Graph[VD, ED]): Iterable[String]
    功能: 获取检查点文件
    val= data.getCheckpointFiles
}
```

#### 基础拓展

1. [对数正态分布]([https://zh.wikipedia.org/zh-hans/%E5%AF%B9%E6%95%B0%E6%AD%A3%E6%80%81%E5%88%86%E5%B8%83](https://zh.wikipedia.org/zh-hans/对数正态分布))

2. [图解图算法](https://io-meter.com/2018/03/23/pregel-in-graphs/)

3. [R-MAT模型](http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf)

   图挖掘的迭代模型