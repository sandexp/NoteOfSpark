1. [Edge.scala](# Edge)
2. [EdgeContext.scala](# EdgeContext)
3. [EdgeDirection.scala](# EdgeDirection)
4. [EdgeRDD.scala](# EdgeRDD)
5. [EdgeTriplet.scala](# EdgeTriplet)
6. [Graph.scala](# Graph)
7. [GraphLoader.scala](# GraphLoader)
8. [GraphOps.scala](# GraphOps)
9. [GraphXUtils.scala](# GraphXUtils)
10. [PartitionStrategy.scala](# PartitionStrategy)
11. [Pregel.scala](# Pregel)
12. [VertexRDD.scala](# VertexRDD)

---

#### Edge

```scala
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
    var srcId: VertexId = 0,
    var dstId: VertexId = 0,
    var attr: ED = null.asInstanceOf[ED])
extends Serializable {
    介绍: 单向边,包含起始顶点ID,目标顶点ID,和边的权值
    构造器参数:
        srcId	起始顶点ID
        dstId	目标顶点ID
        attr	边权
    操作集:
    def otherVertexId(vid: VertexId): VertexId
    功能: 给定边的一个顶点,返回另一个顶点
    val= if (srcId == vid) dstId else { assert(dstId == vid); srcId }
    
    def relativeDirection(vid: VertexId): EdgeDirection
    功能: 获取与指定顶点相关的方向@EdgeDirection
    val= if (vid == srcId) EdgeDirection.Out else { 
        assert(vid == dstId); EdgeDirection.In }
}
```

```scala
object Edge {
    操作集:
    def lexicographicOrdering[ED]
    功能: 使用lexico图排序
    val= new Ordering[Edge[ED]] {
        override def compare(a: Edge[ED], b: Edge[ED]): Int = {
            if (a.srcId == b.srcId) {
                if (a.dstId == b.dstId) 0
                else if (a.dstId < b.dstId) -1
                else 1
            } else if (a.srcId < b.srcId) -1
            else 1
        }
    }
    
    def edgeArraySortDataFormat[ED]
    功能: 获取边排序格式
    val= new SortDataFormat[Edge[ED], Array[Edge[ED]]] {
        override def getKey(data: Array[Edge[ED]], pos: Int): Edge[ED] = {
            data(pos)
        }
        override def swap(data: Array[Edge[ED]], pos0: Int, pos1: Int): Unit = {
            val tmp = data(pos0)
            data(pos0) = data(pos1)
            data(pos1) = tmp
        }
        override def copyElement(
            src: Array[Edge[ED]], srcPos: Int,
            dst: Array[Edge[ED]], dstPos: Int): Unit = {
            dst(dstPos) = src(srcPos)
        }
        override def copyRange(
            src: Array[Edge[ED]], srcPos: Int,
            dst: Array[Edge[ED]], dstPos: Int, length: Int): Unit = {
            System.arraycopy(src, srcPos, dst, dstPos, length)
        }
        override def allocate(length: Int): Array[Edge[ED]] = {
            new Array[Edge[ED]](length)
        }
    }
}
```

#### EdgeContext

```scala
abstract class EdgeContext[VD, ED, A] {
    介绍: 边上下文
    代表含有邻接顶点的边,同时可以通过边发送数据
    操作集:
    def srcId: VertexId
    功能: 获取起始顶点编号
    
    def dstId: VertexId
    功能: 获取目标顶点编号
    
    def srcAttr: VD
    功能: 获取起始顶点权值
    
    def dstAttr: VD
    功能: 获取目标顶点权值
    
    def attr: ED
    功能: 获取边权值
    
    def sendToSrc(msg: A): Unit
    功能: 发送消息到起始顶点
    
    def sendToDst(msg: A): Unit
    功能: 发送顶点到目标顶点
    
    def toEdgeTriplet: EdgeTriplet[VD, ED]
    功能: 将边和顶点信息转换为三元组信息@EdgeTriplet,方便存储
    val et = new EdgeTriplet[VD, ED]
    et.srcId = srcId
    et.srcAttr = srcAttr
    et.dstId = dstId
    et.dstAttr = dstAttr
    et.attr = attr
    val= et
}
```

```scala
object EdgeContext {
    def unapply[VD, ED, A](edge: EdgeContext[VD, ED, A]): 
    	Some[(VertexId, VertexId, VD, VD, ED)]
    功能: 获取一个边实例
    val= Some((edge.srcId, edge.dstId, edge.srcAttr, edge.dstAttr, edge.attr))
}
```

#### EdgeDirection

```scala
class EdgeDirection private (private val name: String) extends Serializable {
    介绍: 单向边的方向描述
    操作集:
    def toString: String = "EdgeDirection." + name
    功能: 信息显示
    
    def equals(o: Any): Boolean
    功能: 判断两个边方向是否相等
    val= o match {
        case other: EdgeDirection => other.name == name
        case _ => false
    }
    
    def hashCode: Int = name.hashCode
    功能: hash值
    
    def reverse: EdgeDirection
    功能: 反转边的方向,并返回
    val= this match {
        case EdgeDirection.In => EdgeDirection.Out
        case EdgeDirection.Out => EdgeDirection.In
        case EdgeDirection.Either => EdgeDirection.Either
        case EdgeDirection.Both => EdgeDirection.Both
    }
}
```

```scala
object EdgeDirection {
    #name @In: EdgeDirection = new EdgeDirection("In")	边指向顶点
    #name @Out: EdgeDirection = new EdgeDirection("Out")	边从顶点指出
    #name @Either: EdgeDirection = new EdgeDirection("Either")	边指向/指出顶点
    #name @Both: EdgeDirection = new EdgeDirection("Both")	双向边
}
```

#### EdgeRDD

```markdown
介绍:
 边RDD@EdgeRDD 是@RDD[Edge[ED]]的继承,通过将边存储到每个分区中,按照分栏的形式存储.可以额外的存储顶点权值(与这个边相关的).这个顶点的权值由@impl.ReplicatedVertexView 管理.
```

```scala
abstract class EdgeRDD[ED](
    sc: SparkContext,
    deps: Seq[Dependency[_]]) extends RDD[Edge[ED]](sc, deps) {
    构造器参数:
    deps	依赖列表
    操作集:
    def partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])] forSome { type VD }
    功能: 获取分区RDD
    
    def getPartitions: Array[Partition] = partitionsRDD.partitions
    功能: 获取分区列表
    
    def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]]
    功能: 计算指定分区的中的边RDD
    val p = firstParent[(PartitionID, EdgePartition[ED, _])].iterator(part, context)
    val= if (p.hasNext) {
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
    
    def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDD[ED2]
    功能: 对边进行映射函数@f处理,使之转换为边RDD
    
    def reverse: EdgeRDD[ED]
    功能: 反转RDD中的所有边
    
    def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgeRDD[ED2])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDD[ED3]
    功能: 与指定边RDD@other 内链接
    
    def withTargetStorageLevel(targetStorageLevel: StorageLevel): EdgeRDD[ED]
    功能: 改变目标的存储等级,且保持所有边RDD中的所有其他属性.返回的边RDD会保存这个存储等级   
}
```

```scala
object EdgeRDD {
    def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): EdgeRDDImpl[ED, VD]
    功能: 有知道的边集合创建边RDD
    1. 获取边分区
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
        // 这个是分区映射函数
      val builder = new EdgePartitionBuilder[ED, VD]
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    2. 返回边RDD
    val= EdgeRDD.fromEdgePartitions(edgePartitions)
    
    def fromEdgePartitions[ED: ClassTag, VD: ClassTag](
      edgePartitions: RDD[(Int, EdgePartition[ED, VD])]): EdgeRDDImpl[ED, VD]
    功能: 由已经构建的边分区创建边RDD
    输入参数:
    	ED	边权值
    	VD	点权值
    val= new EdgeRDDImpl(edgePartitions)
}
```

#### EdgeTriplet

```scala
class EdgeTriplet[VD, ED] extends Edge[ED] {
    介绍: 边三元组
    代表着边和相邻的顶点
    构造器参数:
    	VD	顶点权值类型
    	ED	边权类型
    属性:
    #name @srcAttr: VD = _	起始顶点权值
    #name @dstAttr: VD = _ 	目标顶点权值
    操作集:
    def set(other: Edge[ED]): EdgeTriplet[VD, ED]
    功能: 设置三元组的边属性
    srcId = other.srcId
    dstId = other.dstId
    attr = other.attr
    val= this
    
    def otherVertexAttr(vid: VertexId): VD
    功能: 给定边的一个顶点,返回另一个顶点
    val= if (srcId == vid) dstAttr else { assert(dstId == vid); srcAttr }
    
    def vertexAttr(vid: VertexId): VD
    功能: 获取指定顶点的点权值
    val= if (srcId == vid) srcAttr else { assert(dstId == vid); dstAttr }
    
    def toString: String = ((srcId, srcAttr), (dstId, dstAttr), attr).toString()
    功能: 信息显示
    
    def toTuple: ((VertexId, VD), (VertexId, VD), ED) 
    功能: 转化为元组的形式
    val= ((srcId, srcAttr), (dstId, dstAttr), attr)
}
```

#### Graph

```markdown
介绍:
	这个抽象类代表一个图,这个图中可以包含任意数量的顶点和边.图提供了基本的操作,用于获取和操作相关顶点和边的数据.图提供基本操作,用于获取和操作相关数据,和操作底层数据结构.和spark RDD相似,图式基本的操作数据结构,可以操作并返回新的图.
	构造器参数:
	VD	顶点权值类型
	ED	边权类型
```

```scala
abstract class Graph[VD: ClassTag, ED: ClassTag] protected () extends Serializable {
    属性:
    #name @vertices: VertexRDD[VD]	包含顶点和相关属性的RDD
    #name @edges: EdgeRDD[ED]	包含边和相关属性的RDD
    	数据内部只包含起始和终止顶点以及边数据
    #name @triplets: RDD[EdgeTriplet[VD, ED]]	边三元组信息
    #name @ops = new GraphOps(this)	图操作实例
    操作集:
    def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED]
    功能: 图数据持久化(顶点和边的信息),使用指定形式的存储等级
    
    def cache(): Graph[VD, ED]
    功能: 缓存图数据到内存中
    
    def checkpoint(): Unit
    功能: 对图数据设置检查点,可以使用@SparkContext.setCheckpointDir() 设置检查点目录,所有父级RDD的应用会被移除.强烈推荐将图数据持久化到内存中.保存在文件中需要重新计算.
    
    def isCheckpointed: Boolean
    功能: 检查图的数据是否设置检查点
    
    def getCheckpointFiles: Seq[String]
    功能: 获取检查点文件名称
    
    def unpersist(blocking: Boolean = false): Graph[VD, ED]
    功能: 解除图数据的持久化,用于在一个迭代器中使用迭代算法
    
    def unpersistVertices(blocking: Boolean = false): Graph[VD, ED]
    功能: 解除顶点数据的持久化,但是不接触边数据的持久化,用于修改点权但是还需要重用边信息的操作.这个方法可以解除之前迭代器的点权(一旦这个迭代器不再需要的时候)用于提示GC性能.
    
    def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED]
    功能: 通过分区策略@partitionStrategy 对图中的边进行重分区
    
    def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: Int)
    	: Graph[VD, ED]
    功能: 同上,这里指定了分区的数量
    
    def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)
    (implicit eq: VD =:= VD2 = null): Graph[VD2, ED]
    功能: 使用map函数转换顶点的权值,新的图拥有相同的数据结构.
    示例:
    {{{
        val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
        val root = 42
        var bfsGraph = rawGraph.mapVertices[Int](
            (vid, data) => if (vid == root) 0 else Math.MaxValue)
    }}}
    
    def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2]
    功能: 使用map函数对边进行映射
    val= {
        mapEdges((pid, iter) => iter.map(map))
    }
    
    def mapEdges[ED2: ClassTag](map: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])
    : Graph[VD, ED2]
    功能: 同上,映射的结果是迭代器
    
    def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] 
    功能: 使用map函数,对三元组信息进行映射
    val= {
        mapTriplets((pid, iter) => iter.map(map), TripletFields.All)
    }
    
    def mapTriplets[ED2: ClassTag](
      map: EdgeTriplet[VD, ED] => ED2,
      tripletFields: TripletFields): Graph[VD, ED2]
    功能: 同上,这里指定了三元组的属性@tripletFields
    val= {
        mapTriplets((pid, iter) => iter.map(map), tripletFields)
    }
    
    def mapTriplets[ED2: ClassTag](
      map: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2],
      tripletFields: TripletFields): Graph[VD, ED2]
    功能: 同上,这里使用映射函数@map 获取的是一个迭代器数据类型
    
    def reverse: Graph[VD, ED]
    功能: 反转图中的所有边
    
    def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean = (x => true),
      vpred: (VertexId, VD) => Boolean = ((v, d) => true))
    : Graph[VD, ED]
    功能: 获取子图,子图满足
    对于使得@vpred为true的点保存在子图中,对于满足@epred为true的边保存在子图中.
    
    def mask[VD2: ClassTag, ED2: ClassTag](other: Graph[VD2, ED2]): Graph[VD, ED]
    功能: 返回一个在本图和@other图中都有的元素.保留这些共同元素并返回(求交集)
    
    def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED]
    功能: 合并在两个节点之间的多个边,对于正确的结果来说,必须使用@partitionBy来进行分区.
    输入参数:
    merge	合并函数
    返回: 使用单个边相连的图
    
    def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All)
    : VertexRDD[A]
    功能: 归并相邻边的value值用户支持@sendMsg函数,用于生成一个或者多条消息,用于发送到边的对端顶点.@mergeMsg函数用于合并所有的目的地消息,用于生成顶点的点权.
    输入参数:
    	A	消息类型
    	sendMsg	发送消息的函数
    	mergeMsg	合并点上接受消息的函数
    	tripletFields	三元组信息
    val= {
        aggregateMessagesWithActiveSet(sendMsg, mergeMsg, tripletFields, None)
    }
    
    def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)])
    : VertexRDD[A]
    功能: 同上,但是需要设置@activeSetOpt 参数去限制边的子集
    
    def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
      (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
    : Graph[VD2, ED]
    功能: 与点进行合并,使用@mapFunc函数对结果进行合并,输入表需要每个顶点至多包含一个条目.如果没有,map函数就会返回None.
    输入参数:
    U	表条目更新类型
    VD2	新的顶点类型
    other	需要join的表
    mapFunc	映射函数,用于计算新的顶点值.对于所有顶点都会计算,无论是否在表中
}
```

```scala
object Graph {
    def fromEdgeTuples[VD: ClassTag](
      rawEdges: RDD[(VertexId, VertexId)],
      defaultValue: VD,
      uniqueEdges: Option[PartitionStrategy] = None,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, Int] 
    功能: 由边集合构建图
    输入参数:
        rawEdges	原始边集
        defaultValue	默认顶点点权
        uniqueEdges	分区策略
        edgeStorageLevel	边的存储等级
        vertexStorageLevel	点的存储等级
    val edges = rawEdges.map(p => Edge(p._1, p._2, 1))
    val graph = GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    val= uniqueEdges match {
      case Some(p) => graph.partitionBy(p).groupEdges((a, b) => a + b)
      case None => graph
    }
    
    def fromEdges[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultValue: VD,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] 
    功能: 由指定边构建图
    val= GraphImpl(edges, defaultValue, edgeStorageLevel, vertexStorageLevel)
    
    def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD = null.asInstanceOf[VD],
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Graph[VD, ED] 
    功能: 由指定带权的点和边构建图.复制的点会被任意选取,且边权重的点但是不是输入顶点的采取默认点权
    val=  GraphImpl(
        vertices, edges, defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
    
    implicit def graphToGraphOps[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphOps[VD, ED] = g.ops
    功能: 获取图操作实例
}
```

#### GraphLoader

```scala
object GraphLoader extends Logging {
    介绍: 图加载器,提供从文件中加载图的方法
    操作集:
    def edgeListFile(
      sc: SparkContext,
      path: String,
      canonicalOrientation: Boolean = false,
      numEdgePartitions: Int = -1,
      edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Graph[Int, Int]
    功能: 从边列表文件中加载图,这个文件中每一行包含两个整数,一个时起始顶点,另一个是目标顶点,跳过以"#"开头的行.如果需要的边可以正向加载(起始顶点号<目标顶点号即正向).通过将@canonicalOrientation设置为true
    示例:
    加载文件的格式如下:
    {{{
        # command	line
        # sourceID	<\t>	TargetId
        1	-5
        1	2
        2	7
        1	8
    }}}
    输入参数:
    sc	spark上下文
    path	文件路径 -> 例如,/home/data/file或者hdfs://file 
    canonicalOrientation	是否产生正向边
    numEdgePartitions	边RDD的分区数量(设置为-1表示使用默认的并行度)
    edgeStorageLevel	边分区的存储等级
    vertexStorageLevel	顶点分区的存储等级
    0. 读取文本中配置的信息
    val lines =
      if (numEdgePartitions > 0) {
        sc.textFile(path, numEdgePartitions).coalesce(numEdgePartitions)
      } else {
        sc.textFile(path)
      }
    1. 将边数据表直接转换为边分区信息,并持久化到指定的存储等级
    val edges = lines.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[Int, Int]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
            // 忽略以#开头的配置行
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line)
          }
          val srcId = lineArray(0).toLong
          val dstId = lineArray(1).toLong
          if (canonicalOrientation && srcId > dstId) {
            builder.add(dstId, srcId, 1)
          } else {
            builder.add(srcId, dstId, 1)
          }
        }
      }
      Iterator((pid, builder.toEdgePartition))
    }.persist(edgeStorageLevel).setName(
        "GraphLoader.edgeListFile - edges (%s)".format(path))
    2. 边计数
    edges.count()
    logInfo(s"It took ${TimeUnit.NANOSECONDS.toMillis(
    	System.nanoTime() - startTimeNs)} ms" +
      " to load the edges")
    3. 生成图
    val= GraphImpl.fromEdgePartitions(
        edges, defaultVertexAttr = 1, edgeStorageLevel = edgeStorageLevel,
      vertexStorageLevel = vertexStorageLevel)
    
}
```

#### GraphOps

```scala
class GraphOps[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {
    介绍: 包含图的额外操作函数,所有的操作都是以提升GraphX API效率,这个类由图实例隐式构建.
    构造器参数:
    	VD	顶点权值类型
    	ED	边权值类型
    属性:
    #name @numEdges: Long = graph.edges.count()	transient lazy	边数
    #name @numVertices: Long = graph.vertices.count()transient lazy	顶点数量
    #name @inDegrees: VertexRDD[Int] transient lazy	图中的每个顶点的入度
    val= degreesRDD(EdgeDirection.In).setName("GraphOps.inDegrees")
    #name @outDegrees: VertexRDD[Int]	transient lazy	图中每个顶点的出度
    val=degreesRDD(EdgeDirection.Out).setName("GraphOps.outDegrees")
    #name @degrees: VertexRDD[Int]	transient lazy	图中每个顶点的读(没有边连接的不会返回)
    val= degreesRDD(EdgeDirection.Either).setName("GraphOps.degrees")
    
    操作集:
    def degreesRDD(edgeDirection: EdgeDirection): VertexRDD[Int]
    功能: 给定边的方向,计算邻接顶点的度
    计算方法: 当方向@edgeDirection 为in,则向目标顶点发送一个1的消息,入度加1
    	当@edgeDirection 为out,则向起始顶点发送一个1的消息,表示出度加1
    	当@edgeDirection 为Either时,表示是一个单向边,两个顶点都需要发送
    val= if (edgeDirection == EdgeDirection.In) {
      graph.aggregateMessages(_.sendToDst(1), _ + _, TripletFields.None)
    } else if (edgeDirection == EdgeDirection.Out) {
      graph.aggregateMessages(_.sendToSrc(1), _ + _, TripletFields.None)
    } else { // EdgeDirection.Either
      graph.aggregateMessages(ctx => { ctx.sendToSrc(1); ctx.sendToDst(1) }, _ + _,
        TripletFields.None)
    }
    
    def collectNeighborIds(edgeDirection: EdgeDirection): VertexRDD[Array[VertexId]]
    功能: 收集邻接顶点的顶点ID列表,并返回相应的RDD
    1. 获取邻接顶点
    val nbrs =
      if (edgeDirection == EdgeDirection.Either) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => { ctx.sendToSrc(Array(ctx.dstId)); ctx.sendToDst(Array(ctx.srcId)) },
          _ ++ _, TripletFields.None)
      } else if (edgeDirection == EdgeDirection.Out) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => ctx.sendToSrc(Array(ctx.dstId)),
          _ ++ _, TripletFields.None)
      } else if (edgeDirection == EdgeDirection.In) {
        graph.aggregateMessages[Array[VertexId]](
          ctx => ctx.sendToDst(Array(ctx.srcId)),
          _ ++ _, TripletFields.None)
      } else {
        throw new SparkException("It doesn't make sense to collect 
        neighbor ids without a " +
          "direction. (EdgeDirection.Both is not supported; 
          use EdgeDirection.Either instead.)")
      }
    2. 将顶点连成一个列表
    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[VertexId])
    }
    
    def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]]
    功能: 收集每个顶点的邻接顶点点权
    1. 获取邻接顶点的点权信息
    val nbrs = edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Array[(VertexId, VD)]](
          ctx => {
            ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr)))
            ctx.sendToDst(Array((ctx.srcId, ctx.srcAttr)))
          },
          (a, b) => a ++ b, TripletFields.All)
      case EdgeDirection.In =>
        graph.aggregateMessages[Array[(VertexId, VD)]](
          ctx => ctx.sendToDst(Array((ctx.srcId, ctx.srcAttr))),
          (a, b) => a ++ b, TripletFields.Src)
      case EdgeDirection.Out =>
        graph.aggregateMessages[Array[(VertexId, VD)]](
          ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr))),
          (a, b) => a ++ b, TripletFields.Dst)
      case EdgeDirection.Both =>
        throw new SparkException("collectEdges does not support 
        EdgeDirection.Both. Use" +
          "EdgeDirection.Either instead.")
    }
    2. 将邻接点信息连接成列表
    val= graph.vertices.leftJoin(nbrs) { (vid, vdata, nbrsOpt) =>
      nbrsOpt.getOrElse(Array.empty[(VertexId, VD)])
    }
    
    def collectEdges(edgeDirection: EdgeDirection): VertexRDD[Array[Edge[ED]]]
    功能: 返回一个RDD,包含所有顶点v的所有边,这个边指向v顶点
    注意: 这个是一个单例的顶点,给定方向但是没有边的不会返回.
    这个方法在幂律图中效率很低,这种图中顶点的度很大,可能导致大量数据存储到一个位置处.
    输入属性:
    edgeDirection	收集本地顶点边的边方向
    返回: 每个顶点的本地边
    val= edgeDirection match {
      case EdgeDirection.Either =>
        graph.aggregateMessages[Array[Edge[ED]]](
          ctx => { // 单向边发送函数
            ctx.sendToSrc(Array(new Edge(ctx.srcId, ctx.dstId, ctx.attr)))
            ctx.sendToDst(Array(new Edge(ctx.srcId, ctx.dstId, ctx.attr)))
          },
          (a, b) => a ++ b, // 边信息合并函数(边收集函数,在顶点上统计边和顶点的信息)
            TripletFields.EdgeOnly)
      case EdgeDirection.In =>
        graph.aggregateMessages[Array[Edge[ED]]](
          ctx => ctx.sendToDst(Array(new Edge(ctx.srcId, ctx.dstId, ctx.attr))),
          (a, b) => a ++ b, TripletFields.EdgeOnly)
      case EdgeDirection.Out =>
        graph.aggregateMessages[Array[Edge[ED]]](
          ctx => ctx.sendToSrc(Array(new Edge(ctx.srcId, ctx.dstId, ctx.attr))),
          (a, b) => a ++ b, TripletFields.EdgeOnly)
      case EdgeDirection.Both =>
        throw new SparkException("collectEdges does not support 
        EdgeDirection.Both. Use" +
          "EdgeDirection.Either instead.")
    }
    
    def removeSelfEdges(): Graph[VD, ED]
    功能: 移除自身形成的边(a->a的边)
    val= graph.subgraph(epred = e => e.srcId != e.dstId)
    
    def joinVertices[U: ClassTag](table: RDD[(VertexId, U)])(
        mapFunc: (VertexId, VD, U) => VD): Graph[VD, ED]
    功能: 与指定RDD进行join,然后使用指定函数@mapFunc映射新的顶点权值,输入表需要包含每个顶点至多一个条目.如果表中没有数据则会使用旧值.
    输入参数:
    U	更新表中条目的类型
    table	需要与图中顶点连接的表,表中对于每个顶点至多一个条目
    mapFunc	用于计算新的顶点权值的更新函数,只有在更新表中含有这个顶点信息时候才能调用,否则使用之前的值.
    示例:
    {{{
        1. 定义一个原始图
        val rawGraph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "webgraph")
   		.mapVertices((_, _) => 0)
        2. 求取原始图中的出度
        val outDeg = rawGraph.outDegrees
        3. 连接更新获取新的图
        val graph = rawGraph.joinVertices[Int](outDeg)
      ((_, _, outDeg) => outDeg)
        /*
        	更新表	outDeg
        */
    }}}
    1. 确定更新函数
    val uf = (id: VertexId, data: VD, o: Option[U]) => {
      o match { // 更新表中没这个数据则不更新
        case Some(u) => mapFunc(id, data, u)
        case None => data
      }
    }
    2. 获取新的图
    val= graph.outerJoinVertices(table)(uf)
    
    def filter[VD2: ClassTag, ED2: ClassTag](
      preprocess: Graph[VD, ED] => Graph[VD2, ED2],
      epred: (EdgeTriplet[VD2, ED2]) => Boolean = (x: EdgeTriplet[VD2, ED2]) => true,
      vpred: (VertexId, VD2) => Boolean = (v: VertexId, d: VD2) => true): Graph[VD, ED]
    功能: 过滤函数
    通过计算过滤函数的值,对参数进行过滤
    输入参数:
    preprocess	在过滤之前,计算新的顶点和边权值的函数
    epred	边过滤函数
    vpred	顶点过滤函数
    VD2	顶点过滤函数操作的数据类型
    ED2	边过滤函数操作的数据类型
    示例:
    {{{
        /*
        这个函数可以基于配置过滤图中的元素,不需要再程序中改变边和顶点的值.例如可以移除度为0的顶点.
        */
        graph.filter(
            graph => { // 预先处理函数
                val degrees: VertexRDD[Int] = graph.outDegrees
                graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
            },
            // 顶点过滤函数(移除度为0的点)
            vpred = (vid: VertexId, deg:Int) => deg > 0
        )
    }}}
    val= graph.mask(preprocess(graph).subgraph(epred, vpred))
    
    def pickRandomVertex(): VertexId 
    功能: 从图中获取随机顶点,并返回id
    val probability = 50.0 / graph.numVertices
    var found = false
    var retVal: VertexId = null.asInstanceOf[VertexId]
    while (!found) {
      val selectedVertices = graph.vertices.flatMap { vidVvals =>
        if (Random.nextDouble() < probability) { Some(vidVvals._1) }
        else { None }
      }
      if (selectedVertices.count > 0) {
        found = true
        val collectedVertices = selectedVertices.collect()
        retVal = collectedVertices(Random.nextInt(collectedVertices.length))
      }
    }
   val= retVal
    
    def convertToCanonicalEdges(
      mergeFunc: (ED, ED) => ED = (e1, e2) => e1): Graph[VD, ED] 
    功能: 转换为正向边
    一些图算法(比如说三角形计数法@TriangleCount)假定所有图都是正向的.这个方法重写了顶点的编号,使得起始顶点的编号小于目标顶点的编号.
    输入参数:
    mergeFunc	合并函数,用户定义的聚合函数,用于合并输出
    1. 重新计算边,使得方向都为正向
    val newEdges =
      graph.edges
        .map {
            // 正向转换函数
          case e if e.srcId < e.dstId => ((e.srcId, e.dstId), e.attr)
          case e => ((e.dstId, e.srcId), e.attr)
        }
        .reduceByKey(mergeFunc)
        .map(e => new Edge(e._1._1, e._1._2, e._2))
    val= Graph(graph.vertices, newEdges)
    
    def pregel[A: ClassTag](
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)(
      vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] 
    功能: 执行类似pregal的顶点并行迭代,用户定义的函数@vprog在每个顶点上并行执行,用于接收入站消息,并计算顶点的新值.函数@sendMsg在所有的出边上使用,用于计算目标顶点的权值.函数@mergeMsg函数用于合并到目标顶点上所有入站消息.
    所有顶点的第一个迭代器接收@initialMsg消息,如果顶点接收不到消息,则顶点处理程序就不会继续调用.
    这个函数会迭代到没有消息位置或者达到了规定的最大迭代数@maxIterations
    输入参数:
    A	Pregal消息类型
    initialMsg	首个迭代器接收到的顶点消息
    maxIterations	最大迭代次数
    activeDirection	边方向
    vprog	用于定义的顶点程序,在每个顶点上运行,接受入站信息并计算新的顶点权值,第一个迭代器上,顶点程序对所有顶点调用,传递默认消息.在子迭代器上,迭代程序仅仅在接收到消息的顶点上调用,直到迭代完成.
    sendMsg	用户提供函数,用于顶点的出边对端接受消息
    mergeMsg	用户定义函数,用于合并入站的两条消息
    返回: 图计算的最终结果
    val= Pregel(
        graph, initialMsg, maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)
    
    def pageRank(tol: Double, resetProb: Double = 0.15): Graph[Double, Double] 
    功能: 运行动态版本的pageRank算法,返回携带顶点权值的图,且包含有pageRank和边权值的属性.
    val= PageRank.runUntilConvergence(graph, tol, resetProb)
    
    def personalizedPageRank(
        src: VertexId, tol: Double,
    	resetProb: Double = 0.15): Graph[Double, Double]
    功能: 对于给定的顶点,返回个性化的pageRank,以便于所有随机值启动在相关的源节点上.
    val= PageRank.runUntilConvergenceWithOptions(graph, tol, resetProb, Some(src))
    
    def staticParallelPersonalizedPageRank(sources: Array[VertexId], numIter: Int,
    resetProb: Double = 0.15) : Graph[Vector, Double]
    功能: 启动并行个性化的pageRank,以便于所有随机值可以启动在相关的源顶点上
    val= PageRank.runParallelPersonalizedPageRank(graph, numIter, resetProb, sources)
    
    def staticPersonalizedPageRank(src: VertexId, numIter: Int,
    resetProb: Double = 0.15): Graph[Double, Double]
    功能: 静态个性化pageRank,运行指定数量的pageRank,返回带有顶点属性的图.这个图包含pageRank和带权的边属性.
    val= PageRank.runWithOptions(graph, numIter, resetProb, Some(src))
    
    def staticPageRank(numIter: Int, resetProb: Double = 0.15): Graph[Double, Double]
    功能: 同上
    val= PageRank.run(graph, numIter, resetProb)
    
    def staticPageRank(numIter: Int, resetProb: Double,
                     prePageRank: Graph[Double, Double]): Graph[Double, Double]
    功能: 运行pageRank用于指定数量@numIter的迭代器,返回一个带有顶点属性(包含pageRank和携带边权的边属性),包含前一个pageRank计算(用于作为新的迭代器计算的起始值)
    val= PageRank.
    	runWithOptionsWithPreviousPageRank(graph, numIter, resetProb, None, prePageRank)
    
    def connectedComponents(): Graph[VertexId, ED]
    功能: 连接组件,返回一个图,这个图中节点包含最小顶点编号(在包含这个顶点所有组件中最小编号)
    val= ConnectedComponents.run(graph)
    
    def connectedComponents(maxIterations: Int): Graph[VertexId, ED]
    功能: 计算连接的顶点的组件关系,返回一个包含顶点属性的图,顶点的编号是连接到顶点的组件中最小的顶点编号
    val= ConnectedComponents.run(graph, maxIterations)
    
    def triangleCount(): Graph[Int, ED]
    功能: 通过顶点计算图中的三角形格式,参考@org.apache.spark.graphx.lib.TriangleCount$#run
    val= TriangleCount.run(graph)
    
    def stronglyConnectedComponents(numIter: Int): Graph[VertexId, ED]
    功能: 计算每个顶点的强连通分量,返回的图的顶点值的顶点编号需要是连通图中最小的.
    val= StronglyConnectedComponents.run(graph, numIter)
}
```

#### GraphXUtils

```scala
object GraphXUtils {
    def registerKryoClasses(conf: SparkConf): Unit 
    功能: 使用kryo这次graphX类
    conf.registerKryoClasses(Array(
      classOf[Edge[Object]],
      classOf[(VertexId, Object)],
      classOf[EdgePartition[Object, Object]],
      classOf[BitSet],
      classOf[VertexIdToIndexMap],
      classOf[VertexAttributeBlock[Object]],
      classOf[PartitionStrategy],
      classOf[BoundedPriorityQueue[Object]],
      classOf[EdgeDirection],
      classOf[GraphXPrimitiveKeyOpenHashMap[VertexId, Int]],
      classOf[OpenHashSet[Int]],
      classOf[OpenHashSet[Long]]))
    
    def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
      g: Graph[VD, ED],
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      reduceFunc: (A, A) => A,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None): VertexRDD[A]
    功能: 用于映射绝对API的代理方法,返回一个顶点RDD
    1. 定义发送消息方法
    def sendMsg(ctx: EdgeContext[VD, ED, A]): Unit = {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }
    2. 返回处理(归并)完的RDD
    val= g.aggregateMessagesWithActiveSet(
      sendMsg, reduceFunc, TripletFields.All, activeSetOpt)
}
```

#### PartitionStrategy

```scala
trait PartitionStrategy extends Serializable {
    介绍: 分区策略
    def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID
    功能: 获取指定边的分区编号
}
```

```scala
object PartitionStrategy {
    内部类:
    case object EdgePartition1D extends PartitionStrategy {
        介绍: 一维边分区
        def getPartition(
            src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID 
        功能: 获取分区编号(hash分区)
        1. 确定hash分区的范围大小(取足够大的素数,防止hash碰撞)
        val mixingPrime: VertexId = 1125899906842597L
        2. 确定分区号
        val= (math.abs(src * mixingPrime) % numParts).toInt
    }
    
    case object RandomVertexCut extends PartitionStrategy {
        介绍: 随机顶点剪切
        通过对起始和目标顶点编号进行hash从而分配边到分区中.这就导致了随机顶点剪切的情况,这样两个顶点之间的同向边就会集中在一起.
        def getPartition(
            src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID
        功能: 获取分区编号
        输入参数:
        numParts	分区数量(hash分区数量)
        val= math.abs((src, dst).hashCode()) % numParts
    }
    
    case object CanonicalRandomVertexCut extends PartitionStrategy {
        介绍: 正向随机顶点剪切
        def getPartition(
            src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID
        功能: 获取分区编号
        val= if (src < dst) {
            math.abs((src, dst).hashCode()) % numParts
        } else {
            math.abs((dst, src).hashCode()) % numParts
        }
    }
    
    case object EdgePartition2D extends PartitionStrategy {
        介绍: 二维边分区
        使用二维分区(使用稀疏邻接矩阵形成),分配边到分区中.保证顶点的副本为2*sqrt(numParts).假设图中含有12个顶点,我要分区到9台机器上.可以使用下述的稀疏矩阵表述:
        * <pre>
        *       __________________________________
        *  v0   | P0 *     | P1       | P2    *  |
        *  v1   |  ****    |  *       |          |
        *  v2   |  ******* |      **  |  ****    |
        *  v3   |  *****   |  *  *    |       *  |
        *       ----------------------------------
        *  v4   | P3 *     | P4 ***   | P5 **  * |
        *  v5   |  *  *    |  *       |          |
        *  v6   |       *  |      **  |  ****    |
        *  v7   |  * * *   |  *  *    |       *  |
        *       ----------------------------------
        *  v8   | P6   *   | P7    *  | P8  *   *|
        *  v9   |     *    |  *    *  |          |
        *  v10  |       *  |      **  |  *  *    |
        *  v11  | * <-E    |  ***     |       ** |
        *       ----------------------------------
        * </pre>
        边E连接了v11和v1连个顶点,用于分配给处理机P6.为了获取分配的处理机编号,,将矩阵分割成sqrt(numParts)个分区.注意邻接与v11的边仅仅可以在第一列(接P0,P3,P6),或者最后一列分区(P2,P5,P8).这样就可以保证副本有至多2*sqrt(numParts)台机器.
        注意到P0中有许多边,但是这个分区的负载均衡性能很差.为了提升平衡性,需要将顶点编号乘以一个大的素数,以便于对顶点位置的shuffle.当请求的分区数不能够开平方的时候,使用稍微不同的方法处理,这里的最后一个列每个数据块的大小与其他列不同.
        
        def getPartition(
            src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID
        功能: 获取分区编号
        输入参数:
        	src	起始顶点号
        	dst	目标顶点编号
        	numParts	需要分成的分区数量
        1. 定义大的素数作为乘数,用于处理分区的平衡性
        val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt
        val mixingPrime: VertexId = 1125899906842597L
        2. 处理分区编号获取
        if (numParts == ceilSqrtNumParts * ceilSqrtNumParts) {
            // 乘以乘数平衡分区的分配,根据上面图示求出分区号
            val col: PartitionID = (math.abs(src * mixingPrime) % ceilSqrtNumParts).toInt
            val row: PartitionID = (math.abs(dst * mixingPrime) % ceilSqrtNumParts).toInt
            (col * ceilSqrtNumParts + row) % numParts
        } else {
            // 非平方分区处理方案,对最后一个列做减负处理
            val cols = ceilSqrtNumParts
            val rows = (numParts + cols - 1) / cols
            val lastColRows = numParts - rows * (cols - 1)
            val col = (math.abs(src * mixingPrime) % numParts / rows).toInt
            val row = (math.abs(dst * mixingPrime) % (if (col < cols - 1) rows 
                                                      else lastColRows)).toInt
            col * rows + row
        }
    }
    
    def fromString(s: String): PartitionStrategy
    功能: 获取分区策略
    val= s match {
        case "RandomVertexCut" => RandomVertexCut
        case "EdgePartition1D" => EdgePartition1D
        case "EdgePartition2D" => EdgePartition2D
        case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
        case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
    }
}
```

#### Pregel

```markdown
介绍:
 类似Pregel的大量同步消息传递API的实现.
 与原始的Pregal API不同,GraphX Pregel API对在边上发送消息@sendMessage会产生影响，促使消息发送计算，用于读取顶点参数和 图数据结构的约束消息。
 这个改变对分布式计算的性能大幅度的提升，然而提供更高的灵活性和基于图的计算。
 示例:
 使用Pragel的抽象实现pageRank.
 {{{
 	val pagerankGraph: Graph[Double, Double] = graph
 		.outerJoinVertices(graph.outDegrees) {
 			// 对于每个顶点,求出其顶点的出度
 		    (vid, vdata, deg) => deg.getOrElse(0)
 		}
 		.mapTriplets(e => 1.0 / e.srcAttr) // 基于出度设置边的权值
 		.mapVertices((id, attr) => 1.0) // 设置顶点权值,用于初始化pagerank初始值
    // 定义顶点处理程序,消息发送信息,消息合并处理
    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
    	resetProb + (1.0 - resetProb) * msgSum
 	def sendMessage(id: VertexId, edge: EdgeTriplet[Double, Double]): 
 		Iterator[(VertexId, Double)] = Iterator((edge.dstId, edge.srcAttr * edge.attr))
 	def messageCombiner(a: Double, b: Double): Double = a + b		
 	
    val initialMessage = 0.0
    // 在指定大小的迭代器上进行pregal运算
    Pregel(pagerankGraph, initialMessage, numIter)(
 		vertexProgram, sendMessage, messageCombiner)
 }}}
```

```scala
object Pregel extends Logging {
    def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED]
    功能: 
    执行类似pregal迭代顶点并行运行,用户定义的顶点处理函数@vprog,会在每个顶点上并行执行,用于接收内部消息,且对顶点计算新的值.发送消息函数@sendMsg然后被调用在所有的出边上,且用于计算目标顶点的消息.合并函数@mergeMsg韩式将发送到目标顶点的消息进行合并,并计算新的值.
       * On the first iteration all vertices receive the `initialMsg` and
   * on subsequent iterations if a vertex does not receive a message
   * then the vertex-program is not invoked.
   *
   * This function iterates until there are no remaining messages, or
   * for `maxIterations` iterations.
    第一次迭代的时候,所有顶点接收初始化消息@initialMsg且如果顶点没有接收到消息就不会调用顶点处理函数.这个迭代操作会执行到没有存留的消息或者到达了最大迭代次数@maxIterations
    输入参数:
        VD	顶点数据类型
        ED	边数据类型
        A	Pregal消息类型
        graph	输入图
        initialMsg	输出消息,所有顶点都会收到
        maxIterations	最大迭代次数
        activeDirection	边的方向
    	vprog	用户定义的顶点处理函数
    	sendMsg	消息发送函数
    	mergeMsg	目标顶点消息合并函数
    0. 参数校验
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")
    1. 获取图的检查点,并进行更新
    val checkpointInterval = graph.vertices.sparkContext.getConf
      .getInt("spark.graphx.pregel.checkpointInterval", -1)
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg))
    val graphCheckpointer = new PeriodicGraphCheckpointer[VD, ED](
      checkpointInterval, graph.vertices.sparkContext)
    graphCheckpointer.update(g)
    2. 计算消息,并持久化,同时更新统计信息
    var messages = GraphXUtils.mapReduceTriplets(g, sendMsg, mergeMsg)
    val messageCheckpointer = new PeriodicRDDCheckpointer[(VertexId, A)](
      checkpointInterval, graph.vertices.sparkContext)
    messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])
    var activeMessages = messages.count()
    3. 迭代进行消息发送和合并已经处理运算
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      prevG = g
      g = g.joinVertices(messages)(vprog)
      graphCheckpointer.update(g)
      val oldMessages = messages
      messages = GraphXUtils.mapReduceTriplets(
        g, sendMsg, mergeMsg, Some((oldMessages, activeDirection)))
      messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])
      activeMessages = messages.count()
      logInfo("Pregel finished iteration " + i)
      oldMessages.unpersist()
      prevG.unpersistVertices()
      prevG.edges.unpersist()
      i += 1
    }
    4. 清除检查点
    messageCheckpointer.unpersistDataSet()
    graphCheckpointer.deleteAllCheckpoints()
    messageCheckpointer.deleteAllCheckpoints()
    val=g
}
```

#### VertexRDD

```markdown
介绍: 
    继承@RDD[(VertexId, VD)],确保每个顶点只有一个条目。通过预先索引操作，加快查询的速度，提升join的速度。连个相同索引的@VertexRDD可以快速join。除了@reindex操作其他操作都会保持索引不变。可以使用@VertexRDD构建@VertexRDD实例。此外，存储路径信息可以开启顶点权值的job(使用EdgeRDD)
    示例,使用普通RDD,构建@VertexRDD
    {{{
         val someData: RDD[(VertexId, SomeType)] = loadData(someFile)
         val vset = VertexRDD(someData)
         val vset2 = VertexRDD(someData, reduceFunc)
         val otherData: RDD[(VertexId, OtherType)] = loadData(otherFile)
         val vset3 = vset2.innerJoin(otherData) { (vid, a, b) => b }
         val vset4: VertexRDD[(SomeType, OtherType)] = vset.leftJoin(vset3)
    }}}
    输入参数:
    VD	顶点权值类型
```

```scala
abstract class VertexRDD[VD](
    sc: SparkContext,
    deps: Seq[Dependency[_]]) extends RDD[(VertexId, VD)](sc, deps) {
    属性:
    def vdTag: ClassTag[VD]
    功能: 获取顶点权值类型
    
    def partitionsRDD: RDD[ShippableVertexPartition[VD]]
    功能: 获取分区RDD
    
    def getPartitions: Array[Partition] = partitionsRDD.partitions
    功能: 获取分区列表
    
    def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] 
    功能: 计算RDD中指定分区的数据
    val= firstParent[ShippableVertexPartition[VD]].iterator(
        part, context).next().iterator
    
    def reindex(): VertexRDD[VD]
    功能: 重新索引
    构建已经索引过的新的顶点RDD@VertexRDD,返回的RDD会重新索引,且不能和这个RDD进行快速join
    
    def mapVertexPartitions[VD2: ClassTag](
      f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])
    : VertexRDD[VD2]
    功能: 顶点RDD分区映射,使用分区映射函数@f
    
    def filter(pred: Tuple2[VertexId, VD] => Boolean): VertexRDD[VD]
    功能: 对顶点RDD中的顶点进行过滤
    val= this.mapVertexPartitions(_.filter(Function.untupled(pred)))
    
    def mapValues[VD2: ClassTag](f: VD => VD2): VertexRDD[VD2]
    功能: 映射每个顶点的属性,保持分区情况
    
    def mapValues[VD2: ClassTag](f: (VertexId, VD) => VD2): VertexRDD[VD2]
    功能: 同上,此外需要使用顶点的编号参数
    
    def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD]
    功能: 
    对于每个顶点,使得当前rdd减去@otherrdd,返回当前rdd中唯一的元素(本类去除公有)
    
    def minus(other: VertexRDD[VD]): VertexRDD[VD]
    功能: 同上
    
    def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD]
    功能: 返回两个RDD中不同元素组成的顶点RDD@VertexRDD
    
    def diff(other: VertexRDD[VD]): VertexRDD[VD]
    功能: 同上
    
    def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
      (other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): VertexRDD[VD3]
    功能: 与指定RDD@other进行左连接,如果两个RDD没有共同的元素则会失败,结果集中包含本类RDD的所有对象
    输入参数:
    VD2	other RDD元素类型
    VD3 结果RDD的元素类型
    other	需要连接的RDD
    f	映射函数,将顶点编号,顶点属性转换为新的顶点属性(实现了顶点类型的装换)
    
    def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: RDD[(VertexId, VD2)])
      (f: (VertexId, VD, Option[VD2]) => VD3)
    : VertexRDD[VD3]
    功能: 同上
    
    def innerZipJoin[U: ClassTag, VD2: ClassTag](other: VertexRDD[U])
      (f: (VertexId, VD, U) => VD2): VertexRDD[VD2]
    功能: 内链接
    函数@f用于对顶点属性的类型做转换
    
    def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
      (f: (VertexId, VD, U) => VD2): VertexRDD[VD2]
    功能: 同上
    
    def aggregateUsingIndex[VD2: ClassTag](
      messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): VertexRDD[VD2]
    功能: 使用索引进行聚合
    输入参数:
    messages	包含需要聚合的消息,每个消息都是顶点编号->消息数据的键值对
    reduceFunc	聚合函数
    
    def reverseRoutingTables(): VertexRDD[VD]
    功能: 返回一个新的RDD,其中对边的方向反向
    
    def withEdges(edges: EdgeRDD[_]): VertexRDD[VD]
    功能: 当前顶点RDD域指定边RDD进行join
    
    def withPartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[ShippableVertexPartition[VD2]]): VertexRDD[VD2]
    功能: 替换顶点分区,同时保留顶点RDD中的所有数据
    
    def withTargetStorageLevel(
      targetStorageLevel: StorageLevel): VertexRDD[VD]
    功能: 改变成目标的存储等级,同时保留RDD中的所有数据,注意这个操作不会触发缓存,为了使之发生缓存,需要调用@VertexRDD#cache
    
    def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[VD])]
    功能: 创建一个顶点属性的RDD,使之适合传送边分区
    
    def shipVertexIds(): RDD[(PartitionID, Array[VertexId])]
    功能: 创建一个顶点编号的RDD,使之可以传送边分区
}
```

```scala
object VertexRDD {
    def apply[VD: ClassTag](vertices: RDD[(VertexId, VD)]): VertexRDD[VD] 
    功能: 由指定的顶点编号->顶点属性,构建一个独立的顶点RDD(不可以与边RDD进行高效的连接)
    val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
      case Some(p) => vertices
      case None => vertices.partitionBy(new HashPartitioner(vertices.partitions.length))
    }
    val vertexPartitions = vPartitioned.mapPartitions(
      iter => Iterator(ShippableVertexPartition(iter)),
      preservesPartitioning = true)
    new VertexRDDImpl(vertexPartitions)
    
    def apply[VD: ClassTag](
      vertices: RDD[(VertexId, VD)], edges: EdgeRDD[_], defaultVal: VD): VertexRDD[VD]
    功能: 由顶点编号-> 顶点属性构建顶点RDD,结果的RDD可以与边RDD进行join
    val= VertexRDD(vertices, edges, defaultVal, (a, b) => a)
    
    def apply[VD: ClassTag](
      vertices: RDD[(VertexId, VD)], edges: EdgeRDD[_], 
        defaultVal: VD, mergeFunc: (VD, VD) => VD
    ): VertexRDD[VD]
    功能: 同上,不仅可以与边RDD进行join,而且当边缺失时可以使用默认值表示边的属性
    val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
      case Some(p) => vertices
      case None => vertices.partitionBy(new HashPartitioner(vertices.partitions.length))
    }
    val routingTables = createRoutingTables(edges, vPartitioned.partitioner.get)
    val vertexPartitions = vPartitioned.zipPartitions(routingTables, preservesPartitioning = true) {
      (vertexIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        Iterator(ShippableVertexPartition(vertexIter, routingTable, defaultVal, mergeFunc))
    }
    new VertexRDDImpl(vertexPartitions)
    
    def fromEdges[VD: ClassTag](
      edges: EdgeRDD[_], numPartitions: Int, defaultVal: VD): VertexRDD[VD] 
    功能: 由边RDD构建顶点RDD,可以使用默认值代替丢失顶点的默认属性值,可以与边RDD join
    val routingTables = createRoutingTables(edges, new HashPartitioner(numPartitions))
    val vertexPartitions = routingTables.mapPartitions({ routingTableIter =>
      val routingTable =
        if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
      Iterator(ShippableVertexPartition(Iterator.empty, routingTable, defaultVal))
    }, preservesPartitioning = true)
    new VertexRDDImpl(vertexPartitions)
    
    def createRoutingTables(
      edges: EdgeRDD[_], vertexPartitioner: Partitioner): RDD[RoutingTablePartition]
    功能: 创建路由表
    val vid2pid = edges.partitionsRDD.mapPartitions(_.flatMap(
      Function.tupled(RoutingTablePartition.edgePartitionToMsgs)))
      .setName("VertexRDD.createRoutingTables - vid2pid (aggregation)")
    val numEdgePartitions = edges.partitions.length
    val= vid2pid.partitionBy(vertexPartitioner).mapPartitions(
      iter => Iterator(RoutingTablePartition.fromMsgs(numEdgePartitions, iter)),
      preservesPartitioning = true)
}
```

