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

