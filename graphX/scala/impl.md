1. [EdgePartition.scala](# EdgePartition)
2. [EdgePartitionBuilder.scala](# EdgePartitionBuilder)
3. [EdgeRDDImpl.scala](# EdgeRDDImpl)
4. [GraphImpl.scala](# GraphImpl)
5. [ReplicatedVertexView.scala](# ReplicatedVertexView)
6. [RoutingTablePartition.scala](# RoutingTablePartition)
7. [ShippableVertexPartition.scala](# ShippableVertexPartition)
8. [VertexPartition.scala](# VertexPartition)
9. [VertexPartitionBase.scala](# VertexPartitionBase)
10. [VertexPartitionBaseOps.scala](# VertexPartitionBaseOps)
11. [VertexRDDImpl.scala](# VertexRDDImpl)

---

#### EdgePartition

```markdown
介绍: 
    边的集合,引用顶点属性和激活的顶点集,用于在边上的过滤计算.
    这个边存储以列的格式,比如本地起始点`localSrcIds`,本地目标点`localDstIds`,和数据`data`.所有引用的全局顶点编号会被映射包本地顶点集中.通过的是@global2lock 的映射.每个本地顶点编号在`vertexAttrs`中是失效索引,这个属性存储了相应的顶点属性,且本地变量映射全局变量@local2global,存储了到全局变量的映射.激活的全局变量存储在@activeSet中.
    边存储在源顶点编号中,全局变量到相应边的映射存储在@index中.
    构造器参数:
    VD	顶点属性
    ED	边属性
    localSrcIds	本地起始顶点列表,可以使用@local2global映射到全局参数
    localDstIds	本地目标顶点列表,可以使用@local2global映射到全局参数
    data	每个边的属性值
    index	起始顶点编号的集群索引编号,作为全局起始顶点到边偏移量的映射
    global2local	全局顶点编号映射到本地顶点编号的映射关系
    local2global	本地顶点编号映射到全局顶点编号的映射
    vertexAttrs	顶点属性列表
    activeSet	激活顶点集,用于过滤边的计算
```

```scala
private[graphx]
class EdgePartition[
    @specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassTag, VD: ClassTag](
    localSrcIds: Array[Int],
    localDstIds: Array[Int],
    data: Array[ED],
    index: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    local2global: Array[VertexId],
    vertexAttrs: Array[VD],
    activeSet: Option[VertexSet])
extends Serializable {
    构造器:
    def this() = this(null, null, null, null, null, null, null, null)
    功能: 无参数构造器
    
    属性:
    #name @size: Int = localSrcIds.length	分区中边的数量
    
    操作集:
    def withData[ED2: ClassTag](data: Array[ED2]): EdgePartition[ED2, VD]
    功能: 获取指定边数据@data的边分区
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs, activeSet)
    
    def withActiveSet(iter: Iterator[VertexId]): EdgePartition[ED, VD]
    功能: 使用指定的激活集合的边分区
    1. 获取激活集合
    val activeSet = new VertexSet
    2. 将迭代器@iter中的内容添加到激活集合中
    while (iter.hasNext) { activeSet.add(iter.next()) }
    3. 返回激活集合的边分区
    val= new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs,
      Some(activeSet))
    
    def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[ED, VD]
    功能: 更新顶点属性,并返回相应的边分区
    1. 更新顶点属性
    val newVertexAttrs = new Array[VD](vertexAttrs.length)
    System.arraycopy(vertexAttrs, 0, newVertexAttrs, 0, vertexAttrs.length)
    while (iter.hasNext) {
      val kv = iter.next()
      newVertexAttrs(global2local(kv._1)) = kv._2
    }
    2. 获取边分区
    val= new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, newVertexAttrs,
      activeSet)
    
    def withoutVertexAttributes[VD2: ClassTag](): EdgePartition[ED, VD2]
    功能: 不使用本地缓存顶点属性更新
    1. 获取顶点属性表(空的,不使用本地缓存的顶点属性)
    val newVertexAttrs = new Array[VD2](vertexAttrs.length)
    val= new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, newVertexAttrs,
      activeSet)
    
    def srcIds(pos: Int): VertexId = local2global(localSrcIds(pos))
    功能: 获取全局起始顶点编号
    
    def dstIds(pos: Int): VertexId = local2global(localDstIds(pos))
    功能: 获取全局目标顶点编号
    
    def attrs(pos: Int): ED = data(pos)
    功能: 获取指定位置的边属性值
    
    def isActive(vid: VertexId): Boolean
    功能: 确定当前顶点是否在激活顶点表中
    val= activeSet.get.contains(vid)
    
    def numActives: Option[Int] = activeSet.map(_.size)
    功能: 获取激活的顶点数量
    
    def reverse: EdgePartition[ED, VD]
    功能: 反转分区中所有边的方向
    val builder = new ExistingEdgePartitionBuilder[ED, VD](
      global2local, local2global, vertexAttrs, activeSet, size)
    var i = 0
    while (i < size) {
      val localSrcId = localSrcIds(i)
      val localDstId = localDstIds(i)
      val srcId = local2global(localSrcId)
      val dstId = local2global(localDstId)
      val attr = data(i)
      builder.add(dstId, srcId, localDstId, localSrcId, attr)
      i += 1
    }
    val= builder.toEdgePartition
    
    def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[ED2, VD] 
    功能: 使用边属性映射出新的边分区
    注意: 输入迭代器需要返回边分区属性,按照边的属性(由@EdgePartition.iterator返回,且返回的边分区等于之前边的数量)
    输入参数:
    iter	新属性值的迭代器
    ED2	新属性的类型
    返回: 使用替换的属性值的新边分区
    val newData = new Array[ED2](data.length)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }
    assert(newData.length == i)
    this.withData(newData)
    
    def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgePartition[ED, VD]
    功能: 活力函数
    输入参数:
    epred	边过滤函数
    vpred	顶点过滤函数
    val builder = new ExistingEdgePartitionBuilder[ED, VD](
      global2local, local2global, vertexAttrs, activeSet)
    var i = 0
    while (i < size) {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      val localSrcId = localSrcIds(i)
      val localDstId = localDstIds(i)
      val et = new EdgeTriplet[VD, ED]
      et.srcId = local2global(localSrcId)
      et.dstId = local2global(localDstId)
      et.srcAttr = vertexAttrs(localSrcId)
      et.dstAttr = vertexAttrs(localDstId)
      et.attr = data(i)
      if (vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et)) {
        builder.add(et.srcId, et.dstId, localSrcId, localDstId, et.attr)
      }
      i += 1
    }
    val= builder.toEdgePartition
    
    def foreach(f: Edge[ED] => Unit): Unit
    功能: 迭代处理函数
    iterator.foreach(f)
    
    def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED, VD]
    功能: 合并所有起始顶点和目标顶点相同的边,使用合并逻辑@merge对边进行合并
    val builder = new ExistingEdgePartitionBuilder[ED, VD](
      global2local, local2global, vertexAttrs, activeSet)
    var currSrcId: VertexId = null.asInstanceOf[VertexId]
    var currDstId: VertexId = null.asInstanceOf[VertexId]
    var currLocalSrcId = -1
    var currLocalDstId = -1
    var currAttr: ED = null.asInstanceOf[ED]
    var i = 0
    while (i < size) {
      if (i > 0 && currSrcId == srcIds(i) && currDstId == dstIds(i)) {
        currAttr = merge(currAttr, data(i))
      } else {
        if (i > 0) {
          builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
        }
        currSrcId = srcIds(i)
        currDstId = dstIds(i)
        currLocalSrcId = localSrcIds(i)
        currLocalDstId = localDstIds(i)
        currAttr = data(i)
      }
      i += 1
    }
    if (size > 0) {
      builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
    }
    val= builder.toEdgePartition
    
    def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgePartition[ED2, _])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgePartition[ED3, VD] 
    功能: 与指定边分区@other 进行内连接,其中函数@f 是将两个边分区进行合并的函数,应用在每条边上
    val builder = new ExistingEdgePartitionBuilder[ED3, VD](
      global2local, local2global, vertexAttrs, activeSet)
    var i = 0
    var j = 0
    while (i < size && j < other.size) {
      val srcId = this.srcIds(i)
      val dstId = this.dstIds(i)
      while (j < other.size && other.srcIds(j) < srcId) { j += 1 }
      if (j < other.size && other.srcIds(j) == srcId) {
        while (j < other.size && other.srcIds(j) == srcId && other.dstIds(j) < dstId) { j += 1 }
        if (j < other.size && other.srcIds(j) == srcId && other.dstIds(j) == dstId) {
          // ... run `f` on the matching edge
          builder.add(srcId, dstId, localSrcIds(i), localDstIds(i),
            f(srcId, dstId, this.data(i), other.attrs(j)))
        }
      }
      i += 1
    }
    val= builder.toEdgePartition
    
    def indexSize: Int = index.size
    功能: 获取分区唯一起始顶点的数量
    
    def iterator: Iterator[Edge[ED]]
    功能: 获取当前分区边的迭代器
    val= new Iterator[Edge[ED]] {
        private[this] val edge = new Edge[ED]
        private[this] var pos = 0
        override def hasNext: Boolean = pos < EdgePartition.this.size
        override def next(): Edge[ED] = {
            edge.srcId = srcIds(pos)
            edge.dstId = dstIds(pos)
            edge.attr = data(pos)
            pos += 1
            edge
        }
    }
    
    def tripletIterator(
      includeSrc: Boolean = true, includeDst: Boolean = true)
      : Iterator[EdgeTriplet[VD, ED]]
    功能: 获取当前分区三元组分区的迭代器
    val= new Iterator[EdgeTriplet[VD, ED]] {
        private[this] var pos = 0
        override def hasNext: Boolean = pos < EdgePartition.this.size
        override def next(): EdgeTriplet[VD, ED] = {
          val triplet = new EdgeTriplet[VD, ED]
          val localSrcId = localSrcIds(pos)
          val localDstId = localDstIds(pos)
          triplet.srcId = local2global(localSrcId)
          triplet.dstId = local2global(localDstId)
          if (includeSrc) {
            triplet.srcAttr = vertexAttrs(localSrcId)
          }
          if (includeDst) {
            triplet.dstAttr = vertexAttrs(localDstId)
          }
          triplet.attr = data(pos)
          pos += 1
          triplet
        }
    }
    
    def aggregateMessagesEdgeScan[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeness: EdgeActiveness): Iterator[(VertexId, A)] 
    功能: 沿着边发送消息,并在接受顶点接受消息,通过循环地扫描所有边处理
    输入函数:
        sendMsg	消息发送函数
        mergeMsg	消息合并函数
        tripletFields	发送消息时使用的三元组消息
        activeness	基于激活属性的边过滤验证
    1. 设置存储结果的数据结构
    val aggregates = new Array[A](vertexAttrs.length)
    val bitset = new BitSet(vertexAttrs.length)
    val ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    2. 扫描所有的边，返送消息给对端顶点，并合并消息
    var i = 0
    while (i < size) {
      val localSrcId = localSrcIds(i)
      val srcId = local2global(localSrcId)
      val localDstId = localDstIds(i)
      val dstId = local2global(localDstId)
      val edgeIsActive =
        if (activeness == EdgeActiveness.Neither) true
        else if (activeness == EdgeActiveness.SrcOnly) isActive(srcId)
        else if (activeness == EdgeActiveness.DstOnly) isActive(dstId)
        else if (activeness == EdgeActiveness.Both) isActive(srcId) && isActive(dstId)
        else if (activeness == EdgeActiveness.Either) isActive(srcId) || isActive(dstId)
        else throw new Exception("unreachable")
      if (edgeIsActive) {
        val srcAttr = if (tripletFields.useSrc) vertexAttrs(localSrcId) else null.asInstanceOf[VD]
        val dstAttr = if (tripletFields.useDst) vertexAttrs(localDstId) else null.asInstanceOf[VD]
        ctx.set(srcId, dstId, localSrcId, localDstId, srcAttr, dstAttr, data(i))
        sendMsg(ctx)
      }
      i += 1
    }
    val= bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
    
    def aggregateMessagesIndexScan[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeness: EdgeActiveness): Iterator[(VertexId, A)]
    功能: 发送消息给边的对端,通过过滤起始顶点编号,然后扫描边集实现
    1. 设置结果的数据结构
    val aggregates = new Array[A](vertexAttrs.length)
    val bitset = new BitSet(vertexAttrs.length)
    val ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    2. 循环扫描边集
    index.iterator.foreach { cluster =>
      val clusterSrcId = cluster._1
      val clusterPos = cluster._2
      val clusterLocalSrcId = localSrcIds(clusterPos)
      val scanCluster =
        if (activeness == EdgeActiveness.Neither) true
        else if (activeness == EdgeActiveness.SrcOnly) isActive(clusterSrcId)
        else if (activeness == EdgeActiveness.DstOnly) true
        else if (activeness == EdgeActiveness.Both) isActive(clusterSrcId)
        else if (activeness == EdgeActiveness.Either) true
        else throw new Exception("unreachable")
      if (scanCluster) {
        var pos = clusterPos
        val srcAttr =
          if (tripletFields.useSrc) vertexAttrs(clusterLocalSrcId) else null.asInstanceOf[VD]
        ctx.setSrcOnly(clusterSrcId, clusterLocalSrcId, srcAttr)
        while (pos < size && localSrcIds(pos) == clusterLocalSrcId) {
          val localDstId = localDstIds(pos)
          val dstId = local2global(localDstId)
          val edgeIsActive =
            if (activeness == EdgeActiveness.Neither) true
            else if (activeness == EdgeActiveness.SrcOnly) true
            else if (activeness == EdgeActiveness.DstOnly) isActive(dstId)
            else if (activeness == EdgeActiveness.Both) isActive(dstId)
            else if (activeness == EdgeActiveness.Either) isActive(clusterSrcId) || isActive(dstId)
            else throw new Exception("unreachable")
          if (edgeIsActive) {
            val dstAttr =
              if (tripletFields.useDst) vertexAttrs(localDstId) else null.asInstanceOf[VD]
            ctx.setRest(dstId, localDstId, dstAttr, data(pos))
            sendMsg(ctx)
          }
          pos += 1
        }
      }
    }
    val= bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
}
```

```scala
private class AggregatingEdgeContext[VD, ED, A](
    mergeMsg: (A, A) => A,
    aggregates: Array[A],
    bitset: BitSet)
extends EdgeContext[VD, ED, A] {
    介绍: 合并边上下文
    构造器参数:
    mergeMsg	消息合并函数
    aggregates	聚合数组
    bitset	位集合
    属性:
    #name @_srcId: VertexId = _	起始顶点编号
    #name @_dstId: VertexId = _	目标顶点编号
    #name @_localSrcId: Int = _	本地起始顶点编号
    #name @_localDstId: Int = _	本地终点编号
    #name @_srcAttr: VD = _	起始顶点属性
    #name @_dstAttr: VD = _	目标顶点属性
    #name @_attr: ED = _	边属性
    操作集:
    def set(
      srcId: VertexId, dstId: VertexId,
      localSrcId: Int, localDstId: Int,
      srcAttr: VD, dstAttr: VD,
      attr: ED): Unit
    功能: 参数设置
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
    
    def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD): Unit
    功能: 只设置起始顶点信息
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
    
    def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED): Unit
    功能: 设置其他信息(目标顶点,边)
    _dstId = dstId
    _localDstId = localDstId
    _dstAttr = dstAttr
    _attr = attr
    
    override def srcId: VertexId = _srcId
    override def dstId: VertexId = _dstId
    override def srcAttr: VD = _srcAttr
    override def dstAttr: VD = _dstAttr
    override def attr: ED = _attr
    功能:获取起始顶点编号/目标顶点编号/起始顶点属性/目标顶点属性/边属性
    
    def sendToSrc(msg: A): Unit
    功能: 发送消息到起始顶点
    send(_localSrcId, msg)
    
    def sendToDst(msg: A): Unit
    功能: 发送消息到目标顶点
    send(_localDstId, msg)
    
    def send(localId: Int, msg: A): Unit 
    功能: 发送消息到指定顶点
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
}
```

#### EdgePartitionBuilder

```scala
private[graphx]
class EdgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    size: Int = 64) {
    介绍: 边分区构建器
    构造器参数:
    ED	边类型
    VD	顶点类型
    属性:
    #name @edges = new PrimitiveVector[Edge[ED]](size)	边集合
    操作集:
    def add(src: VertexId, dst: VertexId, d: ED): Unit
    功能: 添加新的边到分区中
    edges += Edge(src, dst, d)
    
    def toEdgePartition: EdgePartition[ED, VD]
    功能: 转化为边分区的形式
    val edgeArray = edges.trim().array
    new Sorter(Edge.edgeArraySortDataFormat[ED])
      .sort(edgeArray, 0, edgeArray.length, Edge.lexicographicOrdering)
    val localSrcIds = new Array[Int](edgeArray.length)
    val localDstIds = new Array[Int](edgeArray.length)
    val data = new Array[ED](edgeArray.length)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val global2local = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    val local2global = new PrimitiveVector[VertexId]
    var vertexAttrs = Array.empty[VD]
    if (edgeArray.length > 0) {
      index.update(edgeArray(0).srcId, 0)
      var currSrcId: VertexId = edgeArray(0).srcId
      var currLocalId = -1
      var i = 0
      while (i < edgeArray.length) {
        val srcId = edgeArray(i).srcId
        val dstId = edgeArray(i).dstId
        localSrcIds(i) = global2local.changeValue(srcId,
          { currLocalId += 1; local2global += srcId; currLocalId }, identity)
        localDstIds(i) = global2local.changeValue(dstId,
          { currLocalId += 1; local2global += dstId; currLocalId }, identity)
        data(i) = edgeArray(i).attr
        if (srcId != currSrcId) {
          currSrcId = srcId
          index.update(currSrcId, i)
        }

        i += 1
      }
      vertexAttrs = new Array[VD](currLocalId + 1)
    }
    val= new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global.trim().array, vertexAttrs,
      None)
}
```

```scala
private[impl]
class ExistingEdgePartitionBuilder[
    @specialized(Long, Int, Double) ED: ClassTag, VD: ClassTag](
    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],
    local2global: Array[VertexId],
    vertexAttrs: Array[VD],
    activeSet: Option[VertexSet],
    size: Int = 64) {
    介绍: 存在的边分区的构建器,允许本地顶点列表.只允许内部使用
    #name @edges = new PrimitiveVector[EdgeWithLocalIds[ED]](size)	边集
    操作集:
    def add(src: VertexId, dst: VertexId, localSrc: Int, localDst: Int, d: ED): Unit
    功能: 添加边到分区中
    edges += EdgeWithLocalIds(src, dst, localSrc, localDst, d)
    
    def toEdgePartition: EdgePartition[ED, VD]
    功能: 转换为边分区
    val edgeArray = edges.trim().array
    new Sorter(EdgeWithLocalIds.edgeArraySortDataFormat[ED])
      .sort(edgeArray, 0, edgeArray.length, EdgeWithLocalIds.lexicographicOrdering)
    val localSrcIds = new Array[Int](edgeArray.length)
    val localDstIds = new Array[Int](edgeArray.length)
    val data = new Array[ED](edgeArray.length)
    val index = new GraphXPrimitiveKeyOpenHashMap[VertexId, Int]
    if (edgeArray.length > 0) {
      index.update(edgeArray(0).srcId, 0)
      var currSrcId: VertexId = edgeArray(0).srcId
      var i = 0
      while (i < edgeArray.length) {
        localSrcIds(i) = edgeArray(i).localSrcId
        localDstIds(i) = edgeArray(i).localDstId
        data(i) = edgeArray(i).attr
        if (edgeArray(i).srcId != currSrcId) {
          currSrcId = edgeArray(i).srcId
          index.update(currSrcId, i)
        }
        i += 1
      }
    }
    val=  new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs, activeSet)
}
```

```scala
private[impl] case class EdgeWithLocalIds[@specialized ED](
    srcId: VertexId, dstId: VertexId, localSrcId: Int, localDstId: Int, attr: ED)
介绍: 带有本地顶点列表的边
构造器参数:
srcId	起始顶点编号
dstId	目标顶点编号
localSrcId	本地起始顶点
localDstId	本地目标顶点
attr	边属性
```

```scala
private[impl] object EdgeWithLocalIds {
    介绍: 带有本地顶点编号的边
    implicit def lexicographicOrdering[ED]: Ordering[EdgeWithLocalIds[ED]]
    功能: lexico图排序策略(以目标顶点编号为第一关键字,起始顶点编号为第二关键字)
    val= (a: EdgeWithLocalIds[ED], b: EdgeWithLocalIds[ED]) =>
      if (a.srcId == b.srcId) {
        if (a.dstId == b.dstId) 0
        else if (a.dstId < b.dstId) -1
        else 1
      }
      else if (a.srcId < b.srcId) -1
      else 1
    
    def edgeArraySortDataFormat[ED]
    功能: 边数组排序方式
    val= new SortDataFormat[EdgeWithLocalIds[ED], Array[EdgeWithLocalIds[ED]]] {
        // 获取指定位置的边
      override def getKey(data: Array[EdgeWithLocalIds[ED]], pos: Int): EdgeWithLocalIds[ED] = {
        data(pos)
      }
        // 交换边
      override def swap(data: Array[EdgeWithLocalIds[ED]], pos0: Int, pos1: Int): Unit = {
        val tmp = data(pos0)
        data(pos0) = data(pos1)
        data(pos1) = tmp
      }
	// 边数据拷贝,src->dst
      override def copyElement(
          src: Array[EdgeWithLocalIds[ED]], srcPos: Int,
          dst: Array[EdgeWithLocalIds[ED]], dstPos: Int): Unit = {
        dst(dstPos) = src(srcPos)
      }
	// 边范围拷贝
      override def copyRange(
          src: Array[EdgeWithLocalIds[ED]], srcPos: Int,
          dst: Array[EdgeWithLocalIds[ED]], dstPos: Int, length: Int): Unit = {
        System.arraycopy(src, srcPos, dst, dstPos, length)
      }
	// 分配指定长度的内存区域给边存储
      override def allocate(length: Int): Array[EdgeWithLocalIds[ED]] = {
        new Array[EdgeWithLocalIds[ED]](length)
      }
    }
}
```

#### EdgeRDDImpl

```scala
class EdgeRDDImpl[ED: ClassTag, VD: ClassTag] private[graphx] (
    @transient override val partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
extends EdgeRDD[ED](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {
    介绍: 边RDD的实现
    构造器参数:
        partitionsRDD	边分区RDD的映射RDD
        targetStorageLevel	存储等级(默认存储到内存)
    属性:
    #name @partitioner	边RDD分区器
    val= partitionsRDD.partitioner.orElse(Some(new HashPartitioner(partitions.length)))
    
    初始化操作:
    setName("EdgeRDD")
    功能: 设置RDD名称
    操作集:
    def setName(_name: String): this.type 
    功能: 设置边RDD的名称
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    val= this
    
    def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()
    功能: 返回RDD中所有边组成的列表
    
    def persist(newLevel: StorageLevel): this.type
    功能: 持久化边分区,使用指定的存储等级
    partitionsRDD.persist(newLevel)
    val= this
    
    def unpersist(blocking: Boolean = false): this.type
    功能: 解除持久化,可以选择是否阻塞执行(同步),默认异步执行
    partitionsRDD.unpersist(blocking)
    val= this
    
    def cache(): this.type 
    功能: 使用目标存储等级缓存数据
    partitionsRDD.persist(targetStorageLevel)
    val= this
    
    def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel
    功能: 获取存储等级
    
    def checkpoint(): Unit
    功能: 设置检查点
    partitionsRDD.checkpoint()
    
    def isCheckpointed: Boolean
    功能: 检查是否设置了检查点
    val= firstParent[(PartitionID, EdgePartition[ED, VD])].isCheckpointed
    
    def getCheckpointFile: Option[String]
    功能: 获取检查点文件
    val= partitionsRDD.getCheckpointFile
    
    def count(): Long
    功能: 计算RDD中的边数量
    partitionsRDD.map(_._2.size.toLong).fold(0)(_ + _)
    
    def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDDImpl[ED2, VD]
    功能: 使用映射函数@f对value进行映射,边类型ED->ED2
    val= mapEdgePartitions((pid, part) => part.map(f))
    
    def reverse: EdgeRDDImpl[ED, VD] = mapEdgePartitions((pid, part) => part.reverse)
    功能: 图中的边方向反转
    
    def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgeRDDImpl[ED, VD]
    功能: 使用过滤函数过滤顶点和边
    val= mapEdgePartitions((pid, part) => part.filter(epred, vpred))
    
    def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgeRDD[ED2])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDDImpl[ED3, VD]
    功能: 与指定边RDD@other 进行内连接,使用函数@f对聚合的值进行合并
    val ed2Tag = classTag[ED2]
    val ed3Tag = classTag[ED3]
    val= this.withPartitionsRDD[ED3, VD](partitionsRDD.zipPartitions(other.partitionsRDD, true) {
      (thisIter, otherIter) =>
        val (pid, thisEPart) = thisIter.next()
        val (_, otherEPart) = otherIter.next()
        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)(ed2Tag, ed3Tag)))
    })
    
    def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag](
      f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): EdgeRDDImpl[ED2, VD2]
    功能: 边分区映射,可以选择是否保持原来的分区,其中映射函数为@f,用于将原先的边分区映射RDD转换成一个新的边RDD
    val= this.withPartitionsRDD[ED2, VD2](partitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {// 迭代处理
        val (pid, ep) = iter.next()
          // 对边人去@ep进行映射@f(pid,ep)与原理的pid组成元组,形成新的边RDD,也就是保持pid不变,映射边RDD
        Iterator(Tuple2(pid, f(pid, ep)))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true)) // 保持原分区,这样不用shuffle,省时间
    
    def withPartitionsRDD[ED2: ClassTag, VD2: ClassTag](
      partitionsRDD: RDD[(PartitionID, EdgePartition[ED2, VD2])]): EdgeRDDImpl[ED2, VD2]
    功能: 形成以指定RDD@partitionsRDD 组成的新的边RDD
    val= new EdgeRDDImpl(partitionsRDD, this.targetStorageLevel)
    
    def withTargetStorageLevel(
      targetStorageLevel: StorageLevel): EdgeRDDImpl[ED, VD] 
    功能: 转换为目标存储等级
    val= new EdgeRDDImpl(this.partitionsRDD, targetStorageLevel)
}
```

#### GraphImpl

```scala
class GraphImpl[VD: ClassTag, ED: ClassTag] protected (
    @transient val vertices: VertexRDD[VD],
    @transient val replicatedVertexView: ReplicatedVertexView[VD, ED])
extends Graph[VD, ED] with Serializable {
    介绍: @Graph 的实现,用于支持图的计算.使用两个RDD表示.
    1. 顶点:
    包含顶点属性,和路径信息,用于将顶点属性传递到边上.
    2. 副本顶点视图:
    包含边和每个边的顶点
    构造器参数:
    vertices	顶点RDD
    replicatedVertexView	副本顶点视图
    构造器:
    def this() = this(null, null)
    功能: 空构造器
    属性:
    #name @edges: EdgeRDDImpl[ED, VD] = replicatedVertexView.edges	副本顶点视图的边集
    #name @triplets: RDD[EdgeTriplet[VD, ED]] transient lazy	边三元组RDD
    val= {
        replicatedVertexView.upgrade(vertices, true, true)
        replicatedVertexView.edges.partitionsRDD.mapPartitions(_.flatMap {
            case (pid, part) => part.tripletIterator()
        })
    }
    操作集:
    def persist(newLevel: StorageLevel): Graph[VD, ED]
    功能: 使用指定存储等级持久化
    vertices.persist(newLevel)
    replicatedVertexView.edges.persist(newLevel)
    val= this
    
    def cache(): Graph[VD, ED]
    功能: 缓存图数据
    vertices.cache()
    replicatedVertexView.edges.cache()
    val= this
    
    def checkpoint(): Unit
    功能: 检查点设置
    vertices.checkpoint()
    replicatedVertexView.edges.checkpoint()
    
    def isCheckpointed: Boolean
    功能: 确定是否设置了检查点
    val= vertices.isCheckpointed && replicatedVertexView.edges.isCheckpointed
    
    def getCheckpointFiles: Seq[String]
    功能: 获取检查点文件
    val= Seq(vertices.getCheckpointFile, replicatedVertexView.edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None => Seq.empty
    }
    
    def unpersist(blocking: Boolean = false): Graph[VD, ED]
    功能: 解除持久化,可以设置是否阻塞执行
    unpersistVertices(blocking)
    replicatedVertexView.edges.unpersist(blocking)
    val= this
    
    def unpersistVertices(blocking: Boolean = false): Graph[VD, ED]
    功能: 持久化顶点数据
    vertices.unpersist(blocking)
    val= this
    
    def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED]
    功能: 按照指定的分区策略进行分区
    val= partitionBy(partitionStrategy, edges.partitions.length)
    
    def partitionBy(
      partitionStrategy: PartitionStrategy, numPartitions: Int): Graph[VD, ED]
    功能: 同上,并指定了分区数量
    val edTag = classTag[ED]
    val vdTag = classTag[VD]
    val newEdges = edges.withPartitionsRDD(edges.map { e =>
      val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)
      (part, (e.srcId, e.dstId, e.attr))
    }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex(
        { (pid: Int, iter: Iterator[(PartitionID, (VertexId, VertexId, ED))]) =>
          val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
          iter.foreach { message =>
            val data = message._2
            builder.add(data._1, data._2, data._3)
          }
          val edgePartition = builder.toEdgePartition
          Iterator((pid, edgePartition))
        }, preservesPartitioning = true)).cache()
    val= GraphImpl.fromExistingRDDs(vertices.withEdges(newEdges), newEdges)
    
    def reverse: Graph[VD, ED] 
    功能: 图中边的方向反转
    val= new GraphImpl(vertices.reverseRoutingTables(), replicatedVertexView.reverse())
    
    def mapVertices[VD2: ClassTag]
    (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] 
    功能: 对顶点使用顶点映射@f,获取映射后的图
    val= if (eq != null) {
      vertices.cache()
      val newVerts = vertices.mapVertexPartitions(_.map(f)).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      GraphImpl(vertices.mapVertexPartitions(_.map(f)), replicatedVertexView.edges)
    }
    
    def mapEdges[ED2: ClassTag](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2]
    功能: 对边进行映射@f,获取映射之后的图
    val newEdges = replicatedVertexView.edges
      .mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    val= new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
    
    def subgraph(
        epred: EdgeTriplet[VD, ED] => Boolean = x => true,
        vpred: (VertexId, VD) => Boolean = (a, b) => true): Graph[VD, ED] 
    功能: 获取过滤后的子图
    vertices.cache()
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))
    replicatedVertexView.upgrade(vertices, true, true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    val= new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
    
    def mask[VD2: ClassTag, ED2: ClassTag] (
      other: Graph[VD2, ED2]): Graph[VD, ED]
    功能: 与指定图@other 进行内连接
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = replicatedVertexView.edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    val= new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
    
    def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED]
    功能: 对分区内的数据进行合并@merge,返回计算之后的图
    val newEdges = replicatedVertexView.edges.mapEdgePartitions(
      (pid, part) => part.groupEdges(merge))
    val= new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
    
    ---
    底层转换方法:
    def aggregateMessagesWithActiveSet[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]): VertexRDD[A]
    功能: 使用激活的集合合并消息
    输入参数:
    sendMsg	消息发送函数
    mergeMsg	消息合并函数
    tripletFields	发送的三元组信息
    activeSetOpt	激活顶点信息
    1. 计算之前,将顶点信息持久化到内存中,作为容错措施
    vertices.cache()
    2. 对于每个顶点来说,复制顶点信息只到边相关的位置
    replicatedVertexView.upgrade(vertices, tripletFields.useSrc, tripletFields.useDst)
    val view = activeSetOpt match {
      case Some((activeSet, _)) =>
        replicatedVertexView.withActiveSet(activeSet)
      case None =>
        replicatedVertexView
    }
    val activeDirectionOpt = activeSetOpt.map(_._2)
    3. 映射和合并
    val preAgg = view.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, edgePartition) =>
        // 选择扫描策略
        val activeFraction = edgePartition.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
        activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.Both)
            }
          case Some(EdgeDirection.Either) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Either)
          case Some(EdgeDirection.Out) =>
            if (activeFraction < 0.8) {
              edgePartition.aggregateMessagesIndexScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            } else {
              edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
                EdgeActiveness.SrcOnly)
            }
          case Some(EdgeDirection.In) =>
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.DstOnly)
          case _ => // None
            edgePartition.aggregateMessagesEdgeScan(sendMsg, mergeMsg, tripletFields,
              EdgeActiveness.Neither)
        }
    }).setName("GraphImpl.aggregateMessages - preAgg")
    4. 重用索引表,进行最终的聚合
    vertices.aggregateUsingIndex(preAgg, mergeMsg)
    
    def outerJoinVertices[U: ClassTag, VD2: ClassTag]
      (other: RDD[(VertexId, U)])
      (updateF: (VertexId, VD, Option[U]) => VD2)
      (implicit eq: VD =:= VD2 = null): Graph[VD2, ED] 
    功能: 与指定RDD@other 进行外链接,并返回使用函数@updateF 映射之后的图
    val= if (eq != null) {
      vertices.cache()
      val newVerts = vertices.leftJoin(other)(updateF).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      val newVerts = vertices.leftJoin(other)(updateF)
      GraphImpl(newVerts, replicatedVertexView.edges)
    }
    
    def accessesVertexAttr(closure: AnyRef, attrName: String): Boolean =
    功能: 测试时候含有@attrName 的闭包名称
    val= try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
}
```

```scala
object GraphImpl {
    def apply[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED]
    功能: 有给定的边RDD创建图,设置顶点引用为@defaultVertexAttr
    val= fromEdgeRDD(
        EdgeRDD.fromEdges(edges), defaultVertexAttr, edgeStorageLevel, vertexStorageLevel)
    
    def fromEdgePartitions[VD: ClassTag, ED: ClassTag](
      edgePartitions: RDD[(PartitionID, EdgePartition[ED, VD])],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] 
    功能: 由边分区创建图,设置顶点引用为@defaultVertexAttr
    val= fromEdgeRDD(EdgeRDD.fromEdgePartitions(edgePartitions), defaultVertexAttr, edgeStorageLevel,
      vertexStorageLevel)
    
    def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED] 
    功能: 由顶点和边创建图,设置丢失顶点引用为@defaultVertexAttr
    1. 构建边RDD
    val edgeRDD = EdgeRDD.fromEdges(edges)(classTag[ED], classTag[VD])
      .withTargetStorageLevel(edgeStorageLevel)
    2. 构建顶点RDD
    val vertexRDD = VertexRDD(vertices, edgeRDD, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel)
    val= GraphImpl(vertexRDD, edgeRDD)
    
    def apply[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): GraphImpl[VD, ED] 
    功能: 由顶点RDD和边RDD构建图
    vertices.cache()
    val newEdges = edges.asInstanceOf[EdgeRDDImpl[ED, _]]
      .mapEdgePartitions((pid, part) => part.withoutVertexAttributes[VD])
      .cache()
    val= GraphImpl.fromExistingRDDs(vertices, newEdges)
    
    def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED]): GraphImpl[VD, ED]
    功能: 由存在的RDD创建图
    val= new GraphImpl(vertices, new ReplicatedVertexView(edges.asInstanceOf[EdgeRDDImpl[ED, VD]]))

    def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
      edges: EdgeRDDImpl[ED, VD],
      defaultVertexAttr: VD,
      edgeStorageLevel: StorageLevel,
      vertexStorageLevel: StorageLevel): GraphImpl[VD, ED]
    功能: 由边RDD创建图,使用正确的顶点格式,设置丢失的顶点引用为@defaultVertexAttr
    val edgesCached = edges.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices =
      VertexRDD.fromEdges(edgesCached, edgesCached.partitions.length, defaultVertexAttr)
      .withTargetStorageLevel(vertexStorageLevel)
    val= fromExistingRDDs(vertices, edgesCached)
}
```

#### ReplicatedVertexView

```markdown
介绍:
    管理传递顶点参数给@EdgeRDD的边分区.顶点属性可能部分传递,用于构建三元组,使用一侧的顶点参数,且它们可以更新.一个激活的顶点集合可以添加顶点到边分区中.注意不要存储边的引用,因为在参数传递等级改变的时候,这个参数可能发生改变.
```

```scala
private[impl]
class ReplicatedVertexView[VD: ClassTag, ED: ClassTag](
    var edges: EdgeRDDImpl[ED, VD],
    var hasSrcId: Boolean = false,
    var hasDstId: Boolean = false) {
    介绍: 副本顶点视图
    构造器参数:
    edges	边集合
    hasSrcId	是否拥有起始顶点编号
    hasDstId	是否用于目标顶点编号
    操作集:
    def withEdges[VD2: ClassTag, ED2: ClassTag](
      _edges: EdgeRDDImpl[ED2, VD2]): ReplicatedVertexView[VD2, ED2]
    功能: 指定边分区RDD,返回对应的@ReplicatedVertexView副本顶点视图
    val= new ReplicatedVertexView(_edges, hasSrcId, hasDstId)
    
    def reverse(): ReplicatedVertexView[VD, ED] 
    功能: 反转视图中边的方向
    val newEdges = edges.mapEdgePartitions((pid, part) => part.reverse)
    val= new ReplicatedVertexView(newEdges, hasDstId, hasSrcId)
    
    def upgrade(vertices: VertexRDD[VD], includeSrc: Boolean, includeDst: Boolean): Unit 
    功能: 通过传输顶点参数更新指定等级传输等级.这个操作修改了@ReplicatedVertexView ,调用者在获取更新后的视图之后可以获取边信息.
    1. 获取传输的起始和终点
    val shipSrc = includeSrc && !hasSrcId
    val shipDst = includeDst && !hasDstId
    2. 更新起始/终点
    if (shipSrc || shipDst) {
        // 获取传输的顶点信息
      val shippedVerts: RDD[(Int, VertexAttributeBlock[VD])] =
        vertices.shipVertexAttributes(shipSrc, shipDst)
          .setName("ReplicatedVertexView.upgrade(%s, %s) - shippedVerts %s %s (broadcast)".format(
            includeSrc, includeDst, shipSrc, shipDst))
          .partitionBy(edges.partitioner.get)
        // 确定边数
      val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
        (ePartIter, shippedVertsIter) => ePartIter.map {
          case (pid, edgePartition) =>
            (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
        }
      })
      edges = newEdges
      hasSrcId = includeSrc
      hasDstId = includeDst
    }
    
    def withActiveSet(actives: VertexRDD[_]): ReplicatedVertexView[VD, ED]
    功能: 返回一个新的@ReplicatedVertexView,其中每个分区中的@activeSet仅仅包含激活状态的顶点,这个会传输顶点编号给所有边分区(边分区被引用,但是忽视属性传输等级)
    1. 获取需要传输的激活参数(顶点编号)
    val shippedActives = actives.shipVertexIds()
      .setName("ReplicatedVertexView.withActiveSet - shippedActives (broadcast)")
      .partitionBy(edges.partitioner.get)
    2. 更新边分区,并获取返回后的边分区
    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedActives) {
      (ePartIter, shippedActivesIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.withActiveSet(shippedActivesIter.flatMap(_._2.iterator)))
      }
    })
    val= new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
    
    def updateVertices(updates: VertexRDD[VD]): ReplicatedVertexView[VD, ED] 
    功能: 更新顶点信息,并返回更新后的视图@ReplicatedVertexView
    val shippedVerts = updates.shipVertexAttributes(hasSrcId, hasDstId)
      .setName("ReplicatedVertexView.updateVertices - shippedVerts %s %s (broadcast)".format(
        hasSrcId, hasDstId))
      .partitionBy(edges.partitioner.get)
    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
      (ePartIter, shippedVertsIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
      }
    })
    val= new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
}
```

#### RoutingTablePartition

```scala
private[graphx]
object RoutingTablePartition {
    介绍: 路由表分区
    属性:
    #name @empty: RoutingTablePartition = new RoutingTablePartition(Array.empty)	空路由表
    
    type RoutingTableMessage = (VertexId, Int)
    介绍: 路由表信息,边分区的消息,由边分区指向顶点(这个顶点是边分区引用这个顶点).边分区使用30位的int进行加密,这个位置的信息使用高2位int进行加密.
    
    def toMessage(vid: VertexId, pid: PartitionID, position: Byte): RoutingTableMessage
    功能: 消息转换函数
    val positionUpper2 = position << 30
    val pidLower30 = pid & 0x3FFFFFFF
    val= (vid, positionUpper2 | pidLower30)
    
    def vidFromMessage(msg: RoutingTableMessage): VertexId = msg._1
    功能: 获取顶点编号
    
    def pidFromMessage(msg: RoutingTableMessage): PartitionID = msg._2 & 0x3FFFFFFF
    功能: 获取消息中的分区编号
    
    def positionFromMessage(msg: RoutingTableMessage): Byte = (msg._2 >> 30).toByte
    功能: 获取消息中的位置
    
    def edgePartitionToMsgs(pid: PartitionID, edgePartition: EdgePartition[_, _])
    : Iterator[RoutingTableMessage]
    功能: 根据指定分区生成消息
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, Byte]
    edgePartition.iterator.foreach { e =>
      map.changeValue(e.srcId, 0x1, (b: Byte) => (b | 0x1).toByte)
      map.changeValue(e.dstId, 0x2, (b: Byte) => (b | 0x2).toByte)
    }
    val= map.iterator.map { vidAndPosition =>
      val vid = vidAndPosition._1
      val position = vidAndPosition._2
      toMessage(vid, pid, position)
    }
    
    def fromMsgs(numEdgePartitions: Int, iter: Iterator[RoutingTableMessage])
    : RoutingTablePartition
    功能: 由@RoutingTableMessage 构建消息@RoutingTablePartition
    val pid2vid = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId])
    val srcFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    val dstFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    for (msg <- iter) {
      val vid = vidFromMessage(msg)
      val pid = pidFromMessage(msg)
      val position = positionFromMessage(msg)
      pid2vid(pid) += vid
      srcFlags(pid) += (position & 0x1) != 0
      dstFlags(pid) += (position & 0x2) != 0
    }
    val= new RoutingTablePartition(pid2vid.zipWithIndex.map {
      case (vids, pid) => (vids.trim().array, toBitSet(srcFlags(pid)), toBitSet(dstFlags(pid)))
    })
    
    def toBitSet(flags: PrimitiveVector[Boolean]): BitSet
    功能: 将给定的bool型参数列表转化为位集合@BitSet
    val bitset = new BitSet(flags.size)
    var i = 0
    while (i < flags.size) {
      if (flags(i)) {
        bitset.set(i)
      }
      i += 1
    }
    val= bitset
}
```

```scala
private[graphx]
class RoutingTablePartition(
    private val routingTable: Array[(Array[VertexId], BitSet, BitSet)]) extends Serializable {
    介绍: 存储边分区的位置,对于每个顶点属性连接其位置,对于传输的顶点属性提供路由信息.
    属性:
    #name @numEdgePartitions: Int = routingTable.length	边分区数量
    操作集:
    def partitionSize(pid: PartitionID): Int = routingTable(pid)._1.length
    功能: 获取边分区数量
    
    def iterator: Iterator[VertexId] = routingTable.iterator.flatMap(_._1.iterator)
    功能: 获取当前路由表分区@RoutingTablePartition 中的迭代器
    
    def reverse: RoutingTablePartition
    功能: 反转路由表分区的边,并返回
    val= new RoutingTablePartition(routingTable.map {
      case (vids, srcVids, dstVids) => (vids, dstVids, srcVids)
    })
    
    def foreachWithinEdgePartition
      (pid: PartitionID, includeSrc: Boolean, includeDst: Boolean)
      (f: VertexId => Unit): Unit
    功能: 对于每个边分区执行函数@f
    val (vidsCandidate, srcVids, dstVids) = routingTable(pid)
    val size = vidsCandidate.length
    if (includeSrc && includeDst) {
      vidsCandidate.iterator.foreach(f)
    } else if (!includeSrc && !includeDst) {
      // Do nothing
    } else {
      val relevantVids = if (includeSrc) srcVids else dstVids
      relevantVids.iterator.foreach { i => f(vidsCandidate(i)) }
    }
}
```

#### VertexAttributeBlock

```scala
private[graphx]
class VertexAttributeBlock[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
extends Serializable {
    介绍: 顶点属性数据块,存储传递到边分区的顶点属性
    构造器参数：
    	vids	顶点编号
    	attrs	边属性列表
    def iterator: Iterator[(VertexId, VD)] 
    功能: 获取迭代器信息
    val=  (0 until vids.length).iterator.map { i => (vids(i), attrs(i)) }   
}
```

```scala
private[graphx]
object ShippableVertexPartition {
    介绍: 传输顶点分区
    操作集:
    def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)]): ShippableVertexPartition[VD]
    功能: 从指定顶点中构建一个@ShippableVertexPartition
    val= apply(iter, RoutingTablePartition.empty, null.asInstanceOf[VD], (a, b) => a)
    
    def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: RoutingTablePartition, defaultVal: VD)
    : ShippableVertexPartition[VD]
    功能: 同上,使用@defaultVal 填充缺失的顶点
    val= apply(iter, routingTable, defaultVal, (a, b) => a)
    
    def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: RoutingTablePartition, defaultVal: VD,
      mergeFunc: (VD, VD) => VD): ShippableVertexPartition[VD] 
    功能: 同上,这里提供了合并函数@mergeFunc 用于合并重复的顶点属性
    1. 设置存储顶点数据的数据结构
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    2. 使用合并函数合并给定迭代器中重复的顶点
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    3. 填充路由表中的缺失值
    routingTable.iterator.foreach { vid =>
      map.changeValue(vid, defaultVal, identity)
    }
    val= new ShippableVertexPartition(map.keySet, map._values, map.keySet.getBitSet, routingTable)
    
    def shippablePartitionToOps[VD: ClassTag](
        partition: ShippableVertexPartition[VD])
    : ShippableVertexPartitionOps[VD]
    功能: 允许在@ShippableVertexPartition直接调用@VertexPartitionBase的实例
    val= new ShippableVertexPartitionOps(partition)
    
    内部类:
    implicit object ShippableVertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[ShippableVertexPartition] {
        介绍: 可以传递的顶点分区操作构造器
        
        def toOps[VD: ClassTag](
            partition: ShippableVertexPartition[VD])
      	: VertexPartitionBaseOps[VD, ShippableVertexPartition]
        功能: 获取顶点分区基本操作实例
        val= shippablePartitionToOps(partition)
    }
}
```

```scala
private[graphx]
class ShippableVertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet,
    val routingTable: RoutingTablePartition)
extends VertexPartitionBase[VD] {
    介绍: 可以传输的顶点分区
    构造器参数:
    index	顶点编号--> 索引映射表
    values	顶点参数值列表
    mask	掩码
    routingTable	路由分区表
    操作集:
    def withRoutingTable(_routingTable: RoutingTablePartition): ShippableVertexPartition[VD]
    功能: 指定路由分区表@_routingTable,返回可传输的顶点分区@ShippableVertexPartition
    val= new ShippableVertexPartition(index, values, mask, _routingTable)
    
    def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): Iterator[(PartitionID, VertexAttributeBlock[VD])]
    功能: 获取可传输的顶点属性
    输入参数:
    shipSrc	是否含有传输起始点
    shipDst 是否含有传输终止点
    val= Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val initialSize = if (shipSrc && shipDst) routingTable.partitionSize(pid) else 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      routingTable.foreachWithinEdgePartition(pid, shipSrc, shipDst) { vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid)
        }
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
    
    def shipVertexIds(): Iterator[(PartitionID, Array[VertexId])]
    功能: 对于每个边分区,产生顶点编号列表,在列表中,可见的顶点编号作为边分区的引用.
    val= Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val vids = new PrimitiveVector[VertexId](routingTable.partitionSize(pid))
      routingTable.foreachWithinEdgePartition(pid, true, true) { vid =>
        if (isDefined(vid)) {
          vids += vid
        }
      }
      (pid, vids.trim().array)
    }
}
```

```scala
private[graphx] class ShippableVertexPartitionOps[VD: ClassTag](self: ShippableVertexPartition[VD])
extends VertexPartitionBaseOps[VD, ShippableVertexPartition](self) {
    def withIndex(index: VertexIdToIndexMap): ShippableVertexPartition[VD]
    功能: 获取指定索引的顶点分区@ShippableVertexPartition
    val= new ShippableVertexPartition(index, self.values, self.mask, self.routingTable)
    
    def withValues[VD2: ClassTag](values: Array[VD2]): ShippableVertexPartition[VD2] 
    功能: 获取指定值@values 对于的顶点分区@ShippableVertexPartition
    val= new ShippableVertexPartition(self.index, values, self.mask, self.routingTable)
    
    def withMask(mask: BitSet): ShippableVertexPartition[VD] 
    功能: 获取指定掩码对于的顶点分区@ShippableVertexPartition
    val= new ShippableVertexPartition(self.index, self.values, mask, self.routingTable)
}
```

#### VertexPartition

```scala
private[graphx] object VertexPartition {
    def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)])
    : VertexPartition[VD] 
    功能: 构建一个顶点分区@VertexPartition
    val (index, values, mask) = VertexPartitionBase.initFrom(iter)
    val= new VertexPartition(index, values, mask)
    
    def partitionToOps[VD: ClassTag](partition: VertexPartition[VD]): VertexPartitionOps[VD] 
    功能: 获取顶点分区操作实例
    val= new VertexPartitionOps(partition)
    
    内部类:
    implicit object VertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[VertexPartition] {
        介绍: 顶点分区操作构建类
        操作集:
        def toOps[VD: ClassTag](partition: VertexPartition[VD])
      		: VertexPartitionBaseOps[VD, VertexPartition]
        功能: 获取顶点分区操作
        val= partitionToOps(partition)
    }
}
```

```scala
private[graphx] class VertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet)
extends VertexPartitionBase[VD]
介绍: 顶点分区
构造器属性:
index	顶点编号->索引映射
values	顶点属性值
mask	掩码(位集合)
```

```scala
private[graphx] class VertexPartitionOps[VD: ClassTag](self: VertexPartition[VD])
extends VertexPartitionBaseOps[VD, VertexPartition](self) {
    def withIndex(index: VertexIdToIndexMap): VertexPartition[VD] 
    功能: 获取指定索引的顶点分区
    val= new VertexPartition(index, self.values, self.mask)
    
    def withValues[VD2: ClassTag](values: Array[VD2]): VertexPartition[VD2] 
    功能: 获取指定值的顶点分区
    val= new VertexPartition(self.index, values, self.mask)
    
    def withMask(mask: BitSet): VertexPartition[VD]
    功能: 获取指定掩码的顶点分区
    val= new VertexPartition(self.index, self.values, mask)    
}
```

#### VertexPartitionBase

```scala
private[graphx] object VertexPartitionBase {
    def initFrom[VD: ClassTag](iter: Iterator[(VertexId, VD)])
    : (VertexIdToIndexMap, Array[VD], BitSet)
    功能: 由给定的顶点构建顶点分区@VertexPartitionBase,合并了任意重复的条目
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    iter.foreach { pair =>
      map(pair._1) = pair._2
    }
    val= (map.keySet, map._values, map.keySet.getBitSet)
    
    def initFrom[VD: ClassTag](iter: Iterator[(VertexId, VD)], mergeFunc: (VD, VD) => VD)
    : (VertexIdToIndexMap, Array[VD], BitSet) 
    功能: 同上
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    val= (map.keySet, map._values, map.keySet.getBitSet)
}
```

```scala
private[graphx] abstract class VertexPartitionBase[@specialized(Long, Int, Double) VD: ClassTag]
extends Serializable {
    介绍: 处理顶点编号和顶点属性的映射,@VertexPartition是对应的离散实现.@VertexPartitionBaseOps提供了这个类的各种实现.
    属性:
    #name @capacity: Int = index.capacity	索引映射表容量
    操作集:
    def index: VertexIdToIndexMap
    def values: Array[VD]
    def mask: BitSet
    功能: 获取顶点分区的索引映射表/顶点属性值/掩码
    
    def size: Int = mask.cardinality()
    功能: 获取掩码的基数
    
    def apply(vid: VertexId): VD = values(index.getPos(vid))
    功能: 获取指定顶点编号的顶点参数
    
    def isDefined(vid: VertexId): Boolean
    功能: 确定指定顶点是否被定义
    val pos = index.getPos(vid)
    val= pos >= 0 && mask.get(pos)
    
    def iterator: Iterator[(VertexId, VD)] 
    功能: 获取迭代信息
    val= mask.iterator.map(ind => (index.getValue(ind), values(ind)))
}
```

```scala
private[graphx] trait VertexPartitionBaseOpsConstructor[T[X] <: VertexPartitionBase[X]] {
  def toOps[VD: ClassTag](partition: T[VD]): VertexPartitionBaseOps[VD, T]
    功能: 转换成对于的顶点分区操作实例
}
```

#### VertexPartitionBaseOps

```scala
private[graphx] abstract class VertexPartitionBaseOps
    [VD: ClassTag, Self[X] <: VertexPartitionBase[X]: VertexPartitionBaseOpsConstructor]
    (self: Self[VD])
extends Serializable with Logging {
    介绍: 这个类包含了子类@VertexPartitionBase的额外操作
   	操作集:
    def withIndex(index: VertexIdToIndexMap): Self[VD]
    def withValues[VD2: ClassTag](values: Array[VD2]): Self[VD2]
    def withMask(mask: BitSet): Self[VD]
    功能: 处理指定索引表/顶点分区值/掩码
    
    def map[VD2: ClassTag](f: (VertexId, VD) => VD2): Self[VD2]
    功能: 通过map函数将顶点属性沿着顶点编号传递,映射函数为@f,类型由VD->VD2
    val newValues = new Array[VD2](self.capacity)
    var i = self.mask.nextSetBit(0)
    while (i >= 0) {
      newValues(i) = f(self.index.getValue(i), self.values(i))
      i = self.mask.nextSetBit(i + 1)
    }
    val= this.withValues(newValues)
    
    def filter(pred: (VertexId, VD) => Boolean): Self[VD]
    功能: 过滤当前实例中的元素,并返回
    val newMask = new BitSet(self.capacity)
    var i = self.mask.nextSetBit(0)
    while (i >= 0) {
      if (pred(self.index.getValue(i), self.values(i))) {
        newMask.set(i)
      }
      i = self.mask.nextSetBit(i + 1)
    }
    val= this.withMask(newMask)
    
    def minus(other: Self[VD]): Self[VD]
    功能: 隐藏顶点编号(等于this或者@other的)
    if (self.index != other.index) {
      logWarning("Minus operations on two VertexPartitions with different indexes is slow.")
      minus(createUsingIndex(other.iterator))
    } else {
      self.withMask(self.mask.andNot(other.mask))
    }
    
    def minus(other: Iterator[(VertexId, VD)]): Self[VD]
    功能: 隐藏与this和@other相同的顶点编号
    val= minus(createUsingIndex(other))
    
    def diff(other: Self[VD]): Self[VD]
    功能: 检查当前@this与@other不同的地方,保留@other中不同的顶点,并返回
    if (self.index != other.index) {
      logWarning("Diffing two VertexPartitions with different indexes is slow.")
      diff(createUsingIndex(other.iterator))
    } else {
      val newMask = self.mask & other.mask
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        if (self.values(i) == other.values(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      this.withValues(other.values).withMask(newMask)
    }
    
    def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: Self[VD2])
      (f: (VertexId, VD, Option[VD2]) => VD3): Self[VD3]
    功能: 与指定顶点分区@other进行左连接,并使用@f 进行合并
    if (self.index != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[VD3](self.capacity)
      var i = self.mask.nextSetBit(0)
      while (i >= 0) {
        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(self.index.getValue(i), self.values(i), otherV)
        i = self.mask.nextSetBit(i + 1)
      }
      this.withValues(newValues)
    }
    
    def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: Iterator[(VertexId, VD2)])
      (f: (VertexId, VD, Option[VD2]) => VD3): Self[VD3]
    功能: 与指定顶点分区@other进行左连接,并使用@f进行合并
    leftJoin(createUsingIndex(other))(f)
    
    def innerJoin[U: ClassTag, VD2: ClassTag]
      (other: Self[U])
      (f: (VertexId, VD, U) => VD2): Self[VD2]
    功能: 与指定顶点分区@other进行内连接,并使用函数@f进行合并处理
    if (self.index != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = self.mask & other.mask
      val newValues = new Array[VD2](self.capacity)
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
        i = newMask.nextSetBit(i + 1)
      }
      this.withValues(newValues).withMask(newMask)
    }
    
    def innerJoin[U: ClassTag, VD2: ClassTag]
      (iter: Iterator[Product2[VertexId, U]])
      (f: (VertexId, VD, U) => VD2): Self[VD2] 
    功能: 同上,使用的是迭代器合并
    innerJoin(createUsingIndex(iter))(f)
    
    def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
    : Self[VD2] 
    功能: 效果类似于@aggregateUsingIndex((a, b) => a),使用索引创建顶点分区
    1. 设置顶点属性集合和掩码值
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD2](self.capacity)
    2. 根据迭代器中的每个条目,获取顶点编号作为位置,设置其掩码值和相应的属性值
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    val= this.withValues(newValues).withMask(newMask)
    
    def innerJoinKeepLeft(iter: Iterator[Product2[VertexId, VD]]): Self[VD]
    功能: 内连接,但是左边的顶点不会出现在迭代器中
    1. 设置顶点属性集合和掩码值
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD2](self.capacity)
    2. 覆盖左边的信息(顶点参数值)
    System.arraycopy(self.values, 0, newValues, 0, newValues.length)
    3. 根据迭代器中的每个条目,获取顶点编号作为位置,设置其掩码值和相应的属性值
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    val= this.withValues(newValues).withMask(newMask)
    
    def aggregateUsingIndex[VD2: ClassTag](
      iter: Iterator[Product2[VertexId, VD2]],
      reduceFunc: (VD2, VD2) => VD2): Self[VD2] =
    功能: 使用索引进行聚合
    输入参数: reduceFunc	聚合函数
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD2](self.capacity)
    iter.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val pos = self.index.getPos(vid)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newValues(pos) = reduceFunc(newValues(pos), vdata)
        } else { // otherwise just store the new value
          newMask.set(pos)
          newValues(pos) = vdata
        }
      }
    }
    val= this.withValues(newValues).withMask(newMask)
    
    def reindex(): Self[VD]
    功能: 重新索引,构建一个顶点分区,这个顶点分区中仅仅包含掩码中表示的顶点
    val hashMap = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    val arbitraryMerge = (a: VD, b: VD) => a
    for ((k, v) <- self.iterator) {
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    val=this.withIndex(hashMap.keySet).withValues(hashMap._values).withMask(hashMap.keySet.getBitSet)
    
    implicit def toOps[VD2: ClassTag](partition: Self[VD2])
    : VertexPartitionBaseOps[VD2, Self] 
    功能: 转换为顶点分区操作实例
    val= implicitly[VertexPartitionBaseOpsConstructor[Self]].toOps(partition)
}
```

#### VertexRDDImpl

```scala
class VertexRDDImpl[VD] private[graphx] (
    @transient val partitionsRDD: RDD[ShippableVertexPartition[VD]],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  (implicit override protected val vdTag: ClassTag[VD])
extends VertexRDD[VD](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {
    介绍: 顶点RDD
    构造器参数:
    partitionsRDD	分区RDD
    targetStorageLevel	目标存储等级
    vdTag	顶点属性类型
    属性:
    #name @partitioner = partitionsRDD.partitioner	分区器
    初始化操作:
    require(partitionsRDD.partitioner.isDefined)
    功能: 分区器存在性校验
    
    setName("VertexRDD")
    功能: 设置当前顶点RDD名称
    
    操作集:
    def reindex(): VertexRDD[VD] = this.withPartitionsRDD(partitionsRDD.map(_.reindex()))
    功能: 当前顶点RDD的重新索引
    
    def getPreferredLocations(s: Partition): Seq[String]
    功能: 获取最佳执行位置
    val= partitionsRDD.preferredLocations(s)
    
    def setName(_name: String): this.type
    功能: 设置当前顶点RDD的名称
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    val= this
    
    def persist(newLevel: StorageLevel): this.type 
    功能: 顶点RDD持久化
    partitionsRDD.persist(newLevel)
    val= this
    
    def unpersist(blocking: Boolean = false): this.type 
    功能: 解除顶点RDD的持久化
    partitionsRDD.unpersist(blocking)
    val= this
    
    def cache(): this.type 
    功能: 顶点RDD缓存到指定的存储等级
    partitionsRDD.persist(targetStorageLevel)
    val= this
    
    def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel
    功能: 获取当前存储等级
    
    def checkpoint(): Unit
    功能: 当前RDD进行检查点处理
    partitionsRDD.checkpoint()
    
    def isCheckpointed: Boolean
    功能: 确定当前RDD是否设置了检查点
    val= firstParent[ShippableVertexPartition[VD]].isCheckpointed
    
    def getCheckpointFile: Option[String] 
    功能: 获取检查点文件
    val= partitionsRDD.getCheckpointFile
    
    def count(): Long
    功能: 计算RDD中的顶点数量
    val= partitionsRDD.map(_.size.toLong).fold(0)(_ + _)
    
    def mapVertexPartitions[VD2: ClassTag](
      f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])
    : VertexRDD[VD2]
    功能: 顶点分区映射,映射函数为@f
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(f), preservesPartitioning = true)
    val= this.withPartitionsRDD(newPartitionsRDD)
    
    def mapValues[VD2: ClassTag](f: VD => VD2): VertexRDD[VD2] 
    功能: 对value进行@f映射
    val= this.mapVertexPartitions(_.map((vid, attr) => f(attr)))
    
    def mapValues[VD2: ClassTag](f: (VertexId, VD) => VD2): VertexRDD[VD2] 
    功能: 同上
    val= this.mapVertexPartitions(_.map(f))
    
    def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD]
    功能: 从当前RDD中从@other移除当前RDD中的顶点(求差集)
    val= minus(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
    
    def minus (other: VertexRDD[VD]): VertexRDD[VD]
    功能: 同上
    val= other match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        this.withPartitionsRDD[VD](
          partitionsRDD.zipPartitions(
            other.partitionsRDD, preservesPartitioning = true) {
            (thisIter, otherIter) =>
              val thisPart = thisIter.next()
              val otherPart = otherIter.next()
              Iterator(thisPart.minus(otherPart))
          })
      case _ =>
        this.withPartitionsRDD[VD](
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.minus(msgs))
          }
        )
    }
    
    def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD]
    功能: 检查两个RDD中的不同元素,返回@other中的不同元素
    
    def diff(other: VertexRDD[VD]): VertexRDD[VD]
    功能: 同上
    1. 获取@other中的分区
    val otherPartition = other match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        other.partitionsRDD
      case _ =>
        VertexRDD(other.partitionBy(this.partitioner.get)).partitionsRDD
    }
    2. 获取@other形成的分区RDD
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      otherPartition, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.diff(otherPart))
    }
    val= this.withPartitionsRDD(newPartitionsRDD)
    
    def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
      (other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): VertexRDD[VD3]
    功能: 左连接,使用函数@f进行结果的合并
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.leftJoin(otherPart)(f))
    }
    val= this.withPartitionsRDD(newPartitionsRDD)
    
    def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: RDD[(VertexId, VD2)])
      (f: (VertexId, VD, Option[VD2]) => VD3)
    : VertexRDD[VD3] 
    功能: 同上,但是当当前RDD@other 是顶点RDD与上面相同.但是如果不是,则需要处理完毕时候压缩,效率不如上面的.
    other match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        leftZipJoin(other)(f)
      case _ =>
        this.withPartitionsRDD[VD3](
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.leftJoin(msgs)(f))
          }
        )
    }
    
    def innerZipJoin[U: ClassTag, VD2: ClassTag](other: VertexRDD[U])
      (f: (VertexId, VD, U) => VD2): VertexRDD[VD2]
    功能: 与@other 内连接,使用函数@f 进行结果合并
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(thisPart.innerJoin(otherPart)(f))
    }
    val= this.withPartitionsRDD(newPartitionsRDD)
    
    def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
      (f: (VertexId, VD, U) => VD2): VertexRDD[VD2]
    功能: 同上,但是如果不是顶点RDD,那么使用@innerZipJoin 更加有效
    val= other match {
      case other: VertexRDD[_] if this.partitioner == other.partitioner =>
        innerZipJoin(other)(f)
      case _ =>
        this.withPartitionsRDD(
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true) {
            (partIter, msgs) => partIter.map(_.innerJoin(msgs)(f))
          }
        )
    }
    
    def aggregateUsingIndex[VD2: ClassTag](
      messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): VertexRDD[VD2]
    功能: 使用索引进行聚合
    val shuffled = messages.partitionBy(this.partitioner.get)
    val parts = partitionsRDD.zipPartitions(shuffled, true) { (thisIter, msgIter) =>
      thisIter.map(_.aggregateUsingIndex(msgIter, reduceFunc))
    }
    val= this.withPartitionsRDD[VD2](parts)
    
    def reverseRoutingTables(): VertexRDD[VD]
    功能: 反转路由表
    val= this.mapVertexPartitions(vPart => vPart.withRoutingTable(vPart.routingTable.reverse))
    
    def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] 
    功能: 使用边生成顶点RDD
    val routingTables = VertexRDD.createRoutingTables(edges, this.partitioner.get)
    val vertexPartitions = partitionsRDD.zipPartitions(routingTables, true) {
      (partIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        partIter.map(_.withRoutingTable(routingTable))
    }
    val= this.withPartitionsRDD(vertexPartitions)
    
    def withPartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[ShippableVertexPartition[VD2]]): VertexRDD[VD2]
    功能: 由@ShippableVertexPartition 生成顶点RDD
    val= new VertexRDDImpl(partitionsRDD, this.targetStorageLevel)
    
    def withTargetStorageLevel(
      targetStorageLevel: StorageLevel): VertexRDD[VD]
    功能: 生成指定存储等级的顶点RDD
    val= new VertexRDDImpl(this.partitionsRDD, targetStorageLevel)
    
    def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[VD])]
    功能: 传输顶点属性,获取RDD
    val= partitionsRDD.mapPartitions(_.flatMap(_.shipVertexAttributes(shipSrc, shipDst)))
    
    def shipVertexIds(): RDD[(PartitionID, Array[VertexId])]
    功能: 传输顶点编号,获取RDD
    val= partitionsRDD.mapPartitions(_.flatMap(_.shipVertexIds()))
}
```

#### 基础拓展

#### 基础拓展

1.  BitSet的使用
2.  BitMap的使用