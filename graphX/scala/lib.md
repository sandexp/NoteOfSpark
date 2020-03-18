1. [ConnectedComponents.scala](# ConnectedComponents)
2. [LabelPropagation.scala](# LabelPropagation)
3. [PageRank.scala](# PageRank)
4. [ShortestPaths.scala](# ShortestPaths)
5. [StronglyConnectedComponents.scala](# StronglyConnectedComponents)
6. [SVDPlusPlus.scala](# SVDPlusPlus)
7. [TriangleCount.scala](# TriangleCount)
8. [算法原理介绍](# 算法原理介绍)

---

#### ConnectedComponents

```scala
object ConnectedComponents {
    介绍: 连通分量算法
    def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                      maxIterations: Int): Graph[VertexId, ED]
    功能: 计算连通分量中每个顶点的关系,返回一个图,包含该顶点的连通分量,返回图的顶点编号去连通图中编号最小的.
    输入参数:
    VD	顶点参数类型(计算中会抛弃)
    ED	边参数类型(计算中保留)
    graph	需要计算连通图的图
    maxIterations	最大迭代次数
    0. 参数校验
    require(maxIterations > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${maxIterations}")
    1. 获取只有顶点编号的图
    val ccGraph = graph.mapVertices { case (vid, _) => vid }
    2. 定义消息发送函数
    def sendMessage(edge: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, VertexId)] = {
      if (edge.srcAttr < edge.dstAttr) {// 保证前者的属性值大
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }
    3. 获取初始化消息(所有顶点都需要接受这个值)
    val initialMessage = Long.MaxValue
    4. 使用pregal迭代计算顶点上的属性值(目的是完成图中顶点属性值的赋予工作)
    val pregelGraph = Pregel(ccGraph, initialMessage,
      maxIterations, EdgeDirection.Either)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
    5. 解除图的持久化,并返回
    ccGraph.unpersist()
    val= pregelGraph
    
    def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VertexId, ED]
    功能: 同上,但是不设置迭代上限
    val= run(graph, Int.MaxValue)
}
```

#### LabelPropagation

```scala
object LabelPropagation {
    def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED]
    功能: 运行标签传播算法,用于获取网络(拓扑结构)中的组
    网络中的每个结点都需要在自己的组下初始化,在每一个超级步中,结点发送组的归属信息给邻接组,并更新状态,用于到来消息的模式组归属信息.
    LPA是一个标准的图组的检测算法,计算廉价且可计算性强.
    但是
    1. 收敛性得不到保障
    2. 计算结束之后的后续步骤繁琐
    输入参数:
    ED	边属性类型
    graph	需要计算图组的归属性的图
    maxSteps	最大步长(LPA超级步的数量,由于是静态实现,所以算法会运行许多个超级步)
    0. 参数校验
    require(maxSteps > 0, s"Maximum 
    	of steps must be greater than 0, but got ${maxSteps}")
    1. 获取需要进行LPA的图
    val lpaGraph = graph.mapVertices { case (vid, _) => vid }
    2. 定义发送消息的函数
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(
        VertexId, Map[VertexId, Long])] = {
        // 顶点编号计数逻辑
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }
    3. 定义顶点合并消息的函数
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
      : Map[VertexId, Long] = {
      val map = mutable.Map[VertexId, Long]()
      // 这里的消息合并逻辑是对顶点编号进行计数,如果多个组中包含同一个元素,这个元素就会多次计数
      (count1.keySet ++ count2.keySet).foreach { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        map.put(i, count1Val + count2Val)
      }
      map
    }
    4. 定义顶点处理函数
    def vertexProgram(
        vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }
    5. 获取初始化消息
    val initialMessage = Map[VertexId, Long]()
    6. 迭代计算顶点消息,并返回最终计算结果
    val= Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
}
```

#### PageRank

```scala
介绍:
PageRank算法有两种实现.
第一种实现使用独立的图接口,运行pageRank算法在指定数量的迭代器上.
{{{
   	var PR = Array.fill(n)( 1.0 )
 	val oldPR = Array.fill(n)( 1.0 )
 	for( iter <- 0 until numIter ) {
 	 swap(oldPR, PR)
 	 for( i <- 0 until n ) {
         // pageRank
 	    PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 	 }
 	}
}}}

第二种实现使用Pregal接口,运行pageRank直到收敛为止.
{{{
	var PR = Array.fill(n)( 1.0 )
 	val oldPR = Array.fill(n)( 0.0 )
 	while( max(abs(PR - oldPr)) > tol ) {
 	 swap(oldPR, PR)
 	 for( i <- 0 until n if abs(PR[i] - oldPR[i]) > tol ) {
         // pageRank
 	   PR[i] = alpha + (1 - \alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 	  }
	 }
 }}}
alpha是随机重置概率(典型值0.15).
inNbrs[i]	是邻接点集合,用于连接i的连接点
outDeg[j]	是顶点j的出度
注意： 这个不是标准的pageRank，因为列表页(没有内连接)有一个@alpha的pageRank
```

```scala
object PageRank extends Logging {
    def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15): Graph[Double, Double]
    功能: 运行pageRank,迭代指定次数@numIter.返回一个图,包含带有边权的边属性
    输入参数:
    VD	原始顶点属性
    ED	原始边属性
    graph	需要计算pageRank的图
    numIter	pageRank的迭代次数
    resetProb	随机重置概率(alpha)
    val= runWithOptions(graph, numIter, resetProb, None)
    
    def runUpdate(rankGraph: Graph[Double, Double], personalized: Boolean,
                resetProb: Double, src: VertexId): Graph[Double, Double]
    功能: 运行pageRank算法的更新,更新pageRank每个节点的值
    输入参数:
    rankGraph	当前的pageRank图
    personalized	如果是true则为个性化pageRank
    resetProb	随机重置概率(alpga)
    src	个性化pageRank的源顶点
    1. 定义获取增量的函数
    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }
    2. 计算每个顶点的出向rank,使用本地预聚合,在接受顶点的时候进行最终的聚合.需要shuffle.
    val rankUpdates = rankGraph.aggregateMessages[Double](
      ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)
    3. 使用rank更新函数,用于获取新的rank.没有接收到消息的时候使用join去保持顶点的rank值.需要shuffle,用于使用广播变量更新边分区的rank值.
    // 更新重置概率(alpha)
    val rPrb = if (personalized) {
      (src: VertexId, id: VertexId) => resetProb * delta(src, id)
    } else {
      (src: VertexId, id: VertexId) => resetProb
    }
    // 使用join保持没有收到消息的顶点rank值
    rankGraph.outerJoinVertices(rankUpdates) {
        // map映射函数,(1.0 - resetProb) * msgSumOpt.getOrElse(0.0)恒为0
      (id, oldRank, msgSumOpt) => rPrb(src, id) + (1.0 - resetProb) * msgSumOpt.getOrElse(0.0)
    }
    
    def runWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15,
      srcId: Option[VertexId] = None): Graph[Double, Double]
    功能: 迭代指定次数@numIter,返回一个带有顶点属性的图,属性中包含了pageRank和带边权的边属性
    输入参数:
    VD	原始顶点属性
    ED	原始边属性
    graph	需要计算pageRank的图
    numIter	迭代次数
    resetProb	随机重置概率(alpha)
    srcId	自定义pageRank的源顶点
    0. 参数校验
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    1. 确定是否是个性化设置和源顶点编号
    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)
    2. 使用边属性初始化pageRank图,这个边属性的权值为`1/出度` 且每个顶点的属性都是 1.0,如果运行了个性化的pageRank,仅仅源顶点的属性值为 1.0..其他点都是0.
    var rankGraph: Graph[Double, Double] = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // 设置顶点的权值
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      .mapVertices { (id, attr) =>
          // 个性化顶点属性值设置
        if (!(id != src && personalized)) 1.0 else 0.0
      }
    3. 迭代计算pageRank
    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()
      prevRankGraph = rankGraph
      rankGraph = runUpdate(rankGraph, personalized, resetProb, src)
      rankGraph.cache()
      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logInfo(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist()
      prevRankGraph.edges.unpersist()
      iteration += 1
    }
    4. 检查图是否是正确的rank和形式,并格式化,参考SPARK-18847
    normalizeRankSum(rankGraph, personalized)
    
    def runWithOptionsWithPreviousPageRank[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double, srcId: Option[VertexId],
      preRankGraph: Graph[Double, Double]): Graph[Double, Double]
    功能: 根据上一个pageRank计算当前pageRank
    输入参数:
    graph	需要计算pageRank的图
    numIter	迭代次数
    resetProb	随机重置概率
    srcId	个性化pageRank的源顶点
    preRankGraph	保持迭代信息的pageRank图
    0. 参数校验
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    1. 获取上次迭代的顶点信息
    val graphVertices = graph.numVertices
    val prePageRankVertices = preRankGraph.numVertices
    require(graphVertices == prePageRankVertices, s"Graph and previous pageRankGraph" +
      s" must have the same number of vertices but got ${graphVertices} and ${prePageRankVertices}")
    2. 获取源顶点信息
    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)
    3. 初始化pageRank图,边属性为1/出度,每个顶点的属性为 1.0.如果是个性化配置,源顶点为1.0,其他顶点为0.
    var rankGraph: Graph[Double, Double] = preRankGraph
    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    4. 有上一个pageRank开始迭代计算pageRank
    while (iteration < numIter) {
      rankGraph.cache()
      prevRankGraph = rankGraph
      rankGraph = runUpdate(rankGraph, personalized, resetProb, src)
      rankGraph.cache()
      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logInfo(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist()
      prevRankGraph.edges.unpersist()
      iteration += 1
    }
    5. 检查图是否是正确的rank和形式,并格式化,参考SPARK-18847
    normalizeRankSum(rankGraph, personalized)
    
    def runParallelPersonalizedPageRank[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      numIter: Int,
      resetProb: Double = 0.15,
      sources: Array[VertexId]): Graph[Vector, Double]
    功能: 运行自定义的pagerank,进行指定次数的迭代,用于并行启动节点的集合.返回一个图,这个图包含了所有的启动节点,且含有带有边权的边属性.
    graph	用于计算个性化pageRank的图
    numIter	迭代次数
    resetProb	随机重置概率
    sources	计算pageRank的资源列表
    0. 参数校验
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    require(sources.nonEmpty, s"The list of sources must be non-empty," +
      s" but got ${sources.mkString("[", ",", "]")}")
    1. 获取迭代初始值
    val zero = Vectors.sparse(sources.size, List()).asBreeze
    // map of vid -> vector where for each vid, the _position of vid in source_ is set to 1.0
    val sourcesInitMap = sources.zipWithIndex.map { case (vid, i) =>
      val v = Vectors.sparse(sources.size, Array(i), Array(1.0)).asBreeze
      (vid, v)
    }.toMap
    2. 确定参加pageRank的初始图
    val sc = graph.vertices.sparkContext
    val sourcesInitMapBC = sc.broadcast(sourcesInitMap)
    var rankGraph = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      .mapVertices((vid, _) => sourcesInitMapBC.value.getOrElse(vid, zero))
    3. 迭代计算pageRank
    var i = 0
    while (i < numIter) {
      val prevRankGraph = rankGraph
      val rankUpdates = rankGraph.aggregateMessages[BV[Double]](
        ctx => ctx.sendToDst(ctx.srcAttr *:* ctx.attr),
        (a : BV[Double], b : BV[Double]) => a +:+ b, TripletFields.Src)
      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (vid, oldRank, msgSumOpt) =>
          val popActivations: BV[Double] = msgSumOpt.getOrElse(zero) *:* (1.0 - resetProb)
          val resetActivations = if (sourcesInitMapBC.value contains vid) {
            sourcesInitMapBC.value(vid) *:* resetProb
          } else {
            zero
          }
          popActivations +:+ resetActivations
        }.cache()
      rankGraph.edges.foreachPartition(_ => {}) // also materializes rankGraph.vertices
      prevRankGraph.vertices.unpersist()
      prevRankGraph.edges.unpersist()
      logInfo(s"Parallel Personalized PageRank finished iteration $i.")
      i += 1
    }
    4. 检查图是否是正确的rank和形式,并格式化,参考SPARK-18847
    val rankSums = rankGraph.vertices.values.fold(zero)(_ +:+ _)
    rankGraph.mapVertices { (vid, attr) =>
      Vectors.fromBreeze(attr /:/ rankSums)
    }
    
    def runUntilConvergence[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double]
    功能: 计算pageRank直到收敛为止
    输入参数:
    graph	需要计算pageRank的图
    tol	收敛误差(小于这个值才可以被看做收敛)
    resetProb	随机重置概率
    val= runUntilConvergenceWithOptions(graph, tol, resetProb)
    
    def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15,
      srcId: Option[VertexId] = None): Graph[Double, Double]
    功能: 同上
    0. 参数校验
    require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    1. 确定需要参加pageRank的图
    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) =>
        if (id == src) (0.0, Double.NegativeInfinity) else (0.0, 0.0)
      }
      .cache()
    2. 定义顶点处理函数(新的pageRank更新逻辑)
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }
    3. 定义个性化顶点处理函数
    def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
      msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = if (lastDelta == Double.NegativeInfinity) {
        1.0
      } else {
        oldPR + (1.0 - resetProb) * msgSum
      }
      (newPR, newPR - oldPR)
    }
    4. 定义消息发送函数
    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }
    5. 定义消息合并函数
    def messageCombiner(a: Double, b: Double): Double = a + b
    6. 确定初始化消息,用于pregal计算(需要发送到所有顶点上)
    val initialMessage = if (personalized) 0.0 else resetProb / (1.0 - resetProb)
    7. 执行动态pregal
    val vp = if (personalized) {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        personalizedVertexProgram(id, attr, msgSum)
    } else {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)
    }
    8. 迭代进行pregal计算
    val rankGraph = Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
    9. 检查图是否是正确的rank和形式,并格式化,参考SPARK-18847
    normalizeRankSum(rankGraph, personalized)
    
    def normalizeRankSum(rankGraph: Graph[Double, Double], personalized: Boolean)
    功能: 正常化pageRank中的rank和
    1. 获取rank和
    val rankSum = rankGraph.vertices.values.sum()
    2. 顶点属性处理
    if (personalized) {
      rankGraph.mapVertices((id, rank) => rank / rankSum)
    } else {
      val numVertices = rankGraph.numVertices
      val correctionFactor = numVertices.toDouble / rankSum
      rankGraph.mapVertices((id, rank) => rank * correctionFactor)
    }
}
```

#### ShortestPaths

```scala
object ShortestPaths extends Serializable {
    介绍: 最短路径算法
    type SPMap = Map[VertexId, Int]
    介绍: 存储指定顶点编号对应的顶点到当前顶点之间的距离
    
    def makeMap(x: (VertexId, Int)*) = Map(x: _*)
    功能: 生成顶点距离列表信息
    
    def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }
    功能: 距离+1
    
    def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap
    功能: 将两个最短距离表合并(形成新的最短路径表)
    
    def run[VD, ED: ClassTag](
        graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[SPMap, ED]
    功能: 计算给定图中标记顶点的最短路径
    输入参数:
    ED	边属性类型
    graph	参与计算最短路径的图
    landmarks	标记顶点ID列表,对于这些id都会计算最短路径
    返回:
    这个图会映射到标记顶点的最短路径
    1. 获取含有最短路的图
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }
    2. 获取初始化最短路消息(空表)
    val initialMessage = makeMap()
    3. 定义顶点处理程序(最短路径的更新处理)
    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }
    4. 定义消息发送函数(发送顶点和最短路信息)
    def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }
    5. 迭代计算获取各个顶点的最短路
    val= Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
}
```

#### StronglyConnectedComponents

```scala
object StronglyConnectedComponents {
    功能: 强连通分量算法
    def run[VD: ClassTag, ED: ClassTag](
        graph: Graph[VD, ED], numIter: Int): Graph[VertexId, ED]
    功能: 计算每个顶点的强连通分量,返回一个图,其中顶点属性值包括强连通分量中最小顶点编号.
    输入参数:
    VD	顶点参数类型
    ED	边参数类型
    graph	需要计算强连通分量的图
    0. 参数校验
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")
    1. 获取需要进行求强连通分量的图,并响应的图进行缓存
    var sccGraph = graph.mapVertices { case (vid, _) => vid }
    var sccWorkGraph = graph.mapVertices { case (vid, _) => (vid, false) }.cache()
    var prevSccGraph = sccGraph // 辅助参数,用于解除图的持久化
    2.迭代进行强连通分量的求取
    while (sccWorkGraph.numVertices > 0 && iter < numIter) {
      iter += 1
      do {
          // 循环求取连通图
        numVertices = sccWorkGraph.numVertices
        sccWorkGraph = sccWorkGraph.outerJoinVertices(sccWorkGraph.outDegrees) {
          (vid, data, degreeOpt) => if (degreeOpt.isDefined) data else (vid, true)
        }.outerJoinVertices(sccWorkGraph.inDegrees) {
          (vid, data, degreeOpt) => if (degreeOpt.isDefined) data else (vid, true)
        }.cache()
          // 获取所有需要移除的顶点
        val finalVertices = sccWorkGraph.vertices
            .filter { case (vid, (scc, isFinal)) => isFinal}
            .mapValues { (vid, data) => data._1}
          // 写出value值到ssc中并缓存
        sccGraph = sccGraph.outerJoinVertices(finalVertices) {
          (vid, scc, opt) => opt.getOrElse(scc)
        }.cache()
          // 顶点和边的实体化(获取计数值)
        sccGraph.vertices.count()
        sccGraph.edges.count()
          // 由于已经实体化,那么可以解除之前的持久化
        prevSccGraph.unpersist()
        prevSccGraph = sccGraph // 更新上一个scc图
          // 更新scc,直到到最后执行结束
        sccWorkGraph = sccWorkGraph.subgraph(vpred = (vid, data) => !data._2).cache()
      } while (sccWorkGraph.numVertices < numVertices)
        // 如果迭代次数小于给定的迭代次数,scc图就需要被返回,且并不会被重新计算
      if (iter < numIter) {
          // 收集邻接点scc的最小值,如果这个值较小,则需要更新,然后提示邻接节点表示他的scc值较大
        sccWorkGraph = sccWorkGraph.mapVertices { case (
            vid, (color, isFinal)) => (vid, isFinal) }
        sccWorkGraph = Pregel[(VertexId, Boolean), ED, VertexId](
          sccWorkGraph, Long.MaxValue, activeDirection = EdgeDirection.Out)(
          (vid, myScc, neighborScc) => (math.min(myScc._1, neighborScc), myScc._2),
          e => {
            if (e.srcAttr._1 < e.dstAttr._1) {
              Iterator((e.dstId, e.srcAttr._1))
            } else {
              Iterator()
            }
          },
          (vid1, vid2) => math.min(vid1, vid2))
          // 启动scc,反向变量scc,提示所有的邻接点标签不匹配的时候不要传递
        sccWorkGraph = Pregel[(VertexId, Boolean), ED, Boolean](
          sccWorkGraph, false, activeDirection = EdgeDirection.In)(
          (vid, myScc, existsSameColorFinalNeighbor) => {
            val isColorRoot = vid == myScc._1
            (myScc._1, myScc._2 || isColorRoot || existsSameColorFinalNeighbor)
          },
            // 只要还没有结束且拥有相同的颜色,则激活邻接节点
          e => {
            val sameColor = e.dstAttr._1 == e.srcAttr._1
            val onlyDstIsFinal = e.dstAttr._2 && !e.srcAttr._2
            if (sameColor && onlyDstIsFinal) {
              Iterator((e.srcId, e.dstAttr._2))
            } else {
              Iterator()
            }
          },
          (final1, final2) => final1 || final2)
      }
    }
}
```

#### SVDPlusPlus

```scala
object SVDPlusPlus {
	介绍: SVD++ 算法
    
    class Conf(
      var rank: Int,
      var maxIters: Int,
      var minVal: Double,
      var maxVal: Double,
      var gamma1: Double,
      var gamma2: Double,
      var gamma6: Double,
      var gamma7: Double)
    extends Serializable
    介绍: SVD++的配置
    
    def materialize(g: Graph[_, _]): Unit
    功能: 图的实体化(边,顶点计数)
    g.vertices.count()
    g.edges.count()
    
    def run(edges: RDD[Edge[Double]], conf: Conf)
    : (Graph[(Array[Double], Array[Double], Double, Double), Double], Double)
    功能: 这里SVD++算法的预测规则为 rui=u + bu + bi + qi*(pu + |N(u)|^^-0.5^^*sum(y)),具体内容可以参考,SVD++的实现文章的第6页.
    输入参数:
    edges	构造图的边数
    conf	SVD++参数
    返回: 包含训练模型点属性的图
    0. 参数校验
    require(conf.maxIters > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${conf.maxIters}")
    require(conf.maxVal > conf.minVal, s"MaxVal must be greater than MinVal," +
      s" but got {maxVal: ${conf.maxVal}, minVal: ${conf.minVal}}")
    1. 定义默认点属性生成函数
    def defaultF(rank: Int): (Array[Double], Array[Double], Double, Double) = {
      val v1 = Array.fill(rank)(Random.nextDouble())
      val v2 = Array.fill(rank)(Random.nextDouble())
      (v1, v2, 0.0, 0.0)
    }
    2. 计算全局平均值
    edges.cache()
    val (rs, rc) = edges.map(e => (e.attr, 1L)).fold(
        (0, 0))((a, b) => (a._1 + b._1, a._2 + b._2))
    val u = rs / rc
    3. 构建图
    var g = Graph.fromEdges(edges, defaultF(conf.rank)).cache()
    materialize(g)
    edges.unpersist()
    4. 计算初始偏差
    val t0 = g.aggregateMessages[(Long, Double)](
      ctx => { ctx.sendToSrc((1L, ctx.attr)); ctx.sendToDst((1L, ctx.attr)) },
      (g1, g2) => (g1._1 + g2._1, g1._2 + g2._2))
    5. 使用偏差更新图(更新逻辑为msg)
    val gJoinT0 = g.outerJoinVertices(t0) {
      (vid: VertexId, vd: (Array[Double], Array[Double], Double, Double),
       msg: Option[(Long, Double)]) =>
        (vd._1, vd._2, msg.get._2 / msg.get._1 - u, 1.0 / scala.math.sqrt(msg.get._1))
    }.cache()
    materialize(gJoinT0)
    g.unpersist()
    g = gJoinT0
    6. 定义发送训练结果的函数
    def sendMsgTrainF(conf: Conf, u: Double)
        (ctx: EdgeContext[
          (Array[Double], Array[Double], Double, Double),
          Double,
          (Array[Double], Array[Double], Double)]): Unit = {
      val (usr, itm) = (ctx.srcAttr, ctx.dstAttr)
      val (p, q) = (usr._1, itm._1)
      val rank = p.length
      var pred = u + usr._3 + itm._3 + blas.ddot(rank, q, 1, usr._2, 1)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = ctx.attr - pred
      // updateP = (err * q - conf.gamma7 * p) * conf.gamma2
      val updateP = q.clone()
      blas.dscal(rank, err * conf.gamma2, updateP, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, p, 1, updateP, 1)
      // updateQ = (err * usr._2 - conf.gamma7 * q) * conf.gamma2
      val updateQ = usr._2.clone()
      blas.dscal(rank, err * conf.gamma2, updateQ, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, q, 1, updateQ, 1)
      // updateY = (err * usr._4 * q - conf.gamma7 * itm._2) * conf.gamma2
      val updateY = q.clone()
      blas.dscal(rank, err * usr._4 * conf.gamma2, updateY, 1)
      blas.daxpy(rank, -conf.gamma7 * conf.gamma2, itm._2, 1, updateY, 1)
      ctx.sendToSrc((updateP, updateY, (err - conf.gamma6 * usr._3) * conf.gamma1))
      ctx.sendToDst((updateQ, updateY, (err - conf.gamma6 * itm._3) * conf.gamma1))
    }
    7. 迭代更新
    for (i <- 0 until conf.maxIters) {
      // Phase 1, calculate pu + |N(u)|^(-0.5)*sum(y) for user nodes
      g.cache()
      val t1 = g.aggregateMessages[Array[Double]](
        ctx => ctx.sendToSrc(ctx.dstAttr._2),
        (g1, g2) => {
          val out = g1.clone()
          blas.daxpy(out.length, 1.0, g2, 1, out, 1)
          out
        })
      val gJoinT1 = g.outerJoinVertices(t1) {
        (vid: VertexId, vd: (Array[Double], Array[Double], Double, Double),
         msg: Option[Array[Double]]) =>
          if (msg.isDefined) {
            val out = vd._1.clone()
            blas.daxpy(out.length, vd._4, msg.get, 1, out, 1)
            (vd._1, out, vd._3, vd._4)
          } else {
            vd
          }
      }.cache()
      materialize(gJoinT1)
      g.unpersist()
      g = gJoinT1
      // Phase 2, update p for user nodes and q, y for item nodes
      g.cache()
      val t2 = g.aggregateMessages(
        sendMsgTrainF(conf, u),
        (g1: (Array[Double], Array[Double], Double), g2: (
            Array[Double], Array[Double], Double)) =>
        {
          val out1 = g1._1.clone()
          blas.daxpy(out1.length, 1.0, g2._1, 1, out1, 1)
          val out2 = g2._2.clone()
          blas.daxpy(out2.length, 1.0, g2._2, 1, out2, 1)
          (out1, out2, g1._3 + g2._3)
        })
      val gJoinT2 = g.outerJoinVertices(t2) {
        (vid: VertexId,
         vd: (Array[Double], Array[Double], Double, Double),
         msg: Option[(Array[Double], Array[Double], Double)]) => {
          val out1 = vd._1.clone()
          blas.daxpy(out1.length, 1.0, msg.get._1, 1, out1, 1)
          val out2 = vd._2.clone()
          blas.daxpy(out2.length, 1.0, msg.get._2, 1, out2, 1)
          (out1, out2, vd._3 + msg.get._3, vd._4)
        }
      }.cache()
      materialize(gJoinT2)
      g.unpersist()
      g = gJoinT2
    }
    8. 定义消息发送函数
    def sendMsgTestF(conf: Conf, u: Double)
        (ctx: EdgeContext[(
            Array[Double], Array[Double], Double, Double), Double, Double]): Unit = {
      val (usr, itm) = (ctx.srcAttr, ctx.dstAttr)
      val (p, q) = (usr._1, itm._1)
      var pred = u + usr._3 + itm._3 + blas.ddot(q.length, q, 1, usr._2, 1)
      pred = math.max(pred, conf.minVal)
      pred = math.min(pred, conf.maxVal)
      val err = (ctx.attr - pred) * (ctx.attr - pred)
      ctx.sendToDst(err)
    }
    9. 更新图
    g.cache()
    val t3 = g.aggregateMessages[Double](sendMsgTestF(conf, u), _ + _)
    val gJoinT3 = g.outerJoinVertices(t3) {
      (vid: VertexId, vd: (
          Array[Double], Array[Double], Double, Double), msg: Option[Double]) =>
        if (msg.isDefined) (vd._1, vd._2, vd._3, msg.get) else vd
    }.cache()
    materialize(gJoinT3)
    g.unpersist()
    g = gJoinT3
    10. 将矩阵转化为列表
    val newVertices = g.vertices.mapValues(v => (v._1.toArray, v._2.toArray, v._3, v._4))
    (Graph(newVertices, g.edges), u)
}
```

#### TriangleCount

```markdown
介绍: 三角形计数法
 计算 三角形的数量,这个算法直接相关且可以由下面三个方法计算.
 1. 计算每个顶点的邻接顶点
 2. 对于每个边,计算交叉集合,并发送计数值给双端点
 3. 计算每个顶点的和,通过将三角形数量除以2因为每个三角形需要被计数2次.
 
 有两种实现,默认实现@TriangleCount.run 首次移除自己形成的环和正向化图，为了保证达到下述条件:
 1. 没有自己形成的边(a->a)
 2. 所有边的起始编号小于终止编号
 3. 没有重复的边
 
 但是,正向化是耗时的,因为需要对图进行重分区,如果输入时间已经是正向形式了,且异常了自成环的情况,那么就可以使用@TriangleCount.runPreCanonicalized方法进行三角形计数
 {{{
 	// 去除自称化,并转化为正向图
 	val canonicalGraph = graph.mapEdges(e => 1).removeSelfEdges().canonicalizeEdges()
 	// 计算三角形的个数
 	val counts = TriangleCount.runPreCanonicalized(canonicalGraph).vertices
 }}}
```

```scala
object TriangleCount {
    def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED]
    功能: 三角形计数
    1. 获取正向无环图
    val canonicalGraph = graph.mapEdges(
        e => true).removeSelfEdges().convertToCanonicalEdges()
    2. 获取三角形计数值(返回值是点RDD)
    val counters = runPreCanonicalized(canonicalGraph).vertices
    3. 与计数值RDD进行外部join
    val= graph.outerJoinVertices(counters) { (vid, _, optCounter: Option[Int]) =>
      optCounter.getOrElse(0)
    }
    
    def runPreCanonicalized[VD: ClassTag, ED: ClassTag](
        graph: Graph[VD, ED]): Graph[Int, ED] 
    功能: 在之前已经正向无环化的情况下进行计数,并返回更新后的数据
    1. 构建邻接节点的表示集合
    val nbrSets: VertexRDD[VertexSet] =
      graph.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(nbrs.length)
        var i = 0
        while (i < nbrs.length) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    2. 与当前集合进行join获取更新后的图
    val setGraph: Graph[VertexSet, ED] = graph.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    3. 定义边处理函数
    def edgeFunc(ctx: EdgeContext[VertexSet, ED, Int]): Unit = {
      val (smallSet, largeSet) = if (ctx.srcAttr.size < ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      ctx.sendToSrc(counter)
      ctx.sendToDst(counter)
    }
    4. 计算边的交叉情况
    val counters: VertexRDD[Int] = setGraph.aggregateMessages(edgeFunc, _ + _)
    5. 使用图进行合并计数,并除以2去除
    graph.outerJoinVertices(counters) { (_, _, optCounter: Option[Int]) =>
      val dblCount = optCounter.getOrElse(0)
      require(dblCount % 2 == 0, "Triangle count resulted in an 
      	invalid number of triangles.")
      dblCount / 2
    }
}
```

#### 算法原理介绍

1. [强连通分量算法](https://zh.wikipedia.org/wiki/强连通分量)

2. [Lable Propagation算法](https://en.wikipedia.org/wiki/Label_propagation_algorithm)

3. [最短路径算法]()

4. [SDV++算法]()

5. [SDV++算法实现](http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf)

6. [PageRank算法]()

   