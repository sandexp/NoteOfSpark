## **spark-partial**

---

1.  [ApproximateActionListener.scala](# ApproximateActionListener)
2. [ApproximateEvaluator.scala](# ApproximateEvaluator) 
3.  [BoundedDouble.scala](# BoundedDouble)
4. [CountEvaluator.scala](# CountEvaluator) 
5. [GroupedCountEvaluator.scala](# GroupedCountEvaluator) 
6. [MeanEvaluator.scala](# MeanEvaluator)
7. [PartialResult.scala](# PartialResult)
8. [SumEvaluator.scala](# SumEvaluator)
9.  [基础拓展](# 基础拓展)

---

#### ApproximateActionListener

```markdown
介绍:
	这是一个工作监听器，用于近似的单个结果动作，比如说count()或者是非并行的reduce().这个监听器会等待超时，这是会返回一个局部解，尽管完整结果在那个时刻还没有算出来。
	这个类假定了动作执行在完整的RDD[T]上，通过一个计算每个分区中类型U的结果的函数。且这个操作返回一个局部或者完整的R类型结果。注意这个R类型结果必须包括错误处理(error bars).具体可以参考@BoundedInt作为示例。
```

```scala
private[spark] class ApproximateActionListener[T, U, R] (rdd: RDD[T],
	func: (TaskContext, Iterator[T]) => U,evaluator: ApproximateEvaluator[U, R],timeout: Long)
{
	关系: father --> 任务监听器
	构造器属性:
		rdd			RDD
		func		转换函数
		evaluator	近似评估器@ApproximateEvaluator[U,R]
		timeout		等待时限
	属性: 
	#name @startTime=System.currentTimeMillis()			开始时间
	#name @totalTasks=rdd.partitions.length 			任务总数
	#name @finishedTasks=0 var 						   已完成的任务数量
	#name @failure=None #type @Option[Exception] 		错误原因[None表示没有]
	#name @resultObject #type @Option[PartialResult[R]]  结果集[None表示没有]
	操作集:
	def taskSucceeded(index: Int, result: Any): Unit
	功能: 任务成功处理
	0. 近似评估器@evaluator对结果增量式的合并
	1. 更新@finishedTasks+=1
		如果此时任务达到了任务总数，即: @finishedTasks == @totalTasks
			设置最终结果: 
			resultObject.foreach(r => r.setFinalValue(evaluator.currentResult()))
			唤醒等待的进程
			this.notifyAll()
	
	def jobFailed(exception: Exception): Unit
	功能: 任务失败处理方案
	 	将失败的异常设置错误原因中@failure
	 	failure = Some(exception)
	 	唤醒等待的线程
     	 this.notifyAll()
	
	def awaitResult(): PartialResult[R] 
	功能: 线程等待处理方案[遵循有限等待原则]
	分为如下几种情况
	1. 有错误
		抛出异常throw failure.get
	2. 完成所有任务(finishedTasks == totalTasks)
		返回结果集: new PartialResult(evaluator.currentResult(), true)
	3. 等待超时
		resultObject = Some(new PartialResult(evaluator.currentResult(), false))
		result.get
	当然在上述情况下，都是需要对等待时间进行度量的
}
```

#### ApproximateEvaluator

```markdown
介绍:
	这个类提供通过增量式的合并多个任务产生的部分结果集U的函数,运行在任何时刻调用@currentResult获取部分解。
```

```scala
private[spark] trait ApproximateEvaluator[U, R] {
	操作集:
	def merge(outputId: Int, taskResult: U): Unit
	功能: 合并结果U到输出Id@outputId 下
	
	def currentResult(): R
	功能: 获取当前结果
}
```

#### BoundedDouble

```scala
class BoundedDouble(val mean: Double, val confidence: Double, val low: Double, val high: Double){
	介绍: 有界的double类型
	构造器属性:
		mean		平均值
		low			下限值
		high		上限值
		confidence	 
	def toString(): String = "[%.3f, %.3f]".format(low, high)
	
	def hashCode: Int 
	功能: hashCode计算方法
	val= this.mean.hashCode ^ this.confidence.hashCode ^ this.low.hashCode ^ this.high.hashCode
	
	def equals(that: Any): Boolean
	功能: 相等判断
	val = BoundedDouble => this.mean == that.mean && this.confidence == that.confidence &&
        this.low == that.low && this.high == that.high
	为NaN则直接返回false1
}
```

#### CountEvaluator

```markdown
介绍: 近似评估器@ApproximateEvaluator 的计数状态下执行策略
```

```scala
private[spark] class CountEvaluator(totalOutputs: Int, confidence: Double){
	关系: father --> ApproximateEvaluator[Long, BoundedDouble]
	构造器属性:
		totalOutputs	输入总量
		confidence		信任值???
	属性:
		#name @outputsMerged=0 合并后输出数量
		#name @sum=0L		  总计量
	
	操作集:
		def merge(outputId: Int, taskResult: Long): Unit 
		功能: 合并任务结果@taskResult #type @Long 到输出Id@outputId下
			 outputsMerged += 1
			 sum += taskResult
	
		def currentResult(): BoundedDouble
		功能: 获取当前结果值(计数值)
		分为三种情况:
		1. 合并完所有局部结果 outputsMerged == totalOutputs
		BoundedDouble(sum, 1.0, sum, sum)
		2. 一个都没合并
		BoundedDouble(0, 0.0, 0.0, Double.PositiveInfinity)
		3. 其他情况
		val p = outputsMerged.toDouble / totalOutputs
		@CountEvaluator.bound(confidence, sum, p)
}
```

```scala
private[partial] object CountEvaluator{
	操作集:
	def bound(confidence: Double, sum: Long, p: Double): BoundedDouble 
	功能: 计算接界限值
	计算原理:
		sum元素已经被观测到扫描了一个一部分p的数据。建议数据应当按照sum/p的步长在整个计数集中累加。剩下的总共		的计数次数被分成(1-p)*Poission(sum/p),可以化简为Possion(sum*(1-p)/p).具体请参照计算@泊松分布的方法
	#class @ @org.apache.commons.math3.distribution.PoissonDistribution
		  	
	计算逻辑:
		val dist = new PoissonDistribution(sum * (1 - p) / p)
		val low = dist.inverseCumulativeProbability((1 - confidence) / 2)
    	val high = dist.inverseCumulativeProbability((1 + confidence) / 2)
		return new BoundedDouble(sum + dist.getNumericalMean, confidence, sum + low, sum + high)
}
```

#### GroupedCountEvaluator

```scala
private[spark] class GroupedCountEvaluator[T : ClassTag] (totalOutputs: Int, confidence: Double){
	关系: father --> ApproximateEvaluator[OpenHashMap[T, Long], Map[T, BoundedDouble]]
	构造器属性:
		totalOutputs	总共输出
		confidence 		信任值
	属性:
		#name @outputsMerged=0	已合并数量
		#name @sums=new OpenHashMap[T, Long]()	结果集(是一个快速hashMap方案)
	操作集:
		def merge(outputId: Int, taskResult: OpenHashMap[T, Long]): Unit 
		功能: 合并结果集@taskResult
		1. 更新合并数量@outputsMerged += 1
		2. 向全局结果集中添加记录
			 taskResult.foreach { case (key, value) =>sums.changeValue(key, value, _ + value)}
		
		def currentResult(): Map[T, BoundedDouble] 
		功能: 查找当前记录
		分为3种情况:
		1. 合并数量等于总任务数量 outputsMerged == totalOutputs
			res=sums.map { case (key, sum) => (key, new BoundedDouble(sum, 1.0, sum, sum)) }.toMap
		2. 已合并数量为0
			res=new HashMap[T, BoundedDouble]
		3. 其他情况
			val p = outputsMerged.toDouble / totalOutputs
      		 sums.map { case (key, sum) => (key, CountEvaluator.bound(confidence, sum, p)) }.toMap
			#method @bound 请参照#class @CountEvaluator
}
```

#### MeanEvaluator

```scala
private[spark] class MeanEvaluator(totalOutputs: Int, confidence: Double){
	关系: father --> ApproximateEvaluator[StatCounter, BoundedDouble]
	介绍: 均值估算器
	构造器属性:
		totalOutputs	总输出数量
		confidence		置信值(对应于置信区间)
	属性:
    	#name @outputsMerged=0 已合并数量
    	#name @counter #type @StatCounter	计数器
    操作集:
    	def merge(outputId: Int, taskResult: StatCounter): Unit
    	功能: 合并部分结果到指定outputId下
    	
    	def currentResult(): BoundedDouble
    	功能: 获取当前结果值
    	分为4种情况讨论:
    	1. 合并数量等于任务总数量 outputsMerged == totalOutputs
    	BoundedDouble(counter.mean, 1.0, counter.mean, counter.mean)
    	2. 计数器或已合并数量为0
    	BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
		3. 计数器计数值为1
		BoundedDouble(counter.mean, confidence, Double.NegativeInfinity, Double.PositiveInfinity)
		4. 其他情况
			计算均值@mean=@counter.mean
			计算标准差@stdev=math.sqrt(counter.sampleVariance / counter.count)
			考虑到数据的规模需要
				+ 当世界规模较大 counter.count > 100
					根据概率累积值，求取普通分布下的分布
					NormalDistribution().inverseCumulativeProbability((1 + confidence) / 2)
				+ 否则使用T-分布模拟
					val degreesOfFreedom = (counter.count - 1).toInt
          			TDistribution(degreesOfFreedom).inverseCumulativeProbability((1 + confidence)/2)
}
```

#### PartialResult

```scala
class PartialResult[R] (initialVal: R, isFinal: Boolean) {
	构造器属性:
		initialVal	初始值
		isFinal		最后一条记录标志
	属性:
		#name @finalValue=if (isFinal) Some(initialVal) else None 	最后一条value
		#name @failure=None	失败原因
		#name @failureHandler: Option[Exception => Unit] = None		失败解决方案
		#name @completionHandler: Option[R => Unit] = None 			完成处理方案
	操作集:
	def initialValue: R = initialVal
	功能: 初始化值
	
	def isInitialValueFinal: Boolean = isFinal
	功能: 确定是否为最后一条记录
	
	def getFinalValue(): R 
	功能: 获取最后一条记录
	介绍: 阻塞等待最后一条记录
	   // 阻塞等待
	   while (finalValue.isEmpty && failure.isEmpty) {
     	 this.wait()
    	}
	结果:
		finalValue.isDefined? finalValue.get: throw failure.get
	
	def toString: String
	功能: 显示value值信息(互斥)
	
	def getFinalValueInternal() = finalValue
	功能: 获取最后一条记录
	
	def onComplete(handler: R => Unit): PartialResult[R]
	功能: 完成处理方案
	1. 阻止处理器两次使用
		completionHandler.isDefined? throw new UnsupportedOperationException
	2. 处理最后一条记录
		finalValue.isDefined? handler(finalValue.get)
	
	def onFail(handler: Exception => Unit): Unit
	功能: 失败处理方案
	1. 防止失败两次提交
		failureHandler.isDefined? throw new UnsupportedOperationException
	2. 处理失败情况
		failure.isDefined?handler(failure.get) 
	
	private[spark] def setFinalValue(value: R): Unit
	功能: 设置最后一条value值 
	1. 防止记录对此提交
		finalValue.isDefined ? throw new UnsupportedOperationException
	2. 设置值信息
		completionHandler.foreach(h => h(value))
	3. 唤醒其他等待线程
		this.notifyAll()
		
	private[spark] def setFailure(exception: Exception): Unit 
	功能: 设置失败信息
	1. 防止两次提交
		failure.isDefined ? throw new UnsupportedOperationException
	2. 设置值信息
		failureHandler.foreach(h => h(exception))
	3. 唤醒等待线程
		this.notifyAll()
	
	def map[T](f: R => T) : PartialResult[T] 
	功能: 将PartialResult 转化为T
	return new PartialResult[T](f(initialVal), isFinal){
		内部设置:
		override def getFinalValue() : T = synchronized {
	     	f(PartialResult.this.getFinalValue())
	   	}
	   	override def onComplete(handler: T => Unit): PartialResult[T] = synchronized {
	    	 PartialResult.this.onComplete(handler.compose(f)).map(f)
	   	}
	  	override def onFail(handler: Exception => Unit): Unit = {
	    	synchronized {
	      	PartialResult.this.onFail(handler)
	    	}
	  	}
	  	override def toString : String = synchronized {
	    	PartialResult.this.getFinalValueInternal() match {
	      		case Some(value) => "(final: " + f(value) + ")"
	      		case None => "(partial: " + initialValue + ")"
	    	}
	  	}
	  	def getFinalValueInternal(): Option[T] = PartialResult.this.getFinalValueInternal().map(f)
	}
}
```

#### SumEvaluator

```scala
private[spark] class SumEvaluator(totalOutputs: Int, confidence: Double){
	关系: ApproximateEvaluator[StatCounter, BoundedDouble]
	介绍: 这是一个和估值器，它完成了均值估算器以及计数估值器功能。然后使用公式，对于两个独立的随机变量进行处理，获得计算结果的方差，并计算出置信区间。
	构造器参数:
		totalOutputs	输出总数
		confidence		置信值
	属性值:
		#name @outputsMerged=0 		已合并的输出数量
		#name @counter #type @StatCounter	计数器
	操作集:
	def merge(outputId: Int, taskResult: StatCounter): Unit
	功能: 合并结果集
		更新已合并输出数量@outputsMerged+=1
		计数器合并，值相加
			counter.merge(taskResult)
	
	def currentResult(): BoundedDouble
	功能: 获取当前结果
	分为3类情况考虑
	1. 以及合并了所有结果集 outputsMerged == totalOutputs
		BoundedDouble(counter.sum, 1.0, counter.sum, counter.sum)
	2. 一个结果也没合并 outputsMerged == 0 || counter.count == 0
		BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
	3. 中间情况
		+ 计算当前进度 val p = outputsMerged.toDouble / totalOutputs
		+ 计算以及合并数据的均值
			val meanEstimate = counter.mean
		+ 计算剩余数据量
			val countEstimate = counter.count * (1 - p) / p
		+ 计算已合并数值的和
			val sumEstimate = meanEstimate * countEstimate
		+ 这里有一个假定条件，即以及合并的数据方差=未合并数据的方差，则未合并数据方差
			val meanVar = counter.sampleVariance / counter.count
		根据这个方差值分为两种情况:
		1. meanVar.isNaN || counter.count == 1
			不向后计算
			BoundedDouble(counter.sum + sumEstimate, confidence, Double.NegativeInfinity, 					Double.PositiveInfinity)
		2. 后续有可以合并数据
			+ 计算计数方差值(满足负二项式)
				val countVar = counter.count * (1 - p) / (p * p)
			+ 计算和的方差
				计算公式:
				Var(Sum) = Var(Mean*Count) =[E(Mean)]^2 * Var(Count) + [E(Count)]^2 *
	            	Var(Mean) + Var(Mean) * Var(Count)
			    实际计算:
			    val sumVar = (meanEstimate * meanEstimate * countVar) +
	      			(countEstimate * countEstimate * meanVar) +
	      			(meanVar * countVar)
			+ 计算和的标准差
				val sumStdev = math.sqrt(sumVar)
			+ 获取分布
				1. 计数值>100(counter.count > 100) 使用正常分布
				2. 计数值<=100 使用T-分布
				val degreesOfFreedom = (counter.count - 1).toInt
	      		new TDistribution(degreesOfFreedom).inverseCumulativeProbability((1+confidence) / 2)
			+ 获取置信区间的上下界
				val low = sumEstimate - confFactor * sumStdev
	    		 val high = sumEstimate + confFactor * sumStdev
			返回结果集
			BoundedDouble(counter.sum+sumEstimate, confidence,counter.sum + low, counter.sum + high)
}
```



####  基础拓展

1.  @org.apache.commons.math3.distribution.PoissonDistributio
2.  泊松分布
3.  平均值，置信值，置信区间
4.  普通分布模型
5.  T-分布模型