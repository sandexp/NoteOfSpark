## **spark-util**

---

1.  [collection](# collection)
2.  [io](# io)
3.  [logging](# logging)
4.  [random](# random)

---

#### collection

1.  [AppendOnlyMap.scala](# AppendOnlyMap)

2.  [BitSet.scala](# BitSet)

3.  [CompactBuffer.scala](# CompactBuffer)

4.  [ExternalAppendOnlyMap.scala](# ExternalAppendOnlyMap)

5.  [ExternalSorter.scala](# ExternalSorter)

6.  [MedianHeap.scala](# MedianHeap)

7.  [OpenHashMap.scala](# OpenHashMap)

8.  [OpenHashSet.scala](# OpenHashSet)

9. [PairsWriter.scala](# PairsWriter)

10.  [PartitionedAppendOnlyMap.scala](# PartitionedAppendOnlyMap)

11.  [PartitionedPairBuffer.scala](# PartitionedPairBuffer)

12.  [PrimitiveKeyOpenHashMap.scala](# PrimitiveKeyOpenHashMap)

13.  [PrimitiveVector.scala](# PrimitiveVector)

14.  [SizeTracker.scala](# SizeTracker)

15.  [SizeTrackingAppendOnlyMap.scala](# SizeTrackingAppendOnlyMap)

16.  [SizeTrackingVector.scala](# SizeTrackingVector)

17.  [SortDataFormat.scala](# SortDataFormat)

18.  [Sorter.scala](# Sorter)

19.  [Spillable.scala](# Spillable)

20.  [Utils.scala](# Utils)

21.  [WritablePartitionedPairCollection.scala](# WritablePartitionedPairCollection)

    ---

    #### AppendOnlyMap

    #### BitSet

    #### CompactBuffer

    #### ExternalAppendOnlyMap

    #### ExternalSorter

    #### MedianHeap

    #### OpenHashMap

    #### OpenHashSet

    #### PairsWriter

    #### PartitionedAppendOnlyMap

    #### PartitionedPairBuffer

    #### PrimitiveKeyOpenHashMap

    #### PrimitiveVector

    #### SizeTracker

    #### SizeTrackingAppendOnlyMap

    #### SizeTrackingVector

    #### SortDataFormat

    #### Sorter

    #### Spillable

    #### Utils

    #### WritablePartitionedPairCollection

#### io

1.  [ChunkedByteBuffer.scala](# ChunkedByteBuffer)

2.  [ChunkedByteBufferFileRegion.scala](# ChunkedByteBufferFileRegion)

3.  [ChunkedByteBufferOutputStream.scala](# ChunkedByteBufferOutputStream)

   ---

   #### ChunkedByteBuffer

   #### ChunkedByteBufferFileRegion

   #### ChunkedByteBufferOutputStream

#### logging

1.  [DriverLogger.scala](# DriverLogger)

2.  [FileAppender.scala](# FileAppender)

3.  [RollingFileAppender.scala](# RollingFileAppender)

4.  [RollingPolicy.scala](# RollingPolicy)

   ---

   #### DriverLogger

   #### FileAppender

   #### RollingFileAppender

   #### RollingPolicy

#### random

1.  [Pseudorandom.scala](# Pseudorandom)

2.  [RandomSampler.scala](# RandomSampler)

3.  [SamplingUtils.scala](# SamplingUtils)

4.  [StratifiedSamplingUtils.scala](# StratifiedSamplingUtils)

5.  [XORShiftRandom.scala](# XORShiftRandom)

   ---

   #### Pseudorandom

   ```scala
   @DeveloperApi
   trait Pseudorandom {
       介绍: 伪随机类
       操作集:
       def setSeed(seed: Long): Unit
       功能: 设置伪随机的随机种子
   }
   ```

   #### RandomSampler

   ```markdown
   介绍:
   	随机采样器,可以改变采用类型,例如,如果我们想要添加权重用于分层采样器.仅仅需要使用连接到采样器的转换,而且不能使用在采样后.
   	参数介绍:
   	T	数据类型
   	U	采样数据类型
   ```

   ```scala
   @DeveloperApi
   trait RandomSampler[T, U] extends Pseudorandom with Cloneable with Serializable {
       操作集:
       def sample(items: Iterator[T]): Iterator[U]
       功能: 获取随机采样器
       val= items.filter(_ => sample > 0).asInstanceOf[Iterator[U]]
       
       def sample(): Int
       功能: 是否去采样下一个数据,返回需要多少次下个数据才会被采样.如果返回0,就没有被采样过.
       
       def clone: RandomSampler[T, U]
       功能: 获取一个随机采样器的副本(被禁用)
       val= throw new UnsupportedOperationException("clone() is not implemented.")
   }
   ```

   ```scala
   private[spark] object RandomSampler {
   	属性: 
       #name @defaultMaxGapSamplingFraction = 0.4	默认最大采集
       当采集部分小于这个值的时候,就会采取采集间隙优化策略.假定传统的伯努力采样更快,这个优化值会依赖于RNG.更有价值的RNG会使得这个优化值更大.决定值的最可靠方式就是通过实验获取一个新值.希望在大多数情况下使用 0.5作为初始猜想值.
       #name @rngEpsilon = 5e-11	随机点邻域大小
       默认的RNG采集点邻域大小.采集间距计算逻辑需要计算 log(x)值.x采样于RNG.为了防止获取到 log(0)而引发错误.就使用了正向邻域的下界限.一个优秀的取值是在或者通过@nextDouble() 返回的邻近最小正浮点数的值.
       #name @roundingEpsilon = 1e-6
       采样部分的参数是计算的结果,控制浮点指针.使用这个邻域的溢出因子去阻止假的警告.比如计算数的和用户获取
       1.000000001的采样值.
   }
   ```

   ```scala
   @DeveloperApi
   class BernoulliCellSampler[T](lb: Double, ub: Double, complement: Boolean = false) 
   extends RandomSampler[T, T]{
       介绍: 基于伯努利实验用于分割数据列表,伯努利单体采样器
       构造器参数:
       	lb	接受范围下界
       	ub	接受范围上界
       	complement	是否使用指定范围的实现,默认为false
       	T 数据类型
       属性:
       #name @rng: Random = new XORShiftRandom	随机值RNG
       参数断言:
       require(
       lb <= (ub + RandomSampler.roundingEpsilon),
       	s"Lower bound ($lb) must be <= upper bound ($ub)")
     require(
       lb >= (0.0 - RandomSampler.roundingEpsilon),
       	s"Lower bound ($lb) must be >= 0.0")
     require(
       ub <= (1.0 + RandomSampler.roundingEpsilon),
       	s"Upper bound ($ub) must be <= 1.0")
       
       操作集:
       def setSeed(seed: Long): Unit = rng.setSeed(seed)
   	功能: 设置随机值RNG的随机种子值
       
       def sample(): Int
       功能: 获取随机数值
       val= if (ub - lb <= 0.0) {// 非法界限 处理
         if (complement) 1 else 0
       } else { // 获取下个伯努利实现结果
         val x = rng.nextDouble()
         val n = if ((x >= lb) && (x < ub)) 1 else 0 // 确定下个随机值是否处于范围内
         if (complement) 1 - n else n //获取随机值
       }
       
       def cloneComplement(): BernoulliCellSampler[T]
       功能: 获取指定范围内采样器的实现
       val= new BernoulliCellSampler[T](lb, ub, !complement)
       
       def clone: BernoulliCellSampler[T] = new BernoulliCellSampler[T](lb, ub, complement)
       功能: 克隆一份伯努利采样器
   }
   ```

   ```scala
   @DeveloperApi
   class BernoulliSampler[T: ClassTag](fraction: Double) extends RandomSampler[T, T]{
       介绍: 基于伯努利实验的采样器
       构造器参数:
       	fraction	采样因子
       参数断言:
       require(
           fraction >= (0.0 - RandomSampler.roundingEpsilon)
             && fraction <= (1.0 + RandomSampler.roundingEpsilon),
           s"Sampling fraction ($fraction) must be on interval [0, 1]")
       采用因子在0的左邻域到1的右邻域之间
       属性:
       #name @rng: Random = RandomSampler.newDefaultRNG	随机值
       #name @val gapSampling: GapSampling =
       	new GapSampling(fraction, rng, RandomSampler.rngEpsilon)
       采样间隔
       操作集:
       def setSeed(seed: Long): Unit = rng.setSeed(seed)
       功能: 设置随机采用值的种子值
       
       def sample(): Int
       功能: 获取采样值
       val= if (fraction <= 0.0) {
         0
       } else if (fraction >= 1.0) {
         1
       } else if (fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
         gapSampling.sample() // 小于最大采样因子,则就是目前采样间隔的采样值
       } else {
         if (rng.nextDouble() <= fraction) {
           1
         } else {
           0
         }
       }
       
       def clone: BernoulliSampler[T] = new BernoulliSampler[T](fraction)
       功能: 获取一份伯努利采用副本
   }
   ```

   ```scala
   private[spark] class GapSampling(f: Double,rng: Random = RandomSampler.newDefaultRNG,
       epsilon: Double = RandomSampler.rngEpsilon) extends Serializable {
       介绍: 间隔采样
       构造器参数:
       	f	种子值
       	rng	采样器值
       	epsilon	邻域大小
       断言参数:
       require(f > 0.0  &&  f < 1.0, s"Sampling fraction ($f) must reside on open interval (0, 1)")
       require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")
       
       初始化操作:
       advance()
       功能: 获取第一个采样值作为构造器对象
       
       属性:
       #name @lnq = math.log1p(-f)	lnq值
       #name @countForDropping: Int = 0	弃用计数值
       
       def advance(): Unit
       功能: 决定元素值会不会被舍弃
       val u = math.max(rng.nextDouble(), epsilon)
       countForDropping = (math.log(u) / lnq).toInt
       
       def sample(): Int
       功能: 采样状态位,为1表示采样,为0表示不采用
       val= if (countForDropping > 0) {
         countForDropping -= 1
         0
       } else {
         advance()
         1
       }
   }
   ```

   

   ```scala
   @DeveloperApi
   class PoissonSampler[T](fraction: Double,
       useGapSamplingIfPossible: Boolean) extends RandomSampler[T, T]{
       构造器参数:
       fraction	采样因子
       useGapSamplingIfPossible	是否可以使用间隔采样,为true时当泊松采样比率较低,则切换为等距离采样
       属性断言:
       require(
           fraction >= (0.0 - RandomSampler.roundingEpsilon),
           s"Sampling fraction ($fraction) must be >= 0")
       属性:
       #name @rng=PoissonDistribution(if (fraction > 0.0) fraction else 1.0)  泊松采样值
       #name @rngGap = RandomSampler.newDefaultRNG	等距离采样值
       #name @gapSamplingReplacement=
       	new GapSamplingReplacement(fraction, rngGap, RandomSampler.rngEpsilon)
   		等距离采样替代方案
       操作集:
       def setSeed(seed: Long): Unit
       功能: 设置种子值
       rng.reseedRandomGenerator(seed)
       rngGap.setSeed(seed)
       
       def sample(): Int
       功能: 获取采样值
       val= if (fraction <= 0.0) {
         0
       } else if (useGapSamplingIfPossible && // 采样因子足够小,则使用等距离采样
                  fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
         gapSamplingReplacement.sample()
       } else { // 泊松采样
         rng.sample()
       }
       
       def sample(items: Iterator[T]): Iterator[T]
       功能: 获取指定类型数据的采样值
       val= if (fraction <= 0.0) {
         Iterator.empty
       } else {
         val useGapSampling = useGapSamplingIfPossible &&
           fraction <= RandomSampler.defaultMaxGapSamplingFraction
         items.flatMap { item =>
           val count = if (useGapSampling) gapSamplingReplacement.sample() else rng.sample()
           if (count == 0) Iterator.empty else Iterator.fill(count)(item)
         }
       }
       
       def clone: PoissonSampler[T] = new PoissonSampler[T](fraction, useGapSamplingIfPossible)
       功能: 获取一份泊松采样器
   }
   ```

   ```scala
   private[spark] class GapSamplingReplacement(val f: Double,
       val rng: Random = RandomSampler.newDefaultRNG,
       epsilon: Double = RandomSampler.rngEpsilon) extends Serializable {
       构造器属性:
       	f	采样因子
       	rng	采样随机值
       	epsilon	邻域大小
       属性断言:
       require(f > 0.0, s"Sampling fraction ($f) must be > 0")
       require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")
       
       属性:
       #name @q = math.exp(-f)	
       #name @countForDropping: Int = 0	抛弃采样值计数器
       
       初始化操作:
       advance()
       功能: 跳到第一个采样值
       
       操作集:
       def poissonGE1: Int
       功能: 获取泊松分布采样值
       val= {
   	   // 模拟标准泊松分布采样
           var pp = q + ((1.0 - q) * rng.nextDouble())
           var r = 1
           pp *= rng.nextDouble()
           while (pp > q) {
             r += 1
             pp *= rng.nextDouble()
           }
           r
         }
       
       def sample(): Int
       功能: 获取采样值
       val= if (countForDropping > 0) {
         countForDropping -= 1
         0
       } else {
         val r = poissonGE1
         advance()
         r
       }
       
       def advance(): Unit
       功能: 跳过不需要采样的点
   }
   ```

   

   #### SamplingUtils

   #### StratifiedSamplingUtils

   #### XORShiftRandom

   ```scala
   private[spark] class XORShiftRandom(init: Long) extends JavaRandom(init) {
       构造器属性:
       	init	初始化值
       属性:
       #name @seed = XORShiftRandom.hashSeed(init)	种子
       操作集:
       def next(bits: Int): Int
       功能: 获取下一个随机值
       val= {
           var nextSeed = seed ^ (seed << 21)
           nextSeed ^= (nextSeed >>> 35)
           nextSeed ^= (nextSeed << 4)
           seed = nextSeed
           (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
         }
       
       def setSeed(s: Long): Unit
       功能: 设置种子信息
       seed = XORShiftRandom.hashSeed(s)
   }
   ```

   ```scala
   private[spark] object XORShiftRandom {
       介绍: 运行RNG的基准
       操作集:
       def hashSeed(seed: Long): Long
       功能: 对种子进行散列
       val bytes = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(seed).array()
       val lowBits = MurmurHash3.bytesHash(bytes, MurmurHash3.arraySeed)
       val highBits = MurmurHash3.bytesHash(bytes, lowBits)
       (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
   }
   ```

   

   #### 基础拓展

   1.  伯努利分布 :  [伯努利分布](https://zh.wikipedia.org/wiki/伯努利分布)
   2.  泊松分布: http://en.wikipedia.org/wiki/Poisson_distribution
   3.  XorShift随机采样 https://en.wikipedia.org/wiki/Xorshift