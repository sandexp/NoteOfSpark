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

    ```markdown
介绍:
    	简单的快速hash表,在只添加模式下进行了优化,key不会被移除,但是value可以被改变
	实现采样了平方探测法,保证了可以所有key的位置
    	这个map可以支持高达375809638(0.7*2^29)个元素的存储
    ```
    
    ```scala
    @DeveloperApi
    class AppendOnlyMap[K, V](initialCapacity: Int = 64) extends Iterable[(K, V)] with Serializable{
        构造器参数:
        initialCapacity	初始容量64
        参数断言:
	    require(initialCapacity <= MAXIMUM_CAPACITY,
        s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
        require(initialCapacity >= 1, "Invalid initial capacity")
        属性:
        #name @LOAD_FACTOR = 0.7	加载因子
        #name @capacity = nextPowerOf2(initialCapacity)	容量大小
	    #name @mask = capacity - 1	掩码
        #name @curSize = 0	当前规模大小
        #name @growThreshold = (LOAD_FACTOR * capacity).toInt	增长容量(超过这个值就会引发扩容)
        #name @data = new Array[AnyRef](2 * capacity)	底层存储数组(按照key/value存储)
        #name @haveNullValue = false	是否含有空值
        #name @nullValue: V = null.asInstanceOf[V]	空值
        #name @destroyed = false	迭代器是否损坏(为true则底层存储数组不会再被使用)
        #name @destructionMessage = "Map state is invalid from destructive sorting!"	损坏信息
       	操作集:
        def apply(key: K): V 
        功能: 获取指定key的value值
        操作条件: 底层数据结构没有损坏
        assert(!destroyed, destructionMessage)
        0. 空值处理
        val k = key.asInstanceOf[AnyRef]
        if (k.eq(null)) {
          return nullValue
        }
        1. 非空值处理
        var pos = rehash(k.hashCode) & mask // 获取位置
        var i = 1
        while (true) { // 平方探测法获取实际位置
          val curKey = data(2 * pos)
          if (k.eq(curKey) || k.equals(curKey)) {
            return data(2 * pos + 1).asInstanceOf[V]
          } else if (curKey.eq(null)) {
            return null.asInstanceOf[V]
          } else {
            val delta = i
            pos = (pos + delta) & mask
            i += 1
          }
        }
        val= null.asInstanceOf[V] // 探测不到的值
        
        def update(key: K, value: V): Unit
        功能: 更新key的值为value
        操作条件: 底层存储可以使用
        assert(!destroyed, destructionMessage)
        0. 空值处理
        if (k.eq(null)) {
          if (!haveNullValue) {
            incrementSize()
          }
          nullValue = value
          haveNullValue = true
          return
        }
        1. 设置非空key(使用平方探测法定位)
        var pos = rehash(key.hashCode) & mask
        var i = 1
        while (true) {
          val curKey = data(2 * pos)
          if (curKey.eq(null)) {
            data(2 * pos) = k
            data(2 * pos + 1) = value.asInstanceOf[AnyRef]
            incrementSize()  // Since we added a new key
            return
          } else if (k.eq(curKey) || k.equals(curKey)) {
            data(2 * pos + 1) = value.asInstanceOf[AnyRef]
            return
          } else {
            val delta = i
            pos = (pos + delta) & mask
            i += 1
          }
        }
        
        def size: Int = curSize
        功能: 获取规模大小
        
        def incrementSize(): Unit
        功能: hash表规模+1,如果可能的话进行rehash
        curSize += 1
        if (curSize > growThreshold) {
          growTable()
        }
        
        def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()
        功能: rehash
        
        def iterator: Iterator[(K, V)]
        功能: 获取迭代器
        操作条件: 底层存储系统可用
        assert(!destroyed, destructionMessage)
        1. 获取迭代器
        val= new Iterator[(K, V)] {
          var pos = -1 // 位置指针
          def nextValue(): (K, V) = {
            if (pos == -1) {    //首值处理(可能是控制)
              if (haveNullValue) {
                return (null.asInstanceOf[K], nullValue)
              }
              pos += 1
            }
            while (pos < capacity) {  // 非首个元素的kv处理
              if (!data(2 * pos).eq(null)) {
                return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
              }
              pos += 1
            }
            null // 过界处理
          }
          override def hasNext: Boolean = nextValue() != null // 确认是否有下一个元素
          override def next(): (K, V) = { // 获取下一个kv二元组
            val value = nextValue()
            if (value == null) {
              throw new NoSuchElementException("End of iterator")
            }
            pos += 1
            value
          }
        }
        
        def changeValue(key: K, updateFunc: (Boolean, V) => V): V
        功能: 改变key对应的value值为更新函数@updateFunc	指定的value值
        操作条件: 底层存储数组可以使用
        assert(!destroyed, destructionMessage)
        1. 空值更新处理
        val k = key.asInstanceOf[AnyRef]
        if (k.eq(null)) {
          if (!haveNullValue) {// 原集合中没null,则需要新插入一个null,先扩容
            incrementSize()
          }
          nullValue = updateFunc(haveNullValue, nullValue) // 获取更新的value值
          haveNullValue = true // 修改空值记录标志位
          return nullValue
        }
        2. 计算k的位置
        var pos = rehash(k.hashCode) & mask
        3. 使用平方探测法更新指定kv二元组
        while (true) {
          val curKey = data(2 * pos)
          if (curKey.eq(null)) {
            val newValue = updateFunc(false, null.asInstanceOf[V])
            data(2 * pos) = k
            data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
            incrementSize()
            return newValue
          } else if (k.eq(curKey) || k.equals(curKey)) {
            val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
            data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
            return newValue
          } else {
            val delta = i
            pos = (pos + delta) & mask
            i += 1
          }
        }
        null.asInstanceOf[V] // 这个地方一般情况下到达不了
        
        def atGrowThreshold: Boolean = curSize == growThreshold
        功能: 确定是否再插入一个值就会导致表扩容
        
        def nextPowerOf2(n: Int): Int
        功能： 获取当前@n的2倍
        val highBit = Integer.highestOneBit(n)
        val= if (highBit == n) n else highBit << 1
        
        def growTable(): Unit
        功能: hash表扩容2倍,并进行rehash
        1. 扩容并设置扩容后的参数
        val newCapacity = capacity * 2
        require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
        val newData = new Array[AnyRef](2 * newCapacity)
        val newMask = newCapacity - 1
        2. 将旧的存储数组中的数据迁移到新的存储数组中(使用平方探测法确定新数组的位置)
        var oldPos = 0
        while (oldPos < capacity) {
          if (!data(2 * oldPos).eq(null)) {
            val key = data(2 * oldPos)
            val value = data(2 * oldPos + 1)
            var newPos = rehash(key.hashCode) & newMask
            var i = 1
            var keepGoing = true
            while (keepGoing) {
              val curKey = newData(2 * newPos)
              if (curKey.eq(null)) {
                newData(2 * newPos) = key
                newData(2 * newPos + 1) = value
                keepGoing = false
              } else {
                val delta = i
                newPos = (newPos + delta) & newMask
                i += 1
              }
            }
          }
          oldPos += 1
        }
        3. 更新map中的属性
        data = newData
        capacity = newCapacity
        mask = newMask
        growThreshold = (LOAD_FACTOR * newCapacity).toInt
        
        def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] 
        功能: 返回排序过后的迭代器,这里提供了一种不使用额外内存的排序方式,代价就是损坏map的正确性
        1. 设置一个新的索引范围,用于容纳非空值
        var keyIndex, newIndex = 0
        while (keyIndex < capacity) {
          if (data(2 * keyIndex) != null) {
            data(2 * newIndex) = data(2 * keyIndex)
            data(2 * newIndex + 1) = data(2 * keyIndex + 1)
            newIndex += 1
          }
          keyIndex += 1
        }
    	2. 参数合法性断言
        assert(curSize == newIndex + (if (haveNullValue) 1 else 0))
        3. 对新的非空数据进行排序 // 这里内部的timsort会改变底层存储数组的顺序,故而map的正确性得不到保障
        new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)
        4. 获取迭代器
        val= new Iterator[(K, V)] {
          var i = 0
          var nullValueReady = haveNullValue
          def hasNext: Boolean = (i < newIndex || nullValueReady)
          def next(): (K, V) = {
            if (nullValueReady) {
              nullValueReady = false
              (null.asInstanceOf[K], nullValue)
            } else {
              val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
              i += 1
              item
            }
          }
        }
    }
    ```
    
    ```scala
    private object AppendOnlyMap {
        属性:
        #name @MAXIMUM_CAPACITY = (1 << 29)	最大容量
    }
    ```
    
    #### BitSet
    
    ```scala
    class BitSet(numBits: Int) extends Serializable {
        介绍: 简单,定长位集合实现,这个实现很快因为不要安全/越界检查
    	主要用于存放bit位(0/1)
       	构造器参数:
        numBits	字节数
        属性:
    	#name @words = new Array[Long](bit2words(numBits))	字数列表(每个位)
        #name @numWords = words.length	字长
    操作集:
        def capacity: Int = numWords * 64
        功能: 计算位集合的容量
        
    	def clear(): Unit = Arrays.fill(words, 0)
        功能: 清理位集合
    
        def setUntil(bitIndex: Int): Unit
        功能: 超过指定@bitIndex 位置的置位(在0-index位置填充-1)
        val wordIndex = bitIndex >> 6 
        Arrays.fill(words, 0, wordIndex, -1)
        if(wordIndex < words.length) {
          val mask = ~(-1L << (bitIndex & 0x3f))
          words(wordIndex) |= mask
        }
        
        def clearUntil(bitIndex: Int): Unit
        功能: 清除超过指定@bitIndex 的所有位
        val wordIndex = bitIndex >> 6 
        Arrays.fill(words, 0, wordIndex, 0)
        if(wordIndex < words.length) {
          val mask = -1L << (bitIndex & 0x3f)
          words(wordIndex) &= mask
        }
        
        def &(other: BitSet): BitSet
        功能: 与指定位集合@other 的按位与
        1. 创建一个新的位集合用于记录结果
        val newBS = new BitSet(math.max(capacity, other.capacity))
        val smaller = math.min(numWords, other.numWords)
        2. 新的位集合参数断言
        assert(newBS.numWords >= numWords)
        assert(newBS.numWords >= other.numWords)
        3. 两个集合按位与
        var ind = 0
        while( ind < smaller ) {
          newBS.words(ind) = words(ind) & other.words(ind)
          ind += 1
        }
        val= newBS
        
        def |(other: BitSet): BitSet
        功能: 计算两个集合的按位或
        1. 创建一个新的位集合用于记录结果,并断言
        val newBS = new BitSet(math.max(capacity, other.capacity))
        assert(newBS.numWords >= numWords)
        assert(newBS.numWords >= other.numWords)
        val smaller = math.min(numWords, other.numWords)
        2. 共同部分按位或
        var ind = 0
        while( ind < smaller ) {
          newBS.words(ind) = words(ind) | other.words(ind)
          ind += 1
        }
        3. 超出部分按位或
        while( ind < numWords ) {
          newBS.words(ind) = words(ind)
          ind += 1
        }
        while( ind < other.numWords ) {
          newBS.words(ind) = other.words(ind)
          ind += 1
        }
        val= newBS
        
        def ^(other: BitSet): BitSet
        功能: 按位异或
        1. 创建一个新的位集合用于记录结果,并断言
        val newBS = new BitSet(math.max(capacity, other.capacity))
        val smaller = math.min(numWords, other.numWords) // 计算共同部分
        2. 共同部分异或
        var ind = 0
        while (ind < smaller) {
          newBS.words(ind) = words(ind) ^ other.words(ind)
          ind += 1
        }
        3. 超出部分处理 --> 直接拷贝多余的部分
        if (ind < numWords) {
          Array.copy( words, ind, newBS.words, ind, numWords - ind )
        }
        if (ind < other.numWords) {
          Array.copy( other.words, ind, newBS.words, ind, other.numWords - ind )
        }
        val= newBS
        
        def set(index: Int): Unit
        功能: 设置指定位置@Index 的值为true
        val bitmask = 1L << (index & 0x3f)
        words(index >> 6) |= bitmask 
        
        def unset(index: Int): Unit 
        功能: 解除指定位置@index的置位状态
        val bitmask = 1L << (index & 0x3f)
        words(index >> 6) &= ~bitmask 
        
        def iterator: Iterator[Int]
        功能: 获取位集合的迭代器
        val= new Iterator[Int] {
            var ind = nextSetBit(0) // 下个位指针
            override def hasNext: Boolean = ind >= 0
            override def next(): Int = {
              val tmp = ind
              ind = nextSetBit(ind + 1)
              tmp
            }
          }
        
        def cardinality(): Int
        功能: 获取位集合中处于置位状态的元素个数
        var sum = 0
        var i = 0
        while (i < numWords) {
          sum += java.lang.Long.bitCount(words(i))
          i += 1
        }
        val= sum
        
        def get(index: Int): Boolean
        功能: 获取指定位置@index 的值
        val bitmask = 1L << (index & 0x3f)
        (words(index >> 6) & bitmask) != 0
        
        def bit2words(numBits: Int) = ((numBits - 1) >> 6) + 1
        功能: 获取指定位数@numBits可容纳字数
        
        def nextSetBit(fromIndex: Int): Int
        功能: 返回在@fromIndex之后第一个置位的位置,没有则返回-1
        1. 获取子位置
        var wordIndex = fromIndex >> 6
        if (wordIndex >= numWords) {
          return -1
        }
        2. 获取当前字中下一个置位位置
        val subIndex = fromIndex & 0x3f
        var word = words(wordIndex) >> subIndex
        if (word != 0) {
          return (wordIndex << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word)
        }
        3. 当前字没有找到,找后面的字中第一个置位元素
        wordIndex += 1
        while (wordIndex < numWords) {
          word = words(wordIndex)
          if (word != 0) {
            return (wordIndex << 6) + java.lang.Long.numberOfTrailingZeros(word)
          }
          wordIndex += 1
        }
        4. 其余情况,都表示找不到
        val= -1
    }
    ```
    
    #### CompactBuffer
    
    ```markdown
    介绍:
    	紧凑缓冲区,类似于@ArrayBuffer,但是对于多个小的缓冲区,在内存上更加高效.
    	ArrayBuffer总是分配内存去存储对象,默认情况下也有16条记录的空间,所有他需要大约80-100字节的开销.相反,紧凑缓冲区,保证了主对象超过两个元素,只有当超过这个数值时,才会分配数字内存空间存储元素.使得少量数据的groupBy这类操作更加高效.
    ```
    
    ```scala
    private[spark] class CompactBuffer[T: ClassTag] extends Seq[T] with Serializable {
        属性:
        #Name @element0: T = _
        #name @element1: T = _
        #name @curSize = 0	元素总数量
        #name @otherElements: Array[T] = null	其他元素列表
        操作集:
        def apply(position: Int): T
        功能: 获取指定位置@position的元素
        if (position < 0 || position >= curSize) {
          throw new IndexOutOfBoundsException
        }
        val= if (position == 0) {
          element0
        } else if (position == 1) {
          element1
        } else {
          otherElements(position - 2)
        }
        
        def update(position: Int, value: T): Unit
        功能: 更新指定位置@postion元素值
        if (position < 0 || position >= curSize) {
          throw new IndexOutOfBoundsException
        }
        if (position == 0) {
          element0 = value
        } else if (position == 1) {
          element1 = value
        } else {
          otherElements(position - 2) = value
        }
        
        def += (value: T): CompactBuffer[T]
        功能: 将指定元素@value 添加到紧凑缓冲区中
        val newIndex = curSize // 获取写入指针
        if (newIndex == 0) {
          element0 = value
          curSize = 1
        } else if (newIndex == 1) {
          element1 = value
          curSize = 2
        } else {
          growToSize(curSize + 1)
          otherElements(newIndex - 2) = value
        }
        val= this
        
        def length: Int = curSize
        功能: 获取缓冲区大小
        
        def iterator: Iterator[T]
        功能: 获取迭代器
        val= new Iterator[T] {
            private var pos = 0
            override def hasNext: Boolean = pos < curSize
            override def next(): T = {
              if (!hasNext) {
                throw new NoSuchElementException
              }
              pos += 1
              apply(pos - 1)
            }
          }
        
        def growToSize(newSize: Int): Unit
        功能: 扩容紧凑缓冲区,有必要的化扩充底层数组长度
        val newArraySize = newSize - 2
        val arrayMax = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
        if (newSize < 0 || newArraySize > arrayMax) {
          throw new UnsupportedOperationException(s"Can't grow buffer past $arrayMax elements")
        }
        val capacity = if (otherElements != null) otherElements.length else 0
        if (newArraySize > capacity) {
          var newArrayLen = 8L
          while (newArraySize > newArrayLen) {
            newArrayLen *= 2
          }
          if (newArrayLen > arrayMax) {
            newArrayLen = arrayMax
          }
          val newArray = new Array[T](newArrayLen.toInt)
          if (otherElements != null) {
            System.arraycopy(otherElements, 0, newArray, 0, otherElements.length)
          }
          otherElements = newArray
        }
        curSize = newSize
        
        def ++= (values: TraversableOnce[T]): CompactBuffer[T]
        功能: 合并一列数据到当前紧凑缓冲区
        values match {
            // 合并紧凑缓冲区时可以在cogroup和groupByKey时对其进行优化合并操作
          case compactBuf: CompactBuffer[T] =>
            val oldSize = curSize
            // 获取并入缓冲区的大小和数据元素
            val itsSize = compactBuf.curSize
            val itsElements = compactBuf.otherElements
            growToSize(curSize + itsSize) // 缓冲区扩容
            if (itsSize == 1) { // 仅仅并入了一个元素则直接在源Buffer之后追加一个
              this(oldSize) = compactBuf.element0
            } else if (itsSize == 2) {//仅仅并入了两个个元素则直接在源Buffer之后追加两个
              this(oldSize) = compactBuf.element0
              this(oldSize + 1) = compactBuf.element1
            } else if (itsSize > 2) {// 超过两个则需要将从原来数组拷贝到本数组的指定位置
              this(oldSize) = compactBuf.element0
              this(oldSize + 1) = compactBuf.element1
              System.arraycopy(itsElements, 0, otherElements, oldSize, itsSize - 2)
            }
    
          case _ =>
            values.foreach(e => this += e) // 不是紧凑缓冲区直接添加即可
        }
    }
    ```
    
    ```scala
    private[spark] object CompactBuffer {
        操作集:
        def apply[T: ClassTag](): CompactBuffer[T] = new CompactBuffer[T]
        功能: 获取紧凑缓冲区实例
        
        def apply[T: ClassTag](value: T): CompactBuffer[T] 
        功能: 获取添加有知道value的紧凑缓冲区
        val buf = new CompactBuffer[T]
        buf += value
    }
    ```
    
    #### ExternalAppendOnlyMap
    
    ```markdown
    介绍:
    	外部只添加类型map,溢写排序完成的内容到磁盘上,这种情况会在没有足够的空间去扩容时发生.
    	这个map会越过这两类数据
    	1. 合并到combiner中的数据,这类数据必然会排序和溢写到磁盘上.
    	2. combiner读取磁盘数据并相互合并
    	溢写容量的设置遵照下述方式:
    	如果溢出容量过大,内存中的map数据会占用超过可用内存的内存量,就会导致OOM,然而如果溢写容量设置的过小,溢写频率上述,会增加不必要的磁盘溢写工作.这种情况比较不使用溢写的@AppendOnlyMap 性能退化了.
    ```
    
    ```scala
    @DeveloperApi
    class ExternalAppendOnlyMap[K, V, C](
        createCombiner: V => C,
        mergeValue: (C, V) => C,
        mergeCombiners: (C, C) => C,
        serializer: Serializer = SparkEnv.get.serializer,
        blockManager: BlockManager = SparkEnv.get.blockManager,
        context: TaskContext = TaskContext.get(),
        serializerManager: SerializerManager = SparkEnv.get.serializerManager)
    extends Spillable[SizeTracker](context.taskMemoryManager())
    with Serializable
    with Logging
    with Iterable[(K, C)]{
        构造器参数:
        createCombiner	combiner创建函数
        mergeValue	合并函数
        mergeCombiners	combiner合并函数
        serializer	序列化器
        blockManager	块管理器
        context	任务上下文管理器
        serializerManager	序列化管理器
        属性:
        #name @currentMap = new SizeTrackingAppendOnlyMap[K, C]	字节量大小追踪hashmap
        #name @spilledMaps = new ArrayBuffer[DiskMapIterator]	溢写迭代器列表
        #name @sparkConf = SparkEnv.get.conf	应用程序配置集
        #name @diskBlockManager = blockManager.diskBlockManager	磁盘块管理器
        #name @serializerBatchSize = sparkConf.get(config.SHUFFLE_SPILL_BATCH_SIZE)
        	序列化批量大小
        #name @_diskBytesSpilled = 0L	磁盘溢写量
        #name @fileBufferSize = sparkConf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024
        	文件缓冲大小(单位字节)
        #name @writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics() 写度量器
        #name @_peakMemoryUsedBytes: Long = 0L	峰值内存使用量
        #name @keyComparator = new HashComparator[K]	key比较器
        #name @ser = serializer.newInstance()	序列化实例
        #name @readingIterator: SpillableIterator = null	读取迭代器
        构造器及初始化操作:
        if (context == null) {
            throw new IllegalStateException(
              "Spillable collections should not be instantiated outside of tasks")
          }
        功能: 任务内才可以溢写
        
        def this(
          createCombiner: V => C,
          mergeValue: (C, V) => C,
          mergeCombiners: (C, C) => C,
          serializer: Serializer,
          blockManager: BlockManager) {
        this(createCombiner, mergeValue, mergeCombiners, serializer, blockManager, TaskContext.get())
      }
        功能: 向后兼容二进制的构造器
        操作集:
        def diskBytesSpilled: Long = _diskBytesSpilled
        功能: 获取磁盘字节溢写量
        
        def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes
        功能: 获取峰值内存使用量
        
        def numSpills: Int = spilledMaps.size
        功能: 获取磁盘溢写量
        
        def insert(key: K, value: V): Unit
        功能: 插入一个键值对到map中
        insertAll(Iterator((key, value)))
        
        def insertAll(entries: Iterator[Product2[K, V]]): Unit
        功能: 插入指定迭代器到map中
        操作条件: 当前map存在
        if (currentMap == null) {
          throw new IllegalStateException(
            "Cannot insert new elements into a map after calling iterator")
        }
        1. 使用更新函数,确定是否key是需要更新(value的合并),还是需要新建
        var curEntry: Product2[K, V] = null
        val update: (Boolean, C) => C = (hadVal, oldVal) => {
          if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
        }
        2. 判断是否需要对当前map进行溢写,并更新度量参数:峰值内存使用量@_peakMemoryUsedBytes
        while (entries.hasNext) {
          curEntry = entries.next()
          val estimatedSize = currentMap.estimateSize()
          if (estimatedSize > _peakMemoryUsedBytes) { // 更新度量值: 峰值内存使用量
            _peakMemoryUsedBytes = estimatedSize
          }
          if (maybeSpill(currentMap, estimatedSize)) { // 确定是否需要溢写,如果发生溢写则重新执行当前map
            currentMap = new SizeTrackingAppendOnlyMap[K, C]
          }
          currentMap.changeValue(curEntry._1, update) // 更新内存map中当前key的value,使用更新函数@update
          addElementsRead() // 计量读取元素数量(自从上次溢写以来)
        }
        
        def insertAll(entries: Iterable[Product2[K, V]]): Unit
        功能: 插入给定的迭代器到map中
        	当底层map需要扩容时,检查当前shuffle内存池中是否有足够的内存分配,如果有则分配内存并对map进行扩容,	否则需要溢写内存map到磁盘上.
       	insertAll(entries.iterator)
        
        def spill(collection: SizeTracker): Unit 
        功能: 对内存map存在的内容进行排序,溢写到磁盘上,命名为临时文件
        val inMemoryIterator = currentMap.destructiveSortedIterator(keyComparator)
        val diskMapIterator = spillMemoryIteratorToDisk(inMemoryIterator)
        spilledMaps += diskMapIterator
        
        def forceSpill(): Boolean 
        功能: 强制溢写当前内存map到磁盘,并释放内存空间.调用时机为任务内存不足时,任务内存管理器调用.释放成功返		回true
        val= if (readingIterator != null) {
              // 溢写并释放内存(溢写读取迭代器中的内容)
              val isSpilled = readingIterator.spill()
              if (isSpilled) {
                currentMap = null
              }
              isSpilled
            } else if (currentMap.size > 0) { // 溢写当前map中的内容
              spill(currentMap)
              currentMap = new SizeTrackingAppendOnlyMap[K, C]
              true
            } else {
              false
            }
        
        def flush(): Unit 
    	功能: 刷新磁盘写出器的内容到磁盘上,并且更新相关变量
        val segment = writer.commitAndGet() // 提交并获取文件段
        batchSizes += segment.length // 更新溢写列表
        _diskBytesSpilled += segment.length // 更新磁盘溢写字节量
        objectsWritten = 0 // 重置写出对象数量
        
        def spillMemoryIteratorToDisk(inMemoryIterator: Iterator[(K, C)])
          : DiskMapIterator
        功能: 溢写内存迭代器的内容到磁盘上
        输入参数: inMemoryIterator	内存迭代器
        1. 创建临时块,并根据这个临时块创建写出器实例
        val (blockId, file) = diskBlockManager.createTempLocalBlock()
        val writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, writeMetrics)
        var objectsWritten = 0 // 初始化写出量
        2. 创建溢写字节数量列表
        val batchSizes = new ArrayBuffer[Long]
        3. 溢写迭代器内容
        while (inMemoryIterator.hasNext) {
            // 获取迭代器的内容并进行溢写
            val kv = inMemoryIterator.next()
            writer.write(kv._1, kv._2)
            objectsWritten += 1
            if (objectsWritten == serializerBatchSize) {
              flush() // 批量延时溢写,降低IO频度
            }
          }
         // 溢写最后的参与数据 
          if (objectsWritten > 0) {
            flush()
            writer.close()
          } else {
            writer.revertPartialWritesAndClose()
          }
          success = true
        } finally {
          if (!success) {  // 溢写失败处理方案
            // 获取写出失败的文件,并将其从文件系统删除
            writer.revertPartialWritesAndClose()
            if (file.exists()) {
              if (!file.delete()) {
                logWarning(s"Error deleting ${file}")
              }
            }
          }
        }
    	val= new DiskMapIterator(file, blockId, batchSizes)
    
    	def destructiveIterator(inMemoryIterator: Iterator[(K, C)]): Iterator[(K, C)]
    	功能: 获取损坏的迭代(底层存储数组的数据发送改变),当内存不足时会溢写到磁盘上
    	readingIterator = new SpillableIterator(inMemoryIterator)
        readingIterator.toCompletionIterator
    
    	def freeCurrentMap(): Unit
    	功能: 释放当前map的内存空间
    	if (currentMap != null) {
          currentMap = null // So that the memory can be garbage-collected
          releaseMemory()
        }
    
    	def iterator: Iterator[(K, C)]
    	功能: 获取一个底层已经损坏的迭代器(这个迭代器合并了内存map和溢写map)
    	操作条件: 当前map存在
    	if (currentMap == null) {
          throw new IllegalStateException(
            "ExternalAppendOnlyMap.iterator is destructive and should only be called once.")
        }
    	1. 获取迭代器
    	val= if (spilledMaps.isEmpty) {
          destructiveIterator(currentMap.iterator)
        } else {
          new ExternalIterator()
        }
    }
    ```
    
    #subclass @ExternalIterator
    
    ```scala
    private class ExternalIterator extends Iterator[(K, C)] {
        介绍: 合并并排序内存map和溢写map的合集的迭代器
        属性:
        #name @mergeHeap = new mutable.PriorityQueue[StreamBuffer]	合并堆(非空缓冲队列)
        #name @inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)	输入流列表
        	输入流来自于内存map和磁盘上的溢写map,内存map使用内存排序完毕,外存(溢写)map之前已经排序完毕
        #name @sortedMap = destructiveIterator(
          currentMap.destructiveSortedIterator(keyComparator))
        	排序完成的map
        初始化操作:
        inputStreams.foreach { it =>
          val kcPairs = new ArrayBuffer[(K, C)]
          readNextHashCode(it, kcPairs) 
          if (kcPairs.length > 0) {
            mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
          }
        }
        功能: 将输入流列表中的kv值合并堆队列
        
        操作集:
        def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit
        功能: 使用来自于指定迭代器的同一个hash值的kv对来填充缓冲区@buf.保证了读取一个hashcode的流数据时不会忘记将它们合并,假定给定迭代器是排序好的.一次执行只能合并一个hash值的缓冲区.
        if (it.hasNext) {
            var kc = it.next()
            buf += kc
            val minHash = hashKey(kc)
            while (it.hasNext && it.head._1.hashCode() == minHash) { // 与之前的hash值相等,则合并到buff
              kc = it.next()
              buf += kc
            }
          }
        
        def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C 
        功能: 如果给定buffer包含指定key的值,合并value到基本合并器中,并将相应的kv对从buffer中移除.
        var i = 0
          while (i < buffer.pairs.length) {
            val pair = buffer.pairs(i)
            if (pair._1 == key) { // 缓冲区找到与key匹配的条目,合并到combiner中
              // 这里是假定了一个缓冲区内只有一个值与key匹配(因为溢写前key是唯一的),所以直接返回是没有问题的
              removeFromBuffer(buffer.pairs, i)
              return mergeCombiners(baseCombiner, pair._2)
            }
            i += 1
          }
        val= baseCombiner
        
        def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T 
        功能: 将指定元素从缓冲区中移除
        val elem = buffer(index)
        buffer(index) = buffer(buffer.size - 1) // 维度-1
        buffer.trimEnd(1)// 截取buffer
        val= elem // 返回移除的元素
        
        def hasNext: Boolean = mergeHeap.nonEmpty
        功能: 确定迭代器中是否含有下一个元素
        
        def next(): (K, C) 
        功能: 获取下一个kv键值对
        0. 合并队列检查
        if (mergeHeap.isEmpty) {
            throw new NoSuchElementException
        }
        1. 获取队列中最小的key进行处理
        val minBuffer = mergeHeap.dequeue()
        val minPairs = minBuffer.pairs
        val minHash = minBuffer.minKeyHash
        val minPair = removeFromBuffer(minPairs, 0)
        val minKey = minPair._1
        var minCombiner = minPair._2
        2. 合法性断言
        assert(hashKey(minPair) == minHash)
        3. 将hash值最小的缓冲区合并到缓冲区列表中
        val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
          while (mergeHeap.nonEmpty && mergeHeap.head.minKeyHash == minHash) {
            val newBuffer = mergeHeap.dequeue()
            minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
            mergedBuffers += newBuffer
          }
        4. 将缓冲区列表的缓冲区送回合并堆中
        mergedBuffers.foreach { buffer =>
            if (buffer.isEmpty) {
              readNextHashCode(buffer.iterator, buffer.pairs)
            }
            if (!buffer.isEmpty) {
              mergeHeap.enqueue(buffer)
            }
          }
        5. 返回当前最小的key和combiner
        val= (minKey, minCombiner)
        
        private class StreamBuffer(
            val iterator: BufferedIterator[(K, C)],
            val pairs: ArrayBuffer[(K, C)])
        extends Comparable[StreamBuffer] {
            介绍: 来自一个map迭代器(内存迭代器和磁盘迭代器)的流式缓冲区,按照hash值排序.每个缓冲区维护了所有键值对(使用的是当前流中最小hash值的key).如果hash碰撞时会产生多个value.主要到由于存在有溢写,对于一个key仅仅溢写一个value.每个key至多有一个元素.
            流式缓冲区需要实现比较逻辑,主要是方便与排序和优先队列的使用.
            构造器参数:
                iterator	缓冲迭代器
                pairs	kv缓冲列表
            操作集:
            def isEmpty: Boolean = pairs.length == 0
            功能: 检查缓冲区列表是否为空
            
            def minKeyHash: Int 
            功能: 获取hash最小值
            assert(pairs.length > 0)
            hashKey(pairs.head)
            
            def compareTo(other: StreamBuffer): Int
            功能: 实现降序排列,在大顶堆的情况下获取最小值(hash值)
            val= if (other.minKeyHash < minKeyHash) -1 
            else if (other.minKeyHash == minKeyHash) 0 else 1
        }
    }
    ```
    
    ```scala
    private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[(K, C)]{
        介绍: 磁盘map返回排序完成的迭代器
        构造器属性:
        	file	文件
        	blockId	块编号
        	batchSize	批量处理大小列表
        属性:
        #name @batchOffsets = batchSizes.scanLeft(0L)(_ + _)	批量偏移量
        	大小为batchSize.length + 1
        #name @batchIndex = 0	批量获取指针
        #name @fileStream: FileInputStream = null	文件流
        #name @deserializeStream: DeserializationStream = null	反序列化流
        #name @nextItem: (K, C) = null	下一个键值对
        #name @objectsRead = 0	读取对象数量
        初始化操作:
        assert(file.length() == batchOffsets.last,
          "File length is not equal to the last batch offset:\n" +
          s"    file length = ${file.length}\n" +
          s"    last batch offset = ${batchOffsets.last}\n" +
          s"    all batch offsets = ${batchOffsets.mkString(",")}"
        )
        功能: 文件大小校验
        
         context.addTaskCompletionListener[Unit](context => cleanup())
        功能: 添加任务结束作为任务完成的监听事件
        
        操作集:
        def readNextItem(): (K, C)
        功能: 反序列化读取下一个KV对,如果当前批次的被拉取了,则构建下一个批次的输入流,并读取.如果没有键值对,则返回null.
        try {
            val k = deserializeStream.readKey().asInstanceOf[K]
            val c = deserializeStream.readValue().asInstanceOf[C]
            val item = (k, c) //获取键值对信息
            objectsRead += 1
            if (objectsRead == serializerBatchSize) { // 换取下一个批次
              objectsRead = 0
              deserializeStream = nextBatchStream()
            }
            item
          } catch {
            case e: EOFException =>
              cleanup()
              null
          }
        
        def hasNext: Boolean
        功能: 确认是否含有下一个键值对数据
        if (nextItem == null) {
            if (deserializeStream == null) {
              // 处理反序列化没有开始的情况
              deserializeStream = nextBatchStream()
              if (deserializeStream == null) { // 表示之后已经没有数据了
                return false
              }
            }
            nextItem = readNextItem()
          }
          val= nextItem != null
        
        def next(): (K, C)
        功能: 获取下一个kv对
        0. 末尾元素校验
        if (!hasNext) {
            throw new NoSuchElementException
        }
        1. 获取下一个元素,并修改下一个元素指针为空
        val item = nextItem
        nextItem = null
        val= item
        
        def cleanup(): Unit 
        功能: 清空
        1. 移动批处理位置指针,防止读取到其他批次数据
        batchIndex = batchOffsets.length 
        2. 关闭反序列化流和文件输入流
        if (deserializeStream != null) {
            deserializeStream.close()
            deserializeStream = null
          }
          if (fileStream != null) {
            fileStream.close()
            fileStream = null
          }
        3. 删除文件
        if (file.exists()) {
            if (!file.delete()) {
              logWarning(s"Error deleting ${file}")
            }
          }
    }
    ```
    
    ```scala
    private class SpillableIterator(var upstream: Iterator[(K, C)])
    extends Iterator[(K, C)] {
        结束: 溢写迭代器
        属性:
        #name @SPILL_LOCK = new Object()	溢写控制锁
        #name @cur: (K, C) = readNext()	溢写指针
        #name @hasSpilled: Boolean = false	是否产生溢写标志
        操作集:
        def destroy(): Unit
        功能: 销毁迭代器
        1. 是否map内存空间
        freeCurrentMap()
        2. 置空上游迭代器@upstream
        upstream = Iterator.empty
        
        def toCompletionIterator: CompletionIterator[(K, C), SpillableIterator]
        功能: 转化为完成迭代器@CompletionIterator
        val= CompletionIterator[(K, C), SpillableIterator](this, this.destroy)
        
        def readNext(): (K, C)
        功能: 读取下一个键值(上游迭代器的下一个kv对)对信息
        val=  SPILL_LOCK.synchronized {
          if (upstream.hasNext) {
            upstream.next()
          } else {
            null
          }
        }
        
        def hasNext(): Boolean = cur != null
        功能: 确认是否存在有下一个元素
        
        def next(): (K, C)
        功能: 获取下一个kv对
        val r = cur  // 读取数据
        cur = readNext() // 移动指针
        val= r
        
    }
    ```
    
    ```scala
    private[spark] object ExternalAppendOnlyMap {
        操作集:
        def hash[T](obj: T): Int= if (obj == null) 0 else obj.hashCode()
        功能: 获取指定对象@obj 的hash值
        
        内部类:
        private class HashComparator[K] extends Comparator[K] {
            介绍: 基于hash值的key排序
            操作集:
            def compare(key1: K, key2: K): Int
            功能: 比较两个key的大小
            val hash1 = hash(key1)
            val hash2 = hash(key2)
            val= if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
        }
    }
    ```
    
    #### ExternalSorter
    
    ```markdown
    介绍:
    	外部排序器.排序并且进行有可能的kv键值对合并,产生kv键值对的合并形式.使用分区器@Partitioner 首次将key分组送入分区中.之后在分区内部按照key进行排序,排序规则有用户指定.使用一个分区的不同字节范围可以输出单个分区文件.适合与shuffle获取数据.
    	如果合并失效了,类型C必须要等于V(可以在最后进行类型转换)
    	注意: 尽管外部排序器是一个公平的排序器,一些配置连接向基于排序的shuffle.我们可能需要重新访问外部访问器,如果它在其他没有shuffle的上下文中.
    构造器参数:
    	aggregator	聚合器(结合合并函数用户合并数据)
    	partitioner	分区器,如果给定了,会按照分区编号--> key值关键字次序进行排序
    	ordering	排序方式,可以选择全局排序或者是部分排序
    	serializer	溢写时需要使用的序列化器
    	
    	注意: 如果给定了排序方式,我们总是使用它进行排序,所以仅仅你想要输出的key顺序排序才需要给定它.举个例子,在一个不存在有map侧join的map任务来说,可能你会传递None作为排序方式,去避免不必要的排序.另一方面,如果你需要进行合并,有排序的方式要远远比不排序要高效.
    	用户可以通过以下几种方式进行与本类的交互:
    	1. 实例化一个外部排序器@ExternalSorter
    	2. 一组记录调用@insertAll()
    	3. 请求一个迭代器@iterator()去遍历排序/聚合后的记录或者是调用@writePartitionedFile() 去创建一个包含排序/聚合结果的输出的文件.(可以用在spark shuffle的排序)
    	在高级方式下,这个类由如下几种内部工作方式:
    	1. 反复的填充内存数据的缓冲区.如果需要对数据进行合并则选择@PartitionedAppendOnlyMap 的数据结构,否则采用@PartitionedPairBuffer 的数据结构.在缓冲区内部,使用分区ID进行排序也有可能使用key作为第二关键字进行排序.为了避免使用同一个key多次调用分区.在存储分区ID时需要伴随着记录.
    	2. 当每个缓冲区到达内存限制的时候,将其溢写到文件中.文件按照分区编号进行排序,也有可能key作为第二关键字进行排序(如果需要进行聚合操作).对于每个文件来说,我们追踪了每个分区在内存中有多少个对象,所以对于任意一个元素来说,不需要写出器分区ID.
    	3. 当用户请求一个迭代器或者文件输出的时候,溢写文件被合并,还有一些残留的内存数据,使用相同的排序策略(除非排序和聚合都不能使用).如果我们需要按照key进行聚合,我们既可以使用全局排序,也可以读取相同的hash值的记录,对其作比较之后进行数据的合并.
    	4. 用户在最后调用stop(),去删除所有中间文件.
    ```
    
    ```scala
    private[spark] class ExternalSorter[K, V, C](
        context: TaskContext,
        aggregator: Option[Aggregator[K, V, C]] = None,
        partitioner: Option[Partitioner] = None,
        ordering: Option[Ordering[K]] = None,
        serializer: Serializer = SparkEnv.get.serializer)
    extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
    with Logging{
        属性:
        #name @conf = SparkEnv.get.conf	spark配置
        #name @numPartitions = partitioner.map(_.numPartitions).getOrElse(1)	分区数量
        #name @shouldPartition = numPartitions > 1	是否需要分区
        #name @blockManager = SparkEnv.get.blockManager	块管理器
        #name @diskBlockManager = blockManager.diskBlockManager	盘块管理器
        #name @serializerManager = SparkEnv.get.serializerManager	序列化管理器
        #name @serInstance = serializer.newInstance()	序列化实例
        #name @fileBufferSize = conf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024 文件缓冲大小
        #name @serializerBatchSize = conf.get(config.SHUFFLE_SPILL_BATCH_SIZE) 序列化批次长度
        #name @map = new PartitionedAppendOnlyMap[K, C]	聚合使用的内存数据结构
        #name @buffer = new PartitionedPairBuffer[K, C]	不使用聚合的内存数据结构
        #name @_diskBytesSpilled = 0L	磁盘溢写字节量
        #name @_peakMemoryUsedBytes: Long = 0L	峰值内存使用量
        #name @isShuffleSort: Boolean = true	volatile	shuffle是否排序
        #name @forceSpillFiles = new ArrayBuffer[SpilledFile]	强行溢写列表
        #name @readingIterator: SpillableIterator = null	volatile	读取迭代器
        #name @keyComparator #Type @Comparator[K]	key排序器
        	val= ordering.getOrElse((a: K, b: K) => {
            val h1 = if (a == null) 0 else a.hashCode()
            val h2 = if (b == null) 0 else b.hashCode()
            if (h1 < h2) -1 else if (h1 == h2) 0 else 1
          })
        按照key的hashcode进行排序
        #name @spills = new ArrayBuffer[SpilledFile]	溢写文件列表
        操作集:
        def getPartition(key: K): Int = if (shouldPartition) partitioner.get.getPartition(key) else 0
        功能: 获取当前key所属于的分区编号
        
        def diskBytesSpilled: Long = _diskBytesSpilled
        功能: 获取磁盘溢写字节量
        
        def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes
        功能: 获取峰值内存使用量
        
        def comparator: Option[Comparator[K]]
        功能: 获取比较器
        val= if (ordering.isDefined || aggregator.isDefined) Some(keyComparator) else None
        
        def numSpills: Int = spills.size
        功能: 获取溢写量
        
        def insertAll(records: Iterator[Product2[K, V]]): Unit
        功能: 插入迭代器内部的记录
        1. 确定是否需要合并
        val shouldCombine = aggregator.isDefined
        2. 合并情况的处理
        val mergeValue = aggregator.get.mergeValue
        val createCombiner = aggregator.get.createCombiner
        var kv: Product2[K, V] = null
        val update = (hadValue: Boolean, oldValue: C) => { // 设置更新函数,有则更新,无则创建
            if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
        }
        while (records.hasNext) {
            addElementsRead() // 读取元素数量+1
            kv = records.next() // 获取迭代器中的下一个kv对
            map.changeValue((getPartition(kv._1), kv._1), update) // 改变key值为合并后的值
            maybeSpillCollection(usingMap = true) // 如果内存不够使用则溢写
        }
        3. 不使用合并的处理情况
        while (records.hasNext) {
            addElementsRead()
            val kv = records.next()
            buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C]) // 直接将数据插入缓冲区
            maybeSpillCollection(usingMap = false) // 如果内存不够则溢写
          }
        
        def maybeSpillCollection(usingMap: Boolean): Unit
        功能: 如果内存不够使用则对内部数据结构进行溢写
        输入参数: useMap 为true则释放map内存,为false则释放缓冲区内存
        1. 释放内存
        var estimatedSize = 0L
        if (usingMap) {
          estimatedSize = map.estimateSize()
          if (maybeSpill(map, estimatedSize)) { // 进行可能的溢写和内存重新设置
            map = new PartitionedAppendOnlyMap[K, C]
          }
        } else {
          estimatedSize = buffer.estimateSize()
          if (maybeSpill(buffer, estimatedSize)) { // 进行可能的溢写和内存重新设置
            buffer = new PartitionedPairBuffer[K, C]
          }
        }
        2. 重置可能的度量值@_peakMemoryUsedBytes
        if (estimatedSize > _peakMemoryUsedBytes) {
          _peakMemoryUsedBytes = estimatedSize
        }
        
        def spill(collection: WritablePartitionedPairCollection[K, C]): Unit
        功能: 溢写指定集合@collection
        1. 获取内存迭代器
        val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
        2. 将迭代器中内容溢写到磁盘上
        val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
        3. 将新写出的溢写文件注册到溢写文件列表中
        spills += spillFile
        
        def forceSpill(): Boolean
        功能: 强行溢写当前内存集合到磁盘上,以释放内存.任务没有足够的执行内存时才会由任务内存管理器执行
        if (isShuffleSort) {
          val= false // 采取shuffle排序,不溢写
        } else {
          assert(readingIterator != null)
          val isSpilled = readingIterator.spill() // 不采用shuffle排序,对内存数据结构都进行溢写
          if (isSpilled) {
            map = null
            buffer = null
          }
          val= isSpilled
        }
        
        def flush(): Unit 
        功能: 刷写磁盘写出器的内容到磁盘中,并更新相关变量
        val segment = writer.commitAndGet()
        batchSizes += segment.length
        _diskBytesSpilled += segment.length
        objectsWritten = 0
        
        def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator)
          : SpilledFile
        功能: 将内存迭代器溢写到磁盘上,并返回溢写文件实例
        1. 创建临时shuffle数据块(由于shuffle期间可能被读取,所以压缩必须交由@spark.shuffle.compress控制)
        	所以必须创建一个shuffle数据块
        val (blockId, file) = diskBlockManager.createTempShuffleBlock()
        2. 获取写出相关参数(每次刷写之后都会进行重置)
        var objectsWritten: Long = 0
        val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
        val writer: DiskBlockObjectWriter =
          blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)
        3. 获取写到磁盘的批次长度列表
        val batchSizes = new ArrayBuffer[Long]
        4. 获取每个分区的元素数量
        val elementsPerPartition = new Array[Long](numPartitions)
        5. 写出写出器中的内容
        var success = false
        try {
          while (inMemoryIterator.hasNext) {
            val partitionId = inMemoryIterator.nextPartition()// 获取迭代器所属的分区号
            require(partitionId >= 0 && partitionId < numPartitions,
              s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
            inMemoryIterator.writeNext(writer) // 写出写出器的内容
            elementsPerPartition(partitionId) += 1  // 更新度量值@elementsPerPartition
            objectsWritten += 1 // 更新度量值@objectsWritten
            if (objectsWritten == serializerBatchSize) { // 批量刷写迭代器中的数据
              flush()
            }
          } 
          if (objectsWritten > 0) { // 刷写剩余数据
            flush()
          } else {
            writer.revertPartialWritesAndClose()
          }
          success = true
        } finally {
          if (success) {
            writer.close()
          } else {
            writer.revertPartialWritesAndClose() // 失败处理
            if (file.exists()) {
              if (!file.delete()) {
                logWarning(s"Error deleting ${file}")
              }
            }
          }
        }
        
        def iterator: Iterator[Product2[K, C]]
        功能: 获取迭代器(包含有所有写出数据,使用聚合器继续聚合)
        isShuffleSort = false
        val= partitionedIterator.flatMap(pair => pair._2)
        
        def destructiveIterator(memoryIterator: Iterator[((Int, K), C)]): Iterator[((Int, K), C)] 
        功能: 获取map的具有由破坏性的迭代器,内存不足时强行溢写到磁盘,返回磁盘上map的kv数据
        输入参数: memoryIterator	内存迭代器
        if (isShuffleSort) {
          val= memoryIterator
        } else {
          readingIterator = new SpillableIterator(memoryIterator) // 使用shuffle则会向磁盘溢写
          val= readingIterator
        }
        
        def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])]
        功能: 获取分区迭代器(这个迭代器只能顺序读取),保证返回的每个分区都是按照分区号进行排序的
        	现在,只一次合并所有溢写文件.但是可以修改成可以分成合并.
        1. 确定是否使用聚合,并获取进行处理的内部数据结构
        val usingMap = aggregator.isDefined
        val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
        2. 一次性合并溢写文件
        if (spills.isEmpty) {
    	  // 特殊情况,不存在有溢写出去的数据,只需要对内存数据进行处理即可
          if (ordering.isEmpty) { // 没有指定排序方式,按分区号排序即可	
              groupByPartition(destructiveIterator(
              collection.partitionedDestructiveSortedIterator(None)))
          } else { // 指定了排序,则按照排序规则@keyComparator 进行排序
            groupByPartition(destructiveIterator(
              collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
          }
        } else {
          merge(spills, destructiveIterator(
            collection.partitionedDestructiveSortedIterator(comparator)))
        }
        
        def writePartitionedFile(blockId: BlockId,outputFile: File): Array[Long]
        功能: 写出分区文件
        1. 定位每个分区再输出文件中的位置
        val lengths = new Array[Long](numPartitions) // 分区长度列表
        2. 获取指定写出器
        val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
          context.taskMetrics().shuffleWriteMetrics)
        3. 只有内存数据的处理(spills.isEmpty)
        val collection = if (aggregator.isDefined) map else buffer // 获取使用的数据结构
        val it = collection.destructiveSortedWritablePartitionedIterator(comparator)// 获取迭代器
        while (it.hasNext) { // 处理迭代器中每个分区的长度
            val partitionId = it.nextPartition() // 获取分区编号
            while (it.hasNext && it.nextPartition() == partitionId) { // 写出指定分区编号的内容
                it.writeNext(writer) 
            }
            val segment = writer.commitAndGet() //获取写出的长度
            lengths(partitionId) = segment.length // 设置长度列表该分区的长度
        }
        4. 存在溢写的处理情况
        for ((id, elements) <- this.partitionedIterator) { // 获取分区迭代器,并对其进行长度获取和设置
            if (elements.hasNext) {
              for (elem <- elements) {
                writer.write(elem._1, elem._2)
              }
              val segment = writer.commitAndGet()
              lengths(id) = segment.length
            }
          }
        5. 关闭写出器,更新度量系统的参数值
        context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)
        val= lengths
        
        def writePartitionedMapOutput(shuffleId: Int,mapId: Long,
          mapOutputWriter: ShuffleMapOutputWriter): Unit
        功能: 写出所有添加到外部排序器@ExternalSorter 的数据,按照mapOutput的形式.使用@SortShuffleWriter调用
        1. 获取分区编号
        var nextPartitionId = 0
        2. 处理只有内存数据的情况
        val collection = if (aggregator.isDefined) map else buffer // 选取数据结构
        val it = collection.destructiveSortedWritablePartitionedIterator(comparator) // 获取迭代器
        while (it.hasNext()) {
            // 获取分区ID,分区写出器,分区kv写出器
            val partitionId = it.nextPartition() 
            var partitionWriter: ShufflePartitionWriter = null
            var partitionPairsWriter: ShufflePartitionPairsWriter = null
            TryUtils.tryWithSafeFinally {
                partitionWriter = mapOutputWriter.getPartitionWriter(partitionId)
                // 获取shuffle数据块信息
                val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
                partitionPairsWriter = new ShufflePartitionPairsWriter(
                    partitionWriter,
                    serializerManager,
                    serInstance,
                    blockId,
                    context.taskMetrics().shuffleWriteMetrics)
                while (it.hasNext && it.nextPartition() == partitionId) { // 写出当前分区的内容
                    it.writeNext(partitionPairsWriter)
                }
            } {
                if (partitionPairsWriter != null) {
                    partitionPairsWriter.close()
                }
            }
            nextPartitionId = partitionId + 1 // 指针移动到下一个分区
        }
        3. 处理含有溢写文件的情况
        for ((id, elements) <- this.partitionedIterator) { // 直接从分区迭代器中获取数据写出
            // 获取shuffle块信息
            val blockId = ShuffleBlockId(shuffleId, mapId, id)
            var partitionWriter: ShufflePartitionWriter = null
            var partitionPairsWriter: ShufflePartitionPairsWriter = null
            TryUtils.tryWithSafeFinally {
                // 指定分区写出器,和分区kv写出器
                partitionWriter = mapOutputWriter.getPartitionWriter(id)
                partitionPairsWriter = new ShufflePartitionPairsWriter(
                    partitionWriter,
                    serializerManager,
                    serInstance,
                    blockId,
                    context.taskMetrics().shuffleWriteMetrics)
                if (elements.hasNext) {
                    for (elem <- elements) { // 直接写出分区写出器的kv信息
                        partitionPairsWriter.write(elem._1, elem._2)
                    }
                }
            } {
                if (partitionPairsWriter != null) {
                    partitionPairsWriter.close()
                }
            }
            nextPartitionId = id + 1 // 移动到下一个分区
        }
      	4. 更新度量值
        context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)
        
        def stop(): Unit
        功能: 停止外部排序器
        1. 清理溢写文件列表
        spills.foreach(s => s.file.delete())
        spills.clear()
        forceSpillFiles.foreach(s => s.file.delete())
        forceSpillFiles.clear()
        2. 重置内部数据结构,并释放内存
        if (map != null || buffer != null || readingIterator != null) {
          map = null 
          buffer = null
          readingIterator = null
          releaseMemory()
        }
        
        def groupByPartition(data: Iterator[((Int, K), C)])
          : Iterator[(Int, Iterator[Product2[K, C]])] 
        功能: 按照分区进行聚合,给定@data 假定是按照分区id进行排序的.将分区下所有kv对组成一个子迭代器
        val buffered = data.buffered
        // 创建所有分区的聚合迭代器
        (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
        
        def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
          : Iterator[(Int, Iterator[Product2[K, C]])]
        功能: 合并排序完毕文件,既可以写出新文件,也可以返回数据给用户
        	返回的数据是所有已经写出的对象,按照分区分组.每个分区有其迭代器,需要顺序读取(不支持随机存取).
        输入参数:
        	spilled	已经排序好的溢写文件
        	inMemory	内存数据迭代器
        1. 获取溢写读取器列表和内存数据缓冲区
        val readers = spills.map(new SpillReader(_))
        val inMemBuffered = inMemory.buffered
        2. 每个分区将内存数据域溢写数据进行归并操作
        (0 until numPartitions).iterator.map { p =>
            // 获取内存数据在本分区上的迭代器
          val inMemIterator = new IteratorForPartition(p, inMemBuffered)
            // 获取归并后的迭代器
          val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator) 
          if (aggregator.isDefined) { // 运行归并则进行聚合归并
            (p, mergeWithAggregation(
              iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
          } else if (ordering.isDefined) {// 定义排序则排序
            (p, mergeSort(iterators, ordering.get))
          } else { //其余情况正常输出,按照k-->seq形式输出
            (p, iterators.iterator.flatten)
          }
        }
        
        def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
          : Iterator[Product2[K, C]]
        功能: 归并排序(使用指定的排序器@comparator 对key进行排序),返回一个排序完成的迭代器
        1. 获取迭代器缓冲
        val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
        2. 设置小顶堆用于排序
        heap = new mutable.PriorityQueue[Iter]()(
          (x: Iter, y: Iter) => comparator.compare(y.head._1, x.head._1))
        3. 数据入堆(进行堆排序)
        heap.enqueue(bufferedIters: _*)
        4. 返回迭代器
        val= new Iterator[Product2[K, C]] {
          override def hasNext: Boolean = heap.nonEmpty
          override def next(): Product2[K, C] = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            val firstBuf = heap.dequeue()
            val firstPair = firstBuf.next()
            if (firstBuf.hasNext) {
              heap.enqueue(firstBuf)
            }
            firstPair
          }
        }
        
        def mergeWithAggregation(
          iterators: Seq[Iterator[Product2[K, C]]],
          mergeCombiners: (C, C) => C,
          comparator: Comparator[K],
          totalOrder: Boolean)
          : Iterator[Product2[K, C]]
        功能: 合并并聚合
        通过聚合key值进行合并,假定所有迭代器的key都是按照比较器@comparator 进行排序.如果比较器不是全局有序,我们依旧需要合并它们.通过所有key做相等测试.
        1. 非全局排序(这种情况只有局部排序结果)
         val it = new Iterator[Iterator[Product2[K, C]]] {
            val sorted = mergeSort(iterators, comparator).buffered
            val keys = new ArrayBuffer[K]
            val combiners = new ArrayBuffer[C]
            override def hasNext: Boolean = sorted.hasNext
            override def next(): Iterator[Product2[K, C]] = {
              if (!hasNext) {
                throw new NoSuchElementException
              }
              keys.clear()
              combiners.clear()
              val firstPair = sorted.next()
              keys += firstPair._1
              combiners += firstPair._2
              val key = firstPair._1
              while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
                val pair = sorted.next()
                var i = 0
                var foundKey = false
                while (i < keys.size && !foundKey) {
                  if (keys(i) == pair._1) {
                    combiners(i) = mergeCombiners(combiners(i), pair._2)
                    foundKey = true
                  }
                  i += 1
                }
                if (!foundKey) {
                  keys += pair._1
                  combiners += pair._2
                }
              }
              keys.iterator.zip(combiners.iterator)
            }
          }
        val= it.flatten
        2. 全局排序
        val= new Iterator[Product2[K, C]] {
            val sorted = mergeSort(iterators, comparator).buffered
            override def hasNext: Boolean = sorted.hasNext
            override def next(): Product2[K, C] = {
              if (!hasNext) {
                throw new NoSuchElementException
              }
              val elem = sorted.next()
              val k = elem._1
              var c = elem._2
              while (sorted.hasNext && sorted.head._1 == k) {
                val pair = sorted.next()
                c = mergeCombiners(c, pair._2)
              }
              (k, c)
            }
          }
        
    }
    ```
    
    ```scala
    private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]{
        介绍: 这个迭代器只通过底层缓冲流读取指定分区号的数据,假定分区是下一个读取的数据.使用它可以简单的获取内存中下一个分区迭代器.
        构造器参数:
        partitionId	分区编号
        data	缓冲迭代器
        操作集:
        def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId
        功能: 确认存在下一条数据
        
        def next(): Product2[K, C]
        功能: 获取下一个迭代器
        if (!hasNext) {
            throw new NoSuchElementException
          }
        val elem = data.next()
        val= (elem._1._2, elem._2)
    }
    ```
    
    ```scala
    private[this] class SpillableIterator(var upstream: Iterator[((Int, K), C)])
    extends Iterator[((Int, K), C)]{
        属性:
        #name @SPILL_LOCK = new Object()	溢写控制锁
        #name @nextUpstream: Iterator[((Int, K), C)] = null 下一个上游迭代器
        #name @cur: ((Int, K), C) = readNext()	当前迭代器
        #name @hasSpilled: Boolean = false	是否溢写标志
        操作集:
        def hasNext(): Boolean = cur != null
        功能: 确认之后是否存在迭代器
        
        def next(): ((Int, K), C) 
        功能: 获取当前迭代器,并移动迭代器指针
        val r = cur
        cur = readNext()
        val= r
        
        def readNext(): ((Int, K), C)
        功能: 读取上游迭代器内容
        val= SPILL_LOCK.synchronized {
          if (nextUpstream != null) {
            upstream = nextUpstream
            nextUpstream = null
          }
          if (upstream.hasNext) {
            upstream.next()
          } else {
            null
          }
        }
        
        def spill(): Boolean
        功能: 溢写上游迭代器
        操作条件: 当前没有发送溢写
        if (hasSpilled) {
            false
          } else {
            val inMemoryIterator = new WritablePartitionedIterator {
                // 迭代器指针
              private[this] var cur = if (upstream.hasNext) upstream.next() else null
    			// 写出一条kv数据,并移动迭代器指针
              def writeNext(writer: PairsWriter): Unit = {
                writer.write(cur._1._2, cur._2)
                cur = if (upstream.hasNext) upstream.next() else null
              }
    			// 判断是否到达迭代器末尾
              def hasNext(): Boolean = cur != null
    			// 获取下一个分区
              def nextPartition(): Int = cur._1._1
            }
            logInfo(s"Task ${TaskContext.get().taskAttemptId} force spilling in-memory map to disk " +
              s"and it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
            val spillFile = spillMemoryIteratorToDisk(inMemoryIterator) // 获取溢写文件
            forceSpillFiles += spillFile // 将溢写文件添加到强行溢写列表中
            val spillReader = new SpillReader(spillFile)
            // 读取上游迭代器
            nextUpstream = (0 until numPartitions).iterator.flatMap { p =>
              val iterator = spillReader.readNextPartition()
              iterator.map(cur => ((p, cur._1), cur._2))
            }
            hasSpilled = true // 设置溢写标志
            true
          }
    }
    ```
    
    ```scala
    private[this] class SpillReader(spill: SpilledFile) {
        介绍: 读取经过分区的溢写文件的内部类,所有分区按照顺序请求
        属性:
        #name @batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)	批量偏移值
        #name @partitionId = 0	分区编号
        #name @indexInPartition = 0L	分区内索引
        #name @batchId = 0	批次编号
        #name @indexInBatch = 0	批次内编号
        #name @lastPartitionId = 0 最后一个分区编号
        #name @fileStream: FileInputStream = null	文件输入流
        #name @deserializeStream = nextBatchStream()	反序列化
        #name @nextItem: (K, C) = null	下一个kv对
        #name @finished = false	读取完成标志
        #name @nextPartitionToRead = 0	下一个读取分区指针
        操作集:
        def cleanup(): Unit 
        功能: 清除工作
        batchId = batchOffsets.length  // 获取批次编号,防止读取其他批次数据
        val ds = deserializeStream
        // 重置输入流
        deserializeStream = null 
        fileStream = null
        if (ds != null) { // 关闭序列化流
            ds.close()
        }
        
        def readNextPartition(): Iterator[Product2[K, C]]
        功能: 获取下一个分区迭代器
        val= new Iterator[Product2[K, C]] {
          val myPartition = nextPartitionToRead
          nextPartitionToRead += 1
          override def hasNext: Boolean = {
            if (nextItem == null) {
              nextItem = readNextItem()
              if (nextItem == null) {
                return false
              }
            }
            assert(lastPartitionId >= myPartition)
            lastPartitionId == myPartition
          }
          override def next(): Product2[K, C] = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            val item = nextItem
            nextItem = null
            item
          }
        }
        
        def skipToNextPartition(): Unit
        功能: 跳到下一个分区,如果到达分区的末尾则更新分区编号
        while (partitionId < numPartitions &&
              indexInPartition == spill.elementsPerPartition(partitionId)) { // 到达分区末尾
            partitionId += 1 // 跳到下一个分区
            indexInPartition = 0L
        }
        
        def readNextItem(): (K, C)
        功能: 读取下一个kv对
        操作条件: 读取器运行中,且反序列化流可以使用
        if (finished || deserializeStream == null) {
            return null
        }
        1. 读取kv对
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[C]
        lastPartitionId = partitionId
        2. 如果处理完这个批次的数据,则开始读取下一个批次数据
        indexInBatch += 1
        if (indexInBatch == serializerBatchSize) {// 读取下一个批次的数据
            indexInBatch = 0
            deserializeStream = nextBatchStream()
        }
        3. 更新分区指针
        indexInPartition += 1
        skipToNextPartition()
        4. 处理可能发生的读取结束问题
        if (partitionId == numPartitions) {
            finished = true
            if (deserializeStream != null) {
              deserializeStream.close()
            }
          }
        val= (k,c)
        
        def nextBatchStream(): DeserializationStream
        功能: 获取下一个批次的反序列化流
        if (batchId < batchOffsets.length - 1) {
            if (deserializeStream != null) { // 关闭反序列化流
              deserializeStream.close()
              fileStream.close()
              deserializeStream = null
              fileStream = null
            }
            // 使用文件输入流读取当前批次数据
            val start = batchOffsets(batchId)
            fileStream = new FileInputStream(spill.file)
            fileStream.getChannel.position(start)
            // 移动批次指针
            batchId += 1
            // 获取下一个偏移量位置
            val end = batchOffsets(batchId)
            // 数据合法性断言
            assert(end >= start, "start = " + start + ", end = " + end +
              ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))
            // 获取反序列化流
            val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
            val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
            val= serInstance.deserializeStream(wrappedStream)
          } else {// 所有批次读取结束,直接关闭读取
            cleanup()
            null
          }
    }
    ```
    
    #### MedianHeap
    
    ```markdown
    介绍:
    	中值堆设计用户快速定位一组数据的中位数(可能有重复元素).插入一个新值需要时间复杂度为O(log n),查找中位数的时间复杂度为O(1).
    	基本方法就是维护两个堆,一个小顶堆和一个大顶堆.当一个元素插入时,小顶堆和大顶堆需要重新调整,以至于它们的大小差距不会超过1.因此每次当执行@findMedian 调用时如果检测到两个堆大小相同.如果确实如此,获取两个堆的堆顶的平均数.否则返回较多元素的堆顶.
    ```
    
    ```scala
    private[spark] class MedianHeap(implicit val ord: Ordering[Double]) {
        构造器属性:
        ord	排序完毕的序列
        属性:
    #name @smallerHalf = PriorityQueue.empty[Double](ord)	小堆
        #name @largerHalf = PriorityQueue.empty[Double](ord.reverse)	大堆
    	操作集:
        def isEmpty(): Boolean 
    功能: 判断堆是否为空
        
    def size(): Int
        功能: 获取堆大小
    val= smallerHalf.size + largerHalf.size
        
    def insert(x: Double): Unit
        功能: 插入元素x
    1. 插入元素
        if (isEmpty) { // 堆为空,将元素强制插入到大堆中
      largerHalf.enqueue(x)
        } else {
      // 大于中间值,插入大堆,否则插入小堆
          if (x > median) {
            largerHalf.enqueue(x)
          } else {
            smallerHalf.enqueue(x)
          }
        }
        2. 调整平衡
        rebalance()
        
        def rebalance(): Unit
        功能: 大小堆平衡策略(平衡因子决定值大于1)
        if (largerHalf.size - smallerHalf.size > 1) { // 大堆数据多,大堆拿出堆顶元素放到小堆
          smallerHalf.enqueue(largerHalf.dequeue())
        }
        if (smallerHalf.size - largerHalf.size > 1) {// 小堆数据多,拿出一个数据放到大堆
          largerHalf.enqueue(smallerHalf.dequeue)
        }
        
        def median: Double
        功能: 获取平均值
        if (isEmpty) {
          throw new NoSuchElementException("MedianHeap is empty.")
        }
        val= if (largerHalf.size == smallerHalf.size) {
          (largerHalf.head + smallerHalf.head) / 2.0
        } else if (largerHalf.size > smallerHalf.size) {
          largerHalf.head
        } else {
          smallerHalf.head
        }
    }
    ```
    
    #### OpenHashMap
    
    ```markdown
    介绍:
    	含有key值的hashmap快速实现,这个hashmap支持插入和更新但是不支持移除元素.这个比普通的hashmap
    	(java.util.HashMap) 要快上5倍. 但是占用的空间还有少.
    	覆盖率@OpenHashSet 的实现
    	注意: 如果需要使用数字类型的value值的时候,类的使用者需要小心的分辨0/0.0/0L和空值的区别
    	主要实现手段: 压缩了空值的存储,使得存储空间变小,查找的范围变小,使得查询速度提升
    ```
    
    ```scala
    private[spark] class OpenHashMap[K : ClassTag, @specialized(Long, Int, Double) V: ClassTag](
        initialCapacity: Int) extends Iterable[(K, V)] with Serializable{
        构造器参数:
        	initialCapacity	初始容量
        构造函数:
        def this() = this(64)
        功能: 构造一个初始容量为64的hashmap
        属性:
        #name @_keySet = new OpenHashSet[K](initialCapacity) key集合
        #name @_values: Array[V] = _ value值列表(用于普通1对象) 
        #name @_values = new Array[V](_keySet.capacity)	用于序列化类型T的value
        #name @_oldValues: Array[V] = null transient	旧值列表
        #name @haveNullValue = false	是否有空值标志
        #name @nullValue: V = null.asInstanceOf[V]	空值
        #name @grow = (newCapacity: Int) => { 
        	_oldValues = _values
            _values = new Array[V](newCapacity)
        }
        	增长策略
        #name @move = (oldPos: Int, newPos: Int) => {
            _values(newPos) = _oldValues(oldPos)
          }
        	hash槽移动策略
        操作集:
        def size: Int
        功能:获取hashmap的条目数量(所有空key只当做一个条目处理)
        val= if (haveNullValue) _keySet.size + 1 else _keySet.size
        
        def contains(k: K): Boolean
        功能: 测试是否包含指定key值
        val= if (k == null) {
          haveNullValue
        } else {
          _keySet.getPos(k) != OpenHashSet.INVALID_POS
        }
        
        def apply(k: K): V
        功能: 获取指定key值的value
        val= if (k == null) {
          nullValue
        } else {
          val pos = _keySet.getPos(k) // 获取key所属的位置
          if (pos < 0) {
            null.asInstanceOf[V]
          } else {
            _values(pos) // 获取pos位置的值即value值,容量与keyset一致,节省了空间
          }
        }
        
        def update(k: K, v: V): Unit
        功能: 更新指定key的value为指定val
        if (k == null) {
          haveNullValue = true
          nullValue = v
        } else {
          val pos = _keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
          _values(pos) = v
          _keySet.rehashIfNeeded(k, grow, move)
          _oldValues = null
        }
        
        def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V
        功能: 如果key在hashmap中不存在,设置其值为默认,否则,设置值为合并函数的值,并返回新更新的值
        输入参数:
            k	key值
            defaultValue	默认值
            mergeValue	合并函数
        if (k == null) {
          if (haveNullValue) { // 不存在该key,但是有空值value,则合并空值value作为更新后的值
            nullValue = mergeValue(nullValue)
          } else { // 不存在该key,也没有空值可以使用,则设置为默认值@defaultValue
            haveNullValue = true
            nullValue = defaultValue
          }
          nullValue
        } else { // 存在key的处理方案
          val pos = _keySet.addWithoutResize(k)
          if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) { // pso不存在的处理方案
            val newValue = defaultValue
            _values(pos & OpenHashSet.POSITION_MASK) = newValue // 重新设置一个新的value等于默认值
            _keySet.rehashIfNeeded(k, grow, move) //根据指定的增长策略重新hash
            newValue
          } else { // pos存在直接设置指定位置的值为新值即可
            _values(pos) = mergeValue(_values(pos))
            _values(pos)
          }
        }
        
        def hasNext: Boolean = nextPair != null
        功能: 是否具有下一个条目
        
        def next(): (K, V)
        功能: 获取下一个条目
        val pair = nextPair
        nextPair = computeNextPair()
        val= pair
        
        def iterator: Iterator[(K, V)]
        功能: 获取一个迭代器
        val= new Iterator[(K, V)] {
        var pos = -1 // 初始化指针位置
        var nextPair: (K, V) = computeNextPair() // 获取第一个条目
        def computeNextPair(): (K, V) = { // 获取下一个条目值
          if (pos == -1) {    // 处理第一个条目
            if (haveNullValue) { // 处理第一个条目的空值情况
              pos += 1
              return (null.asInstanceOf[K], nullValue)
            }
            pos += 1
          }
          pos = _keySet.nextPos(pos) 
          if (pos >= 0) { // 获取后面的条目(不存在空,只有第一个条目可能出现空值)
            val ret = (_keySet.getValue(pos), _values(pos))
            pos += 1
            ret
          } else {
            null
          }
        }  
    } 
    ```
    
    #### OpenHashSet
    
    ```markdown
    介绍:
    	简单来说,快速hashset优化了非空仅插入的情况,这种情况下key是不会被移除的.
    	底层实现使用了scala编译器的特殊化处理,产生了优化了4个类型的存储空间(Long,Int,Double,Float).速度要快于java标准的hashSet,而需要更小的内存开销.这个可以服务于更高等级的数据结构,比如说优化后的hashMap.
    	与传统的hashset相比,这个类提供了各式各样的回调接口.比如说分配函数@allocateFunc,@moveFunc.和一些检索key在底层存储数组中位置的接口.
    	这里使用平方探测法,保证有足够的空间给每个key
    ```
    
    ```scala
    @Private
    class OpenHashSet[@specialized(Long, Int, Double, Float) T: ClassTag](
        initialCapacity: Int,
        loadFactor: Double) extends Serializable{
        构造器参数:
        initialCapacity	初始化容量
        loadFactor	加载因子
        参数合法性断言:
        require(initialCapacity <= OpenHashSet.MAX_CAPACITY,
        s"Can't make capacity bigger than ${OpenHashSet.MAX_CAPACITY} elements")
        require(initialCapacity >= 0, "Invalid initial capacity")
        require(loadFactor < 1.0, "Load factor must be less than 1.0")
        require(loadFactor > 0.0, "Load factor must be greater than 0.0")
        自有构造器:
        def this(initialCapacity: Int) = this(initialCapacity, 0.7)
        功能: 初始化一个加载因子为 0.7 的快速hashSet
        
       	def this() = this(64)
        功能: 初始化一个初始容量为64的快速HashSet
        属性:
        #name @_capacity=nextPowerOf2(initialCapacity)	容量值(容量翻倍--> 增长策略)
        #name @_mask = _capacity - 1 掩码
        #name @_size = 0	hashset的规模大小
        #name @_growThreshold = (loadFactor * _capacity).toInt	增长容量
        #name @_bitset = new BitSet(_capacity)	定长集合
        #name @_data: Array[T] = _		为对象设置的底层存储数组
        #name @_data = new Array[T](_capacity)	为指定类型T设置的底层存储数组
        #name @hasher #Type @Hasher[T]	hash处理器
        val= {
            val mt = classTag[T] // 获取类名称,根据指定的类获取hash处理器
            if (mt == ClassTag.Long) {
              (new LongHasher).asInstanceOf[Hasher[T]]
            } else if (mt == ClassTag.Int) {
              (new IntHasher).asInstanceOf[Hasher[T]]
            } else if (mt == ClassTag.Double) {
              (new DoubleHasher).asInstanceOf[Hasher[T]]
            } else if (mt == ClassTag.Float) {
              (new FloatHasher).asInstanceOf[Hasher[T]]
            } else {
              new Hasher[T]
            }
          }
        
        操作集:
        def size: Int = _size
        功能: 获取集合规模大小
        
        def capacity: Int = _capacity
        功能: 获取集合容量
        
        def getBitSet: BitSet = _bitset
        功能: 获取定长集合
        
        def contains(k: T): Boolean = getPos(k) != INVALID_POS
        功能: 确定集合是否包含指定的key
        
        def add(k: T): Unit
        功能: 添加集合元素,如果添加之后超出容量,则需要进行rehash
        1. 添加元素k
        addWithoutResize(k)
        2. 进行可能的rehash
        rehashIfNeeded(k, grow, move)
        
        def union(other: OpenHashSet[T]): OpenHashSet[T]
        功能: 合并指定hashSet到本类
        val iterator = other.iterator
        while (iterator.hasNext) {
          add(iterator.next())
        }
        val= this
        
        def addWithoutResize(k: T): Int
        功能: 在不考虑重新修改set的规模情况下,添加元素
        1. 计算新元素的hashcode
        var pos = hashcode(hasher.hash(k)) & _mask
        2. 使用平方探测法循环探测找到hash槽的位置
        var delta = 1
        while (true) {
          if (!_bitset.get(pos)) {
            // 定长集合中不包含该元素,新增该元素
            _data(pos) = k
            _bitset.set(pos)
            _size += 1
            return pos | NONEXISTENCE_MASK
          } else if (_data(pos) == k) {
            // 找到指定位置,且已存在有值,直接返回(重复值)
            return pos
          } else {
            // 平方探测,确定下一个hash槽位置
            pos = (pos + delta) & _mask
            delta += 1
          }
        }
        
        def rehashIfNeeded(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit): Unit
        功能: 过载状态下对集合进行重新散列
        输入参数:
        	k	函数中未使用的参数,但是可以强迫scala编译器去特殊处理这个方法
        	allocateFunc	回调函数,用于分配一个新的更大的底层存储数组
        	moveFunc	移动函数(将旧数组中指定位置的数据移动到新数组中)
        if (_size > _growThreshold) {
          rehash(k, allocateFunc, moveFunc)
        }
        
        def getPos(k: T): Int
        功能: 获取指定key在底层数组中的位置,INVALID_POS表示找不到
        1. 计算指定key的hashCode,并计算位置
        var pos = hashcode(hasher.hash(k)) & _mask
        2. 使用平方探测法求出具体位置
        var delta = 1
        while (true) {
          if (!_bitset.get(pos)) {
            return INVALID_POS
          } else if (k == _data(pos)) {
            return pos
          } else {
            pos = (pos + delta) & _mask
            delta += 1
          }
        }
        
        def getValue(pos: Int): T = _data(pos)
    	功能: 获取指定位置的value值
        
        def iterator: Iterator[T]
        功能: 获取迭代器
        val= new Iterator[T] {
            var pos = nextPos(0)
            override def hasNext: Boolean = pos != INVALID_POS
            override def next(): T = {
              val tmp = getValue(pos)
              pos = nextPos(pos + 1)
              tmp
            }
          }
        
        def getValueSafe(pos: Int): T
        功能: 获取指定位置的值
        0. 位置断言(避免获取时出错)
        assert(_bitset.get(pos))
        1. 获取value值
        val= _data(pos)
        
        def nextPos(fromPos: Int): Int=_bitset.nextSetBit(fromPos)
        功能: 获取迭代器中下一个值的位置,始于包含给定的的位置
        
        def hashcode(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()
        功能: 计算hashcode 采样MURMUR3_32算法
        
        def nextPowerOf2(n: Int): Int
        功能: 计算2*n
        val= if (n == 0) {
          1
        } else {
          val highBit = Integer.highestOneBit(n)
          if (highBit == n) n else highBit << 1
        }
        
        def rehash(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit): Unit
        功能: rehash,表扩容,并对所有数据进行重新散列,
        输入参数: 
        	k	函数没有使用,但是可以提示scala编译器对其进行特殊化处理
        	allocateFunc	分配内存函数
        	moveFunc	移动元素函数
        1. 计算新容量
        val newCapacity = _capacity * 2
        require(newCapacity > 0 && newCapacity <= OpenHashSet.MAX_CAPACITY,
          s"Can't contain more than ${(loadFactor * OpenHashSet.MAX_CAPACITY).toInt} elements")
        2. 分配新容量的内存空间
        allocateFunc(newCapacity)
        3. 获取新的定长集合,底层数组,以及掩码值
        val newBitset = new BitSet(newCapacity)
        val newData = new Array[T](newCapacity)
        val newMask = newCapacity - 1
        4. 将旧数据迁移到新的底层数组中
        var oldPos = 0
        while (oldPos < capacity) {
          if (_bitset.get(oldPos)) {
            val key = _data(oldPos)
            var newPos = hashcode(hasher.hash(key)) & newMask
            var i = 1
            var keepGoing = true
            // No need to check for equality here when we insert so this has one less if branch than
            // the similar code path in addWithoutResize.
            while (keepGoing) {
              if (!newBitset.get(newPos)) {
                // Inserting the key at newPos
                newData(newPos) = key
                newBitset.set(newPos)
                moveFunc(oldPos, newPos)
                keepGoing = false
              } else {
                val delta = i
                newPos = (newPos + delta) & newMask
                i += 1
              }
            }
          }
          oldPos += 1
        }
        5. 设置新的底层数组指向,以及相关参数更新
        _bitset = newBitset
        _data = newData
        _capacity = newCapacity
        _mask = newMask
        _growThreshold = (loadFactor * newCapacity).toInt
    } 
    ```
    
    ```scala
    private[spark] object OpenHashSet {
        属性:
        #name @MAX_CAPACITY = 1 << 30	最大容量
        #name @INVALID_POS = -1	无效位置
        #name @NONEXISTENCE_MASK = 1 << 31	不存在的掩码值
        #name @POSITION_MASK = (1 << 31) - 1	位置掩码值
        #name @grow = grow1 _ 增长值
        #name @move = move1 _ 移动值
        样例类:
        class LongHasher extends Hasher[Long] {
            override def hash(o: Long): Int = (o ^ (o >>> 32)).toInt
          }
        介绍: long型hash处理器
        
        class IntHasher extends Hasher[Int] {
            override def hash(o: Int): Int = o
          }
        介绍: int型hash处理器
        
        class DoubleHasher extends Hasher[Double] {
            override def hash(o: Double): Int = {
              val bits = java.lang.Double.doubleToLongBits(o)
              (bits ^ (bits >>> 32)).toInt
            }
          }
        介绍: double型hash处理器
        
        class FloatHasher extends Hasher[Float] {
            override def hash(o: Float): Int = java.lang.Float.floatToIntBits(o)
          }
        功能: float型hash处理器
        
        def grow1(newSize: Int): Unit={}
        功能: 增长策略
        
        def move1(oldPos: Int, newPos: Int): Unit = { }
        功能: 移动方案
        
        sealed class Hasher[@specialized(Long, Int, Double, Float) T] extends Serializable {
            def hash(o: T): Int = o.hashCode()
          }
        功能: 通用hash处理器
    }
    ```
    
    #### PairsWriter
    
    ```scala
    /*
    	对于kv对消费者的抽象,持久化分区的数据时首先被使用.尽管也是可以通过shuffle写插件或者盘块写出器
    @DiskBlockObjectWriter 来持久化.
    */
    private[spark] trait PairsWriter {
    	操作集:
      	def write(key: Any, value: Any): Unit
        功能: 写出指定kv值
    }
    ```
    
    #### PartitionedAppendOnlyMap
    
    ```scala
    private[spark] class PartitionedAppendOnlyMap[K, V]
    extends SizeTrackingAppendOnlyMap[(Int, K), V] with WritablePartitionedPairCollection[K, V]{
        介绍: 按照形式(分区编号,key)包装一个map
        操作集:
        def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
        	: Iterator[((Int, K), V)]
        功能: 获取一个分区可破坏的迭代器
        1. 获取分区比较器
        val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
        2. 获取迭代器
        val= destructiveSortedIterator(comparator)
        
        def insert(partition: Int, key: K, value: V): Unit
        功能: 向分区中插入kv值	
        update((partition, key), value)
    }
    ```
    
    #### PartitionedPairBuffer
    
    ```scala
    private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64)
    extends WritablePartitionedPairCollection[K, V] with SizeTracker{
        介绍: kv对类型只添加类型的缓冲区,每个都有相对应的分区号.保证可以追踪到完成的字节量
        	这个缓冲区可以支持1073741819个元素
        属性:
        #name @capacity = initialCapacity	缓冲区容量大小
        #name @curSize = 0	当前完成的字节量大小
        #name @data = new Array[AnyRef](2 * initialCapacity)	数据列表
        	使用两倍容量用于容纳kv信息,可以使用@KVArraySortDataFormat高效排序
        操作集:
        def insert(partition: Int, key: K, value: V): Unit
        功能: 插入kv值到指定分区@partition 中
        1. 处理可能的容量扩充
        if (curSize == capacity) {
          growArray()
        }
        2. 设置kv到缓冲区中,并更新完成字节量大小
        data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
        data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
        curSize += 1
        3. 更新回调
        afterUpdate()
        
        def growArray(): Unit
        功能: 缓冲区扩充
        1. 检验可能出现的缓冲区大小超出限制
        if (capacity >= MAXIMUM_CAPACITY) {
          throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
        }
        2. 计算缓冲区的新容量
        val newCapacity =
          if (capacity * 2 > MAXIMUM_CAPACITY) { // 溢出取最大值
            MAXIMUM_CAPACITY
          } else { // 容量翻倍
            capacity * 2
          }
        3. 更新缓冲区数组,并将之前数据复制过去
        val newArray = new Array[AnyRef](2 * newCapacity)
        System.arraycopy(data, 0, newArray, 0, 2 * capacity)
        4. 重置数据和容量,并重置采样收集器
        data = newArray
        capacity = newCapacity
        resetSamples()
        
        def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
        : Iterator[((Int, K), V)]
        功能: 获取可毁坏的分区排序迭代器
        1. 获取指定分区的分区排序器
        val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
        2. 新建一个排序器,并对其进行排序,获取迭代器
        new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
        val= iterator
        
        def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)]
        功能: 获取迭代器
        val= new Iterator[((Int, K), V)] {
            var pos = 0
    
            override def hasNext: Boolean = pos < curSize
    
            override def next(): ((Int, K), V) = {
              if (!hasNext) {
                throw new NoSuchElementException
              }
              val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
              pos += 1
              pair
            }
          }
    }
    ```
    
    ```scala
    private object PartitionedPairBuffer {
        属性:
        #name @MAXIMUM_CAPACITY: Int = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 2
        	缓冲区最大容量
    }
    ```
    
    #### PrimitiveKeyOpenHashMap
    
    ```markdown
    介绍:
    	实现了非空值的原始的快速hashmap,这个hashmap支持插入和更新,但是不支持删除,这个map的速度要比普通hashmap要快,且占用更少的空间.主要是将空值的存储范围缩小了.
    	底层使用了@OpenHashSet 实现
    ```
    
    ```scala
    private[spark]
    class PrimitiveKeyOpenHashMap[@specialized(Long, Int) K: ClassTag,
                                  @specialized(Long, Int, Double) V: ClassTag](
        initialCapacity: Int) extends Iterable[(K, V)] with Serializable{
        参数断言:
        require(classTag[K] == classTag[Long] || classTag[K] == classTag[Int])
        功能: key必须是long或者int型
        默认构造器:
        def this() = this(64)
        功能: 默认创建64位容量的map
        属性:
        #name @_keySet: OpenHashSet[K] = _	name集合
        #name @_values: Array[V] = _	value集合
        #name @_keySet = new OpenHashSet[K](initialCapacity)	指定大小的key集合(用于指定类型T)
        #name @_values = new Array[V](_keySet.capacity)	value集合(用于指定类型value)
        #name @_oldValues: Array[V] = null	旧有的value列表
        #name @grow = (newCapacity: Int) => {
            _oldValues = _values
            _values = new Array[V](newCapacity)
          }
        	增长策略
        #name @move = (oldPos: Int, newPos: Int) => {
            _values(newPos) = _oldValues(oldPos)
          }
        	将旧位置的value移动到新的位置,用于处理rehash
        操作集:
        def size: Int = _keySet.size
        功能: 获取map的规模
        
        def contains(k: K): Boolean
        功能: 确认map是否包含指定key
        val=  _keySet.getPos(k) != OpenHashSet.INVALID_POS
        
        def apply(k: K): V
        功能: 获取指定key的value
        val pos = _keySet.getPos(k)
        val= _values(pos)
        
        def getOrElse(k: K, elseValue: V): V
        功能: 获取指定key的value,不存在则返回@elseValue
        val pos = _keySet.getPos(k)
        val= if (pos >= 0) _values(pos) else elseValue
        
        def update(k: K, v: V): Unit
        功能: 更新指定的kv
        val pos = _keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK // 获取指定位置pos
        _values(pos) = v // 更新这个位置的key
        _keySet.rehashIfNeeded(k, grow, move) //进行可能的rehash
        _oldValues = null// 重置旧值
        
        def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V
        功能: 如果key存在于hashmap中设置气质为默认值@defaultValue,否则设置为@mergeValue
        1. 获取指定key在value列表中的位置@pos
        val pos = _keySet.addWithoutResize(k)
        2. 判断key是否存在于列表中,存在则合并旧值@mergeValue并返回,否则使用默认值@defaultValue
        val= if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) { // 判断是否存在有这个key
          val newValue = defaultValue
          _values(pos & OpenHashSet.POSITION_MASK) = newValue
          _keySet.rehashIfNeeded(k, grow, move)
          newValue // 获取value为@defaultValue
        } else {
          _values(pos) = mergeValue(_values(pos))
          _values(pos)// 合并旧值,并返回
        }
        
        def iterator: Iterator[(K, V)] 
        功能: 获取迭代器
        val= new Iterator[(K, V)] {
            var pos = 0
            var nextPair: (K, V) = computeNextPair()
            def computeNextPair(): (K, V) = {
              pos = _keySet.nextPos(pos)
              if (pos >= 0) {
                val ret = (_keySet.getValue(pos), _values(pos))
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
    
    #### PrimitiveVector
    
    ```scala
    private[spark]
    class PrimitiveVector[@specialized(Long, Int, Double) V: ClassTag](initialSize: Int = 64) {
        介绍: 专为原语类型优化的,只能添加的,线程不安全的向量表
        属性:
        #name @_numElements = 0	元素数量(向量表末尾指针)
        #name @_array: Array[V] = _	向量表
        输出化操作:
        _array = new Array[V](initialSize)
        功能: 初始化向量表
        
        操作集:
        def apply(index: Int): V
        功能: 获取向量表指定位置@index的元素
        0. 范围断言
        require(index < _numElements)
        1. 获取元素
        val= _array(index)
        
        def +=(value: V): Unit
        功能: 添加元素到向量表中
        0. 处理可能的扩容情况
        if (_numElements == _array.length) {
          resize(_array.length * 2)
        }
        1. 添加到向量表中,并更新向量表元素数量信息
        _array(_numElements) = value
        _numElements += 1
        
        def capacity: Int = _array.length
        功能: 获取向量表长度
        
        def length: Int = _numElements
        功能: 向量表元素个数
        
        def size: Int = _numElements
        功能: 向量表元素个数
        
        def iterator: Iterator[V]
        功能: 获取迭代器
        val= new Iterator[V] {
            var index = 0
            override def hasNext: Boolean = index < _numElements
            override def next(): V = {
              if (!hasNext) {
                throw new NoSuchElementException
              }
              val value = _array(index)
              index += 1
              value
            }
          }
        
        def array: Array[V] = _array
        功能: 获取底层向量表
        
        def trim(): PrimitiveVector[V] = resize(size)
        功能: 截断向量表,使得容量等于向量表元素数量
        
        def resize(newLength: Int): PrimitiveVector[V]
        功能: 改变向量表的大小,异常之后的元素
        1. 获取一份向量表副本
        _array = copyArrayWithLength(newLength)
        2. 修改元素数量参数为新设置的长度
        if (newLength < _numElements) {
          _numElements = newLength
        }
        val= this
        
        def toArray: Array[V]
        功能: 转化为数组
        val= copyArrayWithLength(size)
        
        def copyArrayWithLength(length: Int): Array[V]
        功能: 将向量表拷贝到一份@length 长度的列表,多出的部分截断
        val copy = new Array[V](length)
        _array.copyToArray(copy)
        val= copy
    }
    ```
    
    #### SizeTracker
    
    ```markdown
    介绍:
    	集合类通用接口,追踪已经写完的字节数。
    	由于使用长度估量器@SizeEstimator 在某种程度上是开销较大的(几个ms),所有采样时使用@SizeEstimator带上低指数幂去分摊补偿时间.
    ```
    
    ```scala
    private[spark] trait SizeTracker {
        属性:
        #name @SAMPLE_GROWTH_RATE = 1.1	采样增长率
        	控制采样的指数,如果设置为2,则采用序列为 1,2,4,8...
        #name @samples = new mutable.Queue[Sample]	采样队列
        	自从上次重置采样队列之后,仅仅保有最后两个,用于使用外推插值法
        #name @bytesPerUpdate: Double = _	每次更新的最后两个采样值平均数
        	每次更新最后两个采样值的平均数
        #name @numUpdates: Long = _ 上次重置采样后插入/更新到map的记录数量
        #name @nextSampleNum: Long = _	下一个采样值
        初始化操作:
        resetSamples()
        功能: 重置采样队列
        
        操作集:
        def resetSamples(): Unit
        功能: 重置采样队列
        numUpdates = 1
        nextSampleNum = 1
        samples.clear()
        takeSample()
        
        def afterUpdate(): Unit
        功能: 每次更新之后的回调函数
        numUpdates += 1 // 更新数量+1
        if (nextSampleNum == numUpdates) { // 进行可能的采样工作
          takeSample()
        }
        
        def takeSample(): Unit
        功能: 获取当前集合类规模大小的采样值
        1. 将采样信息入列
        samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
        2. 进行适当的出列操作,使得使用最后两个进行外推插值法
        if (samples.size > 2) {
          samples.dequeue()
        }
        3. 进行外推插值法,先求出字节增长率(单词更新字节增长量)
        val bytesDelta = samples.toList.reverse match {
          case latest :: previous :: tail =>
            (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
          case _ => 0
        }
        bytesPerUpdate = math.max(0, bytesDelta)
        4. 计算下个采样值
        nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
        
        def estimateSize(): Long
        功能: 估量当前集合的大小,时间复杂度O(1)
        0. 采样断言
        assert(samples.nonEmpty)
        1. 估计外推插值法变化率
        val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
        2. 计算当前估量值
        val= (samples.last.size + extrapolatedDelta).toLong
    }
    ```
    
    ```scala
    private object SizeTracker {
      case class Sample(size: Long, numUpdates: Long)
    }
    ```
    
    #### SizeTrackingAppendOnlyMap
    
    ```scala
    private[spark] class SizeTrackingAppendOnlyMap[K, V]
    extends AppendOnlyMap[K, V] with SizeTracker{
        介绍: 只添加类型的map,可以使用统计方法追踪到预估字节量大小
        操作集:
        def update(key: K, value: V): Unit 
        功能: 更新kv对
        super.update(key, value) // 更新kv
        super.afterUpdate() // 进行回调,进行可能的采样工作
        
        def changeValue(key: K, updateFunc: (Boolean, V) => V): V
        功能: 更新key值的条目,默认值为函数@updateFunc 的输出值
        val newValue = super.changeValue(key, updateFunc)
        super.afterUpdate()
        val= newValue
        
        def growTable(): Unit
        功能: 对表进行扩容,并重置采样队列
        super.growTable()
        resetSamples()
    }
    ```
    
    #### SizeTrackingVector
    
    ```scala
    private[spark] class SizeTrackingVector[T: ClassTag] extends PrimitiveVector[T]{
        介绍: 一个只能添加的缓冲区,使用统计方法保证对字节量大小的估算
        操作集:
        def +=(value: T): Unit
        功能: 添加指定值@value 到向量表中
        super.+=(value)
        super.afterUpdate()
        
        def resize(newLength: Int): PrimitiveVector[T]
        功能: 重设向量表长度为@newLength
        super.resize(newLength)
        resetSamples() // 由于向量表长度变化会影响外推插值法的计算值,重置采样队列
    	val= this
    }
    ```
    
    #### SortDataFormat
    
    ```scala
    private[spark] abstract class SortDataFormat[K, Buffer] {
        介绍: 对输入缓冲数据的强制排序抽象,这个接口需要决定指定元素索引的排序key值,和交换元素,以及移动缓冲区数据的方法.
        示例类型: 一个数字组成的数组,每个元素又是key,可以看@KVArraySortDataFormat 获得更精确的格式信息.
        注意: 声明和实例化多个子类会组织JIT内联到以及重写的方法.因此减少了shuffle次数.
        构造器参数:
        	k	me每个元素的sort key值
        	Buffer	指定格式的内部数据结构
        操作集:
        def newKey(): K = null.asInstanceOf[K]
        功能: 创建一个新的可变的可以重用的key,如果你需要重写@getKey(Buffer, Int, K) 的话需要实现它
        
        def getKey(data: Buffer, pos: Int): K
        功能: 获取在指定位置的元素
        
        def getKey(data: Buffer, pos: Int, reuse: K): K
        功能: 返回指定索引位置的key值,如果可能的话重用输入的key.默认实现忽略了重用的情况调用的是@getKey(Buffer, Int),如果你想要的重写这个方法,你必须要实现@newKey()
        val= getKey(data, pos)
        
        def swap(data: Buffer, pos0: Int, pos1: Int): Unit
        功能: 交换两个元素
        
        def copyElement(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int): Unit
        功能: 从源地址拷贝元素到目标地址
        
        def copyRange(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int, length: Int): Unit
        功能: 范围拷贝,拷贝(srcPos到dst位置)
        
        def allocate(length: Int): Buffer
        功能: 分配容纳有@length 个元素的缓冲区,所有数据都被视作不可用知道有数据拷贝进来
    }
    ```
    
    ```scala
    private[spark]
    class KVArraySortDataFormat[K, T <: AnyRef : ClassTag] extends SortDataFormat[K, Array[T]] {
        介绍: 支持kv键值对的排序数组
        构造器参数:
        	K 排序key值
        	T 排序的数组
        操作集:
        def getKey(data: Array[T], pos: Int): K = data(2 * pos).asInstanceOf[K]
        功能: 获取key
        由于数组设计为2倍的长度大小,2*x为key,而2*x+1 为value
        
        def swap(data: Array[T], pos0: Int, pos1: Int): Unit
        功能: 交换位置pos0和pos1处的键值对信息
        val tmpKey = data(2 * pos0)
        val tmpVal = data(2 * pos0 + 1)
        data(2 * pos0) = data(2 * pos1)
        data(2 * pos0 + 1) = data(2 * pos1 + 1)
        data(2 * pos1) = tmpKey
        data(2 * pos1 + 1) = tmpVal
        
        def copyElement(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int): Unit
        功能: 将srcPos处的键值对信息拷贝到dst中
        dst(2 * dstPos) = src(2 * srcPos)
        dst(2 * dstPos + 1) = src(2 * srcPos + 1)
        
        def copyRange(src: Array[T], srcPos: Int,
          dst: Array[T], dstPos: Int, length: Int): Unit
        功能: 范围拷贝srcPos开始的@length个键值对
        System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length)
        
        def allocate(length: Int): Array[T] 
        功能: 分配指定数目@length的内存空间
        val= new Array[T](2 * length)
    }
    ```
    
    #### Sorter
    
    ```scala
    private[spark] class Sorter[K, Buffer](private val s: SortDataFormat[K, Buffer]) {
        介绍: java Timsort的简单包装java实现时私有的,假设不可以外部调用,这个就仅仅对于spark可以使用.
        构造器参数:
        	s	排序形式
        属性:
        #name @timSort = new TimSort(s)	timsort实例
        操作集:
        def sort(a: Buffer, lo: Int, hi: Int, c: Comparator[_ >: K]): Unit 
        功能: [lo,hi)范围之内使用TimSort
        val= timSort.sort(a, lo, hi, c)
    }
    ```
    
    #### Spillable
    
    ```scala
    private[spark] abstract class Spillable[C](taskMemoryManager: TaskMemoryManager)
    extends MemoryConsumer(taskMemoryManager) with Logging {
        介绍: 当超过内存容量时,内存集合类溢写到磁盘上的内容
        属性:
        #name @initialMemoryThreshold #Type @Long	初始化内存容量
        val= SparkEnv.get.conf.get(SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD)
        #name @numElementsForceSpillThreshold #Type @Int	强行溢写容量
        val= SparkEnv.get.conf.get(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD)
        #name @myMemoryThreshold = initialMemoryThreshold volatile	自有内存容量
        #name @_elementsRead = 0	上次溢写以来读取元素数量
        #name @_memoryBytesSpilled = 0L volatile	内存溢写总字节量
        #name @_spillCount = 0	溢写计数器
        操作集:
        def spill(collection: C): Unit
        功能: 将当前内存集合类溢写到磁盘上,并释放溢写部分的内存
        输入参数: collection	需要溢写到磁盘上的集合
        
        def forceSpill(): Boolean
        功能: 获取是否需要强行溢写
        强行溢写内存集合到磁盘上,并释放内存,当任务没有足够内存的时候,有任务内存管理器@TaskMemoryManager 调用这个方法,强行的将内存集合溢写到磁盘上,并释放足够的内存给任务.
        
        def elementsRead: Int = _elementsRead
        功能: 读取自上次溢写以来,输入读取的元素数量
        
        def addElementsRead(): Unit = { _elementsRead += 1 }
        功能: 子类在每次记录被读取时调用,用于检查溢写频率
        
        def memoryBytesSpilled: Long = _memoryBytesSpilled
        功能: 获取内存溢写字节量
        
        def releaseMemory(): Unit
        功能: 释放内存
        freeMemory(myMemoryThreshold - initialMemoryThreshold)
        myMemoryThreshold = initialMemoryThreshold
        
        @inline def logSpillage(size: Long): Unit
        功能: 日志溢写,将标准日志信息写出
        val threadId = Thread.currentThread().getId
        logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
          .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
            _spillCount, if (_spillCount > 1) "s" else ""))
        
        def spill(size: Long, trigger: MemoryConsumer): Long 
        功能: 指定内存消费者@trigger 释放指定大小@size的内存,返回实际释放内存数量
        val= if (trigger != this && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
            // 释放在堆模式下的内存
          val isSpilled = forceSpill() // 确认是否需要强制溢写(任务内存是否足够)
          if (!isSpilled) {
            0L
          } else { // 释放内存
            val freeMemory = myMemoryThreshold - initialMemoryThreshold // 计算需要释放的内存量
            _memoryBytesSpilled += freeMemory // 更新度量值
            releaseMemory() // 释放内存
            val= freeMemory
          }
        } else {
          0L
        }
        
        def maybeSpill(collection: C, currentMemory: Long): Boolean
        功能: 确定指定集合@collection 是否需要被是否
        	返回true表示已经被溢写到磁盘上,false则表示没有溢写到磁盘上
        1. 向shuffle内存池中申请内存(为当前内存的2倍-自有内存)
        var shouldSpill = false
        if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
            // 处理自有内存不足的情况
            val amountToRequest = 2 * currentMemory - myMemoryThreshold // 计算需要申请的内存数量
            val granted = acquireMemory(amountToRequest)	// 申请指定的内存量
            myMemoryThreshold += granted	// 更新自有内存@myMemoryThreshold
        	shouldSpill = currentMemory >= myMemoryThreshold // 确定是否是要溢写
        }
        2. 对需要溢写的情况进行溢写
        // 溢写条件: 1. 自有内存不足 2. 强行溢写容量不足以不足以读取足够的元素
         shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
        if (shouldSpill) {
            // 执行写,并打印出日志,重置相关参数
          _spillCount += 1
          logSpillage(currentMemory)
          spill(collection)
          _elementsRead = 0
          _memoryBytesSpilled += currentMemory
          releaseMemory()
        }
    }
    ```
    
    #### Utils
    
    ```scala
    private[spark] object Utils {
        介绍: 集合类的实用功能
        操作集:
        def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): Iterator[T]
        功能: 获取top K的功能,参数k由@num指定 
        1. 获取排序结果
        val ordering = new GuavaOrdering[T] {
          override def compare(l: T, r: T): Int = ord.compare(l, r)
        }
        2. 获取topK
        val= ordering.leastOf(input.asJava, num).iterator.asScala
    }
    ```
    
    #### WritablePartitionedPairCollection
    
    ```scala
      private[spark] trait WritablePartitionedPairCollection[K, V] {
          介绍: 可写分区键值对集合
          	这是一个通用接口,用于对键值对的字节大小定位,具有如下功能
          	1. 每个键值对都有相关分区
          	2. 支持高效的内存排序的迭代器
          	3. 支持可写分区迭代器,可以直接以字节形式写出
          操作集:
          def insert(partition: Int, key: K, value: V): Unit
          功能: 插入键值对到分区中
          
          def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
        	: Iterator[((Int, K), V)]
          功能: 通过对数据的分区ID排序,这个操作可能破坏底层集合类
          
          def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
          	: WritablePartitionedIterator
          功能: 写出迭代器中的元素而不是直接返回迭代器,记录按照分区编号的顺序返回,这个可能破坏底层集合
            val it = partitionedDestructiveSortedIterator(keyComparator)
            new WritablePartitionedIterator {
              private[this] var cur = if (it.hasNext) it.next() else null // 获取一条记录
              def writeNext(writer: PairsWriter): Unit = { // 写出一条记录,并移动指针到下一个位置
                writer.write(cur._1._2, cur._2)
                cur = if (it.hasNext) it.next() else null
              }
              def hasNext(): Boolean = cur != null // 确认是否有下一个元素
              def nextPartition(): Int = cur._1._1 // 获取下一个分区
            }
      }
    ```
    
    ```scala
    private[spark] object WritablePartitionedPairCollection {
        操作集:
        def partitionComparator[K]: Comparator[(Int, K)] = (a: (Int, K), b: (Int, K)) => a._1 - b._1
        功能: 获取分区比较器(根据分区ID排序)
        
        def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)]
        功能: 获取分区比较器(同时按照分区ID和key排序)
        val= (a: (Int, K), b: (Int, K)) => {
          val partitionDiff = a._1 - b._1
          if (partitionDiff != 0) {
            partitionDiff // 第一关键字排序逻辑
          } else {
            keyComparator.compare(a._2, b._2) // 第二关键字排序逻辑
          }
        }
    }
    ```
    
    ```scala
    private[spark] trait WritablePartitionedIterator {
        介绍: 迭代器,用于将数据写出到磁盘写出器上@DiskBlockObjectWriter 每个元素都有与之相关的分区
        操作集:
        def writeNext(writer: PairsWriter): Unit
        功能: 写出一条记录,并移动读写指针
        
        def hasNext(): Boolean
        功能: 确认是否有下一条记录
        
        def nextPartition(): Int
        功能: 获取下一个分区的分区编号
    }
    ```

#### io

1.  [ChunkedByteBuffer.scala](# ChunkedByteBuffer)

2.  [ChunkedByteBufferFileRegion.scala](# ChunkedByteBufferFileRegion)

3.  [ChunkedByteBufferOutputStream.scala](# ChunkedByteBufferOutputStream)

   ---

   #### ChunkedByteBuffer

   ```markdown
介绍:
   	只读形式的字节缓冲区,物理存储于多个数据块(chunk)中,而非简单的储存在连续的数组空间内存中.
   构造器参数:
   	chunk: 字节缓冲区@ByteBuffer 的数组,每个字节缓冲区读写指针开始都是0,这个缓冲区的使用权移交给
   	@ChunkedByteBuffer,所以如果这些缓冲区也可以用在其他地方,调用者需要将它们复制出来.
   ```
   
   ```scala
   private[spark] class ChunkedByteBuffer(var chunks: Array[ByteBuffer]) {
       属性:
       #name @bufferWriteChunkSize #Type @Int	写缓冲块大小
       val= Option(SparkEnv.get).map(_.conf.get(config.BUFFER_WRITE_CHUNK_SIZE))
         .getOrElse(config.BUFFER_WRITE_CHUNK_SIZE.defaultValue.get).toInt
       #name @disposed: Boolean = false	健康状态标志
       #name @size: Long = chunks.map(_.limit().asInstanceOf[Long]).sum	当前缓冲大小
       操作集:
       def writeFully(channel: WritableByteChannel): Unit
       功能: 将缓冲块中内容写入到指定通道@channel
       for (bytes <- getChunks()) {
         val originalLimit = bytes.limit()
         while (bytes.hasRemaining) {
           // 如果获取的数据@bytes时处于堆上,NIO API写出时会将其拷贝到直接字节缓冲区,这个临时的字节缓冲区每		// 个线程都会被缓存,它的大小随着输入字节缓冲区的扩大没有限制的话,会导致本地内存泄漏,如果不对大量的	   //  直接字节缓冲区的内存资源进行释放,这里需要写出定长的字节数量,去形成直接字节缓冲区,从而避免内存		   // 泄漏
           val ioSize = Math.min(bytes.remaining(), bufferWriteChunkSize)
           bytes.limit(bytes.position() + ioSize) // 限定缓冲区大小,避免内存泄漏
           channel.write(bytes)
           bytes.limit(originalLimit)
         }
       }
       
       def toNetty: ChunkedByteBufferFileRegion
       功能: 转换为netty传输(包装@FileRegion 使其能够传输超过2GB数据)
       val= new ChunkedByteBufferFileRegion(this, bufferWriteChunkSize)
       
       def toArray: Array[Byte] 
       功能: 将缓冲区内容拷贝到数组中
       if (size >= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
         throw new UnsupportedOperationException(
           s"cannot call toArray because buffer size ($size bytes) exceeds maximum array size")
       }
       val byteChannel = new ByteArrayWritableChannel(size.toInt)
       writeFully(byteChannel)
       byteChannel.close()
       byteChannel.getData
       
       def toByteBuffer: ByteBuffer
       功能: 将字节缓冲块转化为普通的字节缓冲区
       val= if (chunks.length == 1) {
         chunks.head.duplicate()
       } else {
         ByteBuffer.wrap(toArray)
       }
       
       def toInputStream(dispose: Boolean = false): InputStream
       功能: 使用输入流读取字节缓冲块的数据
       输入参数: 
       dispose 为true时,会在流式读取之后调用去关闭内存映射文件(这个会返回这个缓冲区)
       val= new ChunkedByteBufferInputStream(this, dispose)
       
       def getChunks(): Array[ByteBuffer]
       功能: 获取字节缓冲区列表副本
       val= chunks.map(_.duplicate())
       
       def dispose(): Unit
       功能: 清除任意一个直接字节缓冲区/内存映射文件缓冲区
       参考@StorageUtils.dispose 获取更多信息
       if (!disposed) {
         chunks.foreach(StorageUtils.dispose)
         disposed = true
       }
       
       def copy(allocator: Int => ByteBuffer): ChunkedByteBuffer
       功能: 拷贝一份
       val= val copiedChunks = getChunks().map { chunk =>
         val newChunk = allocator(chunk.limit())
         newChunk.put(chunk)
         newChunk.flip()
         newChunk
       }
       new ChunkedByteBuffer(copiedChunks)
   }
   ```
   
   ```scala
   private[spark] object ChunkedByteBuffer {
       def fromManagedBuffer(data: ManagedBuffer): ChunkedByteBuffer
       功能: 从管理缓冲区@data 获取字节缓冲块
       val= data match {
         case f: FileSegmentManagedBuffer =>
           fromFile(f.getFile, f.getOffset, f.getLength)
         case e: EncryptedManagedBuffer =>
           e.blockData.toChunkedByteBuffer(ByteBuffer.allocate _)
         case other =>
           new ChunkedByteBuffer(other.nioByteBuffer())
       }
       
       def fromFile(file: File): ChunkedByteBuffer = fromFile(file, 0, file.length())
       功能: 从文件获取缓冲数据块
       
       def fromFile(file: File,offset: Long,length: Long): ChunkedByteBuffer
   	功能: 从文件中获取缓冲数据块
       val is = new FileInputStream(file)
       ByteStreams.skipFully(is, offset)
       val in = new LimitedInputStream(is, length)
       val chunkSize = math.min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH, length).toInt
       val out = new ChunkedByteBufferOutputStream(chunkSize, ByteBuffer.allocate _)
       Utils.tryWithSafeFinally {
         IOUtils.copy(in, out)
       } {
         in.close()
         out.close()
       }
       val= out.toChunkedByteBuffer
   }
   ```
   
   ```scala
   private[spark] class ChunkedByteBufferInputStream(
       var chunkedByteBuffer: ChunkedByteBuffer,dispose: Boolean)
   extends InputStream{
       功能: 数据块输入流
       构造器参数
       chunkedByteBuffer	 缓冲数据块
       dispose		为true 表示流结束时关闭内存映射内存
       属性:
       #name @chunks = chunkedByteBuffer.getChunks().filter(_.hasRemaining).iterator
       	非空缓冲区列表
       #name @currentChunk: ByteBuffer	当前缓冲区
       val= if (chunks.hasNext) chunks.next() else null
       
       操作集:
       def read(): Int
       功能: 读取字节缓冲区的一个字节,并移动缓冲区内读写指针
       if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
         currentChunk = chunks.next()
       }
       val= if (currentChunk != null && currentChunk.hasRemaining) {
         UnsignedBytes.toInt(currentChunk.get())
       } else {
         close()
         -1
       }
       
       def read(dest: Array[Byte], offset: Int, length: Int): Int
       功能: 读取范围内部数据
       if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
         currentChunk = chunks.next()
       }
       val= if (currentChunk != null && currentChunk.hasRemaining) {
         val amountToGet = math.min(currentChunk.remaining(), length)
         currentChunk.get(dest, offset, amountToGet)
         amountToGet
       } else {
         close()
         -1
       }
       
       def skip(bytes: Long): Long
       功能: 跳读指定字节数量的数据@bytes,对应到底层就是移动读写指针,返回实际跳读的字节数量
       val = if (currentChunk != null) {
         val amountToSkip = math.min(bytes, currentChunk.remaining).toInt
         currentChunk.position(currentChunk.position() + amountToSkip)
         if (currentChunk.remaining() == 0) {
           if (chunks.hasNext) {
             currentChunk = chunks.next()
           } else {
             close()
           }
         }
         amountToSkip
       } else {
         0L
       }
       
       def close(): Unit
       功能: 关流
       if (chunkedByteBuffer != null && dispose) {
         chunkedByteBuffer.dispose() // 取消文件映射内存
       }
       chunkedByteBuffer = null
       chunks = null
       currentChunk = null
   }
   ```
   
   
   
   #### ChunkedByteBufferFileRegion
   
   ```markdown
   介绍:
   	已netty的文件区域来暴露一个块状字节缓冲区@ChunkedByteBuffer,一个netty消息仅仅允许发送超过2GB的数据.因为netty不能发送超过2GB大小的缓冲区数据,但是它可以发送一个大的文件区域@FileRegion,尽管这些数据不是由一个文件返回的.
   ```
   
   ```scala
   private[io] class ChunkedByteBufferFileRegion(
       private val chunkedByteBuffer: ChunkedByteBuffer,
       private val ioChunkSize: Int) extends AbstractFileRegion {
       构造器参数:
       chunkedByteBuffer	块状字节缓冲区
       ioChunkSize	io块大小
       属性:
       #name @_transferred: Long = 0	转换字节数量
       #name @chunks = chunkedByteBuffer.getChunks()	字节缓冲块
       #name @size = chunks.foldLeft(0L) { _ + _.remaining() }	字节缓冲块大小
       #name @currentChunkIdx = 0	当前块编号
       操作集:
       def deallocate: Unit = {}
       功能: 解除分配
       
       def count(): Long = size
       功能: 获取字节缓冲区大小
       
       def position(): Long = 0
       功能: 获取返回文件数据的起始指针,不是当读写指针
       
       def transferred(): Long = _transferred
       功能: 获取传输字节量
       
       def transferTo(target: WritableByteChannel, position: Long): Long
       操作条件: 转换的字节指针移动完毕 
       assert(position == _transferred)
       功能: 获取需要转换的字节数量
       if (position == size) return 0L // 全部转换完毕,需要转换数量为0
       1. 设置起始参数
       var keepGoing = true
       var written = 0L
       var currentChunk = chunks(currentChunkIdx)
       2. 读取数据
       while (keepGoing) {
         while (currentChunk.hasRemaining && keepGoing) {
           val ioSize = Math.min(currentChunk.remaining(), ioChunkSize) // 获取当前块数据剩余量
           val originalLimit = currentChunk.limit() // 获取初始状态下,缓冲区上界
           currentChunk.limit(currentChunk.position() + ioSize) 
           val thisWriteSize = target.write(currentChunk) // 写出当前块数据
           currentChunk.limit(originalLimit) // 移动上界位置为@originalLimit(因为写了这么多)
           written += thisWriteSize // 更新写出字节量
           if (thisWriteSize < ioSize) {// 没有写够指定字节数,不允许停止写
             keepGoing = false
           }
         }
         if (keepGoing) { // 写完一个数据缓冲区,,但是仍旧没有写够指定字节量的处理方案
           currentChunkIdx += 1 // 移动块编号指针
           if (currentChunkIdx == chunks.size) { //跳出条件设置(没有更多的数据块了)
             keepGoing = false
           } else {
             currentChunk = chunks(currentChunkIdx)// 设置数据块内容
           }
         }
       }
   }
   ```
   
   #### ChunkedByteBufferOutputStream
   
   ```markdown
   介绍:
   	输出流,用户写数据到定长数据块(chunk)中
   ```
   
   ```scala
   private[spark] class ChunkedByteBufferOutputStream(chunkSize: Int,
       allocator: Int => ByteBuffer) extends OutputStream {
       构造器参数: 
       	chunkSize	块大小
       	allocator	分配函数
       属性:
       #name @toChunkedByteBufferWasCalled = false	是否使用了块状字节缓冲区
       #name @chunks = new ArrayBuffer[ByteBuffer]	字节缓冲块
       #name @lastChunkIndex = -1	最后一块的块号
       #name @position = chunkSize	写指针的位置(处于上次使用块中,如果等于块大小,则需要分配一个新的块并置0)
       #name @_size = 0	数据流大小(字节量)
       #name @closed: Boolean = false	是否关闭标志
       操作集:
       def size: Long = _size
       功能: 获取数据流大小
       
       def close(): Unit
       功能: 关闭输出流
       if (!closed) {
         super.close()
         closed = true
       }
       
       def write(b: Int): Unit
       功能: 写入b到数据块@chunk中,并移动写指针,更新写出大小
       require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
       allocateNewChunkIfNeeded()
       chunks(lastChunkIndex).put(b.toByte)
       position += 1
       _size += 1
       
       def write(bytes: Array[Byte], off: Int, len: Int): Unit 
       功能: 写出指定范围的字节(批量写入)
       require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
       var written = 0
       while (written < len) { // 批量写入
         allocateNewChunkIfNeeded()
         val thisBatch = math.min(chunkSize - position, len - written)
         chunks(lastChunkIndex).put(bytes, written + off, thisBatch)
         written += thisBatch
         position += thisBatch
       }
       _size += len
       
       @inline
       private def allocateNewChunkIfNeeded(): Unit
       功能: 有必要的情况下分配新的数据块
       if (position == chunkSize) {
         chunks += allocator(chunkSize)
         lastChunkIndex += 1
         position = 0
       }
       
       def toChunkedByteBuffer: ChunkedByteBuffer
       功能: 转化为块状缓冲区@ChunkedByteBuffer
       1. 状态断言
       require(closed, "cannot call toChunkedByteBuffer() unless close() has been called")
       require(!toChunkedByteBufferWasCalled, "toChunkedByteBuffer() can only be called once")
       2. 修改调用标记位
       toChunkedByteBufferWasCalled = true
       3. 获取块状字节缓冲区@ChunkedByteBuffer
       if (lastChunkIndex == -1) {
         new ChunkedByteBuffer(Array.empty[ByteBuffer])
       } else {
         val ret = new Array[ByteBuffer](chunks.size)
         for (i <- 0 until chunks.size - 1) {
           ret(i) = chunks(i)
           ret(i).flip()
         }
         if (position == chunkSize) {
           ret(lastChunkIndex) = chunks(lastChunkIndex)
           ret(lastChunkIndex).flip()
         } else {
           ret(lastChunkIndex) = allocator(position)
           chunks(lastChunkIndex).flip()
           ret(lastChunkIndex).put(chunks(lastChunkIndex))
           ret(lastChunkIndex).flip()
           StorageUtils.dispose(chunks(lastChunkIndex))
         }
         new ChunkedByteBuffer(ret)
       }
   }
   ```

#### logging

1.  [DriverLogger.scala](# DriverLogger)

2.  [FileAppender.scala](# FileAppender)

3.  [RollingFileAppender.scala](# RollingFileAppender)

4.  [RollingPolicy.scala](# RollingPolicy)

   ---

   #### DriverLogger

   ```scala
private[spark] class DriverLogger(conf: SparkConf) extends Logging {
       属性:
    #name @UPLOAD_CHUNK_SIZE = 1024 * 1024 上传块大小
       #name @UPLOAD_INTERVAL_IN_SECS	上传时间间隔
       #name @DEFAULT_LAYOUT = "%d{yy/MM/dd HH:mm:ss.SSS} %t %p %c{1}: %m%n"	默认格式
       #name @LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)
       	日志文件权限(默认770)
       #name @localLogFile #Type @String  本地日志文件路径
       val= FileUtils.getFile(
           Utils.getLocalDir(conf),
           DriverLogger.DRIVER_LOG_DIR,
           DriverLogger.DRIVER_LOG_FILE).getAbsolutePath()
   	#name @writer: Option[DfsAsyncWriter] = None 写出器
       初始化操作:
       addLogAppender()
       功能: 初始化日志添加器
       
       操作集:
       def addLogAppender(): Unit
       功能: 日志添加器初始化
       1. 获取日志添加器
       val appenders = LogManager.getRootLogger().getAllAppenders()
       2. 获取日志格式
       val layout = if (conf.contains(DRIVER_LOG_LAYOUT)) {
         new PatternLayout(conf.get(DRIVER_LOG_LAYOUT).get)
       } else if (appenders.hasMoreElements()) {
         appenders.nextElement().asInstanceOf[Appender].getLayout()
       } else {
         new PatternLayout(DEFAULT_LAYOUT)
       }
       3. 设置log4j日志添加器
       val fa = new Log4jFileAppender(layout, localLogFile)
       fa.setName(DriverLogger.APPENDER_NAME)
       LogManager.getRootLogger().addAppender(fa)
       
       def startSync(hadoopConf: Configuration): Unit
       功能: 同步启动
       try {
         val appId = Utils.sanitizeDirName(conf.getAppId)
         // 设置将本地文件到hdfs的写出器(异步写出)
         writer = Some(new DfsAsyncWriter(appId, hadoopConf))
       } catch {
         case e: Exception =>
           logError(s"Could not persist driver logs to dfs", e)
       }
       
       def stop(): Unit
       功能: 停止(停止所有写出器),最后删除本地日志文件
       try {
         val fa = LogManager.getRootLogger.getAppender(DriverLogger.APPENDER_NAME)
         LogManager.getRootLogger().removeAppender(DriverLogger.APPENDER_NAME)
         Utils.tryLogNonFatalError(fa.close())
         writer.foreach(_.closeWriter())
       } catch {
         case e: Exception =>
           logError(s"Error in persisting driver logs", e)
       } finally {
         Utils.tryLogNonFatalError {
           JavaUtils.deleteRecursively(FileUtils.getFile(localLogFile).getParentFile())
         }
       }
   }
   ```
   
   #subclass @DriverLogger.DfsAsyncWriter
   
   ```scala
   private[spark] class DfsAsyncWriter(appId: String, hadoopConf: Configuration) 
   extends Runnable with Logging {
       构造器属性:
       appId	应用ID
       hadoopConf	hadoop配置
       属性:
       #name @streamClosed = false	流关闭标志
       #name @inStream: InputStream = null	输入流
       #name @outputStream: FSDataOutputStream = null	文件系统输出流
       #name @tmpBuffer = new Array[Byte](UPLOAD_CHUNK_SIZE)	临时缓冲区
       #name @threadpool: ScheduledExecutorService = _	线程池
       初始化操作:
       init()
       功能: 初始化操作
       
       操作集:
       def init(): Unit
       功能: 初始化
       1. 获取DFS文件系统的文件目录
       val rootDir = conf.get(DRIVER_LOG_DFS_DIR).get
       2. 获取文件系统实例并校验
       val fileSystem: FileSystem = new Path(rootDir).getFileSystem(hadoopConf)
         if (!fileSystem.exists(new Path(rootDir))) {
           throw new RuntimeException(s"${rootDir} does not exist." +
             s" Please create this dir in order to persist driver logs")
         }
       3. 获取日志在dfs系统上的位置
       val dfsLogFile: String = FileUtils.getFile(rootDir, appId
           + DriverLogger.DRIVER_LOG_FILE_SUFFIX).getAbsolutePath()
       4. 设置写出器的写入@inStream 和写出@outputStream ,并设置文件的权限
       try {
           inStream = new BufferedInputStream(new FileInputStream(localLogFile))
           outputStream = SparkHadoopUtil.createFile(fileSystem, new Path(dfsLogFile),
             conf.get(DRIVER_LOG_ALLOW_EC))
           fileSystem.setPermission(new Path(dfsLogFile), LOG_FILE_PERMISSIONS)
         } catch {
           case e: Exception =>
             JavaUtils.closeQuietly(inStream)
             JavaUtils.closeQuietly(outputStream)
             throw e
         }
       5. 获取线程池对象,并对任务进行调度
       threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("dfsSyncThread")
       threadpool.scheduleWithFixedDelay(this, UPLOAD_INTERVAL_IN_SECS, UPLOAD_INTERVAL_IN_SECS,
           TimeUnit.SECONDS)
       
       def run(): Unit
       功能: 写出器运行过程
       // 读取本地日志文件,并将其写入到dfs系统上
       if (streamClosed) {
           return
         }
         try {
           var remaining = inStream.available()
           val hadData = remaining > 0
           while (remaining > 0) {
             val read = inStream.read(tmpBuffer, 0, math.min(remaining, UPLOAD_CHUNK_SIZE))
             outputStream.write(tmpBuffer, 0, read)
             remaining -= read
           }
           if (hadData) {
             outputStream match {
               case hdfsStream: HdfsDataOutputStream =>
                 hdfsStream.hsync(EnumSet.allOf(classOf[HdfsDataOutputStream.SyncFlag]))
               case other =>
                 other.hflush()
             }
           }
         } catch {
           case e: Exception => logError("Failed writing driver logs to dfs", e)
         }
       
       def close(): Unit 
       功能: 关闭异步写出
       // 修改状态位,输入输出流关闭
       if (streamClosed) {
           return
         }
         try {
           // Write all remaining bytes
           run()
         } finally {
           try {
             streamClosed = true
             inStream.close()
             outputStream.close()
           } catch {
             case e: Exception =>
               logError("Error in closing driver log input/output stream", e)
           }
         }
       
       def closeWriter(): Unit
       功能: 关闭写出器(关闭写出器线程)
       try {
           threadpool.execute(() => DfsAsyncWriter.this.close())
           threadpool.shutdown()
           threadpool.awaitTermination(1, TimeUnit.MINUTES)
         } catch {
           case e: Exception =>
             logError("Error in shutting down threadpool", e)
         }
   }
   ```
   
   ```scala
   private[spark] object DriverLogger extends Logging {
       属性:
       #name @DRIVER_LOG_DIR = "__driver_logs__"	驱动器日志目录
       #name @DRIVER_LOG_FILE = "driver.log"	驱动器日志文件
       #name @DRIVER_LOG_FILE_SUFFIX = "_" + DRIVER_LOG_FILE	驱动器文件前缀
       #name @APPENDER_NAME = "_DriverLogAppender"	日志添加器名称
       操作集:
       def apply(conf: SparkConf): Option[DriverLogger] 
       功能: 获取一个驱动器日志实例@DriverLogger
       val= if (conf.get(DRIVER_LOG_PERSISTTODFS) && Utils.isClientMode(conf)) {
         if (conf.contains(DRIVER_LOG_DFS_DIR)) {
           try {
             Some(new DriverLogger(conf))
           } catch {
             case e: Exception =>
               logError("Could not add driver logger", e)
               None
           }
         } else {
           logWarning(s"Driver logs are not persisted because" +
             s" ${DRIVER_LOG_DFS_DIR.key} is not configured")
           None
         }
       } else {
         None
       }
   }
   ```
   
   #### FileAppender
   
   ```scala
   private[spark] class FileAppender(inputStream: InputStream, file: File, bufferSize: Int = 8192)
   extends Logging {
       构造器参数:
       inputStream	输入流
       file	文件对象
       bufferSize	缓冲区大小(8 kb)
       属性:
       #name @outputStream: FileOutputStream = null volatile 文件输出流
       #name @markedForStop = false	volatile	停止标志
       #name @writingThread #Type @Thread	写出线程
       val= new Thread("File appending thread for " + file) {
           setDaemon(true)
           override def run(): Unit = {
             Utils.logUncaughtExceptions {
               appendStreamToFile()
             }
           }
         }
       操作集:
       def awaitTermination(): Unit
   	功能: 等待写出线程终止
       
       def stop(): Unit
       功能: 停止文件添加器
       markedForStop = true
       
       def appendStreamToFile(): Unit
       功能: 流式读取输入并将其写入文件中
       try {
         logDebug("Started appending thread")
         Utils.tryWithSafeFinally {
           openFile()
           val buf = new Array[Byte](bufferSize)
           var n = 0
           while (!markedForStop && n != -1) {
             try {
               n = inputStream.read(buf)
             } catch {
               case _: IOException if markedForStop => // 停止了什么都不做
             }
             if (n > 0) {
               appendToFile(buf, n)
             }
           }
         } {
           closeFile()
         }
       } catch {
         case e: Exception =>
           logError(s"Error writing stream to file $file", e)
       }
       
       def appendToFile(bytes: Array[Byte], len: Int): Unit
       功能: 添加字节数组@bytes 到输出流中
       1. 边界条件: 不存在输出流的状况
       if (outputStream == null) {
         openFile()
       }
       2. 写出指定内容@bytes
       outputStream.write(bytes, 0, len)
       
       def openFile(): Unit
       功能: 打开文件输出流
       outputStream = new FileOutputStream(file, true)
       
       def closeFile(): Unit
       功能: 关闭文件输出流
       outputStream.flush()
       outputStream.close()
   }
   ```
   
   ```scala
   private[spark] object FileAppender extends Logging {
       操作集:
       def apply(inputStream: InputStream, file: File, conf: SparkConf): FileAppender
       功能: 获取一个文件添加器@FileAppender 实例
       输入参数: 
       	inputStream	指定输入流
       	file	指定输出位置
       	conf	spark应用配置
       1. 获取日志滚动参数
       val rollingStrategy = conf.get(config.EXECUTOR_LOGS_ROLLING_STRATEGY) // 滚动策略 
       val rollingSizeBytes = conf.get(config.EXECUTOR_LOGS_ROLLING_MAX_SIZE)// 互动字节量
       val rollingInterval = conf.get(config.EXECUTOR_LOGS_ROLLING_TIME_INTERVAL) // 滚动时间间隔
       2. 根据指定的策略不同选用不同的创建方式
       rollingStrategy match {
         case "" =>
           new FileAppender(inputStream, file)
         case "time" =>
           createTimeBasedAppender()
         case "size" =>
           createSizeBasedAppender()
         case _ =>
           logWarning(
             s"Illegal strategy [$rollingStrategy] for rolling executor logs, " +
               s"rolling logs not enabled")
           new FileAppender(inputStream, file)
       }
       
       def createSizeBasedAppender(): FileAppender
       功能: 基于大小的文件添加器@FileAppender 
       rollingSizeBytes match {
           case IntParam(bytes) =>
             logInfo(s"Rolling executor logs enabled for $file with rolling every $bytes bytes")
             new RollingFileAppender(inputStream, file, new SizeBasedRollingPolicy(bytes), conf)
           case _ =>
             logWarning(
               s"Illegal size [$rollingSizeBytes] for rolling executor logs, 
               rolling logs not enabled")
             new FileAppender(inputStream, file)
         }
       
       def createTimeBasedAppender(): FileAppender
       功能: 创建时基文件添加器@FileAppender
       val validatedParams: Option[(Long, String)] = rollingInterval match {
           case "daily" =>
             logInfo(s"Rolling executor logs enabled for $file with daily rolling")
             Some((24 * 60 * 60 * 1000L, "--yyyy-MM-dd"))
           case "hourly" =>
             logInfo(s"Rolling executor logs enabled for $file with hourly rolling")
             Some((60 * 60 * 1000L, "--yyyy-MM-dd--HH"))
           case "minutely" =>
             logInfo(s"Rolling executor logs enabled for $file with rolling every minute")
             Some((60 * 1000L, "--yyyy-MM-dd--HH-mm"))
           case IntParam(seconds) =>
             logInfo(s"Rolling executor logs enabled for $file with rolling $seconds seconds")
             Some((seconds * 1000L, "--yyyy-MM-dd--HH-mm-ss"))
           case _ =>
             logWarning(s"Illegal interval for rolling executor logs [$rollingInterval], " +
                 s"rolling logs not enabled")
             None
         }
         validatedParams.map {
           case (interval, pattern) =>
             new RollingFileAppender(
               inputStream, file, new TimeBasedRollingPolicy(interval, pattern), conf)
         }.getOrElse {
           new FileAppender(inputStream, file)
         }
   }
   ```
   
   #### RollingFileAppender
   
   ```markdown
   介绍:
   	连续的从输入流中读取数据,将其写入到给定文件中.在给定的时间间隔滚动文件,滚动文件命名基于给定形式
   构造器给定参数:
   	inputStream	数据源输入流
   	activeFile	写入数据的目的文件
   	rollingPolicy	滚动策略
   	conf	spark配置
   	bufferSize	缓冲区大小
   ```
   
   ```scala
   private[spark] class RollingFileAppender(
       inputStream: InputStream,
       activeFile: File,
       val rollingPolicy: RollingPolicy,
       conf: SparkConf,
       bufferSize: Int = RollingFileAppender.DEFAULT_BUFFER_SIZE
     ) extends FileAppender(inputStream, activeFile, bufferSize) {
       属性:
       #name @maxRetainedFiles = conf.get(config.EXECUTOR_LOGS_ROLLING_MAX_RETAINED_FILES)
       	最大保留文件
       #name @enableCompression = conf.get(config.EXECUTOR_LOGS_ROLLING_ENABLE_COMPRESSION)
       	是否允许压缩
       操作集:
       def stop(): Unit = super.stop()
       功能: 停止添加器并将其移走
       try {
         closeFile()
         moveFile()
         openFile()
         if (maxRetainedFiles > 0) {
           deleteOldFiles()
         }
       } catch {
         case e: Exception =>
           logError(s"Error rolling over $activeFile", e)
       }
       
       def appendToFile(bytes: Array[Byte], len: Int): Unit
       功能: 添加数据到指定文件中@activeFile
       1. 进行必要的文件滚动
       if (rollingPolicy.shouldRollover(len)) {
         rollover()
         rollingPolicy.rolledOver()
       }
       2. 将数据写出到文件,并更新写出数量
       super.appendToFile(bytes, len)
       rollingPolicy.bytesWritten(len)
       
       def rollover(): Unit
       功能: 滚动文件,关闭输出流
       
       def rolloverFileExist(file: File): Boolean
       功能: 检查滚动文件是否存在
       val= file.exists || new File(file.getAbsolutePath +
            RollingFileAppender.GZIP_LOG_SUFFIX).exists
   	
       def rotateFile(activeFile: File, rolloverFile: File): Unit
       功能: 滚动日志,如果允许压缩则对其进行压缩
       if (enableCompression) {
         // 进行可能的压缩
         val gzFile = new File(rolloverFile.getAbsolutePath + RollingFileAppender.GZIP_LOG_SUFFIX)
         var gzOutputStream: GZIPOutputStream = null
         var inputStream: InputStream = null
         try {
           // 读取数据源数据,写出到指定文件中
           inputStream = new FileInputStream(activeFile)
           gzOutputStream = new GZIPOutputStream(new FileOutputStream(gzFile))
           IOUtils.copy(inputStream, gzOutputStream)
           inputStream.close()
           gzOutputStream.close()
           activeFile.delete()
         } finally {
           IOUtils.closeQuietly(inputStream)
           IOUtils.closeQuietly(gzOutputStream)
         }
       } else {
         // 非压缩情况下,直接滚动文件
         Files.move(activeFile, rolloverFile)
       }
       
       def deleteOldFiles(): Unit 
       功能: 删除旧文件,仅仅保留最近的一些文件
       1. 获取滚动文件
       val rolledoverFiles = activeFile.getParentFile.listFiles(new FileFilter {
           def accept(f: File): Boolean = {
             f.getName.startsWith(activeFile.getName) && f != activeFile
           }
       }).sorted
       2. 删除滚动文件中top X个文件
       val filesToBeDeleted = rolledoverFiles.take(
           math.max(0, rolledoverFiles.length - maxRetainedFiles))
         filesToBeDeleted.foreach { file =>
           logInfo(s"Deleting file executor log file ${file.getAbsolutePath}")
           file.delete()
         }
       
       def moveFile(): Unit
       功能: 将激活文件移动形成新的滚动文件
       1. 获取滚动前缀
       val rolloverSuffix = rollingPolicy.generateRolledOverFileSuffix()
       2. 获取滚动文件
       val rolloverFile = new File(
         activeFile.getParentFile, activeFile.getName + rolloverSuffix).getAbsoluteFile
       3. 允许的话滚动文件
       if (activeFile.exists) {
         if (!rolloverFileExist(rolloverFile)) {
           rotateFile(activeFile, rolloverFile)
           logInfo(s"Rolled over $activeFile to $rolloverFile")
         } else {
           var i = 0
           var altRolloverFile: File = null
           do {
             altRolloverFile = new File(activeFile.getParent,
               s"${activeFile.getName}$rolloverSuffix--$i").getAbsoluteFile
             i += 1
           } while (i < 10000 && rolloverFileExist(altRolloverFile))
           logWarning(s"Rollover file $rolloverFile already exists, " +
             s"rolled over $activeFile to file $altRolloverFile")
           rotateFile(activeFile, altRolloverFile)
         }
       } else {
         logWarning(s"File $activeFile does not exist")
       }
   }
   ```
   
   ```scala
   private[spark] object RollingFileAppender {
       属性:
       #name @DEFAULT_BUFFER_SIZE = 8192	默认缓冲区大小
       #name @GZIP_LOG_SUFFIX = ".gz"	gzip前缀
       操作集:
       def getSortedRolledOverFiles(directory: String, activeFileName: String): Seq[File]
       功能: 获取排序后的滚动日志文件
       val rolledOverFiles = new File(directory).getAbsoluteFile.listFiles.filter { file =>
         val fileName = file.getName
         fileName.startsWith(activeFileName) && fileName != activeFileName
       }.sorted
       val activeFile = {
         val file = new File(directory, activeFileName).getAbsoluteFile
         if (file.exists) Some(file) else None
       }
       val= rolledOverFiles.sortBy(_.getName.stripSuffix(GZIP_LOG_SUFFIX)) ++ activeFile
   }
   ```
   
   #### RollingPolicy
   
   ```scala
   private[spark] trait RollingPolicy {
     操作集:
     def shouldRollover(bytesToBeWritten: Long): Boolean
     功能: 确认是否需要滚动 
     
     def rolledOver(): Unit
     功能: 滚动日志文件
       
     def bytesWritten(bytes: Long): Unit
     功能: 写出指定内容
       
     def generateRolledOverFileSuffix(): String
     功能: 产生滚动日志前缀
   }
   ```
   
   ```scala
   private[spark] object TimeBasedRollingPolicy {
       属性:
       #name @MINIMUM_INTERVAL_SECONDS = 60L 最小时间间隔
   }
   ```
   
   ```scala
   private[spark] object SizeBasedRollingPolicy {
       #name @MINIMUM_SIZE_BYTES = RollingFileAppender.DEFAULT_BUFFER_SIZE * 10
       	最小字节量大小
   }
   ```
   
   ```scala
   private[spark] class SizeBasedRollingPolicy(
       var rolloverSizeBytes: Long,
       checkSizeConstraint: Boolean = true    
     ) extends RollingPolicy with Logging {
       介绍: 基于大小的滚动策略
       初始化操作:
       if (checkSizeConstraint && rolloverSizeBytes < MINIMUM_SIZE_BYTES) {
           logWarning(s"Rolling size [$rolloverSizeBytes bytes] is too small. " +
             s"Setting the size to the acceptable minimum of $MINIMUM_SIZE_BYTES bytes.")
           rolloverSizeBytes = MINIMUM_SIZE_BYTES
         }
       功能: 检测滚动字节大小是否符合要求
       
       属性:
       #name @formatter = new SimpleDateFormat("--yyyy-MM-dd--HH-mm-ss--SSSS", Locale.US)	格式器
       操作集:
       def shouldRollover(bytesToBeWritten: Long): Boolean
       功能: 确认是否需要滚动(下一组数据如果要超过大小范围则会滚动)
       val= bytesToBeWritten + bytesWrittenSinceRollover > rolloverSizeBytes
       
       def rolledOver(): Unit = bytesWrittenSinceRollover = 0
       功能: 发送滚动动作,重置计数器
       
       def bytesWritten(bytes: Long): Unit =bytesWrittenSinceRollover += bytes
       功能: 写入指定数量的字节数,计数器计数值变化
       
       def generateRolledOverFileSuffix(): String
       功能: 产生滚动前缀(时间类型前缀)
       val= formatter.format(Calendar.getInstance.getTime)
   }
   ```

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

   ```scala
private[spark] object SamplingUtils {
       
   }
   ```
```
   
​```scala
   private[spark] object PoissonBounds {
       介绍: 泊松分布界限类
       操作集:
       def getLowerBound(s: Double): Double
       功能: 获取下界值,返回P[x>s]以至于这个概率值非常小,满足x ~ Pois(lambda)
       val= math.max(s - numStd(s) * math.sqrt(s), 1e-15)
       
       def getUpperBound(s: Double): Double
       功能: 获取上界限值,使得P[x<s]概率非常小,满足 x ~ Pois(lambda)
   	
       def numStd(s: Double): Double
       功能: 获取标准值
       val= if (s < 6.0) {
         12.0
       } else if (s < 16.0) {
         9.0
       } else {
         6.0
       }
   }
```

   ```scala
   private[spark] object BinomialBounds {
   	介绍: 伯努利界限类
       包含了一些使用函数,用于决定界限值,当没有采样替代时调整采样值去高置信度地保证精确的规模大小
   	属性:
       #name @minSamplingRate = 1e-10 最小采样间隔
       操作集:
       def getLowerBound(delta: Double, n: Long, fraction: Double): Double
       功能: 如果构建一个n次伯努利实验,成功概率为p,不太可能存在有超过fraction * n次成功次数.
       val gamma = - math.log(delta) / n * (2.0 / 3.0)
       fraction + gamma - math.sqrt(gamma * gamma + 3 * gamma * fraction)
       
       def getUpperBound(delta: Double, n: Long, fraction: Double): Double
       功能: 如果构建一个n次伯努利实验,成功概率为p,不太可能存在有少于fraction * n次成功次数.
   	val gamma = - math.log(delta) / n
       math.min(1,math.max(minSamplingRate, 
          fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction)))
   }
   ```

   ```scala
   private[spark] object SamplingUtils {
       def reservoirSampleAndCount[T: ClassTag](input: Iterator[T],k: Int,
         seed: Long = Random.nextLong()): (Array[T], Long)
   	功能: 存储采样值,并计数,返回的二元组为(采样值列表,计数值)
       输入参数:
       	input	输入迭代器
       	k	存入值的数量
       	seed	随机种子
       1. 存入输入到存储列表中
       val reservoir = new Array[T](k)
       var i = 0
       while (i < k && input.hasNext) {
         val item = input.next()
         reservoir(i) = item
         i += 1
       }
       2. 对存储列表赋值
       if (i < k) { // 输入规模小于k,只需要返回与输入数组相同数量的元素即可
         val trimReservoir = new Array[T](i)
         System.arraycopy(reservoir, 0, trimReservoir, 0, i)
         val= (trimReservoir, i)
       } else { // 输入规模大于k,通过采样将列表填满
         var l = i.toLong
         val rand = new XORShiftRandom(seed)
         while (input.hasNext) {
           val item = input.next()
           l += 1
           val replacementIndex = (rand.nextDouble() * l).toLong
           if (replacementIndex < k) {
             reservoir(replacementIndex.toInt) = item
           }
         }
       }
       val= (reservoir, l)
       
       def computeFractionForSampleSize(sampleSizeLowerBound: Int, total: Long,
         withReplacement: Boolean): Double
       功能: 计算采样因子
       输入参数:
       sampleSizeLowerBound 采样大小下限
       total 总计数量
       withReplacement	是否替代
       val= if (withReplacement) {
         PoissonBounds.getUpperBound(sampleSizeLowerBound) / total
       } else {
         val fraction = sampleSizeLowerBound.toDouble / total
         BinomialBounds.getUpperBound(1e-4, total, fraction)
       }
   }
   ```

#### StratifiedSamplingUtils

分层采样工具

   ```markdown
   介绍:
   	在pair RDD函数中@PairRDDFunctions 用作辅助函数和数据结构
   	本质上来说,当给定精确的采样大小时,我们需要通过RDD的计算去得到精确的容量值,用户每个层,去在大概率保证每层的精确采样.这个通过维护一个等待列表实现,大小为O(log(s)),s是一层中需要的采样大小
   	在简单随机抽样中，保证每个随机值都是统一的分布在[0.0,1.0]这个范围内.所有小于等于最小值的记录采样值都会被立即接受.立即接受的容量满足如下函数关系:
   	s - numAccepted = O(sqrt(s))
   	其中s为需要的采样规模大小,因此通过维护一个空间复杂度为O(sqrt(s)) 的等待列表,通过添加等待列表的部分内容到达立即接受集合中,实现创建指定大小的采样器.
   	通过对等待列表的值进行排序且获取处于位置为(s-numAccepted)的值作为容量精确值(等待列表容量).
   	注意到,当计算容量和实际采样值的时候,使用的是RNG的同一个种子,所以计算的容量保证了会生成需要的采样大小.
   ```

   ```scala
   private[spark] object StratifiedSamplingUtils extends Logging {
       介绍: 分层采用工具类
       操作集:
       def getAcceptanceResults[K, V](rdd: RDD[(K, V)],
         withReplacement: Boolean,
         fractions: Map[K, Double],
         counts: Option[Map[K, Long]],
         seed: Long): mutable.Map[K, AcceptanceResult]
       功能: 计算立即接受的采样值数量,并生成等待列表
       注: 这个只会在需要获取精确的采样规模时才会调用
       输入参数:
       	rdd	采样RDD(键值对RDD)
    	withReplacement	是否替换
       	fractions	采样因子
       	counts	采样计数表
       	seed	种子
       1. 获取结果转换函数
       val combOp = getCombOp[K]
       2. 获取分区迭代器
       val mappedPartitionRDD = rdd.mapPartitionsWithIndex { case (partition, iter) =>
         // 获取立即接收结果
         val zeroU: mutable.Map[K, AcceptanceResult] = new mutable.HashMap[K, AcceptanceResult]()
         val rng = new RandomDataGenerator()
         rng.reSeed(seed + partition)
         // 获取指定采样因子等待列表
      val seqOp = getSeqOp(withReplacement, fractions, rng, counts)
         // 连接立即接受表,和等待列表信息 --> 合并采样因子  
      Iterator(iter.aggregate(zeroU)(seqOp, combOp))
       }
    3. 使用RDD聚合,生成指定采样因子的采样集合
       val= mappedPartitionRDD.reduce(combOp)
       
       def getSeqOp[K, V](withReplacement: Boolean,
         fractions: Map[K, Double],
         rng: RandomDataGenerator,
         counts: Option[Map[K, Long]]):
       (mutable.Map[K, AcceptanceResult], (K, V)) => mutable.Map[K, AcceptanceResult]
       功能: 返回合并分区分层采样的函数
       输入参数:
       	withReplacement	是否替代
       	fractions	采集因子列表
       	rng	随机数据采集器
       	counts	采样计数器
       	(mutable.Map[K, AcceptanceResult], (K, V)) => mutable.Map[K, AcceptanceResult]	
       		分区分层采样函数
       1. 获取增量值
       val delta = 5e-5
       2. 根据接收结果和采用值@item 求转换函数
       (result: mutable.Map[K, AcceptanceResult], item: (K, V)) => {
         val key = item._1
         val fraction = fractions(key)
         if (!result.contains(key)) {
           result += (key -> new AcceptanceResult())
         }
         val acceptResult = result(key)
         if (withReplacement) {
           if (acceptResult.areBoundsEmpty) {
             val n = counts.get(key)
             val sampleSize = math.ceil(n * fraction).toLong
             val lmbd1 = PoissonBounds.getLowerBound(sampleSize)
             val lmbd2 = PoissonBounds.getUpperBound(sampleSize)
             acceptResult.acceptBound = lmbd1 / n
             acceptResult.waitListBound = (lmbd2 - lmbd1) / n
           }
           val acceptBound = acceptResult.acceptBound
           val copiesAccepted = if (acceptBound == 0.0) 0L else rng.nextPoisson(acceptBound)
           if (copiesAccepted > 0) {
             acceptResult.numAccepted += copiesAccepted
           }
           val copiesWaitlisted = rng.nextPoisson(acceptResult.waitListBound)
           if (copiesWaitlisted > 0) {
             acceptResult.waitList ++= ArrayBuffer.fill(copiesWaitlisted)(rng.nextUniform())
           }
         } else {
           acceptResult.acceptBound =
             BinomialBounds.getLowerBound(delta, acceptResult.numItems, fraction)
           acceptResult.waitListBound =
             BinomialBounds.getUpperBound(delta, acceptResult.numItems, fraction)
           val x = rng.nextUniform()
           if (x < acceptResult.acceptBound) {
             acceptResult.numAccepted += 1
           } else if (x < acceptResult.waitListBound) {
             acceptResult.waitList += x
           }
         }
         acceptResult.numItems += 1
         result
       }
       
       def getCombOp[K]: (mutable.Map[K, AcceptanceResult], mutable.Map[K, AcceptanceResult])
       功能: 聚合操作函数(两个分区@seqOp 转换为一个@seqOp)
       1. 对两个结果列表进行合并
       (result1: mutable.Map[K, AcceptanceResult], result2: mutable.Map[K, AcceptanceResult]) => {
         result1.keySet.union(result2.keySet).foreach { key =>
           val entry1 = result1.get(key)
           if (result2.contains(key)) { // 存在则res1合并到res2中
             result2(key).merge(entry1)
           } else {
             if (entry1.isDefined) { // 不存在则res2中添加一条记录
               result2 += (key -> entry1.get)
             }
           }
         }
         result2
       }
       
       def computeThresholdByKey[K](finalResult: Map[K, AcceptanceResult],
         fractions: Map[K, Double]): Map[K, Double]
       功能: 通过key计算容量
       给定方法@getCounts 的结果,决定产生接受数据的容量,用于产生精确的采样规模大小.
       为了计算方便,每层计算采样大小@sampleSize = math.ceil(size * samplingRate) ,并将采样大小与立即接受的数量做对比,这些采样数据会放置在当前层的等待列表中.
       在绝大多数情况下,numAccepted <= sampleSize <= (numAccepted + numWaitlisted).意味着我们需要对等待列表的数据元素进行排序,目的是找到值T,使得满足
       |{elements in the stratum whose associated values <= T}| = sampleSize
       注意到等待列表所有元素值都大于等于立即接受的上限值.所以T值范围内的等待列表使得所有元素都会一次被立即接受.
       输入参数: 
       	finalResult	最终接受结果列表
       	fractions	采用因子列表
       1. 获取key的容量列表
       val thresholdByKey = new mutable.HashMap[K, Double]()
       2. 精确获取指定采样规模的数据,置入立即接受组中
       for ((key, acceptResult) <- finalResult) {
         val sampleSize = math.ceil(acceptResult.numItems * fractions(key)).toLong
         if (acceptResult.numAccepted > sampleSize) {
           logWarning("Pre-accepted too many")
           // 采样数已经满足要求,所有采用总数val= acceptBound
           thresholdByKey += (key -> acceptResult.acceptBound)
         } else {
           // 计算等待列表中的元素数量
           val numWaitListAccepted = (sampleSize - acceptResult.numAccepted).toInt
           if (numWaitListAccepted >= acceptResult.waitList.size) {
             logWarning("WaitList too short")
             // 等待列表不足以填充需求的采样规模,情况比较少
             thresholdByKey += (key -> acceptResult.waitListBound)
           } else {
             // 足以填充,则将等待列表排序,并将需求的数量立即接收到结果集中
             thresholdByKey += (key -> acceptResult.waitList.sorted.apply(numWaitListAccepted))
           }
         }
       }
       3. 获取最终采样值列表
       val= thresholdByKey
       
       def getBernoulliSamplingFunction[K, V](rdd: RDD[(K, V)],
         fractions: Map[K, Double],
         exact: Boolean,
         seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] 
       功能: 伯努利采样处理函数
       输入参数:
       	fractions	采样因子列表
       	seed	种子值
       	exact	是否重新采样
       1. 获取采样key列表
       var samplingRateByKey = fractions
       2. 如果需要额外采样,则计算每层容量和重新采样
       if (exact) {
         val finalResult = getAcceptanceResults(rdd, false, fractions, None, seed)
         samplingRateByKey = computeThresholdByKey(finalResult, fractions)
       }
       3. 重新设置种子,并重新采样(但是采样结果与之前还是一致)
       (idx: Int, iter: Iterator[(K, V)]) => {
         val rng = new RandomDataGenerator()
         rng.reSeed(seed + idx)
         // 使用rng的这个调用形式,去获取与之前一样的采样数据列表
         iter.filter(t => rng.nextUniform() < samplingRateByKey(t._1))
       }
       
       def getPoissonSamplingFunction[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
         fractions: Map[K, Double],
         exact: Boolean,
         seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)]
       功能: 泊松采样函数
       每个分区使用可代替式的采样函数
       当需要准确的采样规模时,使用两个额外的RDD去决定准确的采样比例,这个采样比例具有高置信值保证采样大小.第一	个传入数据的计算每层RDD的数据数量.第二个传入参数去决定精确的采样比例.
       注意每个分区的采样函数有一个唯一的种子值.
       输入参数:
       	fractions	采样因子列表
       	exact	是否需要重新采样
       	seed	种子值
       分支1: 需要重新采样 exact=true
         val counts = Some(rdd.countByKey()) // 计算rdd中key的数量
         // 获取直接接受结果集,和容量列表
         val finalResult = getAcceptanceResults(rdd, true, fractions, counts, seed)
         val thresholdByKey = computeThresholdByKey(finalResult, fractions)
         (idx: Int, iter: Iterator[(K, V)]) => {
           val rng = new RandomDataGenerator()
           rng.reSeed(seed + idx)
           iter.flatMap { item =>
             val key = item._1 // 获取key
             val acceptBound = finalResult(key).acceptBound // 获取结果中接受值上限
             // 获取接受结果集采样值副本
             val copiesAccepted = if (acceptBound == 0) 0L else rng.nextPoisson(acceptBound)
             // 接受等待列表
             val copiesWaitlisted = rng.nextPoisson(finalResult(key).waitListBound)
             // 获取采样数量副本
             val copiesInSample = copiesAccepted +
               (0 until copiesWaitlisted).count(i => rng.nextUniform() < thresholdByKey(key))
             if (copiesInSample > 0) { // 对指定采样大小进行泊松采样,并返回数据
               Iterator.fill(copiesInSample.toInt)(item)
             } else {
               Iterator.empty
             }
           }
         }
       分支2: 不需要重新计算
       (idx: Int, iter: Iterator[(K, V)]) => {
           val rng = new RandomDataGenerator()
           rng.reSeed(seed + idx)
           iter.flatMap { item =>
             val count = rng.nextPoisson(fractions(item._1))
             if (count == 0) {
               Iterator.empty
             } else {
               Iterator.fill(count)(item)
             }
           }
         }
   }
   ```

   ```scala
   private class RandomDataGenerator {
       介绍: 随机数生成器,既可以生成统一型随机变量,也可以生成泊松随机变量
       #name @uniform = new XORShiftRandom() 统一随机变量
       #name @poissonCache = mutable.Map[Double, PoissonDistribution]()	泊松缓存列表
       #name @poissonSeed = 0L	泊松种子值
       操作集:
       def reSeed(seed: Long): Unit
       功能: 重新设置种子值
       
       def nextPoisson(mean: Double): Int
       功能: 获取泊松分布采样值
       val poisson = poissonCache.getOrElseUpdate(mean, {
           val newPoisson = new PoissonDistribution(mean)
           newPoisson.reseedRandomGenerator(poissonSeed)
           newPoisson
         })
       val= poisson.sample()
       
       def nextUniform(): Double
       功能: 获取统一性随机变量
       val= uniform.nextDouble()
   }
   ```

   ```scala
   private[random] class AcceptanceResult(var numItems: Long = 0L, var numAccepted: Long = 0L)
     extends Serializable{
   	介绍:
        属性:
        #name @waitList = new ArrayBuffer[Double]	等待列表
        #name @acceptBound: Double = Double.NaN 	直接接收值上限
        #name @waitListBound: Double = Double.NaN	等待列表上限
        操作集:
         def areBoundsEmpty: Boolean = acceptBound.isNaN || waitListBound.isNaN
         功能: 确定上限是否为空
         
         def merge(other: Option[AcceptanceResult]): Unit 
         功能: 设置等待列表,并更新直接接收结果集
         if (other.isDefined) {
             waitList ++= other.get.waitList
             numAccepted += other.get.numAccepted
             numItems += other.get.numItems
           }
   }
   ```

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

   1. 伯努利分布 :  [伯努利分布](https://zh.wikipedia.org/wiki/伯努利分布)
   
   2. 泊松分布: http://en.wikipedia.org/wiki/Poisson_distribution
   
   3. XorShift随机采样 https://en.wikipedia.org/wiki/Xorshift
   
   4. 采样理论 http://jmlr.org/proceedings/papers/v28/meng13a.html
   
   5. 缓冲区设计不当的内存泄漏 http://www.evanjones.ca/java-bytebuffer-leak.html
   
   6. 外推插值法
   
   7.  平方探测法 http://en.wikipedia.org/wiki/Quadratic_probing
   
   8.  哈希函数
   
      32位 murmur3 算法 http://smhasher.googlecode.com/svn/trunk/MurmurHash3.cpp
   
   9.   位集合 https://www.runoob.com/java/java-bitset-class.html