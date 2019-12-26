## **spark-unsafe.map**

---

1. 接口@HashMapGrowthStrategy

   ```markdown
   ADT 模型:
   	基本策略: 容量翻倍
   	数据元素: 翻倍后的增长策略@Double #implement @HashMapGrowthStrategy
   	方案: 容量翻倍，但是不能超过数组所运行的最大返回@MAX_ROUNDED_ARRAY_LENGTH
   	关于这个值，应该是int最大值，但是实际上要小一些@Integer.MAX_VALUE - 15
   ```

   

2. 类@BytesToBytesMap

   1. 基本介绍

   ```markdown
   1. 当kv是连续的字节区域时，这是一个仅添加的hashmap。
   2. 会返回一个2的n次方大小的哈希表，使用@带三角数的平方探查法,确保空间可以完全的使用。
   3. 支持最大2^29次方数据存储，当所需的数据超过这个范围的时候，你可以使用排序用作更好的本地缓存策略。
   4. 关于如何去存储kv(kv是存在一起的)
   	+ size<4 len(k) + len(v)+ 4存储kv
   	+ 8>size>4 len(k)
   	+ 8+len(k)>size>8 : key data
       + 8+len(k)<size<8+len(k)+len(v): value data
       + 8+len(k)+len(v)<size<8+len(k)+len(v)+8 : 跳到下一条数据
   这就意味着前四个字节存储了完整的记录，这个形式与@org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter共通，所有可以直接的把map的记录直
   接传递过去排序。
   ```

   2. 抽象数据模型

      ```markdown
      ADT BytesToBytesMap{
      	数据元素:
      		1. 日志处理器Logger logger
      		2. hashmap增长策略: @HashMapGrowthStrategy #name growthStrategy
      		3. 任务内存管理器: @TaskMemoryManager taskMemoryManager
      		4. 数据页表 #dataPages @LinkedList<MemoryBlock>
      			设计这个可以方便找到指定数据对应的内存，可以方便的将它释放
      		5. 当前页指针 @MemoryBlock #name currentPage
      			当前页用于存储新建的hash键值对，当前页满的时候，会开辟一个新的页面，并将指针指向它。
      		6. 页内指针@pageCursor 指向新数据需要插入的地方,和页基址偏移量不是一个概念
      		7. 最大容量:@MAX_CAPACITY : 2^(29)
      		8. kv存储数组:#name longArray @org.apache.spark.unsafe.array.LongArray可以为空
      			数组下标2*i 对应于位置为i的key值
      			数组下标2*i+1 对应于key值得32位hashcode
      			// 这里文档说明可以进一步的缩小这个空间大小
      		9. 是否允许kv数组扩展#name canGrowArray @boolean
              	为false时，则会有较小的插入上限
              10. 装载因子#name loadFactor @double
              	经典hashmap问题，影响查询数据平均期望时间
              11. 数据页的大小:
              	@pageSizeBytes 用以限制页内最大键值对数量
              12. key的数量
              	@numKeys
              13. value数量
              	@numValues 注意: 一个key可以有多重value
              14. 增长容量
              	@growthThreshold 作为容量扩展的标准
              15. 区间遮罩
              	@mask 这个参数用于分段,以防止数据量超过kv存储数组的大小@longArray
              	使用时这个参数越小，截出来的部分越小，由于最大容量很大，所以通常状态下，这个值不小
              16. 定位指针:
              	# class Location #name loc 返回当前读取位置
              17. 探针数量
              	@numProbes 用于解决hash冲突
              18. key查找数量
              	@numKeyLookups
              19. 使用内存峰值
              	@peakMemoryUsedBytes
              20. 初始容量:
              	@initialCapacity final
              21. 块管理器
              	@blockManager #type BlockManager final
              22. 序列化管理器
              	@serializerManager #type SerializerManager final
              23. 迭代器
              	@MapIterator #name destructiveIterator #volatile
              24. 溢写器列表
              	@LinkedList<UnsafeSorterSpillWriter> #name spillWriters
      		
      	操作集:
      		1. 初始化类:
      		+ BytesToBytesMap(
           		 TaskMemoryManager taskMemoryManager,
           		 BlockManager blockManager,
            		 SerializerManager serializerManager,
            		 int initialCapacity,
            		 double loadFactor,
            		 long pageSizeBytes)
            		 调用下面的简单构造器，但是不同的是块管理器和序列化管理器自己指定
            		此外定位指针@loc需要设定。必要的是对初始容量的处理，基本的需要判断是否满足条件(不要超过内			存管理器所允许最大容量，约为17KB)。合法之后需要修改map的容量
            		-> 相应作出改变参@allocate(int capacity)
            	+ BytesToBytesMap(
            		TaskMemoryManager taskMemoryManager,
            		int initialCapacity,
            		long pageSizeBytes)
            	功能: 指定任务内存管理器@taskMemoryManager,初始化容量@initialCapacity,页长@pageSizeBytes
            	块管理器会从spark的环境变量@SparkEnv中获取,若没有配置则为null;
            	序列化管理器从spark环境变量获取，没有则为null;为了保证kv数组@longArrray的可扩展性加载因子设	置为0.5
       		2. 获取属性类型操作:
              	+ int numKeys() 返回map中定义的key的数量
              	+ int numValues() 返回map中定义的value数量，只有一个key可以有多个value
              	+ MapIterator iterator() 
              		返回迭代器
              	+ MapIterator safeIterator()
              		返回线程安全的迭代器
              	+ MapIterator destructiveIterator()
              		带释放空间的返回迭代器，即当移动到下一页时会释放之前页面的内存空间。需要注意到的是释	          放完毕之后调用其他方法是非法的。
              		实现方面，不仅要在上面的步骤上去释放内存操作#question @如何去释放内存，而且需要更新
      			使用内存峰值属性@peakMemoryUsedBytes,具体参照#methof @updatePeakMemoryUsed()
      			+ TaskMemoryManager getTaskMemoryManager()
      				获取任务内存管理器
      			+ long getPageSizeBytes()
      				获取页长
      			+ long getTotalMemoryConsumption()
      				获取内存总计算量
      				计算方法: 
      			+ int getNumDataPages()
      				获取页数
      			+ LongArray getArray()
      				获取kv存储数组
      			+ long getPeakMemoryUsedBytes()
      				获取峰值使用量
      		3. 位置查找类
      			+ 安全查找(可以用于多线程状态下) 
      			safeLookup(Object keyBase, long keyOffset, int keyLength, Location loc, int hash)
      			功能: 范围查找指定key，返回key对应的定位指针@loc
      			查找策略: 平方探测法(但是这里实际上使用的是线性探测法,步长=1) 探测位置kv数组@longArray
                  	前面说到:longArray数组2i位置存储的是第i位置的key，2i+1位置存储hash值
                  如果没有查找到:
      				说明是一个新的key值，需要插入指定@loc中，
      				具体方法参照#method @with(int pos, int keyHashcode, boolean isDefined)
      			如果查找到这个元素
      				先找到hash值相等的，hash值相等再比较key，相等返回，否则步长增加1，继续探测。
      			+ 基本查找:
      			Location lookup(Object keyBase, long keyOffset, int keyLength, int hash)
      			实际上是调用:
      			safeLookup(keyBase, keyOffset, keyLength, loc, hash)
      			+ 默认查找
      			Location lookup(Object keyBase, long keyOffset, int keyLength)
      			实际上是将hash替换成系统解决的hash方案@Murmur3_x86_32.hashUnsafeWords
      		4. 更新操作类
      			+ void updatePeakMemoryUsed()
      				更新峰值内存使用量
      			
      }
      ```

      3. **拓展**

         ```markdown
         1. volatile关键字
         2. 二进制运算 & 的使用
         ```

         