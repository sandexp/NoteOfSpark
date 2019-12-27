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
   这就意味着前四个字节存储了完整的记录，这个形式与@org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter共通，所有可以直接的把map的记录直接传递过去排序。
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
            		此外定位指针@loc需要设定。必要的是对初始容量的处理，基本的需要判断是否满足条件(不要超过内存管理器所允许最大容量，约为17KB)。合法之后需要修改map的容量
            		-> 相应作出改变参@allocate(int capacity)
            	+ BytesToBytesMap(
            		TaskMemoryManager taskMemoryManager,
            		int initialCapacity,
            		long pageSizeBytes)
            	功能: 指定任务内存管理器@taskMemoryManager,初始化容量@initialCapacity,页长@pageSizeBytes
            	块管理器会从spark的环境变量@SparkEnv中获取,若没有配置则为null;
            	序列化管理器从spark环境变量获取，没有则为null;为了保证kv数组@longArrray的可扩展性加载因子设置为0.5
       		2. 获取属性类型操作:
              	+ int numKeys() 返回map中定义的key的数量
              	+ int numValues() 返回map中定义的value数量，只有一个key可以有多个value
              	+ MapIterator iterator() 
              		返回迭代器
              	+ MapIterator safeIterator()
              		返回线程安全的迭代器
              	+ MapIterator destructiveIterator()
              		带释放空间的返回迭代器，即当移动到下一页时会释放之前页面的内存空间。需要注意到的			是释放完毕之后调用其他方法是非法的。
              		实现方面，不仅要在上面的步骤上去释放内存操作#question @如何去释放内存，而且需			要更新
      			使用内存峰值属性@peakMemoryUsedBytes,具体参照#method @updatePeakMemoryUsed()
      			+ TaskMemoryManager getTaskMemoryManager()
      				获取任务内存管理器
      			+ long getPageSizeBytes()
      				获取页长
      			+ long getTotalMemoryConsumption()
      				获取内存总计算量
      				计算方法: 
      					consume=sigma(datapage[i].size())+longArray.memory().size()
      			+ int getNumDataPages()
      				获取页数
      			+ LongArray getArray()
      				获取kv存储数组
      			+ long getPeakMemoryUsedBytes()
      				获取峰值使用量
      			+ double getAvgHashProbeBucketListIterations()
      				功能: 获取平方探测法的平均次数--> 是用来指示hash冲突的频数参数
      				=探测次数@numProbes/key查询次数@numKeyLookups
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
   			+ long spill(long size, MemoryConsumer trigger)
   				初始条件: 内存消费者@trigger 不是本类，且迭代器@destructiveIterator非空
   				操作:调用迭代器的溢写方法@spill(int size),返回溢写量
      				不满足上述条件，溢写量=0
      			+ void reset()
      				重置map到初始化状态
   			+ void free()
   				功能: 释放map相关的内存区域，包括kv以及hash map本身，本方法具有幂等性，且可以多次			调用。
      				1. 释放kv数组@longArray
      				2. 释放页表@dataPages中的内存块#class @MemoryBlock
      				3. 清空完毕之后，再清楚溢写器列表中的内容去空，去空时需要检查文件是否真的删除
      		5. 获取状态类
      			+ boolean acquireNewPage(long required)
      			从内存管理器中获取请求页,获取方法参考#class @MemoryConsumer #method @allocatePage()
      			1. 如果引发了异常@SparkOutOfMemoryError 则返回false
      			2. 没有引发异常，则将请求新增的页面，加入到页列表中。
      			3. 并将更新@UnsafeAlignedOffset 
      			4. 将页指针@pageCursor指向uao_size,即读取第一个kv的位置
      		6. hash表的扩容和rehash
      			容量增长策略: 参考#class @HashMapGrowthStrategy
      			伪rehash策略: 这里不会重新根据容量计算hash值 这里叫做re-mask
                  	oldArr			newArr
                  	i				key= oldArr(i+1) & mask/ hashcode= newArr(key+1) 
                   仍然使用平方探测法解决hash冲突
      }
      ```
      
      #class @MapIterator
      
      ```markdown
      ADT MapIterator{    
      	数据元素:        
      	1. 记录数量#name @numRecords #type $int        
      	2. 位置 #name @loc #type $Location        
      	3. 当前页内存块#name @currentPage #type $MemoryBlock        
      	4. 页中记录剩余量#name @recordsInPage #type $int        
      	5. 页内偏移量#name @offsetInPage #type $long        
      	6. 页基本对象#name @pageBaseObject #type $Object        
      	7. 内存释放权限标志#name @destructive #type $boolean        
      	8. 非安全状态下的排序阅读器#name @reader #type $UnsafeSorterSpillReader    
      	操作集:        
      	1. 初始化类(构造器)        
      	  private MapIterator(int numRecords, Location loc, boolean destructive)        	注意构造器是私有化的        
      	指定记录数，位置，以及是否可以释放空间，若是可以释放空间就会把kv数组@longArray给释放掉。       2. 获取信息类        
      	+ boolean hasNext()        
      	返回当前记录数量是否大于0，如果等于0且阅读器存在，则会去处理失败状态下的删除工作，具体工    作#method @handleFailedDelete()        
      	+ Location next()        
      	功能: 获取下一个位置            
      	1. 如果当前页面记录数量为0@recordsInPage=0,跳到下一个页面处理#method  @advanceToNextPage(),待消费记录数量-1@numRecords            
      	2. 获取非0剩余记录数量之后，对当前页面处理分为下面两种情况                
      		+ 当前内存块@currentPage非空
      		1. 根据页偏移量@offsetInPage和内存块对象@currentPage 定位loc。参照#method @with(
      		MemoryBlock b,long offsetInpage)
      		2. 更新页内偏移量@offsetInPage=UAO_Size+totalLen+8
      		3. 消费一条记录，页内剩余记录数量-1
      		4. 返回位置参数@loc
      		+ 当前内存块内容为空
      		假定阅读器@reader非空
      			1. 在阅读器剩余记录数量等于0时
      				跳到下一页
      			2. 在上一个步骤上进行处理
      				使用阅读器读取，下一条完整记录长度的数据，并使用@with(Object base, long 		offset, int length)，溢写到@loc中去，若是发生IO异常，则会关闭流，关流发生异常则引发	      错误，错误日志可查。
           3. 操作类
      		void advanceToNextPage() 
             功能: 转向下一页
             设计思路: 跳到下一页就会涉及到内存管理器@TaskMemoryManager对内存释放的操作，保证释放内	存的安全性。会对迭代器	（也就是本类加锁）。同时可能会出现下面的情景，可能另一个内存消费者首先		对内存管理器@TaskMemoryManager加锁，然后当迭代器@MapIterator获得内存且需要对这个迭代器进	   行溢写时就会出现死锁的情况，为了避免死锁，需要保有需要释放页的引用，只有当释放掉迭代器的锁的时	  候再释放内存空间。
             获取迭代器锁进行的动作:
             	1. 计算下一页的index（在页列表中的位置@dataPages）
             	2. 释放引用的设置:
             		+ 当且迭代器允许释放页内容，且当前页对应的内存块非空@currentPage
             		+ 首先，需要从页列表中移除当前页对象（所有要使用链表结构）
             		+ 标定移除对象
             		+ 重新计算下一页的index=(index-1)
             	3. 根据index的大小分为下面两种情况讨论:
             	+ index 在页列表内 可以设置页的信息，包括
             		1. 当前页对应的内存块 --> 从页列表中获取
                  2. 从获得的内存块中获取页对象，和页内偏移量
                  3. 计算当前页记录数量
                  4. 计算当前页面页内偏移量
              + index 不在页列表范围内
              	1. 当前页对应的内存块设置为空
              	2. 若阅读器@UnsafeSorterSpillReader 非空,则需要处理删除失败情
              	况 @handleFailedDelete() -->从溢写器列表中移除首个文件
              	3. 关闭,并重新设置阅读器为溢写列表@spillWriters首个元素的序列化管理器
              4. 最后根据之前设置的引用，释放页的内存空间
             void remove()
             功能: 重写父级方法，主要是这个类不允许去溢出元素，重写目的是防止元素移除的操作。
             移除时抛出异常@UnsupportedOperationException
             void handleFailedDelete()
             功能: 移除从磁盘中溢出来的文件
             策略: 磁盘中移除的文件使用溢写列表装载(装的是文件的元数据信息),如果这个文件事实上存在且非		空，并且没有被删除掉的话，对其进行处理，处理方式，打出文件相对应的error级别日志。
           4. 溢出量的计算
           	synchronized long spill(long numBytes)
           	1. 如果迭代器不可是否空间或者页列表中只有一页的内容。直接返回1
           	2. 不满足上述情况下:
           		+ 更新内存峰值使用量@updatePeakMemoryUsed()
           		计算前的准备工作:
           			设置一个写时间计数器，按照之前笔记，借助@ShuffleWriteMetrics类即可，参数名称			为@writeMetrics
           		+ 获取页列表中的最后一页，如果是当前页，那么当前页是不能计算在释放量之内的。
           		+ 从内存块中获取如下信息:
           			基本页对象: base
           			偏移量: offset
           			页内记录数量: numRecords
           			页内偏移量: uaosize
           		根据上述计算的信息,初始化一个溢写器对象@UnsafeSorterSpillWriter(
            		BlockManager blockManager,int fileBufferSize,
           		ShuffleWriteMetrics writeMetrics,int numRecordsToWrite)
           		其中块管理器为外部类指定，文件缓冲大小这里设置为32k,计数器引用上面的，需要写出的数据			为页内记录数量。
           		+ 这时需要写出的数据大于0时
           			写出以偏移量=offset+uaosize,长度为对象长度，且排序前缀为0的记录到文件中
           			由于写出了一个对象偏移量又向右移动8个字节。此时需要写出的记录减1.
           		+ 重复上述操作，知道页内内容写出完毕
           		+ 关闭溢写器，并将这个使用过的溢写器归档到溢写列表中。(这样页内容就可以通过列表查找				类)
           		+ 从页列表中移除最后一页
           		+ 溢写数量+=内存块的大小，释放这个内存块
           		+ 如果溢写量超过了指定值，提前结束，且返回实际的溢写量
      }
      ```
      
      
      
      #class @Location
      
      ```markdown
      ADT Location{
      	数据元素: 
      		1. map中的位置: @pos
      		2. key存在标志: @isDefined （是否指向的位置存在key1）
      		3. key哈希值: @keyHashcode
      		4. 基本对象: @baseObject (key和value组成的)
      		5. key偏移量: @keyOffset
      		6. key长度: @keyLength
      		7. value偏移量: @valueOffset
      		8. value长度: @valueLength
      		9. 内存块: @memoryPage
      			内存块中包含有记录，且只能在迭代器中被创建
      	操作集:
      		1. 查找类
      		   	+ boolean isDefined()
      			key是否存在于指定的位置，是则返回true反之为false
      			+ Object getKeyBase()
      			在key存在的情况下，返回key的基本对象@baseObject 这里使用了assert关键字
      			+ long getKeyOffset()
      			在key存在的情况下，返回key偏移量@keyOffset
      			+ Object getValueBase()
      			获取key存在下，value对应的基本对象
      			+ long getValueOffset()
      			获取key存在下，value对应的偏移量
      			+ int getKeyLength()
      			获取key存在下，key的长度
      			+ getValueLength()
      			获取key存在下，value的长度
      			+ MemoryBlock getMemoryPage()
      			返回一个内存块，
      			+ boolean nextValue()
      			查找是否存在下一个键值对，且保证下一个key值与之相同
      			实现: 
      				1. 使用Platform获取下一个对象的位置
      				2. 获取结果不为0，则更新key信息@updateAddressesAndSizes(addr)
      					返回true，反之返回false
      		2. 更新类:
      			void updateAddressesAndSizes(final Object base, long offset)
      			首先由于底层操作系统的原因，会出现4-8字节不对齐情况出现，原因是使用4字节的int类型，		存储长度信息，会导致8字节的对象出现4字节的偏移
      			对弈一条完整的信息，其总长假设为totalLen,其中包括了起始的偏移(由操作系统引起),key		长度，以及value长度。
      			计算方法:
      			+ 首先计算总长
      			+ 获取key的长度以及key的偏移量@keyOffset
      				先向后移动uao_size计算key长度，前面是没对齐的不算在内
      				再移动uao_size并且在value段不设置偏移，此时kyeOffset=orgOffset+2*uao
      			+ 获取value的length以及偏移量@valueOffset
      				valueLenght=totalLen-uao_size-keyLength
      				valueOffset=KeyOffset+keylength
      			void updateAddressesAndSizes(long fullKeyAddress)
      			功能: 根据key的地址更新,底层键值对的信息
      			base= 通过地址获取的页对象
      			offset= 获取对应地址的页内偏移量
      			参考#class @TaskMemoryManager
      			底层调用:
      				updateAddressesAndSizes(base,offset)
      		3. 定位操作:
      			+ 内存块 and 页内偏移量定位
      				Location with(MemoryBlock page, long offsetInPage)
      				offset=指定的页内偏移量
      				base=指定的内存块对应的基本对象
      			+ key值定位
      				Location with(int pos, int keyHashcode, boolean isDefined)
      				假定在kv数组@longArray非空情况下
      				获取map中位置@pos,设置key的hashcode为指定，设置定义标记为指定。
      				在定义的状态下，需要从kv数组中获取2*pos位置的元素，作为key传入
      				@updateAddressesAndSizes(fullKeyAddress)更新长度,地址信息
      			+ 溢写定位
      				Location with(Object base, long offset, int length)
      				1. 设置标记@isDefined为true
      				2. 设置内存块为空，不使用内存块定位
      				3. 计算系统偏移量uao_size
      				4. 设置@keyOffset=offset+uao_size
      				5. 获取key的长度
      				6. 获取value的长度@valueOffset=offset+uao_size+keyLength
      				7. 获取@valueLength=length-uao_size-keyLength
      		4. 新增操作
      			boolean append(Object kbase, long koff, int klen, Object vbase, long 					voff, int vlen)
      			功能: 给key插入一个value,这个方法可以单个key可以多次调用。返回值表名了是添加成功还			是因为没有多余的内存而插入失败。	
      }
      ```
      
      
      
      3. **拓展**
      
      ```markdown
      1. volatile关键字
      2. 二进制运算 & 的使用
      3. jvm中如何释放变量的空间
      4. #class @UnsafeAlignedOffset
      5. recordsInPage 页内记录数的计算方法:
      6. offsetInPage 页偏移量的计算方法: 
      7. native 方法的查找
      8. 幂等性
      ```
      
      