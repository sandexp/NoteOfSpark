## **spark-util**

---

1. 类加载器相关

   + #class @ChildFirstURLClassLoader

     ```markdown
     介绍:
     	这是一个可变的类加载器，当加载类和资源的时候，可以将自己的URL置于父类加载器上。
     ```

     ```markdown
     ADT ChildFirstURLClassLoader{
     	初始加载元素
     		ClassLoader.registerAsParallelCapable()
     	数据元素:
     		1. 父类加载器: #name @parent #type @ParentClassLoader
     	操作集
     		ChildFirstURLClassLoader(URL[] urls, ClassLoader parent)
     		初始化本类url,指定父类加载器@parent
     		Class<?> loadClass(String name, boolean resolve)
     		加载类名为name的class类，resolve为true时加载
     		Enumeration<URL> getResources(String name)
     		找到与name匹配的所有资源
     		URL getResource(String name) 
     		找到与name匹配的类地址URL
     }
     ```

   + #class @MutableURLClassLoader

     ```markdown
     ADT MutableURLClassLoader{
     	初始加载元素
     		ClassLoader.registerAsParallelCapable()
     	操作集
     		MutableURLClassLoader(URL[] urls, ClassLoader parent)
     		初始URL类加载器的urls和父类加载器
     		void addURL(URL url)
     		添加新的同一资源定位符
     }
     ```

   + #class  @ParentClassLoader

     ```markdown
     ADT ParentClassLoader{
     	初始加载元素
     		ClassLoader.registerAsParallelCapable();
     	操作集
     		ParentClassLoader(ClassLoader parent)
     		初始化
     		Class<?> findClass(String name)
     		找到与name匹配的类
     		Class<?> loadClass(String name, boolean resolve)
     		匹配与name相同的类
     }
     ```

   + #class @EnumUtil

     ```markdown
     ADT EnumUtil{
     	操作集:
     		static <E extends Enum<E>> E parseIgnoreCase(Class<E> clz, String str)
     		返回与str相等的枚举值
     }
     ```

   + 基础拓展

     #class @ClassLoader

     ```markdown
     
     ```

2. spark-util-collection

   + [TimSort](# TimSort)
   + unsafe.sort
     1.  [PrefixComparator](# PrefixComparator)
     2.  [PrefixComparators](# PrefixComparators)
     3.  [RadixSort](# RadixSort)
     4.  [RecordComparator](# RecordComparator)
     5.  [RecordPointerAndKeyPrefix](# RecordPointerAndKeyPrefix)
     6.  [UnsafeExternalSorter](# UnsafeExternalSorter)
     7.  [UnsafeInMemorySorter](# UnsafeInMemorySorter)
     8.  [UnsafeSortDataFormat](# UnsafeSortDataFormat)
     9.  [UnsafeSorterIterator](# UnsafeSorterIterator)
     10.  [UnsafeSorterSpillMerger](# UnsafeSorterSpillMerger)
     11.  [UnsafeSorterSpillReader](# UnsafeSorterSpillReader)
     12.  [UnsafeSorterSpillWriter](# UnsafeSorterSpillWriter)

   ---

   #### **TimSort**

   介绍:
   	使用了安卓TimSort类,利用稳定的，合适的迭代的归并排序，具体请参照#method @`sort()`，在java中使用了原来的风格以便能够更加的贴近安卓源码，因此非常容易的去识别其正确性。这个类是私有的，使用简单的scala包装类对其进行包装#class @org.apache.spark.util.collection.Sorter ，这样就可以在spark中使用了。

   ​	使用这个端口的目的是产生一个接口，这个接口需要接受输入数据，除此之外还接受简单的数组数据。例如，只添加型map(AppendOnlyMap)使用这个使用这个接口去对数组进行交替排序，形入key/value。这种生成方式使用了最小的开销。详情见@SortDataFormat

   ​	允许key的重用，以防止创建了过多的key对象。

   ```markdown
ADT TimSort{
   	数据元素:
		1. 归并排序最小值 #name @MIN_MERGE=32 #type @int
   		这个是归并排序的最小数据量大小，小于这个值就不会去归并。且这个值设计时需要为2的n次方。如果需要减		小这个值，你必须要在构造器中改变stackLen的计算方式。否则的话极有可能出现数组越界的异常。请参
   		照listsort.txt,获取最小栈长度，使用这个值作为排序数组的长度以及最小归并的大小。
   		2. 排序数据格式 #name @s #type @SortDataFormat<K,Buffer>
       操作集:
       	1. 构造器
           TimSort(SortDataFormat<K, Buffer> sortDataFormat)
           初始化排序数据格式@s
           2. 操作类
           void sort(Buffer a, int lo, int hi, Comparator<? super K> c)
           功能: 使用稳定的合适的归并排序需要少于O(nlg n)次比较，相对于传统排序方式相比，当数据部分有序时执		行次数要低。
           void binarySort(Buffer a, int lo, int hi, int start, Comparator<? super K> c)
   	    功能: 排序指定数组的指定部分，使用二分插入排序(二叉查找树BST) 
   	    	时间复杂度O(nlog n)
   	    	数据移动量O(n^2)
   	   int countRunAndMakeAscending(Buffer a, int lo, int hi, Comparator<? super K> c)
   	   功能: 返回最长递增/递减序列长度
          注意这里递增序列的判断标准为 a[i]<=a[i+1]
          递减序列为 a[i]>a[i-1]
          这里对递减序列做出严格定义的目的是当反转递减序列时不会违反稳定排序的要求。
          void reverseRange(Buffer a, int lo, int hi)
          功能: 区域反转
          反转范围 lo - hi  --> hi - lo
          
          int minRunLength(int n)
          功能: 返回运行时最小的合并长度
          如果 n < MIN_MERGE 返回n
          如果 n 为2的整数次方，返回MIN_MERGE/2
          其他情况返回一个整数k，满足MIN_MERGE/2<=K<=MIN_MERGE,以便于n/k能够快速接近，确切来说是要小于在2		的整数次方的值。
   }
   ```
   
   #### PrefixComparator
   
   ```markdown
   /*
   	比较8个字节的前缀排序器，可以通过子类去实现它
*/
   ADT PrefixComparator{
	操作集:
   		int compare(long prefix1, long prefix2)
   		功能: 返回前缀1和前缀2的大小关系
   }
   ```
   
   #### PrefixComparators 
   
   ```markdown
   // 本类提供了各类排序器
   ADT PrefixComparators{
	数据元素:
   	1. 串排序器 #name @STRING #type @UnsignedPrefixComparator
	2. 串降序排序器 #name @STRING_DESC #type @UnsignedPrefixComparatorDesc
   	3. 串排序器(空串置后) #name @STRING_DESC #type @UnsignedPrefixComparatorDesc
   	4. 串降序排序器(空串置首) #name @STRING_DESC_NULLS_FIRST 
   		#type @UnsignedPrefixComparatorDescNullsFirst
   	5. 二分排序器 #name @BINARY #type @UnsignedPrefixComparator
   	6. 二分降序器 #name @BINARY_DESC #type @UnsignedPrefixComparatorDesc
   	7. 二分排序器(空置尾) #name @BINARY_NULLS_LAST #type @UnsignedPrefixComparatorNullsLast
   	8. 二分降序器(空置首) #nmae @BINARY_DESC_NULLS_FIRST 
   		#type @UnsignedPrefixComparatorDescNullsFirst
   	同类还有
   	LONG
   	LONG_DESC
   	LONG_NULLS_LAST
   	LONG_DESC_NULLS_FIRST
   	DOUBLE
   	DOUBLE_DESC
   	DOUBLE_NULLS_LAST
   	DOUBLE_DESC_NULLS_FIRST
   	
   	9. 串前缀比较器 #subclass @StringPrefixComparator
   	10. 二分前缀比较器
   	11. 双精度前缀比较器
   	12. 基于RadixSortSupport实现的几个实现类
   		UnsignedPrefixComparator
   		UnsignedPrefixComparatorNullsLast
   		UnsignedPrefixComparatorDescNullsFirst
   		UnsignedPrefixComparatorDesc
   		SignedPrefixComparator
   		SignedPrefixComparatorNullsLast
   		SignedPrefixComparatorDescNullsFirst
   		SignedPrefixComparatorDesc
   		每个子类都需要重写 sortDescending() sortSigned() nullsFirst()以及compare(b,a)方法
   }
   ```
   
   #subclass @RadixSortSupport
   
   ```markdown
   // 支持基数排序的参数，比较器实现了这个也就说明其定义的比较器满足基数排序
   ADT RadixSortSupport{
	操作集
   	sortDescending()
	功能: 为true表示排序需要按照二分排序降序顺序排列
   	sortSigned()
   	功能: 为true时，排序时需要考虑标志位(正负号)
   	nullsFirst()
   	功能: 为true时表示，排序时将空元素至于序列首部，否则放在序列尾部
   	
   }
   ```
   
   #### RadixSort
   
   ```markdown
   int sort(LongArray array, long numRecords, int startByteIndex, int endByteIndex,
         	boolean desc, boolean signed)
     功能介绍:
        	对给定的long型数组进行最小关键数字的基数排序。这个规定了你需要在数组之后有足够的空间，这个空间的大		小至少要等于记录数量。这个排序时毁灭性的且可能导致数组中数据的重新定位。
     输出参数:
        	@array long型元素的数组，且根据规定其中需要有足够多的空槽
	     	@numRecords 数组中的记录数量
        	@startByteIndex 从最小关键字节数起来的第一个
        	@endByteIndex 从最小关键字节数起来的最后一个，必须要大于@startByteIndex
        	@desc 是否降序(二分排序)
     	@signed 是否为有符号的排序
        返回参数:
     	返回排序后数据在给定数组中的起始index，比起将之拷贝回0位置处效率要高
        操作条件:
        	7=> @startByteIndex >=0
        	7=> @endByteIndex >=0
        	@endByteIndex > @startByteIndex
        	numRecords*2<=array.size()
        操作逻辑:
        	1. 计算每个值在给定数组中的频数分布
        	参照#method @long[][] getCounts
        	2. 对于从@startByteIndex --> @endByteIndex 的每个字节，只要这个字节的分布非空。就要对其进行局	部排序，局部排序的策略是: 对于每个在特定字节偏移量的字节值，将其数据拷贝到目的偏移量的位置。
     	详情参照 #method @sortAtByte
        	排序完毕交换inIndex与outIndex的位置指针
     	inIndex指向输入起始，outIndex指向输出起始
        	3. 最终返回inIndex指向位置值。
     
        void sortAtByte(LongArray array, long numRecords, long[] counts, int byteIdx, 
     	long inIndex, long outIndex,boolean desc, boolean signed)
        功能介绍: 通过对指定字节偏移量的字节值进行数据拷贝，将数据拷贝到目标偏移量位置，实现的部分排序。
     输入参数:
        	@array 参与部分排序的数组
     	@numRecords 数组中数据记录的数量
        	@counts 每个字节的频数分布情况，这个规定会破坏性的修改数组
     	@byteIdx 需要排序的字节位置，从最小关键字开始
        	@inIndex 数组中@array的起始下标，用于给输入数据定位
        	@outIndex 排序后数据的起始下标，用于指示输出数据应当写到的位置
        	@desc 是否为降序排序
        	@signed 是否为带符号的排序
        操作条件:
        	@counts 维度为256 即这里基数排序的基为256
        操作逻辑:
        	1. 获取目的地偏移量@offsets 
        	详情参照#methid @transformCountsToOffsets
        	2. 获取基本对象@baseObject以及页内偏移量@baseOffset和最大偏移量@maxOffset
        		baseObject=array.getBaseObject() @getBaseObject()
        		baseOffset=array.getBaseOffset() + inIndex * 8L
        			这里最大偏移量还需要加入加入内部起始偏移量@inIndex*8
        		maxOffset=baseOffset+numsRecords*8L
        			计算末尾记录偏移量即最大偏移量
        	3. 针对基本偏移量@baseOffset到最大偏移量@maxOffset之间的记录(每8个位1条)
        		获得该记录的value值(Platform.getLone(baseObject,offset))
        		计算基数排序中所属桶(基数)的编号
        			bucket=(value >>> (byteIdx * 8)) & 0xff
        		使用#method @putLong方法将基本对象@baseObject,@value以及桶编号@offsets[bucket]信息存入
        		将目的地偏移量的桶内指针下移动一个记录的长度单位，以便可以再容纳一条记录
        			offsets[bucket] += 8
        
        long[][] getCounts(LongArray array, long numRecords, int startByteIndex, int endByteIndex)
        功能: 计算给定数组值得频数分布情况
        输入参数:
         @array 给定数组
         @numRecords 给定数组中记录的数量
         @startByteIndex 第一个统计的字节(在这之前的字节会被跳过)
         @endByteIndex 最后一个统计字节
        返回参数:
        	返回一个256字节维的数组，每个数组开始于最小关键字，如果某个桶(基)不需要排序则为null
     操作逻辑:
        	1. 开辟合适维度的数组
     		long[][] counts = new long[8][]
        		设置bitwiseMax，bitwiseMin用于检测每个桶的数据变化
        	2. 对于baseOffset到maxOffset之间的记录
        		获取记录的value(Platform getLong)
        	3. 获取数据变化量(256位long型) = bitwiseMin ^ bitwiseMax
        	4. 对于startByteIndex到endByteIndex之间的数据
        		只要这之间数据发生了变化,则需要对该桶(基数)(共计8个桶)设置256个槽
        			条件: (bitsChanged >>> (i * 8)) & 0xff !=0
        			设置槽: counts[i] = new long[256]
        		使用Platform.getLong(baseObject, offset)获得对象所在位置，并计算其槽的位置
        			slot=(int)((Platform.getLong(baseObject, offset) >>> (i * 8)) & 0xff)
        
        long[] transformCountsToOffsets(long[] counts, long numRecords, long outputOffset, 
        	long bytesPerRecord,boolean desc, boolean signed)
        功能: 将counts[]数组根据排序类型转化为合适的输出偏移量数组
        输入参数:
        	@counts long[] 每个字节的计数器,这个可以毁坏性的修改数组
        	@numRecords int 原数组中的记录数量
        	@outputOffset long 基本数组对象的输出偏移字节数
        	@bytesPerRecord long 每条记录的大小(8- 不同排序 16- 关键字前缀排序)
     	@desc boolean 是否为降序排列
        	@signed 是否为符号排序
     返回参数 :
        	输入计数数组 long[]
     操作条件: 
        	@counts的槽为256个
        	1. 解决带符号数问题，当为负数时，输出范围时129-255
        		int start = signed ? 128 : 0
        	2. 处理排序问题
        		降序排列和升序排列实现上的区别
        			降序时位置指针初始化在最后一条记录，升序时指针位于第一条记录
        		相同点
        			计算槽位的方式=start[i] & 0xff
        			槽位对应值的设置 counts[i & 0xff]=outputOffset + pos * bytesPerRecord
        
        int sortKeyPrefixArray(LongArray array,long startIndex,long numRecords,int startByteIndex,
         	int endByteIndex,boolean desc,boolean signed)
        功能: 为关键字前缀排序的排序方法，在这个排序中记录有两个long型数据的长度512位，但是只有第二部分参与排		序
        输入参数: 
        	@startIndex 数组中开始参与排序的index，不支持普通排序的实现支持
        	@array 排序数组
        	@numRecords 记录数量
        	@startByteIndex 第一个统计的字节(在这之前的字节会被跳过)
        	@endByteIndex 最后一个统计字节
        	@desc 是否为降序排列
        	@signed 是否为符号排序
        操作条件:
        	7 >=startByteIndex >=0
        	7 >= endByteIndex >=0
        	endByteIndex > startByteIndex
        	numRecords * 4 <= array.size()
        操作逻辑:
        	类似#method @sort()
        	指示由于参与排序的不是记录本身，还需要它的前缀信息，所以局部排序使用
        	#method @sortKeyPrefixArrayCounts，获取counts[][]数组使用
     	#method @getKeyPrefixArrayCounts
        	
     long[][] getKeyPrefixArrayCounts(LongArray array, long startIndex, long numRecords,
        	int startByteIndex, int endByteIndex)
        功能: 获取带有关键字前缀的counts数组信息
        操作逻辑: 与@getCounts类似，与之不同的是
        每次计算一条记录的槽位之后，一下条运算的记录位置指针=当前位置指针+16
        
        void sortKeyPrefixArrayAtByte(LongArray array, long numRecords, long[] counts, 
        int byteIdx, long inIndex, long outIndex,boolean desc, boolean signed)
        功能: 对带有关键字前缀的counts信息，进行排序
        实现逻辑:
    	 类似sortAtByte,不同点
    	 修正baseOffset = array.getBaseOffset() + inIndex * 8L
    	 修正maxOffset = baseOffset + numRecords * 16L
    	 修正每次记录计算后指针+=16
    	 增加前缀和关键字的区分
    	 	key = Platform.getLong(baseObject, offset)
    	 	prefix = Platform.getLong(baseObject, offset + 8)
    	 bucket计算方式变更=(int)((prefix >>> (byteIdx * 8)) & 0xff)
    	 在对关键字存储的基础上,支持对前缀的存储
        	Platform.putLong(baseObject, dest, key)
         	Platform.putLong(baseObject, dest + 8, prefix)
    	 修正桶内指针增加量
    	 	offsets[bucket] += 16
   ```
   
   #### RecordComparator
   
   ```markdown
   // 记录比较器 当整个参与sort的key值使用前缀比较器就可以排序完成的化，这个排序直接返回0即可
   ADT RecordComparator{
   	操作集:
   	int compare(Object leftBaseObject,long leftBaseOffset,int leftBaseLength,
       	Object rightBaseObject,long rightBaseOffset,int rightBaseLength)
       功能: 获取页表数据中左右两条记录的大小关系，具体逻辑由子类完成
   }
   ```
   
   #### RecordPointerAndKeyPrefix
   
   ```markdown
   ADT RecordPointerAndKeyPrefix{
   	数据元素:
   	1. 记录指针 #name @recordPointer 
   		用于指向记录的指针，具体地址是如何加密的请参照任务内存管理器@TaskMemoryManager
   	2. 关键字前缀 #name @keyPrefix
   		关键字前缀，用于比较
   }
   ```
   
   #### UnsafeExternalSorter
   
   ```markdown
// 外部排序器基于内部排序器#class @UnsafeInMemorySorter
   ADT UnsafeExternalSorter { father -> #class @MemoryConsumer
	数据元素:
   	1. 日志处理器 #name @logger #type @Logger 
   	2. 前缀比较器 #name @prefixComparator #type @PrefixComparator
   	3. 记录比较器提供者 #name @recordComparatorSupplier #type @Supplier<RecordComparator>
   		RecordComparator保存的可能是上一次比较的结果，所以在外部比较器@UnsafeExternalSorter，不能去保	存@RecordComparator实例,原因是@UnsafeExternalSorter被任务上下文管理器@TaskContext所引用，因此直到	 task结束之前，是不能够被垃圾回收的。故而需要辅助内存去存储这个信息。
   	4. 任务内存管理器 #name @taskMemoryManager #type @TaskMemoryManager
   	5. 块管理器 #name @blockManager #type @BlockManager
   	6. 序列化管理器 #name @serializerManager #type @SerializerManager
   	7. 任务上下文管理器 #name @taskContext #type @TaskContext
   	8. 文件缓冲字节数 #name @fileBufferSizeBytes #type @int
   		写溢出文件所使用的缓冲大小(字节)
   	9. 溢写容量 #name @numElementsForSpillThreshold #type @int
   	10. 已分配页/内存块列表 #name @allocatedPages #type @LinkedList<MemoryBlock>
   	11. 溢写写出器列表 #name @spillWriters #type @LinkedList<UnsafeSorterSpillWriter>
   	12. 内存排序器 #name @inMemSorter #type @UnsafeInMemorySorter volatile 溢写后重置
   	13. 当前页/内存块 #name @currentPage=null #type @MemoryBlock 溢写后重置
   	14. 页指针 #name @pageCursor=-1 溢写后重置
   	15. 峰值内存使用量 #name @peakMemoryUsedBytes=0 溢写后重置
   	16. 溢写字节数 #name @totalSpillBytes=0 溢写后重置
   	17. 总排序时间 #name @totalSortTimeNanos=0 
   	18. 读取迭代器 #type @readingIterator=null #type @volatile SpillableIterator
   	操作集:
   	1. 静态方法
   	static UnsafeExternalSorter createWithExistingInMemorySorter(
         TaskMemoryManager taskMemoryManager,BlockManager blockManager,
         SerializerManager serializerManager,TaskContext taskContext,
         Supplier<RecordComparator> recordComparatorSupplier,
         PrefixComparator prefixComparator,int initialSize,long pageSizeBytes,
         int numElementsForSpillThreshold,UnsafeInMemorySorter inMemorySorter)
   	功能: 在内存排序器存在的情况下创建外部排序器
   	+ 根据外部排序器构造器初始化外部排序器@sorter [需要指定内部排序器，且不支持基数排序]
   	+ 将内部排序器的所有内容溢写出来@sorter #method @spill
   	+ 将内部排序器置空
   	返回 创建的外部排序器
   	static UnsafeExternalSorter create(
         TaskMemoryManager taskMemoryManager,BlockManager blockManager,
         SerializerManager serializerManager,TaskContext taskContext,
         Supplier<RecordComparator> recordComparatorSupplier,PrefixComparator prefixComparator,
         int initialSize,long pageSizeBytes,
         int numElementsForSpillThreshold,boolean canUseRadixSort)
		返回 有给定参数创建的外部排序器,具体参照#constructor@UnsafeExternalSorter,需要指定内部排序器为null
   	2. 构造器 [私有]
		private UnsafeExternalSorter(TaskMemoryManager taskMemoryManager,
         BlockManager blockManager,SerializerManager serializerManager,
         TaskContext taskContext,Supplier<RecordComparator> recordComparatorSupplier,
         PrefixComparator prefixComparator,int initialSize,
         long pageSizeBytes,int numElementsForSpillThreshold,
         @Nullable UnsafeInMemorySorter existingInMemorySorter,boolean canUseRadixSort)
   	功能:
   		初始化任务内存管理器@taskMemoryManager，块管理器@blockManager
   		序列化管理器@serializerManager,上下文管理器@taskContext
   		记录比较器提供者@recordComparatorSupplier,前缀比较器@prefixComparator为指定
   		初始化文件缓冲字节数 @fileBufferSizeBytes=32*1024
   		初始化内存排序器@inMemSorter
   		初始化峰值内存使用量@peakMemoryUsedBytes,溢出容量@numElementsForSpillThreshold
   		任务上下文注册清除任务，用于确保任务结束内存会被释放
   			taskContext.addTaskCompletionListener(context -> {cleanupResources();})
   	3. 查询获取类
   	long getMemoryUsage()
   	功能: 获取内存使用量(排序器使用的)
   	val=内存排序器内存使用量+ 外存所分配页表的总长度
   		= inMemSorter.getMemoryUsage())+ sigma(allocatedPages,page.size())
   	long getPeakMemoryUsedBytes()
   	功能: 获取到执行该命令前的内存峰值使用量
   	long getSortTimeNanos()
   	功能: 获取排序时间@totalSortTimeNanos
   	long getSpillSize()
   	功能: 获取溢写字节总数@totalSpillBytes
   	int getNumberOfAllocatedPages()
   	功能: 获取已分配页/内存块的数量@allocatedPages.size()
   	UnsafeSorterIterator getIterator(int startIndex) 
   	功能: 获取从startIndex开始的排序迭代器，当然消费完迭代器数据之后，需要有调用者自己清空数据，通过:
   		cleanupResources()
   	+ 溢写写出器列表@spillWriters为空，直接返回内存排序器@inMemSorter所对应的迭代器。
   	+ 溢写写出器列表非空，返回一个链式迭代器#class @ChainedIterator，这个链式迭代器中的元素由列表中溢写	  写出器所对应的迭代器链表。
   	
   	4. 操作类
   	void moveOver(UnsafeSorterIterator iter, int steps)
   	功能: 将迭代器的位置指针@pos向后移动steps个位置。如果不能够移动到，则会抛出异常。
   	
   	void updatePeakMemoryUsed()
   	功能: 更新峰值使用量
   	peakMemoryUsedBytes=getMemoryUsage()
   	
   	long freeMemory() 
   	功能: 释放所有已分配内存块的内存，重置已分配页表@allocatedPages，当前页@currentPage，页指针
   	@pageCursor，并返回释放的内存量
   	
   	void deleteSpillFiles()
   	功能： 删除排序器创建的溢写文件
   	+ 获取溢写写出器列表中写出器对应的溢写文件#method @getFile(),并删除其指示的文件
   	删除失败则会以日志的形式提示
   	
   	void cleanupResources()
   	功能: 清除排序器的内存部分数据结构，且删除所有的溢写文件
   	
   	void growPointerArrayIfNecessary()
   	功能: 扩展指针数组
   	操作条件: 内部排序器存在
   	操作逻辑:
   	1. 当内部排序器不存在下一条记录(下一条@pos指出了范围外)
   		检查是否出现溢写
   			出现溢写则直接溢写，并结束
   		检查是否出现@SparkOutOfMemoryError
   			出现则打出error级别日志，并结束
   	2. 内部排序器可以容纳下一条记
       	 释放长数组#name @array #class @LongArray
          不可容纳下一条记录，且未触发溢写和@SparkOutOfMemoryError
          	 将内部排序器扩展到新数组@array(长度=inMemSorter.getMemoryUsage()/8*2)
          	 
       void acquireNewPageIfNecessary(int required)
   	功能: 分配更多的内存用于插入更多的记录,它会向内存管理器申请更多的内存空间。如果申请的内存空间无法获得	则会触发溢写。
   	输出参数: required: 数据页所需要的内存空间(单位:字节数)
   	页表新增内存块条件:
   		当前页不存在@currentPage=null 或者 给定的required使得页指针@pagecursor超出当前页范围，即:
   			pageCursor + required > currentPage.getBaseOffset() + currentPage.size()
   		需要采取的措施	
   			新增一个长度为required的内存块，将页指针@pagecursor指向当前页的开头@getBaseOffset()
   			并将新增的内存块加入到页表@allocatedPages中
   
   	void insertRecord(Object recordBase, long recordOffset, int length, 
   		long prefix, boolean prefixIsNull)
   	功能: 插入一条记录到排序器中
   	操作条件: 内部排序器@inMemSorter存在
   	操作逻辑:
   		1. 内存排序器中的记录数量大于等于溢出容量@numElementsForSpillThreshold
   			对内存排序器中的记录进行溢写
   		2. 检查指针数组@array是否需要扩充@growPointerArrayIfNecessary()并进行相关操作
   		3. 计算可能需要新开辟的内存块大小required=记录长度length+操作系统偏移量uaoSize
   		4. 获取新的基本对象@base指向新开辟的内存块,并通过任务内存管理器@taskMemoryManager
   		#method @encodePageNumberAndOffset计算其地址@recordAddress 
   		5. 将需要插入的基本对象@recordBase 使用内存拷贝复制到@base处。
   		6. 使用内存排序器@inMemSorter 在记录地址@recordAddress处插入记录，并根据给定指示是否有前缀关键		字信息
   	void insertKVRecord(Object keyBase, long keyOffset, int keyLen,
         	Object valueBase, long valueOffset, int valueLen, long prefix, boolean prefixIsNull)
   	功能: 向排序器中插入kv键值对记录
   		记录格式:
   			record length (4 Bytes) + key length(4 Bytes):key data +value data
        操作逻辑:
        	计算需要开辟的内存块大小required=keyLen + valueLen + (2 * uaoSize)
        		这里需要统计两次操作系统偏移量uao_size 
        	使用内存拷贝的方式将k/v的数据拷贝到base地址处，由于kv是相邻的，所有需要注意@pageCursor的位置，注	  意这个2个操作系统偏移量分配在开头，然后是keyLen最后是valueLen
   		最后使用内部排序器插入记录，注意是否指定了带有前缀关键字的排序。	
   	
       void merge(UnsafeExternalSorter other)
       功能: other排序器合并到本排序器，其他排序器置空
       操作逻辑:
       	1. other先将自己内存排序器中内容溢写出来
   		2. 将other溢写器列表@spillWriters 整体迁移到本类溢写器列表中@spillWriters
   		3. 清空other的溢写器列表@spillWriters，且清空其资源#method @cleanupResources()
   	
   	static void spillIterator(UnsafeSorterIterator inMemIterator,
         UnsafeSorterSpillWriter spillWriter)
         功能: 迭代溢写
         将内存迭代器@inMemIterator中的所有元素进行溢写
        
        long spill(long size, MemoryConsumer trigger)
        功能: 排序溢写，并返回溢写数量
        操作条件:
       	必须是本类触发，且内存排序器存在记录数量大于0，否则全部返回0
        操作逻辑:
       	1. 创建一个新的溢写写出器@spillWriter，并将其添加到溢写列表中@spillWriters
       	2. 使用迭代溢写的方式对内存排序器中的迭代器进行溢写@spillIterator
       	3. 统计溢写量=freeMemory()
       	4. 溢写完毕重置内部排序器@reset()
       	5. 任务上下文管理器使用任务度量器@taskMetrics()对溢写内存量与磁盘溢写量进行统计。
       	更新溢写总量@totalSpillBytes
   }
   ```
   
   #subclass @SpillableIterator
   
   ```markdown
   // 支持溢写的排序迭代器@UnsafeSorterIterator
   ADT SpillableIterator{
   	数据元素:
   	1. 上游排序迭代器@upstream #type @UnsafeSorterIterator
   	2. 下一个上游排序迭代器@nextUpstream=null #type @UnsafeSorterIterator
   	3. 上一页/内存块@lastPage=null #type @MemoryBlock
   	4. 是否加载@loaded=false #type @boolean
   	5. 记录数量@numRecords=0
   	操作集:
   	SpillableIterator(UnsafeSorterIterator inMemIterator)
   	功能:通过指定的内部比较器，初始化上游排序迭代器@upstream以及记录数量@numRecords
   	int getNumRecords() 
   	功能: 获取记录数量@numRecords
   	
   	boolean hasNext()
   	功能:确定是否有下一条数据
   	val=numRecords > 0
   	
   	Object getBaseObject()
   	功能: 获取基本对象
   	val= upstream.getBaseObject()
   	
   	long getBaseOffset()
   	功能: 获取页内偏移量
   	val= upstream.getBaseOffset()
   	
   	int getRecordLength()	
   	功能: 获取记录数量
   	val= upstream.getRecordLength()
   	
   	long getKeyPrefix()
   	功能: 获取前缀关键字
   	val= upstream.getKeyPrefix()
   	
   	void loadNext()
   	加载下一条记录
   	
   	long spill()
   	功能: 溢写
   	溢写的工作涉及到读者-写者问题，所以需要设置互斥量，因此需要同步的工作
   	使用迭代溢写@spillIterator的方式对内存迭代器中的内容进行溢写
   	溢写完毕,释放内存排序器中的内容,将其置null
   }
   ```
   
   #subclass @ChainedIterator
   
   ```markdown
   ADT ChainedIterator{
   	数据元素:
   	1. 排序迭代器队列@iterators #type @Queue<UnsafeSorterIterator>
   	2. 当前排序迭代器@current #type @UnsafeSorterIterator
   	3. 记录数@numRecords #type @int
   	操作集:
   	ChainedIterator(Queue<UnsafeSorterIterator> iterators)
   	功能: 
   		初始化记录数@numRecords=sigma(iterators.size)
   		初始化当前迭代器@current为队列头部，并移除队列头部
   	int getNumRecords()
   	功能: 获取记录数量@numRecords
   	boolean hasNext() 
   	功能: 检查是否还有下一个迭代器
   	Object getBaseObject()
   	功能: 获取基本对象
   	long getBaseOffset()
   	功能: 获取页内偏移量
   	int getRecordLength()
   	功能: 获取记录长度
   	long getKeyPrefix()
   	功能: 获取关键字前缀
   	void loadNext()
   	功能: 加载迭代器下一个指针记录
   }
   ```
   
   
   
   #### UnsafeInMemorySorter
   
   ```markdown
   ADT UnsafeInMemorySorter{
   	数据元素:
   	1. 内存消费者 #name @consumer #type @MemoryConsumer
   	2. 任务内存管理器 #name @memoryManager #type @TaskMemoryManager
   	3. 排序比较器 #name @sortComparator #type @Comparator<RecordPointerAndKeyPrefix>
   	4. 基数排序支持参数 #name @radixSortSupport #type @PrefixComparators.RadixSortSupport
   	5. 长数组(long型数组)存储器 #name @array #type @LongArray 
   	6. 排序缓冲插入记录指针 #name @pos(初始化为0)
   	7. 空值上界指针#name @nullBoundaryPos(初始化为0)
   		基数排序时，小于这个值会存放一些空值记录
   		为了排序时不排空值,则需要设定这个参数
   	8. 可用容量 #name @usableCapacity(初始化为0)
   		用于确定长型数组的容量
   	9. 初始化大小 #name @initialSize #type @long
   	10. 排序总时间(ns) #name @totalSortTimeNanos #long
   	操作集:
   	1. 构造器
   	UnsafeInMemorySorter(final MemoryConsumer consumer,final TaskMemoryManager memoryManager,
         final RecordComparator recordComparator,final PrefixComparator prefixComparator,
         LongArray array,boolean canUseRadixSort)
       功能: 初始化内存消费者@consumer, 任务内存管理器@memoryManager，初始化初始化大小@initialSize=array.
       size().
       	初始化排序比较器@sortComparator,基数排序支持参数@radixSortSupport
       	初始化长型数组@array,可用可使用容量@usableCapacity=#method @getUsableCapacity()
   	
   	UnsafeInMemorySorter(final MemoryConsumer consumer,final TaskMemoryManager memoryManager,
       	final RecordComparator recordComparator,final PrefixComparator prefixComparator,
       	int initialSize,boolean canUseRadixSort)
   	功能: initialSize=consumer.allocateArray(initialSize * 2L) 回调上一个构造器
   		
   	2. 查询获取类
   	int getUsableCapacity()
   	功能: 获取可用容量
   	val= (int) (array.size() / (radixSortSupport != null ? 2 : 1.5))
   分别是基数排序/Timsort的实际可用容量
   	int numRecords()
   功能: 获取记录数量
   	val=排序缓冲插入记录指针@pos/2
   	long getSortTimeNanos()
   	功能: 获取排序时间(ns)
   	long getMemoryUsage()
   	功能: 获取内存使用量[长数组@array所占内存量]
   	val= (array==null)?0:array.size()*8
   	boolean hasSpaceForAnotherRecord()
   	功能: 确定当前位置指针@pos之后是否还有其他记录
   	val= pos + 1 < usableCapacity
   	UnsafeSorterIterator getSortedIterator()
   	功能: 按照排序完成后的顺序返回排序迭代器@UnsafeSorterIterator
   	执行逻辑:
   		1. 排序比较器@sortComparator 存在，则:
   		支持基数排序
   			#class @RadixSort #method @sortKeyPrefixArray
   		不支持基数排序(使用TimSort)
   			+ 创建未使用内存块@unused
   			unused=new MemoryBlock(array.getBaseObject(),
   			array.getBaseOffset() + pos * 8L,(array.size() - pos) * 8L)
   			+ 建立对未使用内存块的缓冲区
   			LongArray buffer = new LongArray(unused)
   			+ 创建记录键值对/关键字前缀与缓冲区的排序器
   			Sorter<RecordPointerAndKeyPrefix, LongArray> sorter
   			+ 对缓冲区内记录进行排序(缓存区内记录之后pos的一半)
   		2. 去空值处理
   			首先注意到只有在基数排序的情况下才需要出现空值
   			所以:
   			操作条件: 支持基数排序
   			1. 存在有空值
   			按照#class @radixSortSupport #method @nullsFirst()指示，先/后存放空值
   			返回#class @UnsafeExternalSorter #method @ChainedIterator(queue)
   			2. 不存在空值
   			返回#subclass @SortedIterator(pos / 2, offset)
   		
   	3. 动作类
   	void free()
   	功能: 释放长数组@array的内存空间
   	操作条件: 内存消费者存在，长数组非空
   	操作结果: 长数组@array释放内存空间，并重新置空
   	void reset()
   	功能: 重置为初始化状态
   	void expandPointerArray(LongArray newArray)
   	功能: 将长数组@array扩展为@newArray
   	操作条件: 新数组@newArray大小应当不小于旧数组@array
   	操作逻辑:
   		1. 使用内存拷贝的方式将旧数组的数据拷贝到新数组上
   		2. 内存消费者释放旧数组空间，重新指向长数组@array为新数组@newArray,获取可使用容量
   		@usableCapacity
   	void insertRecord(long recordPointer, long keyPrefix, boolean prefixIsNull)
       功能: 插入记录
       操作条件: 当前@pos所指位置允许再插入一条数据(否则抛出异常)
       操作逻辑:
       假设记录指针@recordPointer指向4字节的整数(int),
       	1. 支持基数排序(方便基数排序的空值处理)
       	从非空临界点@nullBoundaryPos处开始空出两个位置(通过交换pos以及pos+1处与nullBoundaryPos和
       	nullBoundaryPos+1处的数据)
       	将临近点@nullBoundaryPos设为@recordPoint而@nullBoundaryPos+1设置为@keyPrefix
       	设置完毕移动临界点位置(++)
           2. 不支持基数排序
           直接将@recordPoint以及@keyPrefix插入到pos后面，并且更新指针    
   }
   ```
   
   #subclass @SortedIterator
   
   ```markdown
   ADT SortedIterator{
   	数据元素:
   	1. 记录数量@numRecords #type @final int
   	2. 位置指针@position @int
   	3. 偏移量@offset @int
   	4. 基本对象@baseObject @Object
   	5. 页内偏移量@baseOffset @long
   	6. 关键字前缀@keyPrefix @long
   	7. 记录长度@recordLength @int
   	8. 当前页编号@currentPageNumber @long
   	9. 任务上下文管理器@taskContext @final TaskContext
   	操作集:
   	1. 构造器
   	SortedIterator(int numRecords, int offset)
   	功能： 初始化记录数量@numRecords，位置指针@position=0，偏移量@offset
   	2. 其他
   	int getNumRecords()
   	功能: 获取记录数量
   	boolean hasNext()
   	功能: 确认是否存在下一条记录
   	position/2<numRecords
   	Object getBaseObject()
   	功能: 获取基本对象
   	long getBaseOffset()
   	功能: 获取页内偏移量
   	long getCurrentPageNumber()
   	功能: 获取当前页编号
   	int getRecordLength()
   	功能: 获取记录长度
   	long getKeyPrefix()
   	功能: 获取关键字前缀
   	void loadNext()
   	功能: 获取下一条记录
   	操作条件: 任务上下文管理器存在，且当前任务被标记为killed，需要先kill任务再进行下一步操作，且需要有下	一条记录，使用hasNext()确认
   	需要获取如下参数:
   		1. 基本对象@baseObject
   		2. 页内偏移量@baseOffset
   		3. 记录长度@recordLength
   		4. 关键字前缀@keyPrefix
   		5. 当前页编号@currentPageNumber
   	获取完成之后,位置指针向后移动2位(recordpoint和prefix)
   	SortedIterator clone()
   	功能: 迭代器克隆
   	操作逻辑: 
   		1. 新设置一个迭代器对象SortedIterator
   		2. 设置新的对象属性值=当前对象属性值
   }
   ```
   
   #subclass @SortComparator
   
   ```markdown
   ADT SortComparator{
   	数据元素:
   	1. 记录比较器@recordComparator #type @RecordComparator
   	2. 前缀比较器@prefixComparator #type @PrefixComparator
   	3. 任务内存管理器@memoryManager #type @TaskMemoryManager
   	操作集:
   	1. 构造器
   	   SortComparator(RecordComparator recordComparator,PrefixComparator prefixComparator,
           	TaskMemoryManager memoryManager)
   	   初始化: 记录比较器，前缀比较器，任务内存管理器
   	2. 其他   
          int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2)
   	   功能: 实现两个记录指针/关键字前缀的比较逻辑
    	   1. 参与比较的主关键字为关键字前缀@prefixKey
          2. 主关键字相同，比较次关键字
          主关键字不同
          val= #class @PrefixComparator #method @compare
          次关键字的比较逻辑:
          val= #class @RecordComparator #method @compare
   }
   ```
   
   #### UnsafeSortDataFormat
   
   介绍:
   
   ​		支持记录指针，关键字对的排序。用于内存排序器@UnsafeInMemorySorter。
   
   在一个数组中2*i位置放置的是记录指针的值(地址)，2 * i +1位置放置的是8字节关键字信息。
   
   ```markdown
   ADT UnsafeSortDataFormat{
   	数据元素:
   	1. 长数组缓冲(8位宽) #name @buffer #type @LongArray
   	操作集:
   	1. 构造器
   	UnsafeSortDataFormat(LongArray buffer)
   	初始化长数组
   	2. 查询获取类
   	RecordPointerAndKeyPrefix getKey(LongArray data, int pos)
   	不支持按位置查找功能[查找的意义改变了]，若是调用类则会抛出异常
   	
   	RecordPointerAndKeyPrefix newKey()
   	功能: 新建一个关键字，使用@RecordPointerAndKeyPrefix默认构造器初始化
   	
   	RecordPointerAndKeyPrefix getKey(LongArray data, int pos,
   		RecordPointerAndKeyPrefix reuse)
   	功能: 设置记录指针/关键字键值对的值为pos*2 / pos*2+1
   	返回记录指针/关键字对象
   	
   	void swap(LongArray data, int pos0, int pos1)
   	功能: 交换长数组pos0,pos1位置记录指针/关键字键值对信息
   	
   	void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos)
   	功能: 拷贝srcPos到dstPos的记录指针/关键字键值对信息
   	
   	void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length)
   	功能: 拷贝srcPos开始的length个数据到dstPos，由于数据量较大，需要使用内存拷贝的方式。
   	LongArray allocate(int length)
   	功能: 一个大小为length的长数组缓冲@LongArray
   }
   ```
   
   #### UnsafeSorterIterator
   
   ```markdown
   ADT UnsafeSorterIterator{
   	操作集:
   	boolean hasNext()
   	检查迭代是否还有其他元素
   	void loadNext()
   	装载下一个元素/记录
   	Object getBaseObject()
   	获取页表中的基本对象
   	long getBaseOffset()
   	获取页表中的基本偏移量[页内偏移地址]
   	int getRecordLength()
   	获取记录长度
   	long getKeyPrefix()
   	获取关键字前缀
   	int getNumRecords()
   	获取记录数量
   }
   ```
   
   
   
   #### UnsafeSorterSpillMerger
   
   ```markdown
   ADT UnsafeSorterSpillMerger{
   	数据元素:
   	1. 记录数量 @numRecords=0
   	2. 排序迭代器优先队列 #name @priorityQueue #type @PriorityQueue<UnsafeSorterIterator>
   	操作集:
   	1. 构造器
   	  UnsafeSorterSpillMerger(RecordComparator recordComparator,
   	  	PrefixComparator prefixComparator,int numSpills)
   	  初始化获得记录比较器
   	  	优先返回前缀比较器的比较值，相等时返回记录比较器的值
   	  将获得的比较器形成排序器迭代器，并将其置入优先队列中。
   	2. 操作类
   	void addSpillIfNotEmpty(UnsafeSorterIterator spillReader)
   	功能: 将排序器迭代器放入优先队列中
   	如果迭代器中还有数据，则将数据置入到优先队列@priorityQueue，更新记录数量
   	@numRecords+=spillReader.getNumRecords()
   	
   	UnsafeSorterIterator getSortedIterator()
   	功能: 获取迭代器排序器
   	使用匿名内部类实现@UnsafeSorterIterator,实现时
   		getNumRecords()=numRecords
   		hasNext() = isEmpty(priortyQueue)
   		loadNext()={priortyQueue.push()}
   		Object getBaseObject()=spillReader.getBaseObject()
   		int getBaseOffset = spillReader.getBaseOffset()
   		int getRecordLength()  =  spillReader.getRecordLength()
   		long getKeyPrefix()  =  spillReader.getKeyPrefix()
   }
   ```
   
   #### UnsafeSorterSpillReader
   
   ```markdown
   // 读取由@UnsafeSorterSpillWriter 写出的溢出文件
   ADT UnsafeSorterSpillReader{
   	数据元素:
   	1. 最大缓冲字节数@MAX_BUFFER_SIZE_BYTES=16777216
   	2. 输入流 #name @in #type @InputStream
   	3. 数据输入流 #name @din #type @DataInputStream
   	4. 记录长度 #name @recordLength #type @int
   	5. 关键字前缀 #name @keyPrefix #type @long
   	6. 记录数量 #name @numRecords #type @int
   	7. 剩余记录数量 #name @numRecordsRemaining #type @int
   	8. 字节数组 #name @arr #type @byte[1024*1024] 
   	9. 基本对象 #name @baseObject=arr #type @Object
   	10. 任务上下文 #name @taskContext #type @TaskContext
   	操作集:
   	1. 构造器
   	UnsafeSorterSpillReader(SerializerManager serializerManager,File file,BlockId blockId)
   	功能: 初始化
   	+ 输入流@in=@NioBufferedFileInputStream(file,buff_size)
   	  如果spark env中配置了预读状态位@UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED()则采用
   	  @ReadAheadInputStream配置输入流，读取速度会更快
   	  高级流@din=DataInputStream(in)
   	2. 查询获取类
   	int getNumRecords()
   	功能: 获取记录长度
   	boolean hasNext()
   	功能: 检查是否还有剩余数据（@numRecordsRemaining>0）
   	Object getBaseObject()
   	功能: 获取基本对象
   	long getBaseOffset()
   	功能: 获取基本偏移量(页内偏移量)=Platform.BYTE_ARRAY_OFFSET
   	int getRecordLength()
   	功能: 获取记录长度@recordLength
   	getKeyPrefix()
   	功能: 获取关键字前缀
   	void close()
   	功能: 关闭输入流
   	3. 操作类
   	void loadNext()
   	功能: 读入记录长度@recordLength，关键字前缀@keyPrefix，基本对象@baseObject=arr(arr长度为
   	@recordLength)，并更新剩余记录@numRecordsRemaining，没有剩余数据时关流。
   	注: 如果任务被标记为killed时，需要使用任务上下文管理器@taskContext 删除中断任务 #method 
   	@killTaskIfInterrupted()
   }
   ```
   
   #### UnsafeSorterSpillWriter
   
   ```markdown
   // 溢写排序的记录到磁盘，溢写文件遵循如下格式
   // 记录数量 + [长度,前缀(long),数据(字节)]
   ADT UnsafeSorterSpillWriter{
   	数据元素: 
   	1. 应用程序配置集 	#name @conf #type @SparkConf
   	2. 磁盘写缓冲大小 #name @diskWriteBufferSize #type @int 值从conf中获取
   	3. 写缓冲 #name @writeBuffer #type @byte[] size=diskWriteBufferSize
   	4. 文件对象 #name @file #type @File
   	5. 磁盘块写出器 #name @writer #type @DiskBlockObjectWriter
   	6. 块编号 #name @blockId #type @BlockId
   	7. 需要写入的记录数量#name @numRecordsToWrite #type @int
   	8. 溢写记录数量 #name @numRecordsSpilled #type @int
   	操作集:
   	1. 构造器
   	UnsafeSorterSpillWriter(BlockManager blockManager,int fileBufferSize,
         	ShuffleWriteMetrics writeMetrics,int numRecordsToWrite)
   	功能: 初始化文件对象@file，块编号@blockId，需要写入的记录数量@numRecordsToWrite
   		磁盘写出器@writer[这里需要使用伪序列化器@DummySerializerInstance]
   		并将缓冲数据写出
   	2. 查找获取类
   	public File getFile()
   	功能: 获取文件对象
   	int recordsSpilled()
   	功能: 获取溢写记录数量
   	UnsafeSorterSpillReader getReader(SerializerManager serializerManager)
   	功能: 获取溢写器对应的阅读器
   	3. 操作类
   	void close()
   	功能: 关流
   	void writeLongToBuffer(long v, int offset)
   	功能: 将long型数据写入到buffer(8个bit)
   	void writeIntToBuffer(int v, int offset)
   	功能: 将int型数据写入到buffer(4个bit)
   	void write(Object baseObject,long baseOffset,int recordLength,long keyPrefix)
   	功能: 写出记录对象
   	+ 写4位记录长度recordLength到writeBuffer数组中
   	+ 写8位记录长度keyPrefix到writeBuffer数组中
   	+ 计算buffer中空闲空间freeSpaceInWriteBuffer=@diskWriteBufferSize-4-8
   	  数据剩余量dataRemaining=recordLength
   	  记录读取位置指针 recordReadPosition=页内偏移量@baseOffset
   	  将基本对象@baseObject以偏移量@baseOffset开始的部分拷贝到缓冲数组buffer中，拷贝数据量length=
   	  Math.min(freeSpaceInWriteBuffer, dataRemaining)
   	  目的地为缓冲数组writeBuffer，偏移量为
   	  val= Platform.BYTE_ARRAY_OFFSET + (diskWriteBufferSize - freeSpaceInWriteBuffer)
   	+ 使用磁盘块写出器将写出缓冲数组中的数据#method @write,写出数据量为
   	val=(diskWriteBufferSize - freeSpaceInWriteBuffer)+
   	Math.min(freeSpaceInWriteBuffer, dataRemaining)
   	+ 更新读取记录指针@recordReadPosition+=Math.min(freeSpaceInWriteBuffer, dataRemaining)
   	+ 更新数据剩余量@dataRemaining-=Math.min(freeSpaceInWriteBuffer, dataRemaining)
   	+ 写出完毕，重置空闲buffer空间@freeSpaceInWriteBuffer=@diskWriteBufferSize
   	+ 当@dataRemaining<0时，如果最终剩余空间@freeSpaceInWriteBuffer<diskWriteBufferSize
   		则需要将剩余的碎片文件写出。
   	+ 使用#method @recordWritten() 通知写出器输出流已经写出完毕	
   }
   ```
   
   
   
   