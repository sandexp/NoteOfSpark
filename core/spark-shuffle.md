

## spark-shuffle**

---

1. **spark-shuffle io**

   ---

   1. #class @LocalDiskShuffleDataIO

      ```markdown
      本地磁盘数据洗牌IO
      ADT LocalDiskShuffleDataIO{
      	数据元素: 
      		1. spark应用程序配置集 #name @sparkConf #type $SparkConf [final]
      	操作集:
      		1. 构造器
      		LocalDiskShuffleDataIO(SparkConf sparkConf)
      		外部指定本类的spark应用程序配置集@sparkConf
      		2. 参数获取类
      		ShuffleExecutorComponents executor()
      		功能: 获取shuffle操作的执行器组件 #type $LocalDiskShuffleExecutorComponents
      		ShuffleDriverComponents driver()
      		功能：获取shuffle操作驱动器组件 #type $LocalDiskShuffleDriverComponents
      }
      ```

   2. #class @LocalDiskShuffleDriverComponents

      ```markdown
      本地磁盘洗牌操作驱动组件
      ADT LocalDiskShuffleDriverComponents{
      	数据元素:
      		1. 数据块主管理器@blockManagerMaster $type @BlockManagerMaster
      	操作集：
      		Map<String, String> initializeApplication()
      		功能: 初始化应用程序
      		从#class @SparkEnv 中获取数据块管理器中的主管理器，指定给本类@blockManagerMaster
      		2. void cleanupApplication()
      		应用程式完成之后的清除工作，这里什么也没有做
      		3. void removeShuffle(int shuffleId, boolean blocking)
      		功能： 移除给定shuffleid中所有的块
      		使用数据块主管理器@blockManagerMaster 移除给定shuffleId中所有的数据块
      }
      ```

   3. #class @LocalDiskShuffleExecutorComponents

      ```markdown
      本地磁盘洗牌操作执行组件
      ADT LocalDiskShuffleExecutorComponents{
      	数据元素：
      		1.应用程序配置集 @sparkConf 
      		2.块管理器 #name @blockManager $type @BlockManager
      		3.shuffle块映射器 #name @blockResolver $type @IndexShuffleBlockResolver
      	操作集:
      		1. 构造器
      		LocalDiskShuffleExecutorComponents(SparkConf sparkConf)
      		外部指定内部的应用程序配置集
      		LocalDiskShuffleExecutorComponents(SparkConf sparkConf,BlockManager blockManager,
            		IndexShuffleBlockResolver blockResolver)
      		指定应用程序配置集@sparkConf,块管理器@blockManager,shuffle块映射器@blockResolver
      		2. 操作类
      		void initializeExecutor(String appId, String execId, 
      			Map<String, String> extraConfigs)
      		功能: 初始化执行器
      		操作条件: sparkEnv中可以获得非空的块管理器
      		+ 从SparkEnv中获取块管理器@blockManager
      		+ 通过已经获得的块管理器@blockManager以及应用程序配置集@sparkConf初始化出本类的shuffle块构		 造器@blockResolver
      		
      		ShuffleMapOutputWriter createMapOutputWriter(int shuffleId,long mapTaskId,
            		int numPartitions)
            	功能：获得map形式shuffle输出器
            	返回：本地map形式shuffle输出器@LocalDiskShuffleMapOutputWriter
            	
            	Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter
            	功能: 创建单文件map形式的输出器@SingleSpillShuffleMapOutputWriter
            	操作条件: shuffle块映射器非空
            	返回一个本地磁盘单文件map形式溢写输出器@LocalDiskSingleSpillMapOutputWriter
      }
      ```

   4. #class @LocalDiskSingleSpillMapOutputWriter

      ```markdown
      本地磁盘map形式单溢写输出器
      ADT LocalDiskSingleSpillMapOutputWriter{
      	数据元素：
      		1. shuffle ID #name @shuffleId #type @int
      		2. map ID #name @mapId #type $long
      		3. shuffle块映射器 #name @blockResolver #type $IndexShuffleBlockResolver
      	操作集:
          	1. 构造器
          	LocalDiskSingleSpillMapOutputWriter(int shuffleId,long mapId,
            		IndexShuffleBlockResolver blockResolver)
      		指定shuffleID mapID，块映射器
      		2. 操作类
      		void transferMapSpillFile(File mapSpillFile,long[] partitionLengths)
      		功能: 转换map形式的溢写文件
      		+ 使用shuffle 块映射器，根据shuffleID，mapID获取输出文件
      		+ 根据获得到的文件新建一个临时的文件对象（这个对象路径=源文件对象+随机UUID）
      		+ 将给定需要溢写的文件@mapSpillFile重名名为临时文件
      		+ 使用块管理器@blockResolver 写入文件并且提交#class @IndexShuffleBlockResolver #method
      		@writeIndexFileAndCommit ,这个根据的是临时文件的名称。
      }
      ```

   5. #class @LocalDiskShuffleMapOutputWriter

      ```markdown
      ADT LocalDiskShuffleMapOutputWriter{
      	数据元素:
      		1. 日志管理器 #name @log #type $Logger [final]
      		2. shuffle ID #name @shuffleId #type $int [final]
      		3. map ID #name @mapID #type $long [final]
      		4. 块映射器 #name @blockResolver #type $IndexShuffleBlockResolver [final]
      		5. 分区长度列表 #name @partitionLengths #type $long[] [final]
      		6. 缓冲大小 #name @bufferSize #type $int [final]
      		7. 末尾分区编号#name @lastPartitionId #type $int [初始化为-1]
      		8. 当前通道位置#name @currChannelPosition #type $long 
      		9. 写入合并文件的字节数#name @bytesWrittenToMergedFile $type #long 
      		10. 输出文件对象#name @outputFile $type @File [final]
              11. 输出临时文件对象#name @outputTempFile $type 
              12. 输出文件流#name @outputTempFile $type @FileOutputStream
              13. 输出文件通道#name @outputFileChannel $type @java.nio.FileChannel
              14. 缓冲输出流#name @outputBufferedFileStream $type @BufferedOutputStream
              
          操作集:
          	1. 构造器
          	LocalDiskShuffleMapOutputWriter(int shuffleId,long mapId,int numPartitions,
            		IndexShuffleBlockResolver blockResolver,SparkConf sparkConf)
            	功能: 外部指定shuffle ID,map ID，分区数，块映射器@blockResolver，应用程序参数集@sparkConf
            		+ 初始化shuffleID，mapID，块映射器@blockResolver为指定
            		+ 初始化分区长度列表长度为指定@numPartitions
            		+ 根据块管理器,shuffleID，mapID指定输出文件对象@outputFile
            		+ 设置输出文件暂存对象为空
            		+ 根据参数集@sparkConf 中的参数设置缓冲区大小@bufferSize
            	2. 查询获取类
            	ShufflePartitionWriter getPartitionWriter(int reducePartitionId)
            	功能: 通过指定的reduce分区ID@reducePartitionId获取shuffle分区写出器
            	操作条件: 输入的reduce分区号合法
            	动作:
            		1. 创建当前输出文件对象@outputFile的临时输出文件对象@outputTempFile
            		2. 如果当前文件通道非空，则当前通道位置@currChannelPosition为输出文件通道
            		@outputFileChannel当前位置指针@position,否则当前通道位置@currChannelPosition指向0.
            		3. 返回当前reduce分区ID指定的本地磁盘shuffle分区写出器
            		#class @LocalDiskShufflePartitionWriter
            	3. 动作器
            	long[] commitAllPartitions()
            	功能: 提交所有分区
            	操作条件： 当前文件通道存在且位置指针@position 不等于写入合并文件的字节数
            	@bytesWrittenToMergedFile
            	+ 关闭相关的输出流 #method @cleanUp()
      		+ 暂时获取临时输出文件对象@outputTempFile
      		+ 使用块映射器@blockResolver根据当前shufflID，mapID，分区长度列表@partitionLengths以及临			时存储的暂存文件对象，写出到指定文件块中#class @IndexShuffleBlockResolver 
      		#method @writeIndexFileAndCommit
      		void abort(Throwable error)
      		功能: 放弃写出
      		+ 先关闭相关输出流@cleanUp()
      		void cleanUp() 
      		功能：关闭相关输出流
      		+ 关闭输出文件缓冲流@outputBufferedFileStream输出文件通道@outputFileChannel
      		关闭输出文件流@outputFileStream
      		void initStream()
      		功能: 初始化流
      		初始化输出文件流@outputFileStream，初始化输出文件缓冲流@outputBufferedFileStream
      		void initChannel()
      		功能: 初始化通道
      		初始化输出文件通道@outputFileChannel，其中模式为可追加模式
      }
      ```

      #subclass @LocalDiskShufflePartitionWriter

      ```markdown
      本地磁盘shuffle分区写出器
      ADT LocalDiskShufflePartitionWriter{
      	数据元素：
      		1. 分区编号 #name @partitionId $type @int [final]
      		2. 分区写出流 #name @partStream $type @PartitionWriterStream
      		3. 分区写出通道 #name @partChannel $type @PartitionWriterChannel
      	操作集:
      		1. 构造器
      		private LocalDiskShufflePartitionWriter(int partitionId)
      		指定本类的分区号@partitionId
      		2. 查找获取类
      		long getNumBytesWritten()
      		功能： 获取写出字节数
      		+ 优先从分区写出通道中获取数据统计量@partChannel #method @PartChannel.getCount()
               + 在分区写出通道不存在的情况下，从底层输出流获取统计数据@partStream 
               #method @PartStream.getCount()	
               3. 操作类
               OutputStream openStream()
               功能: 开启流@partStream写出功能
               操作条件: 通道@outputFileChannel一定要是处于关闭状态
               + 初始化流 initStream()
               + 根据分区号@partitionId返回分区写出器@partStream
               Optional<WritableByteChannelWrapper> openChannelWrapper()
        		功能: 开启通道交换写出方式
        		操作条件: 流读取方式必须关闭@partStream
        		+ 初始化通道
        		+ 根据分区号@partitionId获取通道分区写出器@partChannel，并返回       
      }
      ```

      #subclass @PartitionWriterStream

      ```markdown
      分区写出流
      ADT PartitionWriterStream{
      	数据元素：
      		1. 分区号 #name @partitionId $type @int [final]
      		2. 写出计数器 #name @count $type @int 
      		3. 流状态标记 #name @isClosed $type @boolean default=false;
      	操作集：
      		PartitionWriterStream(int partitionId)
      		指定分区编号
      		int getCount()
      		功能: 获取写出数量
      		void close()
      		功能: 关流
      		+ 设置流标志位=true
      		+ 更新已写字节数+=写出计数器的值
      		+ 更新当前分区下分区长度=写出计数器的写出量
      		void verifyNotClosed()
      		功能： 检验是否处于关流状态
      		处于关流状态会抛出异常
      		void write(int b)
      		功能: 写出单字节
      		+ 检查是否处于关流状态@verifyNotClosed 没有关闭则写出一个字节，并更新写出计数器@count+=1
      		void write(byte[] buf, int pos, int length)
      		功能：写出多个字节
      		+ 检查是否处于关流状态，没有关流则@写出length个字节，并更新计数器@count+=length
      }
      ```

      #subclass @PartitionWriterChannel

      ```markdown
      ADT PartitionWriterChannel{
      	数据元素:
      		1. 分区编号 #name @partitionId #type $int [final]
      	操作集:	
      		PartitionWriterChannel(int partitionId)
      		指定分区号的构造器
      		long getCount()
      		功能： 获取通道已写字节数
      		val=现在通道指针位置@position-写入前通道指针位置@currChannelPosition
      		WritableByteChannel channel()
      		功能: 返回一个字节通道=输出文件通道@outputFileChannel
      		void close()
      		功能： 关闭通道
      		+ 设置当前分区长度=当前写出量@getCount()
      		+ 已写字节数+=当前分区内部已经写出的字节数
      }
      ```

2. **拓展**

   ```markdown
   1. 集合类 Optional
   ```




**ETC**

---

1.  #class @SpillInfo

   ```markdown
   简介:
   	由外部shuffle排序器写入的数据块元数据信息
   ```

   + **抽象数据模型**

   ```markdown
   ADT SpillInfo{ [final]
   	数据元素:
           1. 分区长度列表#name @partitionLengths $type @long[] [final]
           2. 文件对象#name @file $type @File [final]
           3. 临时shuffle块编号#name @blockId $type @TempShuffleBlockId [final]
   	操作集:
   		1. 构造器
   		SpillInfo(int numPartitions, File file, TempShuffleBlockId blockId)
   		功能: 指定分区数量，文件对象，以及临时shuffle块编号
   		+ 设置分区列表长度为指定@numPartitions
   		+ 设置文件对象为指定
   		+ 设置临时shuffle块编号为指定
   }
   ```

2.  #class @BypassMergeSortShuffleWriter

    ```markdown
    简介:
    	本类实现了排序基准的shuffle的hash格式shuffle备用路径.写路径将进来的数据写到分开的文件中.一个文件对应于一个reduce的partition.连接这些分区文件,进而组成一个输出文件.这个输出文件是共属于各个reduce的.记录没有缓冲在内存中.其写输出时通过了org.apache.spark.shuffle.IndexShuffleBlockResolver shuffle块管理器来处理.
    	如果存在有大量的reduce分区数量时,写路径是无效的.原因是写操作同时打开了各个分区的序列化器,以及文件流.因此对于写路径的选择是由要求的,这个要求以及管理工作交给#class @SortShuffleManager 处理.
    	当满足
    		1. map侧的combine操作没有被指定
    		2.分区数小于等于@spark.shuffle.sort.bypassMergeThreshold 即可
    	本类原来是属于org.apache.spark.util.collection.ExternalSorter类的一部分,但是由于重构就单独的形成了一个类.主要目的是降低外部排序类的代码复杂度. 同样的外部排序类也将这段代码移除.
    ```

    抽象数据模型

    ```markdown
    ADT BypassMergeSortShuffleWriter{
    	数据元素:
    		1. 日志管理器#name @logger $type @Logger  final
    		2. 文件缓冲大小#name @fileBufferSize $type @int	final
    		3. 是否可以转化标记#name @transferToEnabled $type @boolean	final
    		4. 分区数#name @numPartiotions $type @int	final
    		5. 块管理器#name @blockManager $type @BlockManager	final
    		6. 分区器#name @Partitioner $type @partitioner	final
    		7. 写出度量器#name @writeMetrics $type @ShuffleWriteMetricsReporter	final
    		8. shuffleId #name @shuffleId $type @int	final
    		9. mapId #name @mapId $type @long	final
    		10. 序列化器 #name @serializer $type @Serializer	final
    		11. shuffle执行组件 
    			#name @shuffleExecutorComponents $type @ShuffleExecutorComponents	final
    		12. 分区写出器列表 #name @partitionWriters $type @DiskBlockObjectWriter[]
    		13. 分区写出片段 #name @partitionWriterSegments $type @FileSegment[]
    		14. 分区长度列表 #name @partitionLengths $type @long[]
    		15. map侧状态 #name @mapStatus $type @MapStatus
    		16. 停止标志 #name @stopping $type @stopping
    			设置这个标志位是因为map侧任务运行成功是会调用stop()方法,而当遇到异常时运行失败,此时也可以调			用stop()方法,导致同样的标志状态.为了保证不会产生重复尝试删除两次文件的动作,需要这个标志的介入.
    	操作集:
    		1. 构造器
    		  BypassMergeSortShuffleWriter(BlockManager blockManager,
          		BypassMergeSortShuffleHandle<K, V> handle,long mapId,SparkConf conf,
          		ShuffleWriteMetricsReporter writeMetrics,
          		ShuffleExecutorComponents shuffleExecutorComponents)
          	功能: 外部指定块管理器@blockManager,mapId(mapId),写度量器@writeMetrics
          		shuffle执行组件@shuffleExecutorComponents.外部传入应用程序配置集@SparkConf conf,
          		shuffle处理器@BypassMergeSortShuffleHandle hande
          		+ 从shuffle处理器中获取依赖关系@ShuffleDependency dep
          		+ 从依赖关系@dep获取@shuffleId=dep.shuffleId()
          		+ 从依赖关系@dep获取@partitionId=dep.partitioner()
          		+ 从依赖关系@dep中获取序列化器@serializer=dep.serializer()
          	2. 查询获取类
          		long[] getPartitionLengths()
    			功能: 获取分区长度列表
    	    3. 操作类
    			void write(Iterator<Product2<K, V>> records)
                 功能： 写出记录的内容到分区
                 操作条件: 当前分区写出器列表为空
                 + 获取map形式的写出器@ShuffleMapOutputWriter 
                 + 如果没有记录，则使用map形式写出器@ShuffleMapOutputWriter 提交所有的
                 分区	#method @commitAllPartitions()并修改当前map侧的状态@mapStatus
                 + 当前记录集中含有数据，则首先会获取一个序列化对象@SerializerInstance,设置分区写出器列表
                 @partitionWriters 分区写出分段列表@partitionWriterSegments 的长度为分区数，对每
                 个partition进行如下操作，保证对每个写出器获得一个块写出器@DiskWriters
                 	1. 使用块管理器@blockManager中得的磁盘块管理器@diskBlockManager() 创建一个临时
                 	的shuffle块
                 	2. 每个写出器通过块管理器@blockManager获取一个可以将数据直接写入到磁盘的块写出器#mathod
                 	@getDiskWriter(blockId,file,serilizableInstance,buffsize,writeMetrics)
                 + 使用写度量器，度量上述操作的时间，累加到内部累加器中。这个时间就等于
                 + 对迭代器中的每条记录计算其所属分区，并将记录写出到磁盘对应的位置上。
                 + 将每个分区器所写出的内容刷新到磁盘上且将写出的内容提交(原子操作) @commitAndGet()
                 + 更新分区列表中当前写分区的分区长度
                 + 更新map侧状态@mapStatus
                 异常处理:
                 	捕获异常@mapOutputWriter 直接放弃输出
           		
           		long[] writePartitionedData(ShuffleMapOutputWriter mapOutputWriter)
           		功能: 写分区数据，且更新分区长度列表@partitionLengths 
           			连接每个分区的文件，从而形成一个整体的文件
           		返回: 每个分区的大小(单位自己)
           		+ 从分区写出分段@FileSegment[]中获取文件对象
           		+ 获取分区写出器@ShufflePartitionWriter
           		+ 确定是否存在有通道的存在
           			1. 存在有通道 #method @writePartitionedDataWithChannel
    				2. 不存在通道，使用流方式 #method @writePartitionedDataWithStream
    			写出完毕，分区写出器列表置空
    			
    			void writePartitionedDataWithChannel(File file,
          			WritableByteChannelWrapper outputChannel)
    			功能: 使用通道写出分区数据
    			+ 通过spark @Utils 使用NIO文件流拷贝将通过文件@file创建的输入流，放到输出通道中。通道初始指			针@position=0，放置元素为输入通道的大小。
    			+ 拷贝完毕，关闭输入流，关闭输出流
    			
    			void writePartitionedDataWithStream(File file, ShufflePartitionWriter writer)
    			功能： 流式写出分区文件
    			特点: 直接通过基本输入输出流实现文件的输入，以及文件的输出
    			+ 使用spark @Utils 的#method @copyStream 将输入流的内容拷贝的输出流中,此时NIO的读取方式就			会被弃用。
    			
    			Option<MapStatus> stop(boolean success)
    			功能: 停止操作,主要用于处理map侧状态@mapStatus
    			+ 在success=true的情况下，设置mapStatus=Option.apply(mapStatus)
    			+ success不为true
    				1. 返回已经写了但是还没有提交的文件，并对其做删除。如果删除不了，则会打出error级别的日					志。
    				其他情况下返回None$.empty()
    }
    ```

3.  #class @PackedRecordPointer

    ```markdown
    简介: 包装记录指针类
    包装了8个字节，共计64位，前24位分区编号，40位记录指针。
    在long的数据返回内，字节信息表示如下
    	24位分区编号	13位内存页编号 27位页内偏移量
    这就意味着最大的可分配容量为2^27=128M。考虑到页号总容量=128 * 2^13= 1 TB，这是一个任务最多可以分配的RAM空间。
    ```

    ```markdown
    ADT PackedRecordPointer{ [final]
    	数据元素:
    		1. 常量  单页最大容量@MAXIMUM_PAGE_SIZE_BYTES=1 << 27 (128M)
    		2. 常量  最大分区编号@MAXIMUM_PARTITION_ID=(1 << 24) - 1
    		3. 常量  分区号初始位置@PARTITION_ID_START_BYTE_INDEX = 5
    		4. 常量  分区号结束位置@PARTITION_ID_END_BYTE_INDEX
    		5. 常量  低40位截取器@MASK_LONG_LOWER_40_BITS=(1L << 40) - 1
    		6. 常量  高24位截取器@MASK_LONG_UPPER_24_BITS=~MASK_LONG_LOWER_40_BITS
    		7. 常量  低27位截取器@MASK_LONG_LOWER_27_BITS=(1L << 27) - 1
    		8. 常量  低51位截取器@MASK_LONG_LOWER_51_BITS=(1L << 51) - 1
    		9. 常量  高13位截取器@MASK_LONG_UPPER_13_BITS=~MASK_LONG_LOWER_51_BITS
    		10. 包装记录指针@packedRecordPointer private
    	操作集:
    		1. 设置类
    		public static long packPointer(long recordPointer, int partitionId)
            功能： 指定记录指针，分区id
            操作条件: 分区号合法
            返回: 前24位:分区号+后40位压缩后的位置地址
            void set(long packedRecordPointer)
            设置包装记录指针
            int getPartitionId()
            获取分区编号
            long getRecordPointer()
            获取记录指针
    }
    ```

4.  #class @ShuffleSortDataFormat

    ```markdown
    ADT ShuffleSortDataFormat{
    	数据元素:
    		缓冲数组#name @buffer $type @LongArray  [final]
    	操作集:
    		ShuffleSortDataFormat(LongArray buffer)
    		指定缓冲数组
    		PackedRecordPointer getKey(LongArray data, int pos)
    		由于重新使用了key，这个方法禁止调用throw new UnsupportedOperationException()
    		PackedRecordPointer newKey()
    		返回一个包装记录指针类@PackedRecordPointer
    		PackedRecordPointer getKey(LongArray data, int pos, PackedRecordPointer reuse)
    		重设reuse内部记录指针为长数组@LongArray位置为pos的数据(地址)
    		void swap(LongArray data, int pos0, int pos1)
    		交换长数组@LongArray中pos0，pos1位置的值
    		LongArray allocate(int length)
    		操作条件: 指定length< 缓冲数组长度
    		返回buffer数组
    		void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos)
    		将原来元素(src,srcPos) 复制到 (dst,dstPos)
    		void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length)
    		功能: 区域拷贝
    		内存拷贝(src,srcPos+len*8)->(dst,dstPos+len*8)
    }
    ```

5.  **shuffle排序器** 

    1.  外部排序器

        ```markdown
        ADT ShuffleInMemorySorter{
        	数据元素:
        		1. 常量 排序比较逻辑@SORT_COMPARATOR
        		2. 内存消费者#name @consumer $type @MemoryConsumer
        		3. 记录指针数组@array $type @LongArray
        			这个数组用于存放记录地址和分区编号排序操作是对数组的操作而非直接操作记录(改链接)
        			部分空间用于存放记录地址和分区编号，另一部分用于排序的暂态缓冲区
        		4. 基数排序标志@useRadixSort
        		   用了基数排序会比较快
        		5. 新纪录插入位置@pos=0
        		6. 记录容量 @usableCapacity int
        		7. 初始化大小@initialSize final int
        	操作集:
        		1. 构造器
        		ShuffleInMemorySorter(MemoryConsumer consumer, int initialSize, boolean useRadixSort)
        		指定内存消费者@consumer，初始化大小@initialsize，基数排序标志@useRadixSort。
        		根据上面的参数，获取记录指针数组@array，记录容量@usableCapacity
        		2. 查询获取类
        		int numRecords()
        		获取记录数量 返回@pos
        		
        		int getUsableCapacity()
        		获取记录指针数组可用部分大小
        		基数排序需要更多的空间去存储数据，所有可使用的空间相对来说就比较少
        		val=(int) (array.size() / (useRadixSort ? 2 : 1.5))
        		
        		long getMemoryUsage()
        		获取内存使用量，这里的内存使用主要是存储记录指针数组@array
        		val=array.size() * 8L
        		
        		boolean hasSpaceForAnotherRecord()
        		确定是否还能容纳一条记录
        		val=pos<usableCapacity
        		
        		ShuffleSorterIterator getSortedIterator()
        		获取排序好的排序迭代器@ShuffleSorterIterator
        			1. 基数排序
        				采取基数排序对记录进行排序#class @RadixSort #method @sort
        			2. 非基数排序
        				使用#class @TimSort对记录进行排序
        		
        		3. 操作类
        			void free()
        			功能: 用户释放记录指针数组的内存
        			void reset()
        			重置为刚建立的状态，pos=0
        			void insertRecord(long recordPointer, int partitionId)
        			功能: 在插入指针@pos处插入一条记录，器记录指针/分区号为指定
        			void expandPointerArray(LongArray newArray)
        			功能: 扩展记录指针数组大小为指定的新记录指针数组
        			操作条件: 新数组长度大于原数组大小
        			+ 将旧数组内容拷贝到新数组上，释放旧数组空间。重新设置当前访问数组指针(注意不要出现悬挂			访问)，更新可用容量大小@usableCapacity
        }
        ```

        #subclass @ShuffleSorterIterator

        ```markdown
        ADT ShuffleSorterIterator{
        	数据元素:
        		1. 记录指针数组@pointerArray final
        		2. 上限@limit final
        		3. 包装记录指针@packedRecordPointer final
        		4. 位置指针@position
        	操作集:
        		1. ShuffleSorterIterator(int numRecords, LongArray pointerArray,
                	int startingPosition)
                	初始化上限@limit=numRecords+startingPosition
                	初始化@position=startingPosition
                	初始化记录指针数组
                2. boolean hasNext()
        			检测是否有下一个元素
        	    3. void loadNext()
        	   		记录指针数组@packedRecordPointer加载当前位置@position的值	
        	   		移动位置指针
        }
        ```

        

    2.  内存排序器