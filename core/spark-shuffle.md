

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

   