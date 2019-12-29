

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

      

2. **拓展**

   ```markdown
   1. 集合类 Optional
   ```

   