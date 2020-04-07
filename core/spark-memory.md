## **spark-memory**

---

1. **内存模式**  #enum @MemoryMode

   ```markdown
   堆模式	ON_HEAP
   非堆模式 OFF_HEAP
   ```

2. **异常**

   + #class @SparkOutOfMemoryError

     ```markdown
     作用: 任务不能够从内存管理器获取内存时，抛出该异常。为了不使用@java.lang.OutOfMemoryError,从而终止执行，需要抛出此异常，仅仅终止当前任务。(该类是@java.lang.OutOfMemoryError子类)
     ```

   + #class @TooLargePageException

     ```markdown
     属于运行时异常:
     出现情况，内存分页的页容量过大。
     ```

3. **内存消费者** #class @MemoryConsumer

   ```java
   ADT MemoryConsumer{
   	数据元素:
   		1. 内存管理器#name @taskMemoryManager #type @TaskMemoryManager
   		2. 页长#name @pageSize #type @long
   		3. 内存模式#name @mode #type @MemoryMode
   		4. 内存消费量#name @used #type @long
   	操作集:
   		1. 初始化类(构造器)
   		+ MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode)
   		外部指定内存管理器@taskMemoryManager,页长@pageSize,内存模式@mode
   		+ MemoryConsumer(TaskMemoryManager taskMemoryManager)
   		外部指定内存管理器,页长为任务管理器的长度
   		2. 查找获取类
   		+ MemoryMode getMode()
   			返回内存模式
   		+ long getUsed()
   			返回内存使用量
   		3. 内存空间开辟类
   		LongArray allocateArray(long size)
   		功能: 开辟指定大小的长数组#class @LongArray
   		+ 内存管理器@taskManager为当前内存消费者@this开启一个大小为size*8(字节数)的内存块空间。
   		+ 如果新开的页面，大小不足以容纳所需的数据就会抛出OOM，通过#method @throwOom(MemoryBlock,long)
   		+ 开通完毕，更新当前内存使用量@used
   		+ 返回对应新建内存块的长数组，通过#constructor @LongArray(MemoryBlock)
   		
           MemoryBlock allocatePage(long required)
   		功能: 
   		+ 使用内存管理器@taskManager 开启一个大小最少为指定值的内存块，如果指定值小于页长@pageSize,		    则会默认区开启一个与页长等大的内存块。
   		+ 同理，如果内存块的容纳量不足指定的所需值@required，会抛出OOM@throwOom(MemoryBlock,long)
   		+ 开辟完成之后,消费者@this,使用内存量@used更新。
               
   		long acquireMemory(long size) 
   		功能: 获取指定大小@size所分配的内存量      
   		+ 使用内存管理器@taskMemoryManager获取当前内存消费者@this的执行内存,获取内存大小为size
   		若是内存不足，则会使用消费者的溢写@spill()去释放内存。返回成功获取内存的字节数。
               
   		4. 内存释放类
   		void freeArray(LongArray array)
   		功能: 释放长数组@LongArray
   		底层是对长数组@LongArray内部数据元素@MemoryBlock的释放
   		--> freePage(array.memoryBlock())
               
   		void freePage(MemoryBlock page)
   		功能: 页/内存块的内存释放
   		+ 使用内存管理器@taskMemoryManager对当前消费者@this，指定页(内存块)空间进行释放
   		+ 由于释放了空间，所以消费者@this所消费的内存数量需要更新(减小)
               
   		void freeMemory(long size)
   		功能: 释放size字节的内存
   		+ 使用内存管理器@taskMemoryManager对当前消费者@this,释放指定字节数的内存
   		+ 由于释放了空间，所以消费者@this所消费的内存数量需要更新(减小)=size
               
   		5. 内存不足的处理方案，溢写
   		abstract long spill(long size, MemoryConsumer trigger)
   		+ 这个需要子类的实现,实现的思路: 在内存管理器@taskMemoryManager发现当前任务所需的内存不足时，将			数据溢写到磁盘上
   		注意:
   			1. 为了避免死锁，在溢写逻辑中不因当调用获取内存的方法
   			2. 如今，这个只能释放Tungsten-managed管理的页面
                   
   		void spill()
   		底层调用:spill(Long.MAX_VALUE, this) 对内存消费者@this进行消费
   		6. OOM处理
   		void throwOom(final MemoryBlock page, final long required)
   		功能: 如果页(内存块)发生了内存不足的情况，则会释放页现存的内存，并抛异常@SparkOutOfMemoryError
   }
   ```

4. **拓展**

   ```markdown
   1. 内存模式(堆内存模式)
   2. OOM(内存溢出)
   3. 堆的重新调整
   4. 建堆
   5. GC
   6. 内存模式
   	Tungsten memory
   	sun.misc.Unsafe
   ```

5. **任务内存管理器** #class @TaskMemoryManager

   + 基本介绍

```markdown
功能: 管理分配给单个任务的内存
	本类中多数的复杂度是由于处理off-heap模式下转64位编码导致的。在off-heap模式下，内存可以直接使用64位长数据定位。在on-heap模式下，内存定位是使用基本对象引用@base 和64位对象内偏移量定位。现在，一个在其他结构体中存储指向某个数据结构的指针是一个问题。比如说，hashmap/排序缓冲中存储着记录指针#name @record。尽管决定了可以使用128位去定位内存，但是不能仅仅存储基本对象#name @base。原因是由于GC的问题，不能保证堆内元素数据位置不发生变化，如果发生变化且没有规律，就无法定位到内存地址了。
	我们使用下面的方法去编码64位的记录指针:
        + 在不使用堆的模式下(off-heap): 仅仅存储未经加工的地址(raw address)
        + 在on-heap模式下，使用上13位存储页号(page number,决定存储最大页数)，下51位存储页内偏移量(决定单页大		小)这个页号用于定位内存管理器@MemoryManager 中的页表数组中的位置，以便去检索基本对象@base
	允许使用的定位8192页，在on-heap模式下，页最大值受限于long[] array 的最大长度，允许定位8192*(2^31-1)*8个	字节，大于140MM内存大小。
```

+  **抽象数据类型**

  ```java
  ADT TaskMemoryManager{
  	数据元素:
  		1. 日志管理器 #name @logger #type $Logger
           2. 页数编码长度#name @PAGE_NUMBER_BITS=13
           3. 偏移量编码长度#name @OFFSET_BITS=64-@PAGE_NUMBER_BITS
           4. 页表最大条目长度#name @PAGE_TABLE_SIZE=2^(@PAGE_NUMBER_BITS)
           5. 最大页长字节数#name @MAXIMUM_PAGE_SIZE_BYTES=(2^31-1)*8 --> 17kM
           6. 低51位截取器(mask)#name @MASK_LONG_LOWER_51_BITS=0x7FFFFFFFFFFFF(&操作取后51位)
           7. 页表#name @pageTable #type $MemoryBlock[] (内存块/页表) 长度=@PAGE_TABLE_SIZE
           8. 分配页列表#name  @allocatedPages #type #BitSet bit的线性表 长度为@PAGE_TABLE_SIZE
           9. 内存管理器#name @memoryManager #type $MemoryManager 
           10. 任务尝试编号#name @taskAttemptId #type $long
           11. 内存模式: #name @tungstenMemoryMode #type $MemoryMode
           	不使用堆就不用使用截取器了。
           12. 内存消费者列表 #name @consumers #type $HashSet<MemoryConsumer>
           	注意:@GuardedBy("this") 获取本类对象锁,才可以操作 详情参照@GuardedBy
           13. 获取但未使用内存 #name @acquiredButNotUsed #type volatile long
       操作集:
       	1. 初始化类(构造器)
       	public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId)
       	设置任务内存管理器属性:
       		内存模式@tungstenMemoryMode为指定的内存管理器@memoryManager的内存模式
       		内存管理器为指定的内存管理器@memoryManager
       		设置任务的任务尝试id为指定@taskAttemptId
       		新建一个空的内存消费者列表@consumers
       	2. 查询获取类
       		long pageSizeBytes()
       		功能: 使用内存管理器@memoryManager获取页大小(单位:字节)，并返回
       		
       		Object getPage(long pagePlusOffsetAddress)
       		功能: 根据指定的地址,获取页面的基本对象
       		+ 非堆模式下由于不存在对象的存储，所以返回null
       		+ 堆模式下：
       			1. 对输入地址进行解码，获得页编号
       			2. 如果页面合法，则从页表中获取页/内存块的内容，如果非空则返回
       		
       		long getOffsetInPage(long pagePlusOffsetAddress)
       		功能: 获取页内偏移量
       		根据输入地址，解码获得页内偏移量
       		+ 堆模式下:	直接返回这个页内偏移量
       		+ 非堆模式下: 由于非堆模式下，偏移量是一个绝对地址，所有需要转换地址
       			addr=基本对象偏移地址+页内偏移量
       		
       		long getMemoryConsumptionForThisTask()
              功能: 返回本任务的内存消耗量
              + 使用内存管理器@memoryManager 获取id为当前任务尝试编号@taskAttemptId的内存消耗量
             	
             	MemoryMode getTungstenMemoryMode()
             	功能: 获取内存模式
           	ON_HEAP / OFF_HEAP
       		
       	3. 编码解码类
       		static long encodePageNumberAndOffset(int pageNumber, long offsetInPage)
       		功能: 堆模式下，返回页偏移信息(页编号,页内偏移量)
       		val= (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS)
       	
       		long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage)
       		功能：返回页偏移信息
       		+ 堆模式下:	调用上面的方法
       		+ 非堆模式下： 给出的偏移量@offsetInPage 是绝对地址，所以先将地址转化为相对地址，在使用堆模			 式状态下的结论。
       			转换关系: absolute_addr=base_off+relative_off
       		
       		int decodePageNumber(long pagePlusOffsetAddress)
       		功能: 解码页码
       		val=pagePlusOffsetAddress >>> @OFFSET_BITS
       		
       		long decodeOffset(long pagePlusOffsetAddress)
       		功能: 解码偏移量
       		val=pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS
   			
          4. 内存操作类 原理参照#reposity @Base_408 #root @数据结构 #name @动态内存分配
          	long acquireExecutionMemory(long required, MemoryConsumer consumer)
          	功能: 为内存消费者@consumer 获取需要@required字节的内存
          	执行条件: 需求内存量非负且消费者存在
          	+ 由于之后做就地优化时，由于会忘记之后变动的重做工作，所有这里是需要加锁保证安全的
          	执行步骤:
          		1. 从内存管理器@memoryManager中获取指定任务@taskAttemptId在消费者的内存模式下需要的内存量。
          		2. 尝试去释放其他内存消费者的内存，这样可以避免频繁的文件溢写，以至产生大量溢写文件。
          		操作:
          			+ 对其他消费者调用#method @spill()去释放它们的内存
          			+ 由于对同一个内存消费者进行多次溢写会产生很多小的溢写文件，所以真正溢写前需要对消费者的内存占用量进行降序排列，当然这里考虑使用treemap，使用搜索类型二叉树，对其进行排序。
          			+ 当然不能找到一条就直接溢写，需要先形成一个溢写文件列表
          			+ 从上述列表中，获取超过剩余所需容量的最小值=@required-@got
          			+ 在这之中获取消耗内存最大的消费者信息(列表的最后一个)
          			+ 由于占有相同内存量的消费者可能不止一个，所以获得的是消费最大内存量的一个列表，从中获取这个列表。获取一个消费者信息，对其进行溢写。溢写量@size=required-get，并获取其溢写量@released。
          			+ 由于溢写出去required-get数量的数据，这样可以接着向内存管理器@memoryManager申请required-get数量的内存空间。
          			+ 如果此时获得的内存量以及达到需求@required，就不用进行溢写，直接跳出
          			+ 否则将溢写的内存消费者移除列表，继续寻找下一个最大内容占有量的内存消费者，并重复。
          			+ 考虑一种极限的情况: 其他所有内存消费者全部参与了溢写，但是能够获得的内存量还是不够，则会对自己也进行溢写。溢写量的计算同上，异常同上
          			+ 申请完毕，指定消费者加入内存消费者列表@consumers
          	异常:
          		中途捕捉到IO异常，则会抛出@SparkOutOfMemoryError
          		中途捕捉到中断关闭异常@ClosedByInterruptException 抛出运行时异常@RuntimeException
                      
  			void releaseExecutionMemory(long size, MemoryConsumer consumer)
				功能: 释放内存
  		使用内存管理器@memoryManager 释放掉指定任务@taskAttemptId 在指定用户内存模式下指定长度				@size的内存大小
                  
  			void showMemoryUsage()
  			功能: 显示所有内存消费者的内存使用情况
  			执行条件: 在获取锁的情况下执行以下步骤
  				获取内存消费者使用内存总量=sigma(单个内存消费者使用内存量)
  				申请但未使用的内存总量=任务申请内存总量-内存消费者消费的内存总量
  			相关信息会以日志信息输出。
  			
  			MemoryBlock allocatePage(long size, MemoryConsumer consumer)
  			功能: 给指定的内存消费者@consumer分配页/内存块，块大小为@size
  			执行条件: 消费者存在，且内存消费者@consumer内存模式匹配与任务内存分配器@tungstenMemoryMode
  			
  			+ 分配一个内存块，这个内存块可以定位内存管理器的页表
  			+ 当内存以及不足以分配页/内存块的时候返回null，有可能会出现页内容比请求的字节数要少的情况出现，所以调用者需要证实返回页面的大小。
  			+ 如果指定的大小@size大于页最大容量@MAXIMUM_PAGE_SIZE_BYTES，
  				抛出异常@TooLargePageException
  			+如果现存内存足以分配一个新的页/内存块，那么需要先计算页的页号@pageNumber。如果计算的页号不在合法的页编号范围内，先释放掉刚获取的内存，再抛出异常@IllegalStateException
  			+ 内存管理器给当前需要申请的内存量，分配一块连续的内存区域
  				1. 出现了内存溢出
  					出现内存溢出的原因是由于释放的内存不足以容纳需要分配的内存。需要将分配值记为获取但没有分配的内存,清除之前导致内存溢出的页。进而调用溢写去获得更多的空闲块。
  				2，没有出现内存溢出
  					+ 需要分配的内存块@page 的页号设置为为@pageNumber
  					+ 将新开辟出来的内存块/页的信息方法页表中
  			
  			void freePage(MemoryBlock page, MemoryConsumer consumer)
  			功能: 释放指定内存消费者@consumer的占用内存块@page
  			操作条件: 给定内存块的页号合法且页号在已分配的分配页列表@allocatedPages中。
  			+ 删除页表中对应页面的引用(设置为null)
  			+ 分配页列表中去除该页索引(bitset线程不安全，需要加锁)
  			+ 在将页编号传递给内存分配器@MemoryAllocator去释放相应的页之前，需要允许内存管理器去探测是否内存管理的页能够直接使用TMM.freePage()释放内存。直接设置即可:
  				page.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER
  			+ 通过内存管理器中的内存分配器@MemoryAllocator 去释放该页的内容。
  			+ 在通过内存管理器@memoryManager 为指定内存消费者@consumer去释放数量为页长的空间
  			
  			long cleanUpAllAllocatedMemory() 
  			功能: 清除所有分配的空间，返回释放内存的字节数。一个非0的返回值可以用来探测内存泄漏。
  			+ 清空所有内存消费者列表@consumers 
  				清空的过程中如果返现某个消费者@consumer存在没有释放的内存，则打出日志，用于探测内存泄漏
               + 释放页表中所有引用所指向页的内存，并将页表@pageTable中的所有引用置空。
               + 释放完所有页/内存块的内存中，还有一些已经获得但是没有使用的内存需要释放。直接把之前统计的已经获取但没有使用的内存释放即可@acquiredButNotUsed
               + 使用内存管理器释放所有任务使用的内存，并返回释放的字节数
  }	
  ```
  
  