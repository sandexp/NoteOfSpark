**Spark-IO**

---

1. 类名称	NioBufferedInputStream.java

   1. 基本介绍

      ```markdown
       * {@link InputStream} implementation which uses direct buffer
       * to read a file to avoid extra copy of data between Java and 
       * native memory which happens when using {@link java.io.BufferedInputStream}.
       * Unfortunately, this is not something already available in JDK,
       * {@code sun.nio.ch.ChannelInputStream} supports reading a file using nio,
       * but does not support buffering.
       解释:
       	1. 使用java IO包下的缓冲流读取输入时,输入流使用直接缓冲读取文件,从而避免了java程序从本地内存的额外拷贝.
       	2. 使用nio包下的通道输入流的时候,支持文件nio(异步读取),但是不支持缓存功能
       	3. 设计这个类,是为了使得spark基本够异步读取,又能够使用缓冲,提升读取性能
      ```

      ```tiki wiki
      名词解释:	
      	1. 直接缓存
      	2. 异步IO
      ```

      基础拓展:

      ```markdown
      1. final 关键字
      
      ```

      结构设计:

      ```markdown
      1. 默认缓存大小
      	常量: 8192B
      2. 字节型缓存器
      	ByteBuffer/java.nio.ByteBuffer
      	设置形式: 使用final修饰,不可以被子类更改
      	特点:
      		1. 只读属性,不可以被修改,多线程状态下安全
      		2. 无需使用同步,即可保证安全性
      3. 文件通道器
      	FileChannel/java.nio.channels.FileChannel
      	设计形式: 同ByteBuffer
      4. 基本构造器
      	给定文件对象,缓存大小为默认缓冲大小
      5. 自定义构造器
      	给定文件对象,以及缓冲大小
      	初始化设置:
      		新的buffer,初始pos=0,mark=-1,limit=capcity=设定的缓冲大小
      		
      ```

      ```markdown
      分配新的直接字节缓存器
      	新字节缓冲器参数:
      		初始位置(position参数) = 0
      		缓冲上限(limit) = 缓冲容量(使用自定义构造器为自己设置的容量上限,使用默认构造器容量为系统给定的8192B)
      		mark=未定义(-1)
      	注意:
      		基本关系: mark<=position<=limit<=capcity && position<=limit<=capcity
      		满足关系才会初始化成功,否则抛出异常@IllegalArgumentException
      ```

   1. Buffer/java.nio.Buffer设计:

      ```markdown
      1. 基础参数
      	pos 文本指针参数,表示下一个读写的位置 (<=pos)
      	limit: 指向第一个不需要被读写的元素位置(<=capcity)
      	capcity: 表示包含元素的数量,非负,且不会变化
      2. 标记和重置
      	标记参数: mark
      	mark是重置的使用pos应该重新执行的位置(联想到KMP匹配失败,回归点的位置)
      	mark的状态:
      		未定义状态: -1
      		定义状态: 非负数，且小于等于pos
      		当mark不满足基本关系时，mark会回归至未定义状态
      	异常: mark处于未定义状态，pos回调引发异常@InvalidMarkException
      3. 清除，反转，重读功能
      	清除: 即清除缓存空间,将基础参数设置到初始状态，mark=-1 
      		新buffer大小=capacity
      	反转: 回归初始状态，但是读取容量缩减到pos处
      		用于和java.nio.ByteBuffer#compact一起使用，用于移动pos个数据
      	重读: 所有参数初始化，limit不变
      4. 只读缓冲区:
      	当更改类型操作发生在只读缓存区时，抛出异常@ReadOnlyBufferException
      	性质: capcity不能改变，内容不能改变，但是可以更改，读取的范围（limit）
      	支持对pos，mark的修改(这两个时读取数据的必要条件)
      	实现方式: 设置参数@isReadOnly
      5. 线程安全处理方法
      	多线程需要使用适当的同步策略保证线程安全
      6. 支持链式调用
      7. 其他方法
      	+ 结果返回系列
      	a. hasArray()
      		当且仅当有返回数组且缓存区非只读返回true
      		当为true时,arrayOffset(),array()才能安全调用
      	b. array()
      		返回缓冲器元素
      		+ 子类根据具体情境提供了优质高效的处理方法
      		+ 修改缓冲区内容，会导致数组元素的变化，反之亦然(硬链接)
      		+ 使用hasArray()确保缓冲的可获得性
      		不使用hasArray可能的异常:
      		ReadOnlyBufferException: 读到了一个只读的缓冲区
      		UnsupportedOperationException: 没有返回数据的缓冲区
      	c. arrayOffset()
      		+ 返回返回数组中的首个元素在缓冲区的位置
              + 使用hasArray()确保缓冲的可获得性
      		不使用hasArray可能的异常:
      		ReadOnlyBufferException: 读到了一个只读的缓冲区
      		UnsupportedOperationException: 没有返回数据的缓冲区	
      	+ 监测系列
      	d. isDirect()
      		检查是否是直接流
      	e. nextGetIndex() nextGetIndex(int nb) nextPutIndex() nextPutIndex(int nb)
      		获取下一/nb个读取/写入的pos位置,
      	f. checkIndex(i) check(int i,int nb)
      		检查pos=i在不在0-> limit-nb的范围内
      	g. checkBounds(int off, int len, int size)
      		检查缓存区是否含有未读写的元素
      	+ 其他中的其他
      	h. truncate()
      		清除器: 将缓冲区消除掉（capacity=0）
      ```

   1. FileChannel/java.nio.channels.FileChannel 设计:

      基本介绍：
       ADT FileChannel{
       	数据对象: 当前读写位置指针 @position
       	数据关系: 
       	操作集: 
       }
       **性质:** 

      1. 文件通道是一个连接文件的字节寻找通道.在文件内部设置一个当前位置参数,使用它既可以访问也可以修改文件.文件本身包含一个变长的字节数组,它可以以被读写,以及可以查询当前的大小(获取顺序表的长度).

      + 当文件超出当前大小时,文件的大小参数会增加; 文件的大小在清空数组时或缩小.
      + 文件可以关联元数据,比如说文件的使用权限,文件的文件类型,文件的最后修改时间,本类不提供元数据获取方式
      2. 除了定义与字节通道相似的读写,关流操作,文件通道还提供了下面的操作集:
      + 支持绝对定位读取: 通过绝对定位,定位到文件的某个位置,从而不影响通道的当前位置指针@position
      + 支持文件区域映射: 文件区域直接映射到内存中.对于大文件来说使用这种内存映射的方式通常要比普通的读写效率要高.
      + 文件容灾措施: 更新能够使得文件存储在底层存储设备中,确保文件在系统崩溃的状态下不丢失.
      + 支持多种类型通道的转换: 提供通道类型转化的功能,将文件转化成其他类型通道模式
      + 可转化成文件系统缓存: 通过操作系统优化,通过一种快速的方式,转换成文件系统的缓存
      + 支持文件区域加锁: 加锁之后,可以阻止其他程序调用.
      3. 线程安全性
      + 文件通道在多线程状态下是安全的
      + 关流操作任意时刻都可以执行
      + 只有涉及到通道读写位置指针@position的操作,以及可以影响到文件大小的操作的时候,才能够在任意给定时间进行. 当一个操作在进行时,会阻塞直到上一个读写操作完毕.下一个操作才能够执行.
      + 其他诸如获取精确位置的操作,能够并发的执行
      4. 同一个文件在同一个程序下,创建了多个本类的实例,这些实例是一致的.但是由于操作系统缓存以及网络文件传输的延迟,会导致并发执行的程序所看到的结果不一致.且这一条无视所使用的语言以及机器的差异都成立.所以造成这样差异的确切原因时依赖于系统的,具有@系统相关性.

      **基本操作:**

      1. 文件通道的创建

         + 调用本类的@open
         + 使用@getChannel调用io包下的API获得
         + 无论时读写都会改变文件通道的位置,进而会改变初始对象中文件的位置,反之亦然.同理,改变通过文件通道改变文件的长度,也会改变初始对象中所看到的长度参量.

      2. 文件通道的读写操作

         + 无论何种情况下都要指明文件是否可读,可写等状态.但是有io包下输入/输出流获取的文件通道,本身就具有了可读/可写的性质,使用时注意这点.

           注: 在随机模式下@java.io.RandomAccessFile它的读写状态,有构建时给定的参数决定.

      3. 文件的追加模式

         + 处于可写状态下的文件,可以设置追加模式.在追加模式下,写操作之前,会将指针@position,移动到文件的末端,接着去写请求数据.

           注: 指针移动以及数据写出的操作原子性具有@系统相关性

      **操作集:**

      ```markdown
      1. 基本构造器
      	protected FileChannel(){}
      2. 创建文件的文件通道
      	FileChannel open(Path path,Set<? extends OpenOption> options,FileAttribute<?>... attrs)
      	path: 需要新建或是打开的文件
      		常用的打开方式选项:
      			java.nio.file.StandardOpenOption.READ: 
      				读取模式
      			java.nio.file.StandardOpenOption.WRITE: 
      				写模式
      			java.nio.file.StandardOpenOption.APPEND: 
      				追加模式
      			java.nio.file.StandardOpenOption.TRUNCATE_EXISTING: 
      				删除当前文件(在可写状态下可以执行): 顺序表的len=0
      			java.nio.file.StandardOpenOption.CREATE_NEW
      				文件可写状态下可执行,文件存在时创建失败.文件存在性检测以及创建文件的原子性与其他			 文件系统操作有关.
      			java.nio.file.StandardOpenOption.CREATE
      				可写状态下执行,文件存在则打开,不存在则创建原子性与其他文件系统操作有关.
      			java.nio.file.StandardOpenOption.DELETE_ON_CLOSE
      				结束删除文件(close关流/未关流,但是JVM终止运行)
      			java.nio.file.StandardOpenOption.SPARSE
      				给新创建的文件创建稀疏矩阵
      			java.nio.file.StandardOpenOption.SYNC
      				对于文件内容以及元数据的修改同步到底层存储设备
      			java.nio.file.StandardOpenOption.DSYNC
      				对于文件内容修改同步到底层存储设备
      		默认情况下,读写初始化与文件的开头,即@position=0
      	options: 文件打开方式的选择项
      	attrs: 文件属性列表(创建文件时,原子性的一次操作)
      	可能存在的异常:
      	@IllegalArgumentException:
      		非法参数输入
      	@UnsupportedOperationException
      		+ 文件源不支持创建通道
      		+ 参数列表中包含不能被原子化设置的内容
      	@SecurityException
      		+ 安装了安全管理器，导致文件源无法访问
      		+ 或者是当前用户权限不足，无法访问
      	// 文件无参数打开方式(attrs=NO_ATTRIBUTES)
      	FileChannel open(Path path,Set<? extends OpenOption> options)
      3. 文件的读取
      	int read(ByteBuffer dst)
      	从通道中读取数据，将数据送到指定缓冲器中，读取起始位置为通道当前指针位置@position
      	long read(ByteBuffer[] dsts, int offset, int length)
      	从通道中读取数据，起始位通道指针@position位置,读取到缓冲区的指定区域，
      	区域范围
          	[offset,offset+length] 
      		position+=length;
          long read(ByteBuffer[] dsts)
          从通道中读取数据，写满缓冲区
      4. 文件的写出
      	int write(ByteBuffer src)
      	在非追加模式下，起始于初始通道指针@position,将字节写出到指定缓冲器，并更新指针位置为实际以写出的字节数
      	long write(ByteBuffer[] srcs, int offset, int length)
      	在非追加模式下，起始于初始通道指针@position,将字节写出到指定缓冲器区域，并更新指针位置为实际以写出的字节数
      	缓冲器区域范围[*scrs+offset,*scrs+offset+length]
      	在非追加模式下，起始于初始通道指针@position,将字节写满指定缓冲器
      	long write(ByteBuffer[] srcs)
      5. 通道指针相关操作
      	long position() 
      		获取当前通道指针@position
      	FileChannel position(long newPosition)
      		设置通道指针@position
      		这里分为两种情况:
      			+ newPosition>size
      			 read 状态下超出文本返回EOF结束标志
      			 write状态下超出文本系统会自动写满后边的内容，但是内容随机不确定
      			+ newPosition<size
      			 正常调节指针操作
      	long size() 
      		返回文件的字节数，单位(Bytes)
      	FileChannel truncate(long size)
      		文件裁切: 
      			将文件从oldsize调整到size
      			分为两种情况:
      			+ oldsize>=size: 超出部分未定义，文件本身不修改
      			+ oldsize<size: 超出部分抛弃
      			如果通道指针在裁切范围之外，则将通道指针移动到新文件末尾
      6. 文件容灾措施
      	void force(boolean metaData)
      	分两种情况:
      	由于通道创建了，可以确保的是到该方法返回时，所有对文件做出的修改都可以得到担保。也就是，该方法最后一次调用，文件就会落到本地设备上，有效确保了关键信息在系统奔溃下不会丢失。非本地设备没有这个结论成立。
      	metadata=false: 只会落地文件内容
      	metadata=true： 落地文件内容以及元数据	
      7. 通道类型转换
      	long transferTo(long position, long count,WritableByteChannel target)
      	将通道文件转化到指定的可写入通道中,从指定的位置指针@position开始,将count个字节写出到目标通道中.调用此方法时,是否将所有的字节转换取决于通道的性质.
      	不能全部转换的常见情景:
      	1.少于请求字节数的时候,写出到通道的字节数会少于指定的字节数count
      	2.请求的字节数满足全转换的需求,但是目标通道是非阻塞的,且输出缓冲取余留的空间不足以容纳指定字节 
      注意: 
      	1. 调用这个方法不会改变通道指针@position的位置
      	2. 如果通道指针超过文本的范围,将不会读取任何内容
      	3. 目标通道若是含有通道指针@position,那么初始写入位置为源通道的通道指针@position,写完之后,通道指针移动写入的字节数量个.
      	4. 本方法要比单纯的循环读写的效率要高不少,因为许多操作系统可以直接将字节转换为操作系统缓存,从而不需要取拷贝.
      	long transferFrom(ReadableByteChannel src,long position, long count)
      	原理同上,只是目标通道需要的是可读权限,而不是可写权限
      可能引发的异常:
          @IllegalArgumentException  传入非法参数列表
          @NonReadableChannelException 
              1. 目标通道没有读权限,但是却需要从目标通道读取数据
              2. 源通道没有读权限,但是需要将源通道数据写到目标通道上
          @NonWritableChannelException
              1. 目标通道没有写权限,但是却需要从目标通道写数据
              2. 源通道没有写权限,但是需要将目标通道数据写到源通道上
          @ClosedChannelException
              存在有一方通道的关闭
          @AsynchronousCloseException
              其他线程在转换中途关闭通道
          @ClosedByInterruptException
              转换过程中,其他线程中途中断当前线程
          @IOException
              IO异常
      8. 扩展读写
      	int read(ByteBuffer dst, long position)
      	读取工作方式类似基础读写,但是不同的是,这里采用的是绝对定位读取,读取的起始点为给定位文件偏移指针@point而不是通道指针@position.因此操作不改变通道指针@position的位置.超出文件大小的部分,将读取不到内容.
      	int write(ByteBuffer src, long position)
      	同上,当文件偏移指针@point大于文件大小时,从原先的EOF处到实际末尾会出入不确定的字节.
      9. 内存映射
      	+ 内存映射模式:
      		READ_ONLY: 只读(必须具有读权限)
      		此模式下对结果缓冲进行修改,就会抛出异常@ReadOnlyBufferException
      		READ_WRITE: 读写(必须具有读写权限)
      		对缓冲进行修改的结果最终会反应到文件上,但是对于映射相同文件的程序,这种修改的可见性是不可	   预知的.
      		PRIVATE: 私有模式(必须具有读写权限)
      		对缓冲进行修改的结果不会反应到文件上,对于其他映射相同文件的程序是不可见的.但是,它会引起缓	  冲的修改部分的私有拷贝的创建.
      	+ MappedByteBuffer map(MapMode mode,long position, long size)
      	mode: 内存映射模式 position: 区域映射的开始偏移量@point size:区域大小(Bytes)
      	返回@MappedByteBuffer(@ByteBuffer子类) mark=-1,pos=0,limit=capcity=size
      	注: 这个缓冲映射会一直存在,直到被垃圾回收(@GC).一旦映射创建,就不会再依赖于文件通道,即使关闭了文件通道,这个映射依旧存在.
      	+ 内存映射文件与底层操作系统紧密相连,因此不完全包含通道文件的区域,调用这个方法的动作是难以确定的.且是否对文件内容或者大小做出了改变,通过这个方法,反映到缓冲的结果也是难以估计的.且缓冲内容改变比例也难以估测.
      	+ 内存映射文件的开销是相当大的(比起普通读写方法读写数十KB的文件),因此适合用于映射相当大的文件
      10. 文件读写锁
      	功能: 获取指定通道文件的指定区域:
      	注意: 
      	1. 调用这个方法引起阻塞,直到区域@region能够获取锁,通道关闭,或者是调用的线程中断这三种情况
      	2. 调用该方法时,通道被其他线程关闭,抛出异常@AsynchronousCloseException
      	3. 调用线程再等待锁的过程中被中断了,中断状态置位,抛出@FileLockInterruptionException异常
      		中断状态修改有,立即抛出异常,之后它的中断状态不会再次被修改.
          加锁区域:
         	+ 区域@region由位置指针@position和文件大小size构成,保证区域不重叠,且不存在包含关系
          + 如果加锁区域覆盖率文件的末尾,那么文件之外的内容就不会加锁了
          + lock()简单的对文件size()以内的内容加锁
          + 一些操作系统不支持共享锁,这种情况下共享锁会自动的转化为排他锁,这里可以使用@isShared()方法测试时共享锁还是排他锁.
          + 文件读写锁由jvm所持有,不适合在一台机器(JVM虚拟机)使用多线程操作一个文件(同一台机器可以考虑开启多个JVM进程)
          * FileLock lock(long position, long size, boolean shared)
              position: 锁起始位置
              size: 锁区域长度
              shared: 锁的性质
          * FileLock lock() 相当于lock(0L, Long.MAX_VALUE, false)
          	全文件加锁,锁类型:共享锁
      11. 锁的获取
      	功能: 获取文件通道指定区域的锁
      	FileLock tryLock(long position, long size, boolean shared)
      	+ 该方法是非阻塞性的
      	+ 调用时立即返回(成功获取锁,获取锁失败)
      		失败原因: 
      		1. 其他程序持有一个与之重叠区域的锁(返回NULL)
      		2. 其他原因:抛出异常
      	+ 与上面加锁的方法类似,区域需要指出@position @size参数且不能存在覆盖或者重叠关系,超出文本长度范围外的内容,不在获取锁结果返回内.
      	+ 由于操作系统的差异性,需要判断是否位共享锁,如果不是则要转化为排他锁
      	+文件读写锁由jvm所持有,不适合在一台机器(JVM虚拟机)使用多线程操作一个文件(同一台机器可以考虑开启多个JVM进程)
      	FileLock tryLock()=tryLock(0L, Long.MAX_VALUE, false)
      		获取全文锁,锁类型(共享锁)
      ```

      基础拓展:

      ```markdown
      1. 文件系统
      2. 系统相关性
      3. 注解
      4. 文件系统缓存
      	如果将字节转化为文件系统缓存
      5. 内存映射文件
      6. 共享锁
      7. 排他锁
      8. synchronized关键字
      ```
      
   1. 主体设计

      ```mark
      1. 初始化
      	指定文件对象,指定缓冲区大小
   		1. 创建一个大小为指定大小的直接缓冲区
      	2. 由文件创建一个只读的文件通道
      	3. 并将数据读取到缓冲中
      2. 重新填充功能
      	功能: 检查输入流中是否还有数据需要读取
      	1. 缓冲满的情况下,清空缓冲区重新读取数据，如果不能再获得数据，此时说明输入流中以及不存在有数据了，否则还是存在有数据，将它读到缓冲中。
      	2. 缓冲未满情况下,一定存在数据没有读取，返回true
      3. 数据读取(同步)
      	1. 不存在有剩余数据，返回-1
      	2. 获取并返回缓冲区数据 return byteBuffer.get() & 0xFF;
      		保证返回数据只有一个字节的长度
      4.  数据读取(同步,带有偏移量读取方式)
      	synchronized int read(byte[] b, int offset, int len)
      	1. 不存在剩余数据，返回-1
      	2. 使用缓冲读取pos=offset,长度为len的数据(len为指定长度和缓冲区剩余长度的最小值)
      	返回读取的数据长度len
      	注: 这里的起始点offset,结束点offset+len需要满足非负，且不超过b的长度范围
      5. 获取缓冲区状态(同步)[需要同步的原因,这个变量是操作类型指令能够改变的，所有需要加锁]
      	synchronized int available()
      	返回缓冲区存在的空闲区域(limit-position)
      6. 缓冲区内容的跳读
      	+ 缓冲区内部跳读:
      		synchronized long skip(long n)
      		输入: 需要跳读的字节数
      		输出: 实际跳读的字节数
      		分为两种情况:
      			跳读后的指针超出缓冲范围: position+n>buff.remain()
      			跳读数量=缓冲区剩余容量+在文件通道中的缓冲字节数
      			跳读后的指针任然在缓冲范围:
      			跳读数量=n,position+=n;
      	+ 文件通道内的跳读:
      		long skipFromFileChannel(long n)
      		这里根据:跳读长度的大小分为如下两种情况讨论:
      			1. pos+n>size
      				实际跳读的数量为size-pos，因为超出文件部分无法阅读(实际上是空的，没有意义)
      				指针指向文本末尾size处
      			2. pos+n<=size
      				返回n即跳读的数量，此时需要将通道指针pso移动到pos+n,以供下一步调用。
      7. 关流[操作类型指针]
      	synchronized void close()
      	1. 关闭对文件通道输入流
      	2. 将缓存内容存储到底层设备(TODO 有待证明)
      
      ```
      
      

2. 类名称    ReadAheadStream.java

   1. 基本介绍

      ```mark
      /**
       * {@link InputStream} implementation which asynchronously reads ahead from the underlying input
       * stream when specified amount of data has been read from the current buffer. It does it by
       * maintaining two buffers - active buffer and read ahead buffer. Active buffer contains data
       * which should be returned when a read() call is issued. The read ahead buffer is used to
       * asynchronously read from the underlying input stream and once the current active buffer is
       * exhausted, we flip the two buffers so that we can start reading from the read ahead buffer
       * without being blocked in disk I/O.
       */
       这个类异步地从底层输入流中获取指定数据量的数据到当前缓冲区中。
       使用的思路是提供两个缓冲区(活动缓冲区和预读缓冲区)
       1. 活动缓冲区中存储的是使用read方法所需要读取的数据
       2. 预读缓冲区中用于异步地读取底层输入流中的数据，一旦活动缓冲区的内容全部被消费完毕，交换两个缓冲区内容，这样就可以直接读取预读缓冲区的内容，且不用等待磁盘IO
       
      ```

   2. 基础拓展

      ```markdown
      1. 重入锁
      ```
   
3. 其他
   
      ```markdown
      1. 并发包注解 @GuardedBy
      
       "this" : 
       	this field is guarded by the class in which it is defined. 
       class-name.this : 
       	For inner classes, it may be necessary to disambiguate 'this';
       the class-name.this:
       	designation allows you to specify whether or not licate in inner class
       field-name : T
       	he lock object is referenced by the (instance or static) field specified by field-name.
       class-name.field-name : 
       	The lock object is reference by the static field
       method-name() : 
       	The lock object is returned by calling the named nil-ary method. 
       class-name.class : 
       	The Class object for the specified class should be used as the lock object.
       解释:
       被打上这个注解的属性或者是方法，只有在获得特定的锁的时候，才可以被使用。锁的类型为sychronize锁，或者是明确的Lock锁(java.util.concurrent.Lock).参数决定了加锁区域或者加锁方法的属性:
       比如说:
       	串"this": 属性有本类保卫，但是在内部类中需要使用ClassName.this来消除歧义
       	串: 属性名: 属性的锁对象由属性名称(属性可以是静态的也可以是非静态的)来指定
       	串: 类名.属性名: 锁对象通过静态属性指定[类名.属性名]
       	串: 类名: 获取锁对象名称为指定类名
       	串: 方法名称: 通过调用这个无参方法(nil-ary)获取的对象
       	
      ```
   
      

4. **整体设计**

   ```markdown
   1. 内部私有化一个重入锁，保证对外界修改免疫
   2. 数据元素:
   	活动缓冲区@activeBuffer 
   		需要获得重入锁才可获取
   	预读缓冲区@readAheadBuffer
   		需要获得重入锁才可获取
   	流末尾标记@endOfStream
   		需要获得重入锁才可获取
   	异步读取状态位@readInProgress
   		需要获得重入锁才可获取
   	弃读标记位@readAborted
   		需要获得重入锁才可获取
	读异常@readException
   		需要获得重入锁才可获取
   	流状态标记@isClosed
   		需要获得重入锁才可获取
   	底层输入流状态@isUnderlyingInputStreamBeingClosed
   		需要获得重入锁才可获取
   	预读任务标记位@isReading 
   		返回当前是否存在有预读任务，需要获得重入锁
    	等待标志位@isWaiting
    		返回当前是否有用户正在等待读取,返回类型为原子类型的boolean@AtomicBoolean
    	底层输入流:@underlyingInputStream [final]
    	执行服务:@ExecutorService [final]
    		由@org.apache.spark.util.ThreadUtils新建的单线程执行器，名称叫做read-ahead
    	异步读取完成信号:@asyncReadComplete [@java.util.concurrent.locks.Condition] (final)
    	单字节参量: @oneByte [static final] 类别: ThreadLocal<byte[]> @java.lang.ThreadLocal
   3. public ReadAheadInputStream(InputStream inputStream, int bufferSizeInBytes)
   	执行条件: 输入的缓冲大小为正数(反之,抛出异常)
   	根据指定的缓冲大小，创建等长的缓冲区(活动缓冲区,预读缓冲区)
   	并且指定内部底层输入流@underlyingInputStream为传入的输入流
   4. 常用操作集
   	boolean isEndOfStream()
   		功能: 判断输入流是否到达末尾
   		检测标准: 活动缓冲区为空，预读缓冲区为空，且输入流标志置位
   	void checkReadException()
   		功能: 检查读异常
   		弃读标记位置位时，抛出异常@readException
   	void readAsync() 
   		功能: 底层输入流--> 预读缓冲区[针对预读缓冲区的处理]
   		整个操作都在获得重入锁的状态下进行
   		+ 在没有到达输入流末尾或者当前不在异步读取数据时：
   			截取预读缓冲区从@pos=0,开始的部分，重新开启异步读取功能，如果中途出现弃读的情况@readAborted
   		,就会抛出异常@readException.正常读取完毕，则释放重入锁。
   		+ 执行器服务@executorService新开一个线程，执行异步读取的工作
   			* 获取重入锁，获取当前读取状态
   				包括：
   					预读任务标记位@isReading 为true表示可以向下读取
   					异步读取状态位@readInProgress :当底层输入流关闭时，就预示着不能够再异步读取了，此					时整个异步读取也就结束了。
   			* 注意：在释放锁的情况下，读入数据到预读缓冲区是安全的，注意是因为如下两点:
   				1. 活动缓冲区存在有可读的数据，不会从预读缓冲区读入数据
   				2. 在活动缓冲区数据读取完毕时，这是需要等待异步读取的完成（就是这个方法) 
   			所有在上述两种情况下，这个方法的读取，不会再多线程状态下处于不安全的状态
   				（情况1:  不从预读缓冲读数据，状态2： 等待预读缓冲区数据转换）
   ```
   
   
   
   5. **拓展**
   
      ```markdown
      1. class AtomicBoolean
      	这个类用于提供原子类操作的方案，用于原子性的更新标志位，不可以使用java.lang.Boolean代替
      2. condition锁
      3. lamda表达式
      4. juc的原子包
      5. java.lang.ThreadLocal本地线程简介
      6. java.util.concurrent.ExecutorService
      	this.excute(Runnale cmd)
      		新建一个线程执行cmd的内容
      7. java.lang.Runnale
      	性质: FunctionalInterface型注解
      	介绍: 
      ```
   
      