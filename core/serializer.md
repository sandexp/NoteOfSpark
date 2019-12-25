## **Spark - core - serializer**

---

1. **基本介绍**

   ```markdown
    * Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    * Our shuffle write path doesn't actually use this serializer (since we end up calling the
    * `write() OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    * around this, we pass a dummy no-op serializer.
    解释: 需要一个序列化实例去构造出一个写磁盘块文件的对象@DiskBlockObjectWriter。事实上最终的洗牌写路径不是用的这个对象,因为到底层调用的还是输出流的@write()方法。但是这个对象@DiskBlockObjectWriter仍就是调用了一些上面的方法,为了进行与之相关的这些操作，需要传递一个假的no-op序列化器。
   ```

   

2. **注解**

   ```markdown
   1. 注解名称:
   	org.apache.spark.annotation.Private
   		被标注此注解的类，被认作是spark的私有组件。将来很可能被后期版本改变。
   		+ 它只能用在标准的java/scala中，且当这两种语言不足以包含类时的补充操作
   		+ 可以从它target传值,看出所覆盖度范围相当广
   		@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER,
   			ElementType.CONSTRUCTOR, ElementType.LOCAL_VARIABLE, ElementType.PACKAGE})
   ```

3. **构造**

   ```markdown
   1. 单例创建一个实例出来(饿汉式创建)
   	public SerializationStream serializeStream(final OutputStream s) （overwrite）
   		根据实际传入的输入流，重写:
   			序列化 writeObject(T t, ClassTag<T> ev1) 
   			关流 close()
   			刷新 flush()
   	这个实例只支持序列化为序列化流@SerializationStream操作
   		不支持:
   			序列化为字节缓冲@ByteBuffer
   			不支持任何的反序列化操作
   ```

   