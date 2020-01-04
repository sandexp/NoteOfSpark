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