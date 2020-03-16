##### org.apache.spark.graphx.impl.EdgeActiveness

```java
public enum EdgeActiveness {
    介绍: 边激活状态
    // 两个端点都不激活
    Neither,
    // 激活起始点
    SrcOnly,
    // 激活目标点
    DstOnly,
    // 两个端点都激活
    Both,
    // 两个端点都不激活
    Either
}
```

##### org.apache.spark.graphx.TripletFields

```java
public class TripletFields implements Serializable {
    介绍: 三元组属性,代表@EdgeTriplet 或者@EdgeContext	的属性子集,允许系统填入属性
    属性:
    #name @useSrc #type @final boolean	是否包含起始顶点
    #name @useDst #type @final boolean	是否包含目标顶点
    #name @useEdge #type @final boolean	是否包含边
    #name @None = new TripletFields(false, false, false) #type @TripletFields 空元素
    #name @EdgeOnly = new TripletFields(false, false, true) #type @TripletFields 边元素
    #name @Src = new TripletFields(true, false, true) #type @TripletFields	起始点
    #name @Dst = new TripletFields(false, true, true) #type @TripletFields	目标点
    #name @All = new TripletFields(true, true, true)	#type @TripletFields 边+双顶点
    构造器:
    public TripletFields() {
        this(true, true, true);
    }
    
    public TripletFields(boolean useSrc, boolean useDst, boolean useEdge) {
        this.useSrc = useSrc;
        this.useDst = useDst;
        this.useEdge = useEdge;
    }
}
```

