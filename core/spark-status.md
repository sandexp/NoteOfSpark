## **spark-status API**

---

1. **应用状态相关@ApplicationStatus**

   ```markdown
   完成: COMPLETED
   运行: RUNNING
   ```

2. **阶段状态相关@StageStatus**

   ```markdown
     ACTIVE		激活
     COMPLETE		完成
     FAILED		失败
     PENDING		待定
     SKIPPED		跳过
   ```

3. **任务选择@TaskSorting**

   ```markdown
   ID: 编号
   INCREASING_RUNTIME("runtime")	增量式运行状态
   DECREASING_RUNTIME("-runtime")	减量式运行状态
   私有化参数列表
   	private final Set<String> alternateNames;
   基本构造器
   	TaskSorting(String... names)
   	将外部传参，送到列表管理器中
   返回输入参数对应的列表管理器所属对象(TaskSorting)
   	如果当前对象的列表管理器中包含该参数，则返回本对象，否则通过枚举工具类返回
   ```

**拓展**

```markdown
反射:

	枚举工具类: EnumUtil
枚举:
```

