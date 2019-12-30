**spark 信息相关**

---

1.  #enum @JobExecutionStatus

   ```markdown
   工作执行状态
   1. RUNNING 运行状态
   2. SUCCEEDED 执行成功
   3. FAILED 执行失败
   4. UNKNOWN 未知状态
   ```

2. #interface @SparkExecutorInfo

   ```markdown
   spark 执行器信息（这个接口不是设计给外部实现的）
   1. 主机号 @host() $type @String
   2. 端口号 @port() $type @int
   3. 缓存大小 @cacheSize() $type @long
   4. 运行任务数量 @numRunningTasks() $type @int
   5. 堆上内存使用量@usedOnHeapStorageMemory() $type @long
   6. 非堆模式下内存使用量@usedOffHeapStorageMemory() $type @long
   7. 堆模式下内存总量@totalOnHeapStorageMemory() $type @long
   8. 非堆模式下内存总量@totalOffHeapStorageMemory() $type @long
   ```

3. #interface @SparkJobInfo

   ```markdown
   spark 工作信息(这个接口不是设计给外部实现的)
   1. 工作编号 @jobId() $type @int
   2. 阶段id列表 @stageIds() $type @int[]
   3. 工作执行状态 @status() $type @JobExecutionStatus
   ```

4. #class @SparkStageInfo

   ```markdown
   spark 阶段信息(这个接口不是设计给外部实现的)
   1. 阶段名称 @stageId() $type @int()
   2. 当前尝试id @currentAttemptId() $type @int
   3. 提交时间 @submissionTime $type @long
   4. 阶段名称 @name() $type @string
   5. 任务数量 @numTasks() $type @int
   6. 激活中的任务数量 @numActiveTasks() $type @int
   7. 完成的任务数量 @numCompletedTasks() $type @int
   8. 失败任务数量 @numFailedTasks() $type @int
   ```

5. #class @SparkFirehoseListener

   ```markdown
   spark 流水监听器
   介绍:
   	这个类允许用户接受所有spark监听器的时间，用户需要重写onEvent方法。
   ```

   操作集:

   ```markdown
   1. void onEvent(SparkListenerEvent event) 用户自定义spark监听事件
   2. final void onStageCompleted(SparkListenerStageCompleted stageCompleted)
   	本类提供，监听spark阶段结束事件@SparkListenerStageCompleted
   3. final void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted)
   	本类提供，监听阶段提交事件@SparkListenerStageSubmitted
   4. final void onTaskStart(SparkListenerTaskStart taskStart)
   	本类提供，监听任务开始事件@SparkListenerTaskStart
   5. final void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult)
   	本类提供，监听任务获取结果事件@SparkListenerTaskGettingResult
   6. final void onTaskEnd(SparkListenerTaskEnd taskEnd)
   	本类提供，监听任务结束事件@SparkListenerTaskEnd
   7. final void onJobStart(SparkListenerJobStart jobStart)
   	本类提供，监听任务开始事件@SparkListenerJobStart
   8. final void onJobEnd(SparkListenerJobEnd jobEnd)
   	本类提供，监听任务时间结束@SparkListenerJobEnd
   9. final void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate)
   	本类提供,监听环境更新@SparkListenerEnvironmentUpdate
   10. final void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded)
   	本类提供，监听块管理器的增加@SparkListenerBlockManagerAdded
   11. final void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved)
   	本类提供，监听块管理器的减少@SparkListenerBlockManagerRemoved
   12. final void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD)
   	本类提供，监听未持久化的RDD@SparkListenerUnpersistRDD
   13. final void onApplicationStart(SparkListenerApplicationStart applicationStart)
   	本类提供，监听应用程序开始@SparkListenerApplicationStart
   14. final void onApplicationEnd(SparkListenerApplicationEnd applicationEnd)
   	本类提供，监听应用程序结束@SparkListenerApplicationEnd
   15. final void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate)
   	本类提供，监听执行器度量的更新事件@SparkListenerExecutorMetricsUpdate
   16. final void onStageExecutorMetrics(SparkListenerStageExecutorMetrics executorMetrics)
   	本类提供，监听阶段执行器度量事件参照@SparkListenerStageExecutorMetrics
   17. final void onExecutorAdded(SparkListenerExecutorAdded executorAdded)
   	本类提供，监听执行器的新增事件@SparkListenerExecutorAdded
   18. final void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved)
   	本类提供，监听执行器移除的事件@SparkListenerExecutorRemoved
   19. final void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted)
   	本类提供，监听执行器黑名单列表@SparkListenerExecutorBlacklisted
   20. void onExecutorBlacklistedForStage(
         SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage)
   	本类提供，监听阶段内执行器的黑名单列表@SparkListenerExecutorBlacklistedForStage
   21. void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage)
   	本类提供，监听阶段内节点的黑名单列表@SparkListenerNodeBlacklistedForStage
   22. final void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted)
   	本类提供，监听执行器非黑名单列表的元素事件@SparkListenerExecutorUnblacklisted
   23. final void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted)
   	本类提供，监听节点黑名单列表@SparkListenerNodeBlacklisted
   24. final void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted)
   	本类提供，监听节点非黑名单列表@SparkListenerNodeUnblacklisted
   25. void onBlockUpdated(SparkListenerBlockUpdated blockUpdated)
   	本类提供，监听块更新事件@SparkListenerBlockUpdated
   26. void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask)
   	本类提供，监听推测任务提交事件@SparkListenerSpeculativeTaskSubmitted
   27. void onOtherEvent(SparkListenerEvent event)
   	本类提供，监听其他事件@SparkListenerEvent
   ```