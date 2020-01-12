## **spark-mapred**

---

1.  [SparkHadoopMapRedUtil.scala](# SparkHadoopMapRedUtil)
2.  [scala基础拓展](# scala基础拓展)

---

#### SparkHadoopMapRedUtil

```markdown
object SparkHadoopMapRedUtil{
	关系: father -> Logging
	操作集:
	def performCommit(): Unit
	功能: 当你决定去提交任务时,调用这个任务
		committer.commitTask(mrTaskContext)
		如果出现异常则放弃提交任务
		committer.abortTask(mrTaskContext)
	关于提交任务可以参照hadoop中:
		@org.apache.hadoop.mapreduce.commitTask(TaskAttemptContext tac)
		@committer.abortTask(TaskAttemptContext,tac)
	这里不作进一步分析

	def commitTask(committer: MapReduceOutputCommitter,mrTaskContext: MapReduceTaskAttemptContext,
      jobId: Int,splitId: Int): Unit
    功能: 提交任务
    介绍: 
    提交一个任务的输出.在提交任务输出之前,需要去明确是否有其他的任务去竞争(race),进而使得提交同样的输出分区.因此,与驱动器进行配合,去确定是否这个请求能够提交.参照SPARK-4879获取详细信息.
    输出提交的协调者仅当@spark.hadoop.outputCommitCoordination.enabled=true时才会被使用.
    
    输入参数:
    	committer: MR任务输出提交者 @MapReduceOutputCommitter
    	mrTaskContext: MR任务请求上下文 @MapReduceTaskAttemptContext
    	jobId: job ID
    	splitId: 分片Id
	操作逻辑:
    	1. 获取MR任务请求提交的任务id@mrTaskAttemptID=@mrTaskContext.getTaskAttemptID
    	在任务需要提交的情况下: committer.needsTaskCommit(mrTaskContext)=true
    	2. 检查任务输出是否被其他请求所提交
    		val(Boolean) = 
    		sparkConf.getBoolean("spark.hadoop.outputCommitCoordination.enabled",defaultValue = true)
		如果没有被其他任务提交:
			直接调用@performCommit()提交任务
		如果有其他任务提交了相同的任务,则需要检查当前任务是否能提交
			val cancommit
			判断能否提交的思路:
			使用#class @OutputCommitCoordinationMessage #method @canCommit去确认
				作用: 通过任务调用,用于询问是否能够将输出提交到HDFS上
				+ 如果一个任务已经获准去提交任务输出,那么其他相同输出的任务,就会被拒绝去提交
				+ 如果任务请求失败(比如: 执行器丢失),那么它随后的任务将会被允许提交
			如果能够提交
				直接调用performCommit()提交即可
			否则放弃提交,并抛出异常@CommitDeniedException
}
```