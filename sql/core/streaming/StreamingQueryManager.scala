/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming

import java.util.concurrent.{TimeUnit, TimeoutException}
import java.util.{ConcurrentModificationException, UUID}

import javax.annotation.concurrent.GuardedBy
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.execution.streaming.state.StateStoreCoordinatorRef
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.STREAMING_QUERY_LISTENERS
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.util.{Clock, SystemClock, Utils}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  管理sparksession中所有的流式查询@StreamingQuery
  参数列表：
    stateStoreCoordinator 状态存储协调器
    listenerBus  流式查询监听总线
    activeQueries 已激活的查询映射表
    activeQueriesSharedLock 激活查询的共享锁（sparksession用于追踪流式查询）
    awaitTerminationLock  等待查询终止的锁
    lastTerminatedQuery 上次结束的查询
  
 */
@Evolving
class StreamingQueryManager private[sql] (sparkSession: SparkSession) extends Logging {

  private[sql] val stateStoreCoordinator =
    StateStoreCoordinatorRef.forDriver(sparkSession.sparkContext.env)
  private val listenerBus = new StreamingQueryListenerBus(sparkSession.sparkContext.listenerBus)

  @GuardedBy("activeQueriesSharedLock")
  private val activeQueries = new mutable.HashMap[UUID, StreamingQuery]
  // A global lock to keep track of active streaming queries across Spark sessions
  private val activeQueriesSharedLock = sparkSession.sharedState.activeQueriesLock
  private val awaitTerminationLock = new Object

  @GuardedBy("awaitTerminationLock")
  private var lastTerminatedQuery: StreamingQuery = null

  try {
    sparkSession.sparkContext.conf.get(STREAMING_QUERY_LISTENERS).foreach { classNames =>
      Utils.loadExtensions(classOf[StreamingQueryListener], classNames,
        sparkSession.sparkContext.conf).foreach(listener => {
        addListener(listener)
        logInfo(s"Registered listener ${listener.getClass.getName}")
      })
    }
  } catch {
    case e: Exception =>
      throw new SparkException("Exception when registering StreamingQueryListener", e)
  }

  /**
    获取激活查询列表
   */
  def active: Array[StreamingQuery] = activeQueriesSharedLock.synchronized {
    activeQueries.values.toArray
  }

  /**
     获取指定id对应的流式查询
   */
  def get(id: UUID): StreamingQuery = activeQueriesSharedLock.synchronized {
    activeQueries.get(id).orNull
  }

  /**
    * 获取给定id的流式查询
   */
  def get(id: String): StreamingQuery = get(UUID.fromString(id))

  /**
    等待查询相关的SQLContext终止。如果查询异常终止，就会抛出异常。
    如果查询已经终止了，调用@awaitAnyTermination可能立即终止，也可能抛出异常。使用@resetTerminated()清除上次终止，
    且等待新的终止。
    在这种情况下，当多个查询由于重置终止@resetTermination 而终止。如果异常终止，那么这个方法会抛出异常。
   */
  @throws[StreamingQueryException]
  def awaitAnyTermination(): Unit = {
    awaitTerminationLock.synchronized {
      while (lastTerminatedQuery == null) {
        awaitTerminationLock.wait(10)
      }
      if (lastTerminatedQuery != null && lastTerminatedQuery.exception.nonEmpty) {
        throw lastTerminatedQuery.exception.get
      }
    }
  }

  /**
  等待相关SQLContext终止，返回查询是否被终止。如果查询终止，则调用@awaitAnyTermination 会立即返回true或者是抛出异常
  使用@resetTerminated 清除过去的异常，且等待终止。
  在这种情况下多个查询由于@resetTermination的调用而终止。
   */
  @throws[StreamingQueryException]
  def awaitAnyTermination(timeoutMs: Long): Boolean = {

    val startTime = System.nanoTime()
    def isTimedout = {
      System.nanoTime() - startTime >= TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    }

    awaitTerminationLock.synchronized {
      while (!isTimedout && lastTerminatedQuery == null) {
        awaitTerminationLock.wait(10)
      }
      if (lastTerminatedQuery != null && lastTerminatedQuery.exception.nonEmpty) {
        throw lastTerminatedQuery.exception.get
      }
      lastTerminatedQuery != null
    }
  }

  /**
   忘记过去的终止查询，这样@awaitAnyTermination 就可以等待新的终止了
   */
  def resetTerminated(): Unit = {
    awaitTerminationLock.synchronized {
      lastTerminatedQuery = null
    }
  }

  /**
    注册一个流式查询监听器，用于接收流式查询的生命周期事件
   */
  def addListener(listener: StreamingQueryListener): Unit = {
    listenerBus.addListener(listener)
  }

  /**
     解除@StreamingQueryListener 的注册
   */
  def removeListener(listener: StreamingQueryListener): Unit = {
    listenerBus.removeListener(listener)
  }

  /**
    列举有当前流式查询管理器相关的所有流式监听器
   */
  def listListeners(): Array[StreamingQueryListener] = {
    listenerBus.listeners.asScala.toArray
  }

  // 发送流式监听事件
  /** Post a listener event */
  private[sql] def postListenerEvent(event: StreamingQueryListener.Event): Unit = {
    listenerBus.post(event)
  }
  
  /**
    * 创建流式查询
    * @param userSpecifiedName  用户指定的流式查询名称,可以为空
    * @param userSpecifiedCheckpointLocation 用户指定的检查点位置,可以为空
    * @param df DF
    * @param extraOptions 额外的配置表
    * @param sink 流式查询的sink位置
    * @param outputMode 输出模式
    * @param useTempCheckpointLocation  是否使用临时的检查点位置
    * @param recoverFromCheckpointLocation  是否从检查点位置中恢复
    * @param trigger  触发器
    * @param triggerClock 触发锁
    * @return 流式查询包装器
    */
  private def createQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: DataFrame,
      extraOptions: Map[String, String],
      sink: Table,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean,
      recoverFromCheckpointLocation: Boolean,
      trigger: Trigger,
      triggerClock: Clock): StreamingQueryWrapper = {
    // 1. 确定是否停止时删除检查点
    var deleteCheckpointOnStop = false
    // 2. 获取检查点位置
    val checkpointLocation = userSpecifiedCheckpointLocation.map { userSpecified =>
      new Path(userSpecified).toString
    }.orElse {
      // 如果参数没有指定,则使用sparksession中的参数作为路径
      df.sparkSession.sessionState.conf.checkpointLocation.map { location =>
        new Path(location, userSpecifiedName.getOrElse(UUID.randomUUID().toString)).toString
      }
    }.getOrElse {
      // 在sparksession和指定位置都不存在的时候,需要使用临时检查点位置(停止时候会删除检查点)
      if (useTempCheckpointLocation) {
        deleteCheckpointOnStop = true
        val tempDir = Utils.createTempDir(namePrefix = s"temporary").getCanonicalPath
        logWarning("Temporary checkpoint location created which is deleted normally when" +
          s" the query didn't fail: $tempDir. If it's required to delete it under any" +
          s" circumstances, please set ${SQLConf.FORCE_DELETE_TEMP_CHECKPOINT_LOCATION.key} to" +
          s" true. Important to know deleting temp checkpoint folder is best effort.")
        tempDir
      } else {
        // 不设置任何检查点,抛出异常
        throw new AnalysisException(
          "checkpointLocation must be specified either " +
            """through option("checkpointLocation", ...) or """ +
            s"""SparkSession.conf.set("${SQLConf.CHECKPOINT_LOCATION.key}", ...)""")
      }
    }

    // 3. 如果指定检查点路径存在且不支持检查点恢复则抛出异常
    if (!recoverFromCheckpointLocation) {
      val checkpointPath = new Path(checkpointLocation, "offsets")
      val fs = checkpointPath.getFileSystem(df.sparkSession.sessionState.newHadoopConf())
      if (fs.exists(checkpointPath)) {
        throw new AnalysisException(
          s"This query does not support recovering from checkpoint location. " +
            s"Delete $checkpointPath to start over.")
      }
    }
    // 4. 获取解析计划
    val analyzedPlan = df.queryExecution.analyzed
    df.queryExecution.assertAnalyzed()

    val operationCheckEnabled = sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled

    if (sparkSession.sessionState.conf.adaptiveExecutionEnabled) {
      logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} " +
          "is not supported in streaming DataFrames/Datasets and will be disabled.")
    }
    // 5. 获取流式查询包装器
    (sink, trigger) match {
      case (table: SupportsWrite, trigger: ContinuousTrigger) =>
        if (operationCheckEnabled) {
          UnsupportedOperationChecker.checkForContinuous(analyzedPlan, outputMode)
        }
        new StreamingQueryWrapper(new ContinuousExecution(
          sparkSession,
          userSpecifiedName.orNull,
          checkpointLocation,
          analyzedPlan,
          table,
          trigger,
          triggerClock,
          outputMode,
          extraOptions,
          deleteCheckpointOnStop))
      case _ =>
        if (operationCheckEnabled) {
          UnsupportedOperationChecker.checkForStreaming(analyzedPlan, outputMode)
        }
        new StreamingQueryWrapper(new MicroBatchExecution(
          sparkSession,
          userSpecifiedName.orNull,
          checkpointLocation,
          analyzedPlan,
          sink,
          trigger,
          triggerClock,
          outputMode,
          extraOptions,
          deleteCheckpointOnStop))
    }
  }

  /**
   * 启动查询
   * @param userSpecifiedName 用户指定的查询名称,可以为空
   * @param userSpecifiedCheckpointLocation  用户指定的检查点位置
   * @param df 流式DF
   * @param sink  流式输出的sink位置
   * @param outputMode  sink的输出模式
   * @param useTempCheckpointLocation  是否使用临时的检查点位置
   * @param recoverFromCheckpointLocation  是否从检查点位置中恢复
   * @param trigger 查询触发器
   * @param triggerClock 触发时钟
   */
  @throws[TimeoutException]
  private[sql] def startQuery(
      userSpecifiedName: Option[String],
      userSpecifiedCheckpointLocation: Option[String],
      df: DataFrame,
      extraOptions: Map[String, String],
      sink: Table,
      outputMode: OutputMode,
      useTempCheckpointLocation: Boolean = false,
      recoverFromCheckpointLocation: Boolean = true,
      trigger: Trigger = Trigger.ProcessingTime(0),
      triggerClock: Clock = new SystemClock()): StreamingQuery = {
    // 1. 创建查询
    val query = createQuery(
      userSpecifiedName,
      userSpecifiedCheckpointLocation,
      df,
      extraOptions,
      sink,
      outputMode,
      useTempCheckpointLocation,
      recoverFromCheckpointLocation,
      trigger,
      triggerClock)
    // 2. 检查是否存在同名同id的流正在运行.返回一个激活的流,用于停止锁,以便避免死锁
    val activeRunOpt = activeQueriesSharedLock.synchronized {
      // Make sure no other query with same name is active
      userSpecifiedName.foreach { name =>
        if (activeQueries.values.exists(_.name == name)) {
          throw new IllegalArgumentException(s"Cannot start query with name $name as a query " +
            s"with that name is already active in this SparkSession")
        }
      }
      // 3. 保证没有其他同id查询被激活
      val activeOption = Option(sparkSession.sharedState.activeStreamingQueries.get(query.id))
        .orElse(activeQueries.get(query.id)) // shouldn't be needed but paranoia ...

      // 4. 确认表是否需要停止正在运行的流式查询
      val shouldStopActiveRun =
        sparkSession.sessionState.conf.getConf(SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART)
      // 5. 处理流式查询的重启
      if (activeOption.isDefined) {
        if (shouldStopActiveRun) {
          val oldQuery = activeOption.get
          logWarning(s"Stopping existing streaming query [id=${query.id}, " +
            s"runId=${oldQuery.runId}], as a new run is being started.")
          Some(oldQuery)
        } else {
          throw new IllegalStateException(
            s"Cannot start query with id ${query.id} as another query with same id is " +
              s"already active. Perhaps you are attempting to restart a query from checkpoint " +
              s"that is already active. You may stop the old query by setting the SQL " +
              "configuration: " +
              s"""spark.conf.set("${SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART.key}", true) """ +
              "and retry.")
        }
      } else {
        // nothing to stop so, no-op
        None
      }
    }

    // 6. 停止流式查询,会清除查询的查询编号
    activeRunOpt.foreach(_.stop())

    // 7. 添加新的查询(如果之前有过查询,则会抛出异常)
    activeQueriesSharedLock.synchronized {
      // 当两个并发实例同时启动的时候还会有竞争,当另一个已经激活且停止的时候.在这种情况下,会抛出异常
      val oldActiveQuery = sparkSession.sharedState.activeStreamingQueries.put(
        query.id, query.streamingQuery) // we need to put the StreamExecution, not the wrapper
      if (oldActiveQuery != null) {
        throw new ConcurrentModificationException(
          "Another instance of this query was just started by a concurrent session.")
      }
      activeQueries.put(query.id, query)
    }

    // 8. 启动流式查询
    try {
      // 启动查询的时候,会同步调用@StreamingQueryListener.onQueryStarted在这里不能持有锁。否则很容易导致死锁，或者阻塞过长。
      query.streamingQuery.start()
    } catch {
      case e: Throwable =>
        unregisterTerminatedStream(query)
        throw e
    }
    query
  }

  // 提醒指定查询已经被终止
  private[sql] def notifyQueryTermination(terminatedQuery: StreamingQuery): Unit = {
    unregisterTerminatedStream(terminatedQuery)
    awaitTerminationLock.synchronized {
      if (lastTerminatedQuery == null || terminatedQuery.exception.nonEmpty) {
        lastTerminatedQuery = terminatedQuery
      }
      awaitTerminationLock.notifyAll()
    }
    stateStoreCoordinator.deactivateInstances(terminatedQuery.runId)
  }
  
  /**
    * 解除终止的流式查询,考虑到并发情景,需要同步
    * @param terminatedQuery 终止的流式查询
    */
  private def unregisterTerminatedStream(terminatedQuery: StreamingQuery): Unit = {
    activeQueriesSharedLock.synchronized {
      // remove from shared state only if the streaming execution also matches
      sparkSession.sharedState.activeStreamingQueries.remove(
        terminatedQuery.id, terminatedQuery)
      activeQueries -= terminatedQuery.id
    }
  }
}
