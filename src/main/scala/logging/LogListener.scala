package logging

import org.apache.spark.executor.{InputReadData, TaskMetrics}
import org.apache.spark.scheduler._
import java.io._

/**
  * This class should be added as listener to a SparkContext
  * and will track the metrics of executed tasks.
  */
case class LogListener() extends SparkListener {
  var jobIdsToJobs: scala.collection.mutable.Map[Int, LogJob] = scala.collection.mutable.Map.empty[Int, LogJob]
  var stageIdToStage: scala.collection.mutable.Map[Int, LogStage] = scala.collection.mutable.Map.empty[Int, LogStage]
  var jobIdToStageIds: scala.collection.mutable.Map[Int, Seq[Int]] = scala.collection.mutable.Map.empty[Int, Seq[Int]]
  var executionTimeAccumulator: Option[ExecutionTimeAccumulator] = None
  
  // accessors for resulting data return immutable objects
  def getJobData() = { scala.collection.immutable.Map() ++ jobIdsToJobs }
  def getStageData() = { scala.collection.immutable.Map() ++ stageIdToStage }
  def getJobStages() = { scala.collection.immutable.Map() ++ jobIdToStageIds }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    print(jobStart.jobId)
    val tmpJob = LogJob(jobStart.jobId)
    tmpJob.submissionTime = Some(jobStart.time)
    tmpJob.numStages = jobStart.stageInfos.size
    tmpJob.stageIds = jobStart.stageIds
    tmpJob.stageInfos = jobStart.stageInfos
    jobIdsToJobs += jobStart.jobId -> tmpJob
    jobIdToStageIds += jobStart.jobId -> tmpJob.stageIds
    for(stageId <- jobStart.stageIds) {
      if(stageIdToStage.contains(stageId)) { // Should not happen. Only for debugging
        println("Started job with already known stage id. Something must be wrong.")
      }
      else {
        val tmpStage = LogStage(stageId)
        tmpStage.job = Some(tmpJob)
        stageIdToStage += tmpStage.stageId -> tmpStage
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId = stageCompleted.stageInfo.stageId
    val currStage = stageIdToStage.get(stageId)
    if(currStage.isEmpty)
      println("Stage id unknown in onStageCompleted. Should be set at this position.")
    else {
      val stage = currStage.get
      stage.stageInfo = Some(stageCompleted.stageInfo)
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val currStage = stageIdToStage.get(taskEnd.stageId)
    if(currStage.isEmpty)
      println("Stage id unknown in onTaskEnd. Should be set at this position.")
    else {
      val stage = currStage.get
      val logTask = LogTask(taskEnd.stageId)
      logTask.taskInfo = Some(taskEnd.taskInfo)
      logTask.taskMetrics = Some(taskEnd.taskMetrics)
      logTask.submissionTime = stage.stageInfo.get.submissionTime
      //stage.tasks += logTask.taskInfo.get.taskId -> logTask
      stage.tasks += logTask.taskInfo.get.index -> logTask
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val job = jobIdsToJobs.get(jobEnd.jobId)
    job.get.time = Some(jobEnd.time)
    for(stageId <- job.get.stageIds) {
      val stage = stageIdToStage.get(stageId)
      if(stage.nonEmpty) {
        for((taskIndex, task) <- stage.get.tasks) {
          task.jobEnd = Some(jobEnd.time)
          stage.get.job = job
        }
      }
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val currStage = stageIdToStage.get(stageSubmitted.stageInfo.stageId)
    if(currStage.isEmpty) {
      println("Stage id unknown in onStageSubmitted. Should be set at this position.")
      val stage = LogStage(stageSubmitted.stageInfo.stageId)
      stage.stageInfo = Some(stageSubmitted.stageInfo)
      this.stageIdToStage += stage.stageId -> stage
    }
    else {
      val stage = currStage.get
      stage.stageInfo = Some(stageSubmitted.stageInfo)
    }
  }

  /**
    * This function uses the inner status holders of jobs, tasks and stages
    * to create a sequence of case class FlatTask objects which can be
    * converted to a spark DataFrame.
    * @return Sequence of case class FlatTask
    */
  def getTaskMetrics(): scala.collection.mutable.Seq[FlatTask] = {
    var tasks:scala.collection.mutable.Seq[FlatTask] = scala.collection.mutable.Seq[FlatTask]()
    for((stageId,v) <- stageIdToStage) {
      for((taskIndex,task) <- v.tasks) {
        if(task.taskInfo.nonEmpty) {
          val readParams = task.taskMetrics.get.asInstanceOf[TaskMetrics].inputMetrics.readParams
            .lastOption.getOrElse(InputReadData("-1", "No data read", false, 0L, 0L))
          tasks :+= FlatTask(taskIndex, task.taskInfo.get.taskId, stageId, task.taskInfo.get.host,
            task.taskInfo.get.taskLocality == TaskLocality.PROCESS_LOCAL,
            task.jobEnd.getOrElse(0L) - task.submissionTime.getOrElse(0L),
            task.taskInfo.get.launchTime - task.submissionTime.getOrElse(0L),
            task.taskInfo.get.finishTime - task.taskInfo.get.launchTime,
            task.taskMetrics.get.asInstanceOf[TaskMetrics].executorDeserializeTime,
            0.0, task.taskMetrics.get.asInstanceOf[TaskMetrics].executorCpuTime,
            readParams.readTime,
            readParams.locationExecId,
            readParams.readMethod,
            readParams.cachedBlock
          )
        }
      }
    }
    tasks
  }

  def readParamsWithMostData(readParams: Seq[InputReadData]): InputReadData = {
    readParams.foldRight(InputReadData("", "Nothing read", cachedBlock = false, -1, -1))(
      (last, curr) => if (last.bytesRead > curr.bytesRead) last else curr)
  }

  def addExecutionTimeAccumulator(executionTimeAccumulator: ExecutionTimeAccumulator): Unit = {
    this.executionTimeAccumulator = Some(executionTimeAccumulator)
  }

  def getCsvMetrics() = {
    var resultCSV = getCsvLabelLine()

    val executionStats = this.executionTimeAccumulator.fold(Map.empty[String, Long])(_.value.stats)
    var tasks:scala.collection.mutable.Seq[FlatTask] = scala.collection.mutable.Seq[FlatTask]()
    for((stageId,v) <- stageIdToStage) {
      for((taskIndex,task) <- v.tasks) {
        val runTime = executionStats.getOrElse(task.taskInfo.fold(-1L)(_.taskId).toString, 0L)
        if(task.taskInfo.nonEmpty) {
          val extendedTask = ExtendedFlatTask(task, v, runTime)
          resultCSV += s"${extendedTask.toJson()}"
        }
      }
    }
    resultCSV
  }

  def writeCsvToFile(filepath: String): Unit = {
    val pw = new PrintWriter(new File(filepath))
    pw.write(getCsvLabelLine())
    val executionStats = this.executionTimeAccumulator.fold(Map.empty[String, Long])(_.value.stats)
//    var tasks:scala.collection.mutable.Seq[FlatTask] = scala.collection.mutable.Seq[FlatTask]()
    for((stageId,v) <- stageIdToStage) {
      for((taskIndex,task) <- v.tasks) {
        val runTime = executionStats.getOrElse(task.taskInfo.fold(-1L)(_.taskId).toString, 0L)
        if(task.taskInfo.nonEmpty) {
          val extendedTask = ExtendedFlatTask(task, v, runTime)
          pw.write(s"${extendedTask.toJson()}\n")
        }
      }
    }
  }

  def getExecutionTimeAccumulator(): ExecutionTimeAccumulator = {
    if (this.executionTimeAccumulator.isEmpty)
      this.executionTimeAccumulator = Some(ExecutionTimeAccumulator(ExecutionTimeStat()))
    this.executionTimeAccumulator.get
  }

  def getCsvLabelLine(): String = {
    "launchTime,finishTime,jobId,status,stageId,name,taskId,index,attempt,executorId," +
    "duration,sojournTime,waitingTime," +
    "taskLocality,executorDeserializeTime,executorRunTime,executorCpuTime," +
    "executorDeserializeCpuTime,resultSize,gettingResultTime," +
    "jvmGcTime,resultSerializationTime,memoryBytesSpilled,diskBytesSpilled," +
    "peakExecutionMemory,bytesRead,recordsRead,readTime,locationExecId,readMethod,cachedBlock," +
    "bytesWritten, recordsWritten,shuffleRemoteBlocksFetched,shuffleLocalBlocksFetched," +
    "shuffleFetchWaitTime, remoteBytesRead,shuffleRemoteBytesReadToDisk,shuffleLocalBytesRead," +
    "shuffleRecordsRead, shuffleBytesWritten,shuffleWriteTime,shuffleRecordsWritten," +
    "stageCompletionTime,measuredRunTime\n"
  }
}