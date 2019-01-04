package logging

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._

/**
  * This class should be added as listener to a SparkContext
  * and will track the metrics of executed tasks.
  */
class LogListener extends SparkListener {
  var jobIdsToJobs: scala.collection.mutable.Map[Int, LogJob] = scala.collection.mutable.Map.empty[Int, LogJob]
  var stageIdToStage: scala.collection.mutable.Map[Int, LogStage] = scala.collection.mutable.Map.empty[Int, LogStage]
  var jobIdToStageIds: scala.collection.mutable.Map[Int, Seq[Int]] = scala.collection.mutable.Map.empty[Int, Seq[Int]]
  
  // accessors for resulting data return immutable objects
  def getJobData() = { scala.collection.immutable.Map() ++ jobIdsToJobs }
  def getStageData() = { scala.collection.immutable.Map() ++ stageIdToStage }
  def getJobStages() = { scala.collection.immutable.Map() ++ jobIdToStageIds }
  def getTaskData(stageId:Int) = { scala.collection.immutable.Map() ++ stageIdToStage(stageId).tasks }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val tmpJob = new LogJob(jobStart.jobId)
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
        val tmpStage = new LogStage(stageId)
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

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    
  }
  
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    val currStage = stageIdToStage.get(taskEnd.stageId)
    if(currStage.isEmpty)
      println("Stage id unknown in onTaskEnd. Should be set at this position.")
    else {
      val stage = currStage.get
      val logTask = new LogTask(taskEnd.stageId)
      logTask.taskInfo = Some(taskEnd.taskInfo)
      logTask.taskMetrics = Some(taskEnd.taskMetrics)
      logTask.submissionTime = stage.stageInfo.get.submissionTime
      //stage.tasks += logTask.taskInfo.get.taskId -> logTask
      stage.tasks += logTask.taskInfo.get.taskId -> logTask
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val job = jobIdsToJobs.get(jobEnd.jobId)
    job.get.time = Some(jobEnd.time)
    for(stageId <- job.get.stageIds) {
      val stage = stageIdToStage.get(stageId)
      if(stage.nonEmpty) {
        for((taskId, task) <- stage.get.tasks) {
          task.jobEnd = Some(jobEnd.time)
        }
      }
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stage = new LogStage(stageSubmitted.stageInfo.stageId)
    stage.stageInfo = Some(stageSubmitted.stageInfo)
    this.stageIdToStage += stage.stageId -> stage
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
      for((taskId,task) <- v.tasks) {
        tasks :+= FlatTask(task.taskInfo.get.index, task.taskInfo.get.taskId, stageId, task.taskInfo.get.host,
          task.taskInfo.get.taskLocality == TaskLocality.PROCESS_LOCAL,
          task.jobEnd.get - task.submissionTime.get,
          task.taskInfo.get.launchTime - task.submissionTime.get,
          task.taskInfo.get.finishTime - task.taskInfo.get.launchTime,
          task.taskMetrics.get.asInstanceOf[TaskMetrics].executorDeserializeTime,
          0.0, task.taskMetrics.get.asInstanceOf[TaskMetrics].executorCpuTime,
          0 // Represents the time needed to read a RDD. Only possible if using
            // a modified spark version
          //task.taskMetrics.get.asInstanceOf[TaskMetrics].inputMetrics.readTime
        )
      }
    }
    tasks
  }


}