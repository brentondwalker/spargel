package logging

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._

case class FlatTask(taskIndex:Int, taskId:Long, stageId:Long, host:String, local:Boolean,
                    sojournTime:Double, waitingTime:Double, serviceTime:Double,
                    deserializationTime:Double, schedulerOverhead:Double,
                    runtime: Double, readTime:Double) {
  override def toString = {
    ("taskIndex:"+taskIndex+" taskId:"+taskId+" stageId:"+stageId+" host:"+host+" local:"+local+" sojournTime:"+sojournTime 
      +" waitingTime:"+waitingTime+" serviceTime:"+serviceTime+" deserializationTime:"+deserializationTime
      +" schedulerOverhead:"+schedulerOverhead+"runtime:"+runtime)
  }
}

// More detailed FlatTask class
case class FlatTaskDetail(stageId:Long, taskId:Long, duration:Long, executorId:String, finishTime:Long, gettingResultTime:Long, id:String, index:Int,
        launchTime:Long, taskLocality:String, diskBytesSpilled:Long, executorCpuTime:Long, executorDeserializeCpuTime:Long, 
        executorDeserializeTime:Long, executorRunTime:Long, memoryBytesSpilled:Long, peakExecutionMemory:Long, resultSerializationTime:Long, resultSize:Long)

// Following inner classes keeps track of jobs, stages and tasks.
case class LogJob(jobId:Int) extends Serializable {
    var submissionTime: Option[Long] = None
    var numStages: Int = 0
    var stageIds:Seq[Int] = Seq.empty[Int]
    var stageInfos:Seq[StageInfo] = Seq.empty[StageInfo]
    // time in SparkListenerJobEnd. TODO check what time is meant
    var time: Option[Long] = None
    override def toString = { "LogJob("+submissionTime.getOrElse("None")+", "+numStages+", "+stageIds+", "+stageInfos+", "+time.getOrElse("None")+")" }
}

case class LogTask(stageId: Int) extends Serializable {
    var taskInfo: Option[TaskInfo] = None
    var taskMetrics: Option[TaskMetrics] = None
    var submissionTime: Option[Long] = None // Represents the time the stage is submitted
    var jobEnd: Option[Long] = None
    override def toString = { "LogTask("+taskInfo.getOrElse("None")+", "+taskMetrics.getOrElse("None")+", "+submissionTime.getOrElse("None")+", "+jobEnd.getOrElse("None")+")" }
}

case class LogStage(stageId: Int) extends Serializable {
    var stageInfo: Option[StageInfo] = None
    var job: Option[LogJob] = None
    var tasks: scala.collection.mutable.Map[Int, LogTask] = scala.collection.mutable.Map.empty[Int, LogTask]
    override def toString = { "LogStage("+stageInfo.getOrElse("None")+", "+job.getOrElse("None")+", "+tasks+")" }
}
