package spargel

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.SparkEnv
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import logging._
import Workloads._


/**
 * WorkloadRunners object contains function that apply workloads to RDDs.
 * 
 * Each workload runner takes two arguments:
 * - RDD[A]
 * - Workload
 * 
 * The type of a basic workload is very generic: A=>B
 * In this case A must match the type of the entries of the RDD.
 * 
 * The workload runner applies a map to the RDD and runs the workload on each
 * record in the RDD.
 * For each of the workload runners here, each RDD entry is mapped to the pair
 * (hostname, partitionID), where hostname identifies the worker where the
 * partition containing the RDD element was executed.
 * 
 * All of the RDD generators in this package (so far) produce
 * RDD[(Int,Array[Byte])], where each partition contains a single element.
 * The Int is a unique ID we assign to the element, and the Array is the data.
 * This way we can easily control the size and content of each partition.
 * 
 * XXX - this tries to associate the LogListener event data with the manually
 *       collected storage and executor environment data, and it does not work.
 *       We relied on the assumption that TaskEnd.taskInfo.index would match up
 *       with the partitionId.  It seems that they do not.  I see no other key
 *       to use to associate the two data sources.  Luckily Stefan has modified
 *       Spark to get all this info and more through the LogListener, so the
 *       whole idea of having the workload collect data on itself is no longer
 *       needed.
 *       
 *       It's possible that TaskContext.taskAttemptId will help us.
 *       
 */
object WorkloadRunners {
  
    /*
     * Fields we want in the resulting dataset
     * +++ one row per task
     * + TaskEnd
     *   - stageId
     *   - taskType
     * + TaskEnd.taskInfo
     *   - duration
     *   - executorId
     *   - finishTime
     *   - gettingResultTime
     *   - host?
     *   - id (String: what id is this?)
     *   - index
     *   - launchTime
     *   - speculative?
     *   - taskId (long)
     *   - taskLocality
     * + TaskEnd.taskMetrics
     *   - diskBytesSpilled
     *   - executorCpuTime
     *   - executorDeserializeCpuTime
     *   - executorDeserializeTime
     *   - executorRunTime
     *   - memoryBytesSpilled
     *   - peakExecutionMemory
     *   - resultSerializationTime
     *   - resultSize
     * + TaskEnd.taskMetrics.inputMetrics
     *   - bytesRead
     *   - recordsRead
     * + TaskEnd.taskMetrics.outputMetrics
     *   - bytesWritten
     *   - recordsWritten
     * 
     * +++ others and derived
     * - host
     * - task sojourn time
     * - task service time
     * - task waiting time
     * - stage sojourn time
     * - stage service time
     * - stage waiting time
     * - shuffle metrics?
     * 
     */

    def taskDataSchema: StructType = {
          StructType(
            Seq(
              StructField(name = "partId", dataType = IntegerType, nullable = false),
              StructField(name = "partMemSize", dataType = LongType, nullable = false),
              StructField(name = "partDiskSize", dataType = LongType, nullable = false),
              StructField(name = "stageId", dataType = IntegerType, nullable = false),
              //StructField(name = "taskType", dataType = StringType, nullable = false),
              StructField(name = "duration", dataType = LongType, nullable = false),
              StructField(name = "executorId", dataType = StringType, nullable = false),
              StructField(name = "storageExecutorId", dataType = StringType, nullable = false),
              StructField(name = "finishTime", dataType = LongType, nullable = false),
              StructField(name = "gettingResultTime", dataType = LongType, nullable = false),
              StructField(name = "id", dataType = StringType, nullable = false),
              StructField(name = "index", dataType = IntegerType, nullable = false),
              StructField(name = "launchTime", dataType = LongType, nullable = false),
              StructField(name = "taskId", dataType = LongType, nullable = false),
              StructField(name = "taskLocality", dataType = StringType, nullable = false),
              StructField(name = "diskBytesSpilled", dataType = LongType, nullable = false),
              StructField(name = "executorCpuTime", dataType = LongType, nullable = false),
              StructField(name = "executorDeserializeCpuTime", dataType = LongType, nullable = false),
              StructField(name = "executorDeserializeTime", dataType = LongType, nullable = false),
              StructField(name = "executorRunTime", dataType = LongType, nullable = false),
              StructField(name = "memoryBytesSpilled", dataType = LongType, nullable = false),
              StructField(name = "peakExecutionMemory", dataType = LongType, nullable = false),
              StructField(name = "resultSerializationTime", dataType = LongType, nullable = false),
              StructField(name = "resultSize", dataType = LongType, nullable = false)
            )
          )
    }

  
    /**
     * Run a workload on every entry of an RDD.
     * The workload takes a single argument of type (A).
     * Return a pair RDD whose keys are the worker names, and values
     * are task IDs that ran on that worker.
     */
    def workloader[A,B](r:RDD[A], wkld:Workload[A,B]): Dataset[Row] = {
      // get info on the location of each partition
      val bmm = SparkEnv.get.blockManager.master
      val rddId = r.id
      val nparts = r.getNumPartitions
      val partHosts = (0 until nparts).toArray
        .map( i => (i, bmm.getBlockStatus(RDDBlockId(rddId,i), true)
                      .map( x => (x._1.executorId,
                                  x._1.host,
                                  x._2.memSize,
                                  x._2.diskSize,
                                  x._2.storageLevel)
                          ) 
                    )).toMap
      
      // create a listener so we can track the lifecycles of the stages and tasks
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      val sc = spark.sparkContext   //SparkContext.getOrCreate()
      val logListener = new logging.LogListener
      sc.addSparkListener(logListener)

      // execute the workload, and have each task record where
      // (and later how long) it executes
      val execHosts = r.map(rec => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        val partId = ctx.partitionId
        val blockmgr = SparkEnv.get.blockManager
        val host = blockmgr.blockManagerId.host
        val execId = blockmgr.blockManagerId.executorId
        val execIdsparkenv = SparkEnv.get.executorId
        if (execId != execIdsparkenv) { println("WARNING: BlockManager execId is different from SparkEnv execId") }
        val isdriver = if (blockmgr.blockManagerId.isDriver) "driver" else "worker"
        wkld(rec)
        (host, stageId, partId, execId, execIdsparkenv, isdriver)
      }).persist
      
      // force the workload to actually execute
      execHosts.count
      
      // clean up
      sc.removeSparkListener(logListener)
            
      var jobData = logListener.getJobData()
      var stageData = logListener.getStageData()
      var jobStages = logListener.getJobStages()
      stageData.head._2.tasks
      
      val stageId = execHosts.take(1)(0)._2
      
      // We need to move the data into this intermediate case class because the
      // org.apache.spark.status.api.v1.TaskMetrics and org.apache.spark.scheduler.TaskInfo
      // classes are not Serializable, so they cannot be used in the RDD.map below.
      // XXX- knowing this we could change the logging package to extract the data
      //      into something more convenient
      val taskData = stageData.get(stageId).get.tasks.map{ case (k:Int,v:LogTask) => {
        val taskInfo = v.taskInfo.get
        val taskMetrics = v.taskMetrics.get
        (taskInfo.index , FlatTaskDetail(stageId,
                                        taskInfo.taskId,
                                        taskInfo.duration,
                                        taskInfo.executorId,
                                        taskInfo.finishTime,
                                        taskInfo.gettingResultTime,
                                        taskInfo.id,
                                        taskInfo.index,
                                        taskInfo.launchTime,
                                        taskInfo.taskLocality.toString(),
                                        taskMetrics.diskBytesSpilled,
                                        taskMetrics.executorCpuTime,
                                        taskMetrics.executorDeserializeCpuTime,
                                        taskMetrics.executorDeserializeTime,
                                        taskMetrics.executorRunTime,
                                        taskMetrics.memoryBytesSpilled,
                                        taskMetrics.peakExecutionMemory,
                                        taskMetrics.resultSerializationTime,
                                        taskMetrics.resultSize))
      }}
      
      val executionDataRdd = execHosts.map( x => {
        Row(x._3,                               // partId
            partHosts.get(x._3).head.head._3,   // partMemSize
            partHosts.get(x._3).head.head._4,   // partDiskSize
            x._2,                               // stageID
            taskData(x._3).duration,
            taskData(x._3).executorId,
            x._4,                               // storageExecutorId
            taskData(x._3).finishTime,
            taskData(x._3).gettingResultTime,
            taskData(x._3).id,
            taskData(x._3).index,
            taskData(x._3).launchTime,
            taskData(x._3).taskId,
            taskData(x._3).taskLocality.toString(),
            taskData(x._3).diskBytesSpilled,
            taskData(x._3).executorCpuTime,
            taskData(x._3).executorDeserializeCpuTime,
            taskData(x._3).executorDeserializeTime,
            taskData(x._3).executorRunTime,
            taskData(x._3).memoryBytesSpilled,
            taskData(x._3).peakExecutionMemory,
            taskData(x._3).resultSerializationTime,
            taskData(x._3).resultSize
          )
      })
      
      val executionDf = spark.createDataFrame(executionDataRdd, taskDataSchema)
      
      execHosts.unpersist(false)
      
      executionDf
    }
    
    
        
    /**
     * Run a workload on every entry of an RDD.
     * The workload to run will depend on the worker doing the processing.
     * Return a pair RDD whose keys are the worker names, and values
     * are task IDs that ran on that worker.
     */
    def hybridWorkloader[A,B](r:RDD[A], wkldMap:Map[String,Workload[A,B]], wkldDefault:Workload[A,B]): Dataset[Row] = {
      val bmm = SparkEnv.get.blockManager.master
      val rddId = r.id
      val nparts = r.getNumPartitions
      
      // create a listener so we can track the lifecycles of the stages and tasks
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      val sc = spark.sparkContext   //SparkContext.getOrCreate()
      val logListener = new logging.LogListener
      sc.addSparkListener(logListener)
      
      // execute the workload, and have each task record where
      // (and later how long) it executes
      val partHosts = (0 until nparts).toArray
        .map( i => (i, bmm.getBlockStatus(RDDBlockId(rddId,i), true)
                      .map( x => (x._1.executorId,
                                  x._1.host,
                                  x._2.memSize,
                                  x._2.diskSize,
                                  x._2.storageLevel)
                          ) 
                    )).toMap
      val execHosts = r.map(rec => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        val partId = ctx.partitionId
        val blockmgr = SparkEnv.get.blockManager
        val host = blockmgr.blockManagerId.host
        val execId = blockmgr.blockManagerId.executorId
        val execIdsparkenv = SparkEnv.get.executorId
        if (execId != execIdsparkenv) { println("WARNING: BlockManager execId is different from SparkEnv execId") }
        val isdriver = if (blockmgr.blockManagerId.isDriver) "driver" else "worker"
        
        // look up and execute the appropriate workload for this executor
        wkldMap.getOrElse(execIdsparkenv, wkldDefault)(rec)
        (host, stageId, partId, execId, execIdsparkenv, isdriver)
      }).persist
      
      // force the workload to actually execute
      execHosts.count
      
      // clean up
      sc.removeSparkListener(logListener)
            
      var stageData = logListener.getStageData()
      val stageId = execHosts.take(1)(0)._2
      
      // We need to move the data into this intermediate case class because the
      // org.apache.spark.status.api.v1.TaskMetrics and org.apache.spark.scheduler.TaskInfo
      // classes are not Serializable, so they cannot be used in the RDD.map below.
      // XXX- knowing this we could change the logging package to extract the data
      //      into something more convenient
      val taskData = stageData.get(stageId).get.tasks.map{ case (k:Int,v:LogTask) => {
        val taskInfo = v.taskInfo.get
        val taskMetrics = v.taskMetrics.get
        (taskInfo.index , FlatTaskDetail(stageId,
                                        taskInfo.taskId,
                                        taskInfo.duration,
                                        taskInfo.executorId,
                                        taskInfo.finishTime,
                                        taskInfo.gettingResultTime,
                                        taskInfo.id,
                                        taskInfo.index,
                                        taskInfo.launchTime,
                                        taskInfo.taskLocality.toString(),
                                        taskMetrics.diskBytesSpilled,
                                        taskMetrics.executorCpuTime,
                                        taskMetrics.executorDeserializeCpuTime,
                                        taskMetrics.executorDeserializeTime,
                                        taskMetrics.executorRunTime,
                                        taskMetrics.memoryBytesSpilled,
                                        taskMetrics.peakExecutionMemory,
                                        taskMetrics.resultSerializationTime,
                                        taskMetrics.resultSize))
      }}
      
      val executionDataRdd = execHosts.map( x => {
        Row(x._3,                               // partId
            partHosts.get(x._3).head.head._3,   // partMemSize
            partHosts.get(x._3).head.head._4,   // partDiskSize
            x._2,                               // stageID
            taskData(x._3).duration,
            taskData(x._3).executorId,
            x._4,                               // storageExecutorId
            taskData(x._3).finishTime,
            taskData(x._3).gettingResultTime,
            taskData(x._3).id,
            taskData(x._3).index,
            taskData(x._3).launchTime,
            taskData(x._3).taskId,
            taskData(x._3).taskLocality.toString(),
            taskData(x._3).diskBytesSpilled,
            taskData(x._3).executorCpuTime,
            taskData(x._3).executorDeserializeCpuTime,
            taskData(x._3).executorDeserializeTime,
            taskData(x._3).executorRunTime,
            taskData(x._3).memoryBytesSpilled,
            taskData(x._3).peakExecutionMemory,
            taskData(x._3).resultSerializationTime,
            taskData(x._3).resultSize
          )
      })
      
      val executionDf = spark.createDataFrame(executionDataRdd, taskDataSchema)
      
      execHosts.unpersist(false)
      
      executionDf
    }
    
}


