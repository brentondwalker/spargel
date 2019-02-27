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
              StructField(name = "taskId", dataType = LongType, nullable = false),
              StructField(name = "stageId", dataType = IntegerType, nullable = false),
              StructField(name = "index", dataType = IntegerType, nullable = false),
              StructField(name = "attemptNumber", dataType = IntegerType, nullable = false),
              StructField(name = "id", dataType = StringType, nullable = false),
              StructField(name = "failed", dataType = BooleanType, nullable = false),
              StructField(name = "partMemSize", dataType = LongType, nullable = false),
              StructField(name = "partDiskSize", dataType = LongType, nullable = false),
              StructField(name = "executorId", dataType = StringType, nullable = false),
              StructField(name = "storageExecutorId", dataType = StringType, nullable = false),
              StructField(name = "taskLocality", dataType = StringType, nullable = false),
              StructField(name = "duration", dataType = LongType, nullable = false),
              StructField(name = "finishTime", dataType = LongType, nullable = false),
              StructField(name = "gettingResultTime", dataType = LongType, nullable = false),
              StructField(name = "launchTime", dataType = LongType, nullable = false),
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
      
      //partHosts.foreach(println)
                    
      // create a listener so we can track the lifecycles of the stages and tasks
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      val sc = spark.sparkContext   //SparkContext.getOrCreate()
      val logListener = new logging.LogListener
      sc.addSparkListener(logListener)

      // execute the workload, and have each task record where
      // (and later how long) it executes
      val stageId = r.map(rec => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        wkld(rec)
        // randomly make a task fail
        //if (math.random < 0.1) {
        //  throw new RuntimeException("This exception is thrown to simulate task failures and lead to job failure")
        //}
        
        // return the stage ID of this task
        // the stage IDs of all the tasks will be the same
        stageId
      }).collect.max
      
      // clean up
      sc.removeSparkListener(logListener)
      
      val taskData = logListener.getTaskData(stageId)
      val jobData = logListener.getJobData()
      jobData.foreach( x => println(x._1+"\t"+x._2+"\t"+(x._2.time.get-x._2.submissionTime.get)) )
      
      // sometimes a partition is stored nowhere, so we need to be careful about accessing partHosts
      
      val executionData = taskData.map( x => {
        val taskId = x._1
        val taskInfo = x._2.taskInfo.get
        val taskMetrics = x._2.taskMetrics.get
        Row(taskId,                              // taskId
            x._2.stageId,                        // stageId from Logger
            taskInfo.index,                      // task index from Logger
            taskInfo.attemptNumber,
            taskInfo.id,                         // id string from Logger
            taskInfo.failed,
            if (! partHosts.get(taskInfo.index).get.isEmpty) partHosts.get(taskInfo.index).head.head._3 else -1L,  // partMemSize
            if (! partHosts.get(taskInfo.index).get.isEmpty) partHosts.get(taskInfo.index).head.head._4 else -1L,  // partDiskSize
            taskInfo.executorId,                 // execution ExecutorId
            if (! partHosts.get(taskInfo.index).get.isEmpty) partHosts.get(taskInfo.index).head.head._1 else "NONE",  // storage ExecutorId
            taskInfo.taskLocality.toString(),    // taskLocality
            taskInfo.duration,
            taskInfo.finishTime,
            taskInfo.gettingResultTime,
            taskInfo.launchTime,
            taskMetrics.diskBytesSpilled,
            taskMetrics.executorCpuTime,
            taskMetrics.executorDeserializeCpuTime,
            taskMetrics.executorDeserializeTime,
            taskMetrics.executorRunTime,
            taskMetrics.memoryBytesSpilled,
            taskMetrics.peakExecutionMemory,
            taskMetrics.resultSerializationTime,
            taskMetrics.resultSize
            )
      })
      
      val numExecutors = sc.statusTracker.getExecutorInfos.length - 1

      
      val executionDf = spark.createDataFrame(sc.parallelize(executionData.toSeq, numExecutors), this.taskDataSchema)
      
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
      
      //partHosts.foreach(println)
      
      val stageId = r.map(rec => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        val execId = SparkEnv.get.executorId

        // look up and execute the appropriate workload for this executor
        wkldMap.getOrElse(execId, wkldDefault)(rec)
        
        // return the stage ID of this task
        // the stage IDs of all the tasks will be the same
        stageId
      }).collect.max
      
      val taskData = logListener.getTaskData(stageId)
      
      val executionData = taskData.map( x => {
        val taskId = x._1
        val taskInfo = x._2.taskInfo.get
        val taskMetrics = x._2.taskMetrics.get
        Row(taskId,                              // taskId
            x._2.stageId,                        // stageId from Logger
            taskInfo.index,                      // task index from Logger
            taskInfo.attemptNumber,
            taskInfo.id,                         // id string from Logger
            taskInfo.failed,
            if (! partHosts.get(taskInfo.index).get.isEmpty) partHosts.get(taskInfo.index).head.head._3 else -1L,  // partMemSize
            if (! partHosts.get(taskInfo.index).get.isEmpty) partHosts.get(taskInfo.index).head.head._4 else -1L,  // partDiskSize
            taskInfo.executorId,                 // execution ExecutorId
            if (! partHosts.get(taskInfo.index).get.isEmpty) partHosts.get(taskInfo.index).head.head._1 else "NONE",  // storage ExecutorId
            taskInfo.taskLocality.toString(),    // taskLocality
            taskInfo.duration,
            taskInfo.finishTime,
            taskInfo.gettingResultTime,
            taskInfo.launchTime,
            taskMetrics.diskBytesSpilled,
            taskMetrics.executorCpuTime,
            taskMetrics.executorDeserializeCpuTime,
            taskMetrics.executorDeserializeTime,
            taskMetrics.executorRunTime,
            taskMetrics.memoryBytesSpilled,
            taskMetrics.peakExecutionMemory,
            taskMetrics.resultSerializationTime,
            taskMetrics.resultSize
            )
      })
      
      val executionDf = spark.createDataFrame(sc.parallelize(executionData.toSeq, executionData.size), this.taskDataSchema)

      
      executionDf
    }
    
}


