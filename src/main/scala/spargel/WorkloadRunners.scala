package spargel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession, types}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import scala.math.random
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.storage.BlockManager
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.SparkEnv
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
 */
object WorkloadRunners {
        
    /**
     * Run a workload on every entry of an RDD.
     * The workload takes a single argument of type (A).
     * Return a pair RDD whose keys are the worker names, and values
     * are task IDs that ran on that worker.
     */
    def OLDworkloader[A,B](r:RDD[A], wkld:Workload[A,B]): RDD[(String,Int)] = {
      r.map(rec => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        val partId = ctx.partitionId
        val hostname = java.net.InetAddress.getLocalHost().getHostName()
        wkld(rec)
        (hostname, partId)
      })
    }
    
    /**
     * Run a workload on every entry of an RDD.
     * The workload takes a single argument of type (A).
     * Return a pair RDD whose keys are the worker names, and values
     * are task IDs that ran on that worker.
     */
    def workloader[A,B](r:RDD[A], wkld:Workload[A,B]): RDD[(Int,String,String,Long,Long)] = {
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
      })
      
      // fields are (partId, ExecutionExecutorId, StorageExecutorId, memSize, diskSize)
      execHosts.map( x => (x._3, x._4, partHosts.get(x._3).head.head._1, partHosts.get(x._3).head.head._3, partHosts.get(x._3).head.head._4) )
    }
    
        
    /**
     * Run a workload on every entry of an RDD.
     * The workload to run will depend on the worker doing the processing.
     * Return a pair RDD whose keys are the worker names, and values
     * are task IDs that ran on that worker.
     */
    def hybridWorkloader[A,B](r:RDD[A], wkldMap:Map[String,Workload[A,B]], wkldDefault:Workload[A,B]): RDD[(Int,String,String,Long,Long)] = {
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
      })
      
      // fields are (partId, ExecutionExecutorId, StorageExecutorId, memSize, diskSize)
      execHosts.map( x => (x._3, x._4, partHosts.get(x._3).head.head._1, partHosts.get(x._3).head.head._3, partHosts.get(x._3).head.head._4) )
    }
    
}


