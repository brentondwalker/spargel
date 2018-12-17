package spargel

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
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
    def workloader[A,B](r:RDD[A], wkld:Workload[A,B]): RDD[(String,Int)] = {
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
     * Run a time-limited workload on every entry of an RDD.
     * The runtime of each task depends on the hostname of the worker it runs
     * on.
     * The workload must be a timed workload taking arguments (A,Int).
     * 
     * XXX This assumes a small number of hosts and that the last character of
     *     the hostname is a number.  The runtime used will be a function of the
     *     hostnumber.
     *     
     * Return an RDD containing the worker each task ran on.
     */
    def hostnameWorkloader[A](r:RDD[A], wkld:(A,Int)=>Unit): RDD[(String,Int)] = {
      r.map(rec => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        val partId = ctx.partitionId
        val hostname = java.net.InetAddress.getLocalHost().getHostName()
        val hostnum = hostname.last.toInt - '0'.toInt
        val runtime = hostnum*hostnum*hostnum //*hostnum*hostnum
        wkld(rec,runtime)
        (hostname, partId)
      })
    }
    
        
    /**
     * Run a workload on every entry of an RDD.
     * The workload to run will depend on the worker doing the processing.
     * Return a pair RDD whose keys are the worker names, and values
     * are task IDs that ran on that worker.
     */
    def hybridWorkloader[A,B](r:RDD[A], wkldMap:Map[String,Workload[A,B]], wkldDefault:Workload[A,B]): RDD[(String,Int)] = {
      r.map(rec => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        val partId = ctx.partitionId
        val hostname = java.net.InetAddress.getLocalHost().getHostName()
        wkldMap.getOrElse(hostname, wkldDefault)(rec)
        (hostname, partId)
      })
    }
    
}


