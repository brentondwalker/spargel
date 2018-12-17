package spargel

import logging.LogListener
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.TaskContext
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

import scala.math.random

object RddPartitioner {
    /*
     * Get the current SparkSession when this object is instantiated.
     */
    //val spark = SparkSession.builder().getOrCreate()

    /**
     * main()
     */
    def main(args: Array[String]) {
      val numcores = 100

      /**
        * Adding spargel jar to spark config seems to be necessary
        * to be able using functions of it in executors.
        */
      val sparkSession = SparkSession.builder
        .master("spark://172.23.27.10:7077")
        .appName("RddPartitioner")
        .config("spark.cores.max", "100")
        .config("spark.jars", "target/scala-2.11/spargel_2.11-1.0.jar")
        .getOrCreate()
      val logListener = new LogListener
      sparkSession.sparkContext.addSparkListener(logListener)
//      sc.getConf.get("spark.locality.wait")
//      val sqlContext = new SQLContext(sc)
      import sparkSession.implicits._

      val myrdd = getBigZeroRdd(sparkSession.sparkContext, 10, 1)
        .persist(DISK_ONLY)

      myrdd.getNumPartitions
      val myparts = myrdd.partitions
      val p = myparts(0)
      myrdd.preferredLocations(p)

      //def f(x:Iterator[Partition]):String = { yield hostname }
      //myrdd.mapPartitions(f).collect()

      printPartitionHostsMap(myrdd).collect
      WorkloadRunners.hostnameWorkloader(myrdd, NodataWorkloads.timedRandomSquareWorkload)

      // ------------------------------------------------------------------------------------------

      val partiton_size = 1024*1024*128
      val num_partitions = 5

      val mybigrdd = getBigZeroRdd(sparkSession.sparkContext, num_partitions, partiton_size)
        .persist(DISK_ONLY)

      printPartitionHostsMap(mybigrdd).collect
      WorkloadRunners.hostnameWorkloader(mybigrdd, NodataWorkloads.timedRandomSquareWorkload)
      WorkloadRunners.workloader(mybigrdd, ByteArrayWorkloads.IterativeMaxWorkload)
      WorkloadRunners.workloader(mybigrdd, ByteArrayWorkloads.IterativeMaxWorkload)
      sparkSession.sparkContext.removeSparkListener(logListener)
      logListener.getTaskMetrics().toDF.orderBy("taskId").show
    }
    
    
    /**
     * Generate a huge RDD.  Each record is a tuple containing an integer index
     * and a (huge) array of zeros.
     * 
     * This makes the content of the arrays very compressible.
     */
    def getBigZeroRdd(sc:SparkContext, numPartitions:Int, partitionSize:Int):RDD[(Int,Array[Byte])] = {
      
      val mybigrdd = sc.parallelize(1 to numPartitions, numPartitions).map { i =>
        i
      }.map( i => (i,Array.fill[Byte](partitionSize)(0)) )

      return mybigrdd
    }
    
    
    /**
     * Generate a huge RDD.  Each record is a tuple containing an integer index
     * and a (huge) array of random bytes.
     * 
     * This makes the content of the arrays incompressible.
     */
    def getBigRandomRdd(sc:SparkContext, numPartitions:Int, partitionSize:Int):RDD[(Int,Array[Byte])] = {
      
      val mybigrdd = sc.parallelize(1 to numPartitions, numPartitions).map { i =>
        i
      }.map( i => (i,Array.fill[Byte](partitionSize)((scala.util.Random.nextInt(256) - 128).toByte)) )

      return mybigrdd
    }
    
    
    /**
     * Generate a huge RDD.  Each record is a tuple containing an integer index
     * and a (huge) array of random bytes.  This makes the content of the arrays
     * incompressible.
     * 
     * The size of the partitions can be set based on the worker where the
     * partition is being created.
     * The partionSize aparameter should give the desired partition size (in
     * Bytes) for partitions created on each host.  For any host not listed in
     * the Map, it will use a default partition size of 1.
     */
    def getBigRandomHostnameRdd(sc:SparkContext, numPartitions:Int, partitionSize:Map[String,Int], defaultPartitionSize:Int=1):RDD[(Int,Array[Byte])] = {

      val tmprdd = sc.parallelize(1 to numPartitions, numPartitions).map { i =>
        i
      }.persist
      
      val myrdd = tmprdd.mapPartitionsWithIndex((i,it) => {
        val hostname = java.net.InetAddress.getLocalHost().getHostName()
        val psize = partitionSize.getOrElse(hostname, defaultPartitionSize)
        val maxsize = partitionSize.valuesIterator.max
        if (maxsize > psize) {
          val dummy = List((i, Array.fill[Byte](maxsize - psize)((scala.util.Random.nextInt(256) - 128).toByte))).iterator
        }

        List((i, Array.fill[Byte](psize)((scala.util.Random.nextInt(256) - 128).toByte))).iterator
      }, preservesPartitioning=true)
      
      
      return myrdd
    }
    
    
    /**
     * Generate a huge RDD.  Each record is a tuple containing an integer index
     * and a (huge) array of random bytes.  This makes the content of the arrays
     * incompressible.
     * 
     * The size of the partitions can be set based on the worker where the
     * partition is being created.
     * The partionSize aparameter should give the desired partition size (in
     * Bytes) for partitions created on each host.  For any host not listed in
     * the Map, it will use a default partition size of 1.
     * 
     * We want the partitons to be evenly distributed across the workers, even
     * if they are irregular in their sizes.  Therefore we need the
     * partitions-creation tasks to take equal amounts of time.  Add a
     * spin-waiting component to this so all tasks take the same time.
     */
    def getBigRandomHostnameRddTimed(sc:SparkContext, numPartitions:Int, runtime:Int, partitionSize:Map[String,Int], defaultPartitionSize:Int=1):RDD[(Int,Array[Byte])] = {

      val tmprdd = sc.parallelize(1 to numPartitions, numPartitions).map { i =>
        i
      }.persist
      
      val myrdd = tmprdd.mapPartitionsWithIndex((i,it) => {
        val startTime = java.lang.System.currentTimeMillis()
        
        val ctx = TaskContext.get()
        //val stageId = ctx.stageId
        //ctx.getLocalProperty("spark.executor.cores")
        val targetStopTime = startTime + runtime
        
        val hostname = java.net.InetAddress.getLocalHost().getHostName()
        val psize = partitionSize.getOrElse(hostname, defaultPartitionSize)
        val maxsize = partitionSize.valuesIterator.max

        val mypart = List((i, Array.fill[Byte](psize)((scala.util.Random.nextInt(256) - 128).toByte))).iterator
        
        // waste time so all tasks take the same ammt of time
        while (java.lang.System.currentTimeMillis() < targetStopTime) {
            val xx = random * 2 - 1
            val yy = random * 2 - 1
        }

        mypart
      }, preservesPartitioning=true)
      
      return myrdd
    }
    
    
    /**
     * For each partition print out the worker where it is stored.
     * Running on a cluster this will go to stdout on the workers.
     */
    def printPartitionHosts[A](r:RDD[A]) {
      r.foreachPartition( _ => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        val partId = ctx.partitionId
        val hostname = java.net.InetAddress.getLocalHost().getHostName()
        println(s"Stage: $stageId, Partition: $partId, Host: $hostname")
      })
    }
    
    
    /**
     * For each partition record the worker where it is stored and return the
     * result as an pair RDD.  The key is the hostname, the value is a list of
     * partitions on that host.
     */
    def printPartitionHostsMap[A](r:RDD[A]): RDD[(String,Iterable[Int])] = {
      return r.mapPartitions( _ => {
        val ctx = TaskContext.get()
        val stageId = ctx.stageId
        val partId = ctx.partitionId
        val hostname = java.net.InetAddress.getLocalHost().getHostName()
        //println(s"Stage: $stageId, Partition: $partId, Host: $hostname")
        List((hostname, partId)).iterator
      }, preservesPartitioning=true).groupByKey()
    }
    
    
}


