package spargel

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.SparkEnv
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.StorageLevel
import scala.math.random

object RddPartitioner {

    /**
     * Check the version of scala being used internally.
     *
     */
    def checkScalaVersion(sc:SparkContext) {
        println(util.Properties.versionString)
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
     * The partionSize parameter should give the desired partition size (in
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
     * The partionSize parameter should give the desired partition size (in
     * Bytes) for partitions created on each host.  For any host not listed in
     * the Map, it will use a default partition size of 1.
     * 
     * We want the partitions to be evenly distributed across the workers, even
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
     * Generate a huge RDD.  Each record is a tuple containing an integer index
     * and a (huge) array of random bytes.  This makes the content of the arrays
     * incompressible.
     * 
     * The size of the partitions can be set based on the executor ID where the
     * partition is being created.
     * The partionSize parameter should give the desired partition size (in
     * Bytes) for partitions created on each host.  For any host not listed in
     * the Map, it will use a default partition size of 1.
     * 
     * We want the partitions to be evenly distributed across the workers, even
     * if they are irregular in their sizes.  Therefore we need the
     * partitions-creation tasks to take equal amounts of time.  Add a
     * spin-waiting component to this so all tasks take the same time.
     */
    def getBigRandomExecIdRddTimed(sc:SparkContext, numPartitions:Int, runtime:Int, partitionSize:Map[String,Int], defaultPartitionSize:Int=1):RDD[(Int,Array[Byte])] = {

      val tmprdd = sc.parallelize(1 to numPartitions, numPartitions).map { i =>
        i
      }.persist
      
      val myrdd = tmprdd.mapPartitionsWithIndex((i,it) => {
        val startTime = java.lang.System.currentTimeMillis()
        val targetStopTime = startTime + runtime
        
        val execId = SparkEnv.get.executorId
        val psize = partitionSize.getOrElse(execId, defaultPartitionSize)
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
   * Get a big pair RDD with integer keys randomly sampled in some range, and values that
   * are (small) arrays of Byte.  Arbitrarily creating huge RDDs with more than MAX_INT
   * records is not automatic, so it iteratively creates pieces and combines them with union.
   * It tries to use as few unions as possible, though.  If you create this one partition
   * at a time, and then try to union, say, 1024 RDDs, the DAG gets too complex and it fails.
   *
   * @param sc
   * @param numPartitions
   * @param numKeys
   * @param numRecords
   * @param recordSize
   * @return
   */
    def getUniformKeyedRdd(sc:SparkContext, numPartitions:Int, numKeys:Int, numRecords:Long, recordSize:Int): RDD[(Int, Array[Byte])] = {
      if ((numRecords / numPartitions) > Integer.MAX_VALUE) {
        println("ERROR: this function requires that numRecords/numPartitions be less than MAXINT.")
        return sc.emptyRDD[(Int, Array[Byte])]
      }
      var protoRdd: RDD[Int] = sc.emptyRDD[Int]
      val maxbatch = 2000000000l
      var numRecRemaining = numRecords
      var partSize:Int = (numRecords / numPartitions).toInt + 1
      while (numRecRemaining > 0) {
        // fit as many partitions as possible into an iteration
        val nparts = if (numRecRemaining < maxbatch)  (numRecRemaining/partSize) else (maxbatch/partSize)
        protoRdd = protoRdd.union(sc.parallelize(1 to (nparts*partSize).toInt, numSlices = nparts.toInt))
        numRecRemaining -= nparts*partSize
        if (numRecRemaining < partSize) partSize = numRecRemaining.toInt
      }
      val myBigRdd = protoRdd.map { i =>
        (scala.util.Random.nextInt(numKeys), Array.fill[Byte](recordSize)((scala.util.Random.nextInt(256) - 128).toByte))
      }
      return myBigRdd
    }

    
    /**
     * For each partition print out the worker where it is stored.
     */
    def printPartitionHosts[A](r:RDD[A]) {
      val bmm = SparkEnv.get.blockManager.master
      val rddId = r.id
      val nparts = r.getNumPartitions
      
      for (i <- 0 until nparts) {
        val mm = bmm.getBlockStatus(RDDBlockId(rddId,i), true)
        for ((k,v) <- mm) {
            println(rddId+"\t"+i+"\texecId="+k.executorId+"\thost="+k.host+"\tmemSize="+v.memSize+"\tdiskSize="+v.diskSize+"\tstorageLevel="+v.storageLevel)
        }
      }
    }


  case class PartitionHostInfo(rddId:Int, partId:Int, execId:String, host:String, memsize:Long, disksize:Long,
                               storageLevel:String, storageShmd:Int, replicas:Int) {
    override def toString = {
      ("rddId:"+rddId+" partId:"+partId+" execId:"+execId+" host:"+host
        +" memsize:"+memsize+" disksize:"+disksize+" storageLevel:"+storageLevel
        +" storageShmd:"+storageShmd+" replicas:"+replicas)
    }
  }
    /**
     * Get data on the executors/hosts where each partition of an RDD is stored.
     * Returns an array of lists of tuples containing:
     * (rddId, partitionId, executorId, hostIP, memsize, disksize, storagelevel)
     * 
     * Note that a partition /may/ be stored multiple places.
     */
    def getPartitionHosts[A](r:RDD[A]): Array[Iterable[PartitionHostInfo]] = {
      val bmm = SparkEnv.get.blockManager.master
      val rddId = r.id
      val nparts = r.getNumPartitions

      //.map( i => bmm.getBlockStatus(RDDBlockId(rddId,i), askSlaves=true)

      (0 until nparts).toArray
	.map( i => bmm.getBlockStatus(RDDBlockId(rddId,i), askStorageEndpoints=true)  // askSlaves=true) // for spark 3.1 and later
                      .map( x => PartitionHostInfo(rddId,
                        i,
                        x._1.executorId,
                        x._1.host,
                        x._2.memSize,
                        x._2.diskSize,
                        x._2.storageLevel.description,
                        x._2.storageLevel.toInt,
                        x._2.storageLevel.replication) ))
    }
    
    
}


