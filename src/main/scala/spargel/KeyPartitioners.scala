package spargel

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel._

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.SparkEnv
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.StorageLevel
import scala.math.random
import RddPartitioner.getPartitionHosts

object KeyPartitioners {
  //(partId, execId, key, numRecords)
  //(partId, execId, key, recordId)
  case class RecordPartitionInfo(partId:Int, execId:Int, key:Int, recordId:Int)
  case class KeyPartitionInfo(partId:Int, execId:Int, key:Int, size:Int)
  case class KeyHostInfo(execId:Int, key:Int, size:Int)
  
    /**
     * Generate a huge RDD.
     * Parameters specify:
     * - initial number of partitions
     * - number of records
     * - size of each record
     * - number of distinct keys to use
     * 
     * The records are full of zeros, which makes the content very compressible.
     * 
     */
    def getBigKeyedZeroRdd(sc:SparkContext, numRecords:Int, recordSize:Int, numPartitions:Int, numKeys:Int, seed:Int=1):RDD[(Int,(Int,Array[Byte]))] = {
      // seed the RNG, in case this RDD gets recomputed we get the same results.
      var rng:scala.util.Random = null
      val mybigrdd = sc.parallelize(1 to numRecords, numPartitions).map { i =>
        i
      }.map( i => {
        // If we don't do something to customize the RNG for each task, then
        // each partition will end up the exact same sequence of keys.
        // Multiply the seed by the partitionId.  It should keep the partitions
        // different, while still being repeatable.
        if (rng == null) {
          val ctx = TaskContext.get()
          val partId = ctx.partitionId()
          rng = new scala.util.Random(seed*partId)
        }
        ( rng.nextInt(numKeys), (i,Array.fill[Byte](recordSize)(0)) )
      })

      return mybigrdd
    }

  
    /**
     * Generate a huge RDD.
     * Parameters specify:
     * - initial number of partitions
     * - number of records
     * - size of each record
     * - number of distinct keys to use
     * 
     * The records are full of random bytes, which makes them less
     * compressible, and more expensive to move between executors.
     */
    def getBigKeyedRandomRdd(sc:SparkContext, numRecords:Int, recordSize:Int, numPartitions:Int, numKeys:Int, seed:Int=1):RDD[(Int,(Int,Array[Byte]))] = {
      // seed the RNG, in case this RDD gets recomputed we get the same results.
      var rng:scala.util.Random = null
      val mybigrdd = sc.parallelize(1 to numRecords, numPartitions).map { i =>
        i
      }.map( i => {
        // If we don't do something to customize the RNG for each task, then
        // each partition will end up the exact same sequence of keys.
        // Multiply the seed by the partitionId.  It should keep the partitions
        // different, while still being repeatable.
        if (rng == null) {
          val ctx = TaskContext.get()
          val partId = ctx.partitionId()
          rng = new scala.util.Random(seed*partId)
        }
        (rng.nextInt(numKeys), (i, Array.fill[Byte](recordSize)((scala.util.Random.nextInt(256) - 128).toByte)) )
      })
      return mybigrdd
    }

    
    /**
     * For each record print out the partition it belongs to and the
     *  worker where it is stored.
     *  
     *  XXX: the thing this returns has the same number of entries as the RDD.
     *  Maybe it should return an RDD.
     *  
     *  The values returned will be:
     *  (partId, execId, key, recordId)
     */
    def getRecordHosts[A](r:RDD[(Int,(Int,A))]):Array[RecordPartitionInfo] = {
      val partitionHostsArray = getPartitionHosts(r)
      if (partitionHostsArray.map(_.size).sum <= 0) {
        println("WARNING: the RDD does not appear to be persisted.")
        return Array.empty[RecordPartitionInfo]
      }
      
      val partToExecId = partitionHostsArray.map( x => {
        if (x.size > 0) (x.head._2, Integer.parseInt(x.head._3)) else (-1,-1)
      }).toMap
      
      r.map( x => {
        val ctx = TaskContext.get()
        val partId = ctx.partitionId()
        RecordPartitionInfo(partId, partToExecId.get(partId).get, x._1, x._2._1)
      }).collect
      
    }
    
    
    /**
     * For an RDD of the form: (key:Int, (recordId:Int, A))
     * or each record print out the partition it belongs to and the
     * executor where it is stored.
     */
    def printRecordHosts[A](r:RDD[(Int,(Int,A))]) {
      getRecordHosts(r).sortBy(_.recordId).foreach(x => println("recId="+x.recordId+"  \tkey="+x.key+"\tpartId="+x.partId+"\texecId="+x.execId))
    }
    
    
    /**
     * For an RDD of the form: (key:Int, (recordId:Int, A))
     * Items returned contain:
     * (partId, execId, key, numRecords)
     * 
     * Note that a partition /may/ be stored multiple places.
     */
    def getRddKeyPartitions[A](r:RDD[(Int,(Int,A))]): Array[KeyPartitionInfo] = {
      getRecordHosts(r).groupBy(x=>(x.partId,x.execId,x.key)).map( x => KeyPartitionInfo(x._1._1, x._1._2, x._1._3, x._2.size) ).toArray
    }
    
    
    /**
     * For an RDD of the form: (key:Int, (recordId:Int, A))
     * print out the list of distinct (partId, execId, key) tuples along with
     * the number of records under each distinct tuple.
     */
    def printRddKeyPartitions[A](r:RDD[(Int,(Int,A))]) {
      getRddKeyPartitions(r).foreach(x => println("partId="+x.partId+" \texecId="+x.execId+" \tkey="+x.key+" \tnumRecords="+x.size))
    }

    
    /**
     * For an RDD of the form: (key:Int, (recordId:Int, A))
     * Items returned contain:
     * (partId, execId, key, numRecords)
     * 
     * Note that a partition /may/ be stored multiple places.
     */
    def getRddKeyHosts[A](r:RDD[(Int,(Int,A))]): Array[KeyHostInfo] = {
      getRecordHosts(r).groupBy(x=>(x.execId,x.key)).map( x => KeyHostInfo(x._1._1, x._1._2, x._2.size) ).toArray
    }
    
    
    /**
     * For an RDD of the form: (key:Int, (recordId:Int, A))
     * print out the list of distinct (partId, execId, key) tuples along with
     * the number of records under each distinct tuple.
     */
    def printRddKeyHosts[A](r:RDD[(Int,(Int,A))]) {
      getRddKeyHosts(r).foreach(x => println("execId="+x.execId+" \tkey="+x.key+" \tnumRecords="+x.size))
    }

    

    /**
     * For a grouped keyed RDD of the type:
     * (key:Int, Iterable[(key:Int, (recordId:Int, A))])]
     * print out the partition it belongs to and the
     * executor where it is stored.
     *  
     *  The values returned will be:
     *  (partId, execId, key, numRecords)
     */
    def getGroupHosts[A](r:RDD[(Int, Iterable[(Int, (Int, A))])]):Array[KeyPartitionInfo] = {
      val partitionHostsArray = getPartitionHosts(r)
      if (partitionHostsArray.map(_.size).sum <= 0) {
        println("WARNING: the RDD does not appear to be persisted.")
        return Array.empty[KeyPartitionInfo]
      }

      val partToExecId = partitionHostsArray.map( x => {
        if (x.size > 0) (x.head._2, Integer.parseInt(x.head._3)) else (-1,-1)
      }).toMap
      
      r.map( x => {
        val ctx = TaskContext.get()
        val partId = ctx.partitionId()
        KeyPartitionInfo(partId, partToExecId.get(partId).get, x._1, x._2.size)
      }).collect
    }
    
    
    /**
     * For a grouped keyed RDD of the type:
     * (key:Int, Iterable[(key:Int, (recordId:Int, A))])]
     * print out a list of the groups, and what partitions/executors they belog
     * to, and the size of the groups.
     */
    def printGroupHosts[A](r:RDD[(Int, Iterable[(Int, (Int, A))])]) {
      getGroupHosts(r).foreach(x => println("partId="+x.partId+"\texecId="+x.execId+"\tkey="+x.key+"\tnumRecords="+x.size))
    }
    

    /**
     * Perform a repartitioning experiment where the number of partitions stays
     * the same on repartitioning.
     */
    def repartitionExperiment(sc:SparkContext, numRecords:Int, recordSize:Int, numPartitions:Int, numKeys:Int, numTrialsPerSeed:Int=10, numseeds:Int=1, startseed:Int=1): Array[(Int, Array[(Int, Int)])] = {
      return repartitionResizeExperiment(sc, numRecords, recordSize, numPartitions, numPartitions, numKeys, numTrialsPerSeed, numseeds, startseed)
    }

    
    /**
     * Perform a repartitioning experiment.
     */
    def repartitionResizeExperiment(sc:SparkContext, numRecords:Int, recordSize:Int, numPartitions1:Int, numPartitions2:Int, numKeys:Int, numTrialsPerSeed:Int=10, numseeds:Int=1, startseed:Int=1): Array[(Int, Array[(Int, Int)])] = {

      val numMovedKeys = new Array[Array[Int]](numseeds)
      val numMovedRecords= new Array[Array[Int]](numseeds)

      for (s <- 0 until numseeds) {
          val bmrdd1 = getBigKeyedRandomRdd(sc, numRecords, recordSize, numPartitions1, numKeys, seed=(s+startseed)).persist(DISK_ONLY)
          bmrdd1.count
          val bmgrdd1 = bmrdd1.groupBy(_._1).persist(DISK_ONLY)
          bmgrdd1.count  // force the creation of the RDD

          val ghosts1 = getGroupHosts(bmgrdd1).map(x => (x.key,x.execId)).toMap
          val groupSizes = getGroupHosts(bmgrdd1).map(x => (x.key,x.size)).toMap

          numMovedKeys(s) = new Array[Int](numTrialsPerSeed)
          numMovedRecords(s) = new Array[Int](numTrialsPerSeed)

          for (i <- 0 until numTrialsPerSeed) {
              val bmgrdd2 = bmgrdd1.repartition(numPartitions2).persist(DISK_ONLY)
              bmgrdd2.count
              val ghosts2 = getGroupHosts(bmgrdd2).map(x => (x.key,x.execId)).toMap
              val movedKeys = ghosts1.map( x => (x._1, Seq(x._2,ghosts2(x._1))))
              numMovedKeys(s)(i) = movedKeys.toList.map(x => x._2).map(x => if (x.head==x.last) 0 else 1).sum
              numMovedRecords(s)(i) = movedKeys.toList.map(x => if (x._2.head==x._2.last) 0 else groupSizes(x._1)).sum

              bmgrdd2.unpersist()
          }

          bmrdd1.unpersist()
          bmgrdd1.unpersist()
          numMovedKeys(s)
          numMovedRecords(s)
      }

      (0 until numseeds).toArray.map( s => ((s+startseed), numMovedKeys(s).zip(numMovedRecords(s))))
    }
    
    
    /**
     * Perform a groupBy shuffle experiment.
     */
    def groupbyExperiment(sc:SparkContext, numRecords:Int, recordSize:Int, numPartitions:Int, numKeys:Int, numseeds:Int=1, startseed:Int=1): (Array[Array[Int]],Array[Array[Int]]) = {
      val numExecutors = sc.statusTracker.getExecutorInfos.length - 1
      println("numExecutors "+numExecutors)
      val numRecordsIn = new Array[Array[Int]](numseeds)
      val numRecordsOut = new Array[Array[Int]](numseeds)
      
      for (s <- 0 until numseeds) {
          numRecordsIn(s) = new Array[Int](numExecutors)
          numRecordsOut(s) = new Array[Int](numExecutors)
          
          val bmrdd = getBigKeyedRandomRdd(sc, numRecords, recordSize, numPartitions, numKeys, seed=(s+startseed)).persist(DISK_ONLY)
          bmrdd.count
          // for each executor we want to know how many records with each key it holds
          val khosts = getRddKeyHosts(bmrdd).map( x => ((x.execId, x.key), x.size) ).toMap
          
          val bmgrdd = bmrdd.groupBy(_._1).persist(DISK_ONLY)
          bmgrdd.count
          val ghosts = getGroupHosts(bmgrdd).map(x => (x.key,(x.execId,x.size))).toMap
          
          for (k <- 0 until numKeys) {
            for (ex <- 0 until numExecutors) {
              // if we are the executor holding this key after grouping,
              // how many records with this key did we have to pull from elsewhere
              
              numRecordsIn(s)(ex) += (if (ghosts.getOrElse(k,(-1,-1))._1 == ex) (ghosts(k)._2 - khosts.getOrElse((ex,k),0)) else 0)
              numRecordsOut(s)(ex) += (if (ghosts.getOrElse(k,(-1,-1))._1 == ex) 0 else khosts.getOrElse((ex,k),0))
            }
          }
          
          bmrdd.unpersist()
          bmgrdd.unpersist()
      }
      
      (numRecordsIn, numRecordsOut)
    }
}

