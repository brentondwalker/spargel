package spargel

import org.apache.spark.SparkContext
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
  
    /**
     * Generate a huge RDD.
     * Parameters specify:
     * - initial number of partitions
     * - number of records
     * - size of each record
     * - number of distinct keys to use
     * 
     * The records are full of zeros, which makes the content very compressible.
     */
    def getBigKeyedZeroRdd(sc:SparkContext, numRecords:Int, recordSize:Int, numPartitions:Int, numKeys:Int, seed:Int=1):RDD[(Int,(Int,Array[Byte]))] = {
      // seed the RNG, in case this RDD gets recomputed we get the same results.
      val rng = new scala.util.Random(seed)
      val mybigrdd = sc.parallelize(1 to numRecords, numPartitions).map { i =>
        i
      }.map( i => (rng.nextInt(numKeys), (i,Array.fill[Byte](recordSize)(0)) ) )

      return mybigrdd
    }
    
    
    def partitonByKeys[A](r:RDD[(Int,(Int,A))]):RDD[(Int, Iterable[(Int, (Int, A))])] =  {
      val ctx = TaskContext.get()
      ctx.partitionId()
      r.groupBy(_._1)
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
        
}

