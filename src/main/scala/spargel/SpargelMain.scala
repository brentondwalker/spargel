package spargel

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel._
import RddPartitioner._

/**
 * Example of how to use Spargel.
 */
object SpargelMain {
    /*
     * Get the current SparkSession when this object is instantiated.
     */
    //val spark = SparkSession.builder().getOrCreate()
    
    /**
     * main()
     */
    def main(args: Array[String]) {
      val numcores = 100
      
      val conf = new SparkConf()
	    .setAppName("SpargelMain")
	    .set("spark.cores.max", "100")
	    val sc = new SparkContext(conf)
      sc.getConf.get("spark.locality.wait")
            
      val myrdd = getBigZeroRdd(sc, 10, 1).persist(DISK_ONLY)
      
      myrdd.getNumPartitions
      val myparts = myrdd.partitions
      val p = myparts(0)
      myrdd.preferredLocations(p)
      
      printPartitionHosts(myrdd)
      val workloads = Map("0" -> ByteArrayWorkloads.timedRandomMatrixWorkloadGenerator(2500),
                          "1" -> ByteArrayWorkloads.timedRandomMatrixWorkloadGenerator(5000),
                          "2" -> ByteArrayWorkloads.RandomElementWorkload)
      WorkloadRunners.hybridWorkloader(myrdd, workloads, ByteArrayWorkloads.RandomElementWorkload).show
      
      // ------------------------------------------------------------------------------------------
      
      val partiton_size = 1024*1024*1024
      val num_partitions = 10
      
      val mybigrdd = getBigZeroRdd(sc, num_partitions, partiton_size).persist(DISK_ONLY)
      
      printPartitionHosts(mybigrdd)
      WorkloadRunners.hybridWorkloader(mybigrdd, workloads, ByteArrayWorkloads.RandomElementWorkload).show
      
    }
}

