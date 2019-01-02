package spargel

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.storage.StorageLevel._
import RddPartitioner._
import logging.LogListener
import org.apache.spark.sql.SparkSession
import scala.math.random

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
      /**
        * Adding spargel jar to spark config seems to be necessary
        * to be able using functions of it in executors.
        */
      val sparkSession = SparkSession.builder
        .master("spark://172.23.27.10:7077")
        .appName("RddPartitioner")
        .config("spark.cores.max", "100")
        .config("spark.eventLog.enabled", "true")
        .config("spark.executor.memory", "10g")
        .config("spark.jars", "target/scala-2.11/spargel_2.11-1.0.jar")
        .getOrCreate()
      val logListener = new LogListener
      sparkSession.sparkContext.addSparkListener(logListener)
      import sparkSession.implicits._

      val myrdd = getBigZeroRdd(sparkSession.sparkContext, 10, 1).persist(DISK_ONLY)

      myrdd.getNumPartitions
      val myparts = myrdd.partitions
      val p = myparts(0)
      myrdd.preferredLocations(p)

      printPartitionHosts(myrdd)
      val workloads = Map("0" -> ByteArrayWorkloads.timedRandomMatrixWorkloadGenerator(2500),
                          "1" -> ByteArrayWorkloads.timedRandomMatrixWorkloadGenerator(5000),
                          "2" -> ByteArrayWorkloads.RandomElementWorkload)
      WorkloadRunners.hybridWorkloader(myrdd, workloads, ByteArrayWorkloads.RandomElementWorkload)
        .groupBy(_._2).collect.foreach(x => { println("\nExecutor: "+x._1);  x._2.foreach(println) })

      // ------------------------------------------------------------------------------------------

      val partiton_size = 1024*1024*128
      val num_partitions = 10

      val mybigrdd = getBigZeroRdd(sparkSession.sparkContext, num_partitions, partiton_size).persist(DISK_ONLY)

      printPartitionHosts(mybigrdd)
      WorkloadRunners.hybridWorkloader(mybigrdd, workloads, ByteArrayWorkloads.RandomElementWorkload)
        .groupBy(_._2).collect.foreach(x => { println("\nExecutor: "+x._1);  x._2.foreach(println) })

    }
}

