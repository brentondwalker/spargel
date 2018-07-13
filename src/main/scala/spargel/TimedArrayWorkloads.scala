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

/**
 * Workloads that run for a fixed length of time on each entry.
 * The actual workloads have to be defined as functions so they can be
 * serialized and used in map operations.
 */
object TimedArrayWorkloads extends Serializable {
  
  
    /**
     * Intended to run on a big array for a controlled amount of time.
     * Generates random points in a 2x2 square centered at the origin.
     * Does not touch the RDD at all.
     */
    val randomSquareWorkload:(((Int,Array[Byte]), Int) => Unit) = {
      (x:(Int,Array[Byte]), runtime:Int) => {
          val startTime = java.lang.System.currentTimeMillis()
          val targetStopTime = startTime + 1000*(runtime)

          while (java.lang.System.currentTimeMillis() < targetStopTime) {
          val xx = random * 2 - 1
          val yy = random * 2 - 1
        }
      }
    }
    
  
    /**
     * Intended to run on a big array for a controlled amount of time.
     * Fills in the entries with random bytes for the specified amount of time.
     * Returns the partition ID and hostname, so the modified array actually is
     * discarded.
     */
    val randomMatrixWorkload:(((Int,Array[Byte]), Int) => Unit) = {
      (x:(Int,Array[Byte]), runtime:Int) => {
        val startTime = java.lang.System.currentTimeMillis()
        val targetStopTime = startTime + 1000*(runtime)
        val arr = x._2
        val arraySize = arr.length
        
        while (java.lang.System.currentTimeMillis() < targetStopTime) {
          arr(scala.util.Random.nextInt(arraySize)) = (scala.util.Random.nextInt(256) - 128).toByte
        }
      }
    }
    
    
    
}