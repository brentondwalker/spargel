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
object ByteArrayWorkloads extends Serializable {
    
    
    /**
     * Iterate through the array once to find the maximum value and returns
     * that value as an Int.
     */
    val IterativeMaxWorkload:(((Int,Array[Byte])) => Int) = {
      (rec) => {
        var mx:Byte = Byte.MinValue
        rec._2.foreach(x => mx = if (x > mx) x else mx)
        mx.toInt
      }
    }
    
    
    /**
     * Apply the built-in sorted method to the array data.  It will be somewhere
     * between linear and quadratic.  Assuming it doesn't realize it can
     * bucketsort.
     */
    val SortedWorkload:(((Int,Array[Byte])) => Int) = {
      (rec) => {
        rec._2.sorted.head.toInt
      }
    }
    
    
    /**
     * Do something quadratic.
     */
    val QuadraticCompareWorkload:(((Int,Array[Byte])) => Int) = {
      (rec) => {
        val arr = rec._2
        var ct:Int = 0
        var i:Int = 0
        var j:Int = 0
        for (i <- 0 until arr.length) {
          for (j <- (i+1) until arr.length) {
            if (arr(i) > arr(j)) {
              ct += 1
            } else if (arr(i) < arr(j)) {
              ct -= 1
            }
          }
        }
        ct
      }
    }
    
    
    /**
     * Just return a random element of the array.
     * This is a constant-time operation.
     */
    val RandomElementWorkload:(((Int,Array[Byte])) => Int) = {
      (rec) => {
        val arr = rec._2
        arr(scala.util.Random.nextInt(arr.length)).toInt
      }
    }
    
    
    /**
     * Return a random element of the array by doing a random walk with
     * logarithmic steps.
     */
    val LogRandomWalkWorkload:(((Int,Array[Byte])) => Int) = {
      (rec) => {
        val arr = rec._2
        var x:Int = arr.length
        var i:Int = arr.length/2
        var sum = 0
        while (x > 0) {
          if (scala.util.Random.nextDouble() < 0.5) {
            i += x
          } else {
            i -= x
          }
          if (i > arr.length) {
            i = arr.length - 1
          } else if (i < 0) {
            i = 0
          }
          sum += arr(i)
          x /= 2
        }
        arr(scala.util.Random.nextInt(arr.length)).toInt
      }
    }
    
    
    /**
     * Returns a
     * ByteArrayWorkload function that will run for the specified number
     * of milliseconds.  The resulting function has the type:
     * (((Int,Array[Byte])) => Int) like all other ByteArrayWorkload functions.
     */
    val timedRandomMatrixWorkload:Int => (((Int,Array[Byte])) => Int) = {
      t => {
        (x:(Int,Array[Byte])) => {
          val startTime = java.lang.System.currentTimeMillis()
          val targetStopTime = startTime + t
          val arr = x._2
          val arraySize = arr.length
        
          while (java.lang.System.currentTimeMillis() < targetStopTime) {
            arr(scala.util.Random.nextInt(arraySize)) = (scala.util.Random.nextInt(256) - 128).toByte
          }
          arr.head.toInt
        }
      }
    }
    
    
}