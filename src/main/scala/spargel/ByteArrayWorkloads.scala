package spargel

import Workloads._

/**
 * Workloads that process an Array[Byte].  Some have their runtime set by a
 * function argument, and others' runtime will depend on the size and contents
 * of the Array[Byte].
 */
object ByteArrayWorkloads extends Serializable {

  /**
   * The signature of a ByteArrayWorkload.  It is a function that takes
   * an Int and an Array[Byte] and returns an Int.  There is no general
   * meaning to the return value; it depends on the particular workload.
   * 
   * The Int argument is not currently used in any of the workloads.
   * 
   * The Array[Byte] is the object to be processed by the workload.  The
   * workload's runtime and memory may therefore depend on the size and content
   * of the Array.
   * 
   */
  type ByteArrayWorkload = Workload[(Int,Array[Byte]), Int]
  
  
  /**
   * A function that returns a ByteArrayWorkload.  The idea is that the function
   * returned is a workload that will run for a specified number of
   * milliseconds.  During that time it will do some workload-specific
   * processing of the supplied Array[Byte].
   */
  type TimedByteArrayWorkload = TimedWorkload[(Int,Array[Byte]), Unit]
  
  /**
   * A TimedByteArrayWorkloadGenerator takes an integer argument and returns
   * a workload that will run for the specified number ot milliseconds.
   * 
   * From the user's perspective, the difference between TimedByteArrayWorkload and
   * TimedByteArrayWorkloadGenerator is when the run time needs to be specified.
   * With TimedByteArrayWorkload you get a function that takes its runtime as an
   * argument.  With TimedByteArrayWorkloadGenerator you get a Workload function
   * that runs for a specified amount of time.
   * 
   * XXX: Do we really need both of these types?
   */
  type TimedByteArrayWorkloadGenerator = (Int => ByteArrayWorkload)
  
    
    /**
     * Iterate through the array once to find the maximum value and returns
     * that value as an Int.
     */
    val IterativeMaxWorkload:ByteArrayWorkload = {
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
    val SortedWorkload:ByteArrayWorkload = {
      (rec) => {
        rec._2.sorted.head.toInt
      }
    }
    
    
    /**
     * Do something quadratic.
     */
    val QuadraticCompareWorkload:ByteArrayWorkload = {
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
    val RandomElementWorkload:ByteArrayWorkload = {
      (rec) => {
        val arr = rec._2
        arr(scala.util.Random.nextInt(arr.length)).toInt
      }
    }
    
    
    /**
     * Return a random element of the array by doing a random walk with
     * logarithmic steps.
     */
    val LogRandomWalkWorkload:ByteArrayWorkload = {
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
     * Intended to run on a big array for a controlled amount of time.
     * Fills in the entries with random bytes for the specified amount of time.
     * Returns the partition ID and hostname, so the modified array actually is
     * discarded.
     * 
     * type TimedWorkload[A,B] = ((A, Int) => B)
     */
    val timedRandomMatrixWorkload:TimedByteArrayWorkload = {
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
    
    
    /**
     * Returns a ByteArrayWorkload function that will run for the specified
     * number of milliseconds.
     */
    val timedRandomMatrixWorkloadGenerator:TimedByteArrayWorkloadGenerator = {
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