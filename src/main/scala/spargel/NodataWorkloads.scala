package spargel

import scala.math.random
import Workloads._

/**
 * Workloads that do not accept or process any external data.
 * They may run for a certain amount of time, or for a certain number of
 * iterations.
 * 
 * The key property of these workloads is that they do not require any RDD
 * elements to be loaded or moved between workers.
 */
object NodataWorkloads extends Serializable {

  /**
   * The signature of a NodataWorkload.  It is a function that takes
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
  type NodataWorkload = Workload[AnyRef, Int]
  
  
  /**
   * A function that returns a ByteArrayWorkload.  The idea is that the function
   * returned is a workload that will run for a specified number of
   * milliseconds.  During that time it will do some workload-specific
   * processing of the supplied Array[Byte].
   */
  type TimedNodataWorkload = TimedWorkload[AnyRef, Unit]
  
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
  type TimedNodataWorkloadGenerator = (Int => NodataWorkload)
  

  /**
   * Generates random points in a 2x2 square centered at the origin.
   */
  val timedRandomSquareWorkload:TimedNodataWorkload  = {
    (x:AnyRef, runtime:Int) => {
      val startTime = java.lang.System.currentTimeMillis()
      val targetStopTime = startTime + 1000*(runtime)
      
      while (java.lang.System.currentTimeMillis() < targetStopTime) {
        val xx = random * 2 - 1
        val yy = random * 2 - 1
      }
    }
  }
    

  
  
  
}

