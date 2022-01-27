package spargel

import Workloads._
//import scala.reflect.ClassTag
import scala.util.Sorting._

/**
 * This object contains workloads that can run on RDDs of type
 * RDD[(key:Long, Array[Array[Byte]])]
 * These RDDs are the result of creating a key/value RDD of type RDD[(Long,Array[Byte])],
 * executing a groupByKey, and then converting the Iterables to Array.
 */
object KeyedRecordsWorkloads {

  /**
   * The signature of a KeyedRecordsWorkload.  It is a function that takes
   * a long and an Array[Array[Byte]] and returns a Long.  There is no general
   * meaning to the return value; it depends on the particular workload.
   *
   * The Array[Array[Byte]] is the object to be processed by the workload, and the
   * inner Array[Byte] are the elements.  The workload's runtime and memory may
   * therefore depend on the size and content of the Array.
   *
   */
  type KeyedRecordsWorkload = Workload[(Int,Array[Array[Byte]]), Long]

  /**
   * Compare two arrays of bytes as if they were the Bytes of an
   * arbitrary-sized big-endian integer.
   *
   * Returns 1 if a>b, -1 if a<b, 0 if a==b.
   *
   * @param a
   * @param b
   * @return
   */
  def compareRecords(a:Array[Byte], b:Array[Byte]): Boolean = {
    var aa = a
    var bb = b

    // normalize the inputs fo that aa.length >= bb.length
    var inversion_factor = true
    if (aa.length < bb.length) {
      aa = b
      bb = a
      inversion_factor = false
    }

    // if the lengths are not equal, check if any of the
    // non-overlapping bytes are non-zero
    val offset = aa.length - bb.length
    for (i <- 0 until offset) {
      if (aa(i) > 0) return !inversion_factor
      if (aa(i) < 0) return inversion_factor
    }

    // compare the overlapping bytes
    for (i <- offset until aa.length) {
      if (aa(i+offset) > bb(i)) return !inversion_factor
      if (aa(i+offset) < bb(i)) return inversion_factor
    }

    return false
  }

  /**
   * Iterate through the array once to find the maximum value and returns
   * that value as an Int.
   */
  val IterativeMaxWorkload:KeyedRecordsWorkload = {
    (rec) => {
      var mx:Array[Byte] = rec._2(0).clone()
      rec._2.foreach(x => mx = if (compareRecords(mx, x)) x.clone() else mx)
      mx(mx.length-1).toLong
    }
  }

  /**
   * Apply the built-in stableSort method to the array data.  It will be somewhere
   * between linear and quadratic.
   */
  val SortedWorkload:KeyedRecordsWorkload = {
    (rec) => {
      stableSort(rec._2, compareRecords(_,_)) //(ClassTag[Array[Array[Byte]]])
      0
    }
  }
  
}
