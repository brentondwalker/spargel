package spargel;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.concurrent.CountDownLatch
import org.apache.spark.scheduler
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkEnv
import org.apache.spark.BarrierTaskContext

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.MissingOptionException;

import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.distribution.WeibullDistribution;

import scala.math.random
import scala.collection.mutable.ListBuffer


object BarrierPollingTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("test")
      //.set("spark.cores.max", "2")
      .set("spark.executor.cores", "1")
      .set("spark.executor.instances", "2")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
    //sc.setLogLevel("WARN")
    val numJobs = 30
    val numTasks = 2

    // execute one job, so the executor(s) get(s) created
    sc.parallelize(1 to numTasks, numTasks).barrier().mapPartitions { i =>
      Thread.sleep(1000)
      Iterator(1)
    }.count()

    // queue up a bunch of jobs to make sure that the BEM jobs execute immediately
    val threadList = ListBuffer[Thread]()
    var doneSignal: CountDownLatch = new CountDownLatch(numJobs)
    val startTime = java.lang.System.currentTimeMillis()
    for (jobNum <- 0 to numJobs) {
      println("submitting job: " + jobNum)
      val t = new Thread(new Runnable {
        def run(): Unit = {
          val jobId = jobNum
          sc.parallelize(1 to numTasks, numTasks).barrier().mapPartitions { i =>
            // make the first job take some time, so the other jobs queue behind it.
            if (jobId == 0) {
              Thread.sleep(3000)
            }
            Iterator(1)
          }.count()
          doneSignal.countDown()
        }
      })
      threadList += t;
      t.start()
      // make sure the first job actually starts first
      if (jobNum == 0) {
        Thread.sleep(1000)
      }
    }
    //Thread.sleep(2000)

    println("*** waiting for jobs to finish... ***")
    doneSignal.await()
    for (t <- threadList) {
      t.join()  // unnecessary?
    }
    val stopTime = java.lang.System.currentTimeMillis()
    // could create a listener and look at the times between tasks manually, or
    // just look at the total time it takes to complete all the jobs.
    println("\n\n *******************************")
    println("startTime: "+startTime+"\tstopTime: "+stopTime+"\telapsed: "+(stopTime-startTime))
    println(" *******************************\n\n")

    // queue up one super-long job so we can inspect what jost happened with the app running
    //sc.parallelize(1 to numTasks, numTasks).barrier().mapPartitions { i =>
      //Thread.sleep(5*60000)
    //  Iterator(1)
    //}.count()

  }

}