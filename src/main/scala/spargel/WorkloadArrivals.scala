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


object WorkloadArrivals {
  
  
  /**
   * Parse command line arguments using commons-cli 1.2
   * We have to use 1.2 even though it clunky in scala, because it is used
   * in Spark.  If we try to use 1.3 or later the packages will conflilct.
   * 
   */
  def parseArgs(args: Array[String]): CommandLine = {
    val cli_options: Options = new Options();
		//cli_options.addOption("h", "help", false, "print help message");
		cli_options.addOption("w", "numworkers", true, "number of workers/servers");
		cli_options.addOption("t", "numtasks", true, "number of tasks per job");
		cli_options.addOption("n", "numsamples", true, "number of jobs to run");
		cli_options.addOption("s", "sequential-jobs", false, "submit the jobs sequentially instead of from separate threads");
		cli_options.addOption("p", "persisted-rdd", false, "jobs are tied to particular executors, subject to spark.locality.wait configuration");

		// I'm trying to re-use code here, but using OptionBuilder from commons-cli 1.2
		// in scala is problematic because it has these static methods and the have to
		// be called on the class in scala.
		OptionBuilder.isRequired();
		OptionBuilder.hasArgs();
		OptionBuilder.withDescription("arrival process");
		OptionBuilder.withLongOpt("arrivalprocess");
		cli_options.addOption(OptionBuilder.create("A"));
		OptionBuilder.isRequired();
		OptionBuilder.hasArgs();
		OptionBuilder.withDescription("service process");
		OptionBuilder.withLongOpt("serviceprocess");
		cli_options.addOption(OptionBuilder.create("S"));
		
		val parser: CommandLineParser = new PosixParser();
		var options: CommandLine = null;
		try {
			options = parser.parse(cli_options, args);
		} catch {
		case e: MissingOptionException => {
			System.err.println("\nERROR: Missing a required option!\n")
			val formatter: HelpFormatter = new HelpFormatter();
			formatter.printHelp(this.getClass.getSimpleName, cli_options);
			//e.printStackTrace();
			System.exit(0);
		}
		case e: ParseException => {
			System.err.println("ERROR: ParseException")
			val formatter: HelpFormatter = new HelpFormatter();
			formatter.printHelp(this.getClass.getSimpleName, cli_options);
			e.printStackTrace();
			System.exit(0);
		}
		}
		options
  }
  
  
  /**
   * Take the command line spec for a random intertime process and 
   * return a function that generates samples.
   * Maybe I am getting carried away with the scala functional stuff.
   * 
   * TODO: Normal?
   */
  def parseProcessArgs(args: Array[String]): () => Double = {
    if (args.length == 0) {
      () => 1.0
    } else {
    	args(0) match {

    	  // constant rate process
    	  case "c" if (args.length == 2) => {
    	    val rate = args(1).toDouble
    	    ( () => 1.0/rate )
    	  }
    	  
    	  // exponential/Poisson process
    	  case "x" if (args.length == 2) => {
    	    val rate = args(1).toDouble
    	    ( () => -Math.log(random)/rate )
    	  }
    	  
    	  // Erlang-k process
    	  case "e" if (args.length == 3) => {
    	    val k = args(1).toInt
    	    val rate = args(2).toDouble
    	    ( () => {
    	    	var p = 1.0
    	    	for ( i <- 1 to k) {
    	    		p *= random
    	    	}
    	    	-Math.log(p)/rate
    	    } )
    	  }
    	  
    	  // Weibull
    	  case "w" if (args.length == 2 || args.length == 3) => {
    	    val shape = args(1).toDouble
    	    var scale = 1.0
    	    if (args.length == 3) {
    	      scale = args(2).toDouble
    	    } else {
    	      scale = 1.0/Gamma.gamma(1.0 + 1.0/shape);
    	    }
    	    val weibul = new WeibullDistribution(shape, scale)
    	    ( () => weibul.sample() )
    	  }
    	  
    	  // otherwise
    	  case _ => {
    	    println("\n\nWARNING: unrecognized random process spec: \""+args.mkString(" ")+"\"  Using constant rate of 1.0\n\n")
    	    () => 1.0
    	  }
    	}
    }
  }
  
  /**
   * Run s slices on the Spark cluster, with service times drawn
   * from the given serviceProcess.
   * 
   * Within each slice we just generate random numbers for the specified amount of time.
   * This is from the SparkPi demo program, generating random numbers in a square.
   * Each slice returns 1, and we do a count() to force Spark to execute the slices.
   * Therefore the shuffle/reduce step is trivial.
   * 
   * Note: the stdout produced from these println() will appear on the stdout of the workers,
   *       not the driver.  I could just as well remove it.
   *       
   *  Note: I would like to pass in the serviceProcess instead of a list of serviceTimes, but since
   *        this is parallelized I ran into the problem that in some cases we would be passing
   *        identical RNGs to the workers, and generating identical service times.
   */
  def runEmptySlices(spark:SparkContext, slices: Int, serviceTimes: List[Double], jobId: Int): Long = {
    //println("serviceTimes = "+serviceTimes)
    spark.parallelize(1 to slices, slices).map { i =>
      val taskId = i
      val jobLength = serviceTimes(i-1)
      val startTime = java.lang.System.currentTimeMillis()
      val targetStopTime = startTime + 1000*jobLength
			println("    +++ TASK "+jobId+"."+taskId+" START: "+startTime)
			while (java.lang.System.currentTimeMillis() < targetStopTime) {
				val x = random * 2 - 1
				val y = random * 2 - 1
			}

      val stopTime = java.lang.System.currentTimeMillis()
      println("    --- TASK "+jobId+"."+taskId+" STOP: "+stopTime)
      println("    === TASK "+jobId+"."+taskId+" ELAPSED: "+(stopTime-startTime))
      1
    }.count()
  }
  
    /**
   * Run s slices on the Spark cluster, with service times drawn
   * from the given serviceProcess.
   * 
   * Within each slice we just generate random numbers for the specified amount of time.
   * This is from the SparkPi demo program, generating random numbers in a square.
   * Each slice returns 1, and we do a count() to force Spark to execute the slices.
   * Therefore the shuffle/reduce step is trivial.
   * 
   * Note: the stdout produced from these println() will appear on the stdout of the workers,
   *       not the driver.  I could just as well remove it.
   *       
   *  Note: I would like to pass in the serviceProcess instead of a list of serviceTimes, but since
   *        this is parallelized I ran into the problem that in some cases we would be passing
   *        identical RNGs to the workers, and generating identical service times.
   */
  def runEmptyPersistedSlices(r:RDD[Int], serviceTimes:List[Double], jobId:Int): Long = {
    //println("serviceTimes = "+serviceTimes)
    r.map { i =>
      val taskId = i
      val jobLength = serviceTimes(i-1)
      val startTime = java.lang.System.currentTimeMillis()
      val targetStopTime = startTime + 1000*jobLength
			println("    +++ TASK "+jobId+"."+taskId+" START: "+startTime)
			while (java.lang.System.currentTimeMillis() < targetStopTime) {
				val x = random * 2 - 1
				val y = random * 2 - 1
			}

      val stopTime = java.lang.System.currentTimeMillis()
      println("    --- TASK "+jobId+"."+taskId+" STOP: "+stopTime)
      println("    === TASK "+jobId+"."+taskId+" ELAPSED: "+(stopTime-startTime))
      1
    }.count()
  }
  
  
  /**
   * In order to make the system behave like traditional Fork-Join, we need to
   * create a persisted RDD that we can run maps on.
   */
  def createPersistedRdd(spark:SparkContext, slices:Int): RDD[Int] = {
    val r = spark.parallelize(1 to slices, slices).persist(StorageLevel.MEMORY_ONLY)
    
    //TODO: check out which host each of the partitions is on
    val partHosts = r.map( partId => {
      (SparkEnv.get.executorId, partId)
    })
    .groupByKey()
    .map( x => (x._1, x._2.toArray) )
    .collect()
    
    System.out.println("Partition hosts:")
    partHosts.foreach(println)
    
    return r
  }
  
  
  /**
   * A wrapper for the unpersist() function.
   */
  def destroyPersistedRdd(r:RDD[Int]) {
    r.unpersist()
  }
  
    
  /**
   * Main method
   * 
   * Sets up Spark environment ad runs the main loop of the program,
   * sending sliced jobs to the task manager.
   * 
   */
  def main(args: Array[String]) {

	  val options: CommandLine = parseArgs(args);

    val arrivalProcess = parseProcessArgs(options.getOptionValues("A"))
    val serviceProcess = parseProcessArgs(options.getOptionValues("S"))

    val totalJobs = if (options.hasOption("n")) options.getOptionValue("n").toInt else 0
		val slicesPerJob = if (options.hasOption("t")) options.getOptionValue("t").toInt else 1
		val numWorkers = if (options.hasOption("w")) options.getOptionValue("w").toInt else 1
		val serialJobs = options.hasOption("s")
		val persistedRdd = options.hasOption("p")
		
	  val conf = new SparkConf()
	    .setAppName("ThreadedMapJobs")
	    .set("spark.cores.max", numWorkers.toString())
		val spark = new SparkContext(conf)
    spark.setLogLevel("ERROR")
		
		// give the system a little time for the executors to start... 30 seconds?
		// this is stupid b/c the full set of executors actually take less tha 1s to start
		print("Waiting a moment to let the executors start...")
		Thread sleep 1000*10
    		
		// check how many cores were actually allocated
		// one core in the list will be the driver.  The rest should be workers
		// http://stackoverflow.com/questions/39162063/spark-how-many-executors-and-cores-are-allocated-to-my-spark-job
		val coresAllocated = spark.getExecutorMemoryStatus.map(_._1).toList.length - 1
		println("*** coresAllocated = "+coresAllocated+" ***")
    if (coresAllocated != numWorkers) {
      println("ERROR: could only allocate "+coresAllocated+" workers ("+numWorkers+" requested) - exiting.")
      spark.stop()
      System.exit(1)
    }
    
		var jobsRun = 0
		var doneSignal: CountDownLatch = new CountDownLatch(totalJobs)
		val initialTime = java.lang.System.currentTimeMillis()
		var lastArrivalTime = 0L
		var totalInterarrivalTime = 0.0
		val threadList = ListBuffer[Thread]()
		
		// if we persist the RDD, tasks are bound to the executor where their partition lives
		//val r = if (persistedRdd) createPersistedRdd(spark, slicesPerJob) else None
		val r = createPersistedRdd(spark, slicesPerJob)
			
    while (jobsRun < totalJobs) {
			println("")
			
			val t = new Thread(new Runnable {
			  def run() {
				  val jobId = jobsRun
					val startTime = java.lang.System.currentTimeMillis();
				  lastArrivalTime = startTime
					println("+++ JOB "+jobId+" START: "+startTime)
					
					// we would like to pass the serviceProcess in to this method and let the workers
					// generate their own service times, but in some cases they end up generating
					// from identical RNGs, and get the same result.  So we generate the service times here.
					// These are actually generated in different threads, but it's the same process, same JVM.
				  if (persistedRdd) {
				    runEmptyPersistedSlices(r, List.tabulate(slicesPerJob)(n => serviceProcess()), jobId)
				  } else {
  					runEmptySlices(spark, slicesPerJob, List.tabulate(slicesPerJob)(n => serviceProcess()), jobId)
				  }
					
					val stopTime = java.lang.System.currentTimeMillis()
					println("--- JOB "+jobId+" STOP: "+stopTime)
					println("=== JOB "+jobId+" ELAPSED: "+(stopTime-startTime))
					doneSignal.countDown()
				}
			});
			threadList += t;
			jobsRun += 1;
			t.start()
			
			//XXX The way I do this does start the jobs in a Slpit-Merge fashion, but we are effectively 
			//    spoofing the job arrivals.  The jobs aren't actually queued to the Spark system until
			//    right before they start executing.  Therefore, to do any sort of analysis we need to
			//    record the job arrivals separately from the spark eventlog.
			if (serialJobs) {
			  println("waiting done signal...")
			  t.join();
				println("got done signal!")
			}
			
			val curTime = java.lang.System.currentTimeMillis()
			val totalElapsedTime = (curTime- initialTime)/1000.0
			//XXX working on this...
			// need to track total elapsed time in the run, and total used inter-arrival times
			val interarrivalTime = arrivalProcess();
			println("*** inter-arrival time: "+interarrivalTime+" ***")
			totalInterarrivalTime += interarrivalTime
			println("totalInterarrivalTime = "+totalInterarrivalTime+"\t totalElapsedTime = "+totalElapsedTime)
			
			if (totalElapsedTime < totalInterarrivalTime) {
			  println("sleep "+(math.round((totalInterarrivalTime - totalElapsedTime) * 1000.0)))
				Thread sleep math.round((totalInterarrivalTime - totalElapsedTime) * 1000.0)
			}
			
		}
		
		println("*** waiting for jobs to finish... ***")
		doneSignal.await()
		for (t <- threadList) {
		  t.join()  // unnecessary?
		}
		
		destroyPersistedRdd(r)
		
		println("*** FINISHED!! ***")
		if (! spark.isStopped) {
			spark.stop()
		}
  }
  
}

