package spargel


object Workloads {
  
    /**
     * The basic workload type is just a function between A and B.
     * A is the type of the elements of an RDD, and B is the type that the
     * workload will return after processing each element.
     */
    type Workload[A,B] = A => B
    
    /**
     * A timed workload is a function that takes an Int argument and returns a
     * function that will run for the specified number of milliseconds.
     */
    type TimedWorkload[A,B] = (A, Int) => B
      
    /**
     * A timed workload generator is a  function that takes an Int argument
     * and returns a workload that will run for the specified number of
     * milliseconds.
     */
    type TimedWorkloadGenerator[A,B] = Int => TimedWorkload[A,B]
    
}

