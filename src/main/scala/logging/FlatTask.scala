package logging

case class FlatTask(taskId: Long, stageId: Long, host: String, local: Boolean,
                    sojournTime: Double, waitingTime:Double, serviceTime: Double,
                    deserializationTime: Double, schedulerOverhead: Double,
                    runtime: Double, readTime:Double, locationExecId: String,
                    readType: String, cachedPartition: Boolean) {
  override def toString = {
    ("taskId:"+taskId+" stageId:"+stageId+" host:"+host+" local:"+local+" sojournTime:"+sojournTime 
      +" waitingTime:"+waitingTime+" serviceTime:"+serviceTime+" deserializationTime:"+deserializationTime
      +" schedulerOverhead:"+schedulerOverhead+"runtime:"+runtime)
  }
}
