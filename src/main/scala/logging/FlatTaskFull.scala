package logging

case class FlatTaskFull(taskId: Long, stageId: Long, taskIndex: Int, executorId: String, host: String, local: Boolean,
                        sojournTime: Double, waitingTime:Double, serviceTime: Double,
                        executorDeserializeTime: Long,
                        executorDeserializeCpuTime: Long,
                        executorRunTime: Long,
                        executorCpuTime: Long,
                        resultSize: Long,
                        jvmGcTime: Long,
                        resultSerializationTime: Long,
                        memoryBytesSpilled: Long,
                        diskBytesSpilled: Long,
                        peakExecutionMemory: Long,
                        bytesRead: Long,
                        recordsRead: Long,
                        bytesWritten: Long,
                        recordsWritten: Long,
                        shuffleRemoteBlocksFetched: Long,
                        shuffleLocalBlocksFetched: Long,
                        shuffleTotalBlocksFetched: Long,
                        shuffleFetchWaitTime: Long,
                        shuffleRemoteBytesRead: Long,
                        shuffleLocalBytesRead: Long,
                        shuffleTotalBytesRead: Long,
                        shuffleRemoteBytesReadToDisk: Long,
                        shuffleRecordsRead: Long,
                        shuffleBytesWritten: Long,
                        shuffleWriteTime: Long,
                        shuffleRecordsWritten: Long) {
  override def toString = {
    ("taskId:"+taskId+" stageId:"+stageId+" taskIndex:"+taskIndex+" executorId:"+executorId+" host:"+host+" local:"+local+" sojournTime:"+sojournTime
      +" waitingTime:"+waitingTime+" serviceTime:"+serviceTime+" executorDeserializeTime:"+executorDeserializeTime
      +" executorRunTime:"+executorRunTime+" executorCpuTime:"+executorCpuTime)
  }
}
