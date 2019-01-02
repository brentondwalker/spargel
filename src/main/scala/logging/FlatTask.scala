package logging

case class FlatTask(taskId: Long, stageId: Long, host: String, local: Boolean,
                    sojournTime: Double, waitingTime:Double, serviceTime: Double,
                    deserializationTime: Double, schedulerOverhead: Double,
                    runtime: Double, readTime:Double, locationExecId: String,
                    readType: String, cachedPartition: Boolean) {
}
