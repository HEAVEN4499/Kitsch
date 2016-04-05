package kitsch

/**
 * Created by wulicheng on 15/12/22.
 */
object TaskContext {
  def get(): TaskContext = taskContext.get

  def getPartitionId(): Int = {
    val tc = taskContext.get
    if (tc.eq(null)) 0
    else tc.partitionId
  }

  private val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  private[kitsch] def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

  private[kitsch] def remove(): Unit = taskContext.remove()


}

abstract class TaskContext extends Serializable {
  def isCompleted(): Boolean

  def isInterrupted(): Boolean

//  def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext

  def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext

  def stageId(): Int

  def partitionId(): Int

  def attemptNumber(): Int

  def taskAttemptId(): Long
}
