package kitsch.util

import java.util.EventListener

import kitsch.TaskContext

/**
 * Created by wulicheng on 15/12/22.
 */
trait TaskCompletionListener extends EventListener {
  def onTaskCompletion(context: TaskContext)
}

private[kitsch] class TaskCompletionListenerException(info: Seq[String]) extends Exception {

  override def getMessage: String =
    if (info.size == 1) info.head
    else info.zipWithIndex.map {case (msg, i) => s"Exception $i: $msg"}.mkString("\n")
}
