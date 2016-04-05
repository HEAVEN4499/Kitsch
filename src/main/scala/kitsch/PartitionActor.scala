package kitsch

import akka.actor.{ActorLogging, Actor}
import akka.util.Timeout
import kitsch.exception.KitschException

import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/4.
 */
class PartitionActor[T: ClassTag](seq: Seq[T]) extends Actor with ActorLogging {
  implicit val timeout = Timeout(10 second)

  def receive = {
    case _ => throw new KitschException("Get unknown message")
  }
}
