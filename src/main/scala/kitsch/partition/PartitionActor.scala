package kitsch.partition


import akka.actor.{TypedActor, TypedProps}
import kitsch.Kitsch

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/7.
 */
class PartitionActor[T: ClassTag](val kitsch: Kitsch,
                                  val iter: Future[Iterator[T]])
  extends Partition[T] {

  val id = Kitsch.newPartitionId
  lazy val array = iter.map(_.toArray)

  def getId = id

  def data = array.map(_.iterator)

  def zip[U: ClassTag](other: Partition[U]): Future[Iterator[(T, U)]] = {
    println(s"[Kitsch-Partition] $this.zip")
    data zip other.data map {
      case (s, o) => s zip o
    }
  }

  def iterator[U: ClassTag](func: Iterator[T] => Iterator[U]): Partition[U] = {
    println(s"[Kitsch-Partition] $this.iterator")
    PartitionActor.props(kitsch, data map {
      i => func(i)
    })
  }
}

object PartitionActor {
  def props[T: ClassTag](kitsch: Kitsch, iterator: Future[Iterator[T]]) =
    TypedActor(kitsch.system)
      .typedActorOf(
        TypedProps(classOf[Partition[T]],
          new PartitionActor(kitsch, iterator)))
}