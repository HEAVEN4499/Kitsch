package kitsch.partition

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/4.
 */
trait Partition[T] {
  def getId(): Int

  def data(): Future[Iterator[T]]

  def zip[U: ClassTag](other: Partition[U]): Future[Iterator[(T, U)]]

  def iterator[U: ClassTag](func: Iterator[T] => Iterator[U]): Partition[U]
}