package kitsch.rdd

import kitsch.partition.Partition

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/3/29.
 */
private[kitsch] class MapPartitionsRDD[
  U: ClassTag,
  T: ClassTag](prev: RDD[T],
               func: Iterator[T] => Iterator[U])
  extends RDD[U](prev) {
//  override def compute(): Iterator[U] = f(prev.iterator())
  override def compute(): Seq[Future[Partition[U]]] =
    prev.iterator map {
      future => future.map(f => f.iterator(func))
    }
}
