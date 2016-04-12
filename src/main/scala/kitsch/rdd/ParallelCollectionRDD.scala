package kitsch.rdd

import kitsch.Kitsch
import kitsch.partition.Partition

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/2.
 */
private[kitsch] class ParallelCollectionRDD[T: ClassTag](kitsch: Kitsch, @transient private val data: Seq[Future[Partition[T]]])
  extends RDD[T](kitsch) {
//  override def compute(): Iterator[T] = data.iterator
  override def compute(): Seq[Future[Partition[T]]] = data
}
