package kitsch.rdd

import kitsch.Kitsch

import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/2.
 */
private[kitsch] class ParallelCollectionRDD[T: ClassTag](kitsch: Kitsch, @transient private val data: Seq[T])
  extends RDD[T](kitsch) {
  override def compute(): Iterator[T] = data.iterator
}
