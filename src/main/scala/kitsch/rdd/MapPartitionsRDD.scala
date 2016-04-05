package kitsch.rdd

import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/3/29.
 */
private[kitsch] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
                                                                  prev: RDD[T],
                                                                 f: Iterator[T] => Iterator[U])
  extends RDD[U](prev) {
  override def compute(): Iterator[U] = f(prev.iterator())
}
