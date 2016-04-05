package kitsch.rdd

import kitsch.Kitsch

import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/3.
 */
private[kitsch] class CartesianRDD[T: ClassTag, U: ClassTag](kitsch: Kitsch,
                                                             var rdd1: RDD[T],
                                                             var rdd2: RDD[U])
  extends RDD[Pair[T, U]](kitsch) with Serializable {
  override def compute(): Iterator[(T, U)] = {
    for {
      x <- rdd1.iterator();
      y <- rdd2.iterator()
    } yield (x, y)
  }
}
