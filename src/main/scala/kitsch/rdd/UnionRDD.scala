package kitsch.rdd

import kitsch.Kitsch

import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/3/31.
 */
private[kitsch] class UnionRDD[T: ClassTag](kitsch: Kitsch, var rdds: Seq[RDD[T]])
  extends RDD[T](kitsch){
  override def compute(): Iterator[T] = rdds.map(_.iterator()).reduce(_ ++ _)
}
