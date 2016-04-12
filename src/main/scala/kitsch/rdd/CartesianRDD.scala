package kitsch.rdd

import kitsch.Kitsch
import kitsch.partition.{PartitionActor, Partition}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/3.
 */
private[kitsch] class CartesianRDD[T: ClassTag, U: ClassTag](kitsch: Kitsch,
                                                             var rdd1: RDD[T],
                                                             var rdd2: RDD[U])
  extends RDD[Pair[T, U]](kitsch) with Serializable {

  override def compute(): Seq[Future[Partition[(T, U)]]] =
    rdd1.iterator() zip rdd2.iterator() map {
      case (af, bf) => {
        af zip bf map {
          case (a, b) => PartitionActor.props(kitsch, a zip b)
        }
      }
    }
}
