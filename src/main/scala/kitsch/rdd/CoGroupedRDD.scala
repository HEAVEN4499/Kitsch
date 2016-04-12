package kitsch.rdd

import kitsch.partition.Partition
import kitsch.util.collection.CompactBuffer

import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/3.
 */
private[kitsch] class CoGroupedRDD[K: ClassTag](@transient var rdds: Seq[RDD[_ <: Product2[K, _]]])
  extends RDD[(K, Seq[Iterator[_]])](rdds.head.kitsch) {

  private type T = Product2[K, _]
  private type CoGroup = CompactBuffer[Any]
  private type CoGroupValue = (T, Int)  // Int is the idx of the rdd in the cogroup
  private type CoGroupCombiner = Seq[CoGroup]

  override def compute(): Seq[Future[Partition[(K, Seq[Iterator[_]])]]] = {
    val numRdds = rdds.length
    val rddIterators = rdds.zipWithIndex.map {
      case (rdd, depNum) => {
        val partitions = Await.result(Future.sequence(rdd.compute()), Duration.Inf)
        partitions.map { p =>
          (Await.result(p.data, Duration.Inf).asInstanceOf[Iterator[Product2[K, T]]], depNum)
        }
      }
    }

    val hashMap = HashMap[K, CoGroupCombiner]()

    val createCombiner: () => CoGroupCombiner =
      () => Seq.fill(numRdds)(new CoGroup)

    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
        combiner(value._2) += value._1
        combiner
      }

    for (seq <- rddIterators) {
      for ((iter, rddIndex) <- seq) {
        iter foreach { pair =>
          mergeValue(hashMap.getOrElseUpdate(pair._1, createCombiner()),
            new CoGroupValue(pair._2, rddIndex))
        }
      }
    }

    kitsch.parallelize(hashMap.map{
      case (k, v) => (k, v.map(_.iterator))
    }.toSeq).compute
  }
}
