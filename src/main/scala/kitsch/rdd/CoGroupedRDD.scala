package kitsch.rdd

import kitsch.util.collection.CompactBuffer

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/3.
 */
private[kitsch] class CoGroupedRDD[K: ClassTag](
                                                @transient var rdds: Seq[RDD[_ <: Product2[K, _]]])
  extends RDD[(K, Seq[Iterable[_]])](rdds.head.kitsch) {

  private type CoGroup = CompactBuffer[Any]
  private type CoGroupValue = (Any, Int)  // Int is the idx of the rdd in the cogroup
  private type CoGroupCombiner = Seq[CoGroup]

  override def compute(): Iterator[(K, Seq[Iterable[_]])] = {
    val numRdds = rdds.length

    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((rdd, depNum) <- rdds.zipWithIndex) {
      rddIterators += ((rdd.compute(), depNum))
    }

    val map = HashMap[K, CoGroupCombiner]()

    val createCombiner: (() => CoGroupCombiner) = () => {
      val newCombiner = Seq.fill(numRdds)(new CoGroup)
      newCombiner
    }
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
        combiner(value._2) += value._1
        combiner
      }

    for ((it, rddIdx) <- rddIterators) {
      it.foreach { pair =>
        mergeValue(map.getOrElseUpdate(pair._1, createCombiner()), new CoGroupValue(pair._2, rddIdx))
      }
    }

    map.iterator
  }
}
