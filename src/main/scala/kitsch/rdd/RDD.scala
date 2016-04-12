package kitsch.rdd

import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.slf4j.LazyLogging
import kitsch._
import kitsch.exception.KitschException
import kitsch.partition.Partition
import kitsch.util.Utils

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}
import scala.util.Success

abstract class RDD[T: ClassTag](@transient private var _kitsch: Kitsch)
  extends Serializable with LazyLogging {

  implicit val timeout = Timeout(5 second)

  if (classOf[RDD[_]].isAssignableFrom(elementClassTag.runtimeClass))
    logger.warn("Nested RDDs are not supported")

  private[kitsch] def conf = kitsch.conf

  val id = Kitsch.newRddId

  private[kitsch] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](_kitsch)(body)

  def kitsch: Kitsch = {
    if (_kitsch == null)
      throw new KitschException(
        "RDD transformations and actions can only be invoked by the driver, not inside of other")
    _kitsch
  }

  @transient var name: String = null

  def this(@transient oneParent: RDD[_]) = this(oneParent.kitsch)

  def setName(n: String) = {
    name = n
    this
  }

  private[kitsch] def elementClassTag: ClassTag[T] = classTag[T]

  def compute(): Seq[Future[Partition[T]]]

  final def iterator() = {
    compute
  }

  protected def getPartitionNum = ask(_kitsch.server, "routes").mapTo[Int].value match {
    case Some(Success(partitions)) => partitions
    case _                         => 0
  }

  def cache: RDD[T] = kitsch.makeRDD(collect)

  def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U]): RDD[U] =
    withScope {
      new MapPartitionsRDD(this, (iter: Iterator[T]) => f(iter))
    }

  def union(other: RDD[T]): RDD[T] = withScope(new UnionRDD(_kitsch, Array(this, other)))

  def ++(other: RDD[T]): RDD[T] = withScope(this.union(other))

  def map[U: ClassTag](f: T => U): RDD[U] = withScope(
    new MapPartitionsRDD[U, T](this, iter => iter.map(f)))

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope{
    new MapPartitionsRDD[U, T](this, iter => iter.flatMap(f))
  }

  def filter(f: T => Boolean): RDD[T] = withScope{
    new MapPartitionsRDD[T, T](this, iter => iter.filter(f))
  }

  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    new CartesianRDD(kitsch, this, other)
  }

  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope{
    this.map(t => (f(t), t)).groupByKey()
  }

  def foreach(f: T => Unit): Unit = withScope {
    kitsch.runJob(this, (iter: Iterator[T]) => iter.foreach(f))
  }

  def foreachPartition(f: Iterator[T] => Unit): Unit = withScope {
    kitsch.runJob(this, (iter: Iterator[T]) => f(iter))
  }

  def count(): Long = kitsch.runJob(this, Utils.getIteratorSize)

  def collect(): Array[T] = withScope {
    kitsch.runJob(this, (iter: Iterator[T]) => iter.toArray)
//    Array.concat(results: _*)
  }


  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = withScope {
    filter(f.isDefinedAt).map(f)
  }

  def keyBy[K](f: T => K): RDD[(K, T)] = withScope { map(x => (f(x), x)) }

  def reduce(f: (T, T) => T): T = withScope {
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(f))
      } else {
        None
      }
    }

    val mergeResult = (l: Option[T], r: Option[T]) => (l, r) match {
      case (Some(a), Some(b)) => Some(f(a, b))
      case (None, b) => b
      case (a, None) => a
      case _ => None
    }

    Array(kitsch.runJob(this, reducePartition)).reduce(mergeResult).getOrElse(throw new UnsupportedOperationException("empty collection"))
  }
}

object RDD {
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
                                          (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }
}