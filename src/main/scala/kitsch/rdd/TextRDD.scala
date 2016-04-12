package kitsch.rdd

import com.typesafe.scalalogging.slf4j.LazyLogging
import kitsch._
import kitsch.partition.Partition

import scala.concurrent.Future

private[kitsch] class TextRDD(kitsch: Kitsch,
                              @transient private val data: Seq[Future[Partition[String]]])
  extends RDD[String](kitsch) with LazyLogging {

  override def compute() = data
}

