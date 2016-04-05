package kitsch.message

import scala.reflect.ClassTag

/**
 * Created by wulicheng on 15/12/22.
 * An identifier for a partition in an RDD.
 */
case class Partition[T: ClassTag](seq: Seq[T])
