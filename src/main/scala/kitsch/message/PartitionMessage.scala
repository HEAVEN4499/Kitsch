package kitsch.message

import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/8.
 */
case class PartitionMessage[T: ClassTag](seq: Seq[T])
