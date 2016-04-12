package kitsch.message

import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/5.
 */
case class PartitionCompute[T: ClassTag, U: ClassTag](func: Iterator[T] => Iterator[U])
