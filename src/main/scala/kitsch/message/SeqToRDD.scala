package kitsch.message

import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/3.
 */
case class SeqToRDD[T: ClassTag](seq: Seq[T])
