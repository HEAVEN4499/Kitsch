package kitsch.message

import kitsch.rdd.RDD

/**
 * Created by wulicheng on 16/4/3.
 */
case class Job[T, U](rdd: RDD[T],  func: Iterator[T] => U)
