package kitsch

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.util.Timeout
import kitsch.linear.PartialLeastSquares
import kitsch.rdd.{ParallelCollectionRDD, RDD, RDDOperationScope, TextRDD}

import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 15/12/19.
 */
class Kitsch(name: String) {
      println(
      """
        | _  ___ _            _
        || |/ (_) |_ ___  ___| |__
        || ' /| | __/ __|/ __| '_ \
        || . \| | |_\__ \ (__| | | |
        ||_|\_\_|\__|___/\___|_| |_|
        |
      """.stripMargin)
  implicit val system = ActorSystem(name)
  implicit val timeout = Timeout(10 second)

  system.log.info(s"Running Kitsch version ${system.settings.config.getString("kitsch.version")}")

  val conf = KitschConf(
    name = name,
    mode = system.settings.config.getString("kitsch.mode"),
    localhost = system.settings.config.getString("kitsch.web.local.interface"),
    port = system.settings.config.getInt("kitsch.web.local.port"))

  val server = system.actorOf(Props(
    new KitschServer(this)), "server")

  def this() = this("kitsch")

  def halt = system.shutdown

  private[kitsch] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  def makeRDD[T: ClassTag](seq: Seq[T]): RDD[T] = parallelize(seq)

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] =
    Array(func(rdd.iterator()))

  def parallelize[T: ClassTag](seq: Seq[T]): RDD[T] =
    withScope(new ParallelCollectionRDD[T](this, seq))

  def textFile(path: String): RDD[String] = withScope {
    new TextRDD(this, path)
  }

//  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U) =
//    ask(server, Job(rdd, func)).mapTo[List[U]].value match {
//      case Some(Success(result)) => result
//      case _                     => throw new KitschException("Run job failed")
//    }
//
//  def makeRDD[T: ClassTag](seq: Seq[T]): RDD[T] =
//    ask(server, SeqToRDD(seq)).mapTo[RDD[T]].value match {
//      case Some(Success(result)) => result
//      case Some(Failure(_))      => throw new KitschException("Failed to make RDD")
//      case None                  =>
//        throw new KitschException("Cannot make RDD")
//    }
}

object Kitsch {
  object Mod {
    val Master = "master"
    val Slave = "slave"
  }

  private val nextRddId = new AtomicInteger(0)
  private[kitsch] def newRddId: Int = nextRddId.getAndIncrement()

  private val nextPartitionId = new AtomicInteger(0)
  private[kitsch] def newPartitionId: Int = nextPartitionId.getAndIncrement()

  def main (args: Array[String]){
    val kitsch = new Kitsch("test")
    val text = kitsch.textFile("./ml-100k/u.data")
    val pls = PartialLeastSquares.train(
      text,
      (s:String) => s.split("\t").toSeq,
      0)
    println(pls.collect())
    kitsch.halt
  }
}