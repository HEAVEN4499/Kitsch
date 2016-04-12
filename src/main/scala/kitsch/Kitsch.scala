package kitsch

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import kitsch.message.{Job, ReadFile, SeqToRDD}
import kitsch.rdd.{RDD, RDDOperationScope}

import scala.concurrent.Await
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

//  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] =
//    Array(func(rdd.iterator()))

//  def parallelize[T: ClassTag](seq: Seq[T]): RDD[T] =
//    withScope(new ParallelCollectionRDD[T](this, seq))
//
//  def textFile(path: String): RDD[String] = withScope {
//    new TextRDD(this, path)
//  }

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U) =
    Await.result(ask(server, Job(rdd, func)), timeout.duration).asInstanceOf[U]

  def parallelize[T: ClassTag](seq: Seq[T]): RDD[T] =
    Await.result(ask(server, SeqToRDD(seq)), timeout.duration).asInstanceOf[RDD[T]]

  def textFile(path: String): RDD[String] = {
    system.log.info(s"loading text file: $path")
    Await.result(ask(server, ReadFile(path)), timeout.duration).asInstanceOf[RDD[String]]
  }
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
    val kc = new Kitsch("test")
    val text = kc.textFile("./ml-100k/u.data")
      .map(_.split("\t"))
      .map(_(1).toInt)
      .reduce(_+_)
    println(text)
//    val pls = PartialLeastSquares.train(
//      text,
//      (s:String) => s.split("\t").toSeq,
//      0)
//    println(pls.collect())
    kc.halt
  }
}