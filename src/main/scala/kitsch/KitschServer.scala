package kitsch

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import akka.pattern.ask
import akka.util.Timeout
import kitsch.message.{InitRouteInfo, Job, Partition, SeqToRDD}
import kitsch.rdd.{ParallelCollectionRDD, RDD, RDDOperationScope}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 * Created by wulicheng on 16/4/4.
 */
class KitschServer(kitsch: Kitsch)
  extends Actor with ActorLogging {

  implicit val timeout = Timeout(10 second)

  private[kitsch] def withScope[U](body: => U): U =
    RDDOperationScope.withScope[U](kitsch)(body)

  val routes = {
    def toSelectionString(domain: String, name: String = "kitsch") =
      s"akka.tcp://$name@$domain/user/server"
    val slaveList = kitsch.system.settings.config.getStringList("kitsch.web.slave")
    val slaves =
      if (kitsch.conf.mode == Kitsch.Mod.Master)
        for (s <- 0 until slaveList.size)
          yield Await.result(
            context.actorSelection(toSelectionString(slaveList.get(s))).resolveOne,
            timeout.duration)
      else Nil
    self :: slaves.toList
  }

  val routePartitions: List[Int] = {
    context.system.log.info("counting partitions...")
    if (routes.length > 1) {
      for (num <- routes.map(ask(_, InitRouteInfo).mapTo[Int]))
        yield Await.result(num, timeout.duration)
    } else {
      List(scala.sys.runtime.availableProcessors)
    }
  }

  val minPartitions = routePartitions.reduce(_+_)

  override def preStart(): Unit = {
    context.system.log.info(s"current routes: ${routes.size.toString}")
    context.system.log.info(s"current partitions: ${minPartitions}")
  }

  def receive = {
    case InitRouteInfo    =>
      sender ! scala.sys.runtime.availableProcessors
    case Job(rdd, func)   =>
      sender ! runJob(rdd, func)
    case SeqToRDD(seq)    => makeRDD(seq)
    case Partition(seq)   =>
      sender ! context.actorOf(
        Props(new PartitionActor(seq)),
        Kitsch.newPartitionId.toString)
    case "routes"         => sender ! minPartitions
  }

  def makeRDD[T: ClassTag](seq: Seq[T]): RDD[T] = parallelize(seq)

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): List[U] =
    List(func(rdd.iterator()))

  def parallelize[T: ClassTag](seq: Seq[T]): RDD[T] = {
    val size = (seq.size + seq.size % minPartitions) / minPartitions
    val iter = seq.sliding(size, size)
    val actors = routes.zip(routePartitions).flatMap { case (route, num) =>
      for (i <- 0 until num)
        yield Await.result(
          ask(route, Partition(iter.next)).mapTo[ActorRef],
          timeout.duration)
    }
    withScope(new ParallelCollectionRDD[T](kitsch, seq))
  }

//  def textFile(name: String): RDD[String] = TextRDD(kitsch, null)
}
