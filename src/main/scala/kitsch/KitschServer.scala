package kitsch

import akka.actor.{Actor, ActorLogging}
import akka.pattern.ask
import akka.util.Timeout
import kitsch.message._
import kitsch.partition.{Partition, PartitionActor}
import kitsch.rdd.{TextRDD, ParallelCollectionRDD, RDD, RDDOperationScope}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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
    super.preStart()
  }

  def receive = {
    case InitRouteInfo           =>
      sender ! scala.sys.runtime.availableProcessors
    case Job(rdd, func)          =>
      sender ! runJob(rdd, func)
    case SeqToRDD(seq)           => makeRDD(seq)
    case PartitionMessage(seq)   =>
      sender ! PartitionActor.props(kitsch, Future(seq.iterator))
    case ReadFile(filePath)      => sender ! textFile(filePath)
    case "routes"                => sender ! minPartitions
  }

  def makeRDD[T: ClassTag](seq: Seq[T]): RDD[T] = parallelize(seq)

  def runJob[T: ClassTag, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): U = {
    println(s"[Kitsch-INFO] running job on $rdd with function $func")
    val partitions = Await.result(
      Future.sequence(rdd.iterator),
      Duration.Inf).map(_.data)
    val iterator = Await.result(Future.sequence(partitions), Duration.Inf).reduce(_++_)
    func(iterator)
  }

  def parallelize[T: ClassTag](seq: Seq[T]): RDD[T] = {
    println(s"[Kitsch-INFO] loading seq")
    val size = (seq.size + seq.size % minPartitions) / minPartitions
    val iter = seq.sliding(size, size)
    val actors = routes.zip(routePartitions).flatMap { case (route, num) =>
      for (i <- 0 until num)
        yield ask(route, PartitionMessage(iter.next)).mapTo[Partition[T]]
    }
    withScope(new ParallelCollectionRDD[T](kitsch, actors))
  }

  def textFile(filePath: String): RDD[String] = {
    println(s"[Kitsch-INFO] file path is $filePath")
    val lines = readFile(filePath).getLines().toList
    val lineSize = lines.size
    val size = (lineSize + lineSize % minPartitions) / minPartitions
    val iter = lines.sliding(size, size)
    println()
    val actors = routes.zip(routePartitions).flatMap { case (route, num) =>
      for (i <- 0 until num)
        yield ask(route, PartitionMessage(iter.next)).mapTo[Partition[String]]
    }
    context.system.log.info(s"$filePath loaded")
    withScope(new TextRDD(kitsch, actors))
  }

  def readFile(filePath: String) = scala.io.Source.fromFile(filePath)
}
