package refs

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, SourceRef}
import com.typesafe.config.ConfigFactory

object Receiver {

  case class Elem(payload: Int)
  case class StreamData(ref: SourceRef[Elem])
  final case object Ack
  final case object StreamInitialized
  final case object StreamCompleted
  final case class  StreamFailure(ex: Throwable)

  class  ReceiverActor extends Actor  with ActorLogging {

    implicit val mat: ActorMaterializer = ActorMaterializer()(context)

    override def receive: Receive = {

      case StreamInitialized =>
        log.info("Initialization")
        sender ! Ack

      case StreamCompleted =>
        log.info("Completed")
        sender ! Ack

      case StreamFailure(t) =>
        log.info(s"Failed $t")

      case Elem(payload) => {
        log.info(s"Consumed element $payload")
        Thread.sleep(500)
        sender ! Ack
      }
      case StreamData(ref) =>
        log.info(s"Running stream $ref")
        ref.runWith(
        Sink.actorRefWithAck(
          self,
          onInitMessage = StreamInitialized,
          ackMessage = Ack,
          onCompleteMessage = StreamCompleted,
          onFailureMessage = (ex: Throwable) ⇒ StreamFailure(ex)
        ))
    }
  }

  def start(): Unit = {
    val config = ConfigFactory.parseString(
      """
        |
        |akka {
        |  loglevel = INFO
        |  stdout-loglevel = INFO
        |
        |  actor {
        |    provider = remote
        |  }
        |  remote {
        |    artery {
        |      enabled = on
        |      canonical.hostname = "127.0.0.1"
        |      canonical.port = 25521
        |    }
        |  }
        | stream.materializer.stream-ref.subscription-timeout = 10 seconds
        |}
        |
      """.stripMargin)

    val system = ActorSystem("receiver-system",config )
    system.actorOf(Props(new ReceiverActor), "receiver-actor")
  }

}
