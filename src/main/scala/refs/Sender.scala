package refs


import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.pipe
import akka.stream.scaladsl.{Source, StreamRefs}
import akka.stream.{ActorMaterializer, SourceRef}
import com.typesafe.config.ConfigFactory
import refs.Receiver.{Elem, StreamData}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Sender {


  class SenderActor extends Actor with ActorLogging {

    import context._
    implicit val materializer = ActorMaterializer()(context)
    override def preStart(): Unit = {
      // not that we will block in such a manner in real life...
      val receiver  = Await.result(context.actorSelection("akka://receiver-system@127.0.0.1:25521/user/receiver-actor").resolveOne(10 seconds), 10.seconds)
      val sourceRefFuture:   Future[SourceRef[Elem]]  = Source(1 to 10000).map(Elem.apply).runWith(StreamRefs.sourceRef())
      pipe(sourceRefFuture.map(StreamData.apply)).to(receiver)
    }

    override def receive: Receive = Actor.emptyBehavior
  }

  def start(): Unit = {

    val config = ConfigFactory.parseString(
      """
        |
        |akka {
        |  loglevel = DEBUG
        |  stdout-loglevel = DEBUG
        |
        |  actor {
        |    provider = remote
        |  }
        |  remote {
        |    artery {
        |      enabled = on
        |      canonical.hostname = "127.0.0.1"
        |      canonical.port = 25522
        |    }
        |  }
        |  stream.materializer.stream-ref.subscription-timeout = 10 seconds
        |}
        |
        |
        |
      """.stripMargin)


    implicit val system = ActorSystem("sender-system", config)
    system.actorOf(Props(new SenderActor()), "sender-actor")
  }
}

