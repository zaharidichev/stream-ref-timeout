package refs

object StreamingTest extends App {
  Receiver.start()
  Sender.start()
}
