import java.awt.EventQueue
import org.nlogo.app.App
object Example1 {
  def main(args: Array[String]) {
    App.main(args)
    wait {
      App.app.open("/home/martin/DA/prototype/Sum.nlogo")
    }
    App.app.command("sumupto 10")
  }
  def wait(block: => Unit) {
    EventQueue.invokeAndWait(
      new Runnable() { def run() { block } } ) }
}

