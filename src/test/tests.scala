package mechanism.tests

import mechanism._
import probably._

import scala.util._

object Main extends Suite("Mechanism tests") {
  def run(test: Runner): Unit = {
    test("simple") {
      for {
        x <- Mechanism.spawn("one") { ctx => 1 + 1 }.await()
        y <- Mechanism.spawn("two") { ctx => 2 + 2 }.await()
      } yield x + y
    }.assert(_ == Success(6))

    test("hierarchy") {
      Mechanism.spawn("workers") { ctx =>
        val one = ctx.spawn("one") { ctx => ctx.pause(400) }
        val two = ctx.spawn("two") { ctx => ctx.pause(200) }
        ctx.pause(300)
        ctx.subtasks.size
      }.await()
    }.assert(_ == Success(1))

    test("follow-on") {
      val t1 = Mechanism.spawn("start") { ctx => ctx.pause(200); 3 }
      val t2 = t1.then("middle") { ctx => t => ctx.pause(200); t*5 }
      val t3 = t2.then("end") { ctx => t => ctx.pause(200); t*7 }
      t3.await()
    }.assert(_ == Success(3*5*7))
    
    test("sequence") {
      val t1 = Mechanism.spawn("one") { ctx => ctx.pause(200); 3 }
      val t2 = Mechanism.spawn("two") { ctx => ctx.pause(200); 5 }
      val t3 = Mechanism.spawn("three") { ctx => ctx.pause(200); 7 }
      
      List(t1, t2, t3).sequence.await()
    }.assert(_ == Success(List(3, 5, 7)))

    test("interrupt") {
      val t1 = Mechanism.spawn("example") { ctx =>
        val process = Runtime.getRuntime.exec(Array("sleep", "10"))
        ctx.pause(100)
        while(process.isAlive) {
          ctx.juncture {
            case Terminate =>
              process.destroy()
              process.waitFor
              throw new Exception()
          }
          ctx.pause(100)
        }
        process.waitFor
      }
      Mechanism.pause(500)
      Mechanism.abort()
      t1.await()
    }.assert(_.isFailure)
  }
}