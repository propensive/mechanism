package mechanism.tests

import mechanism._
import probably._

import scala.util._

object Main extends Suite("Mechanism tests") {
  def run(test: Runner): Unit = {
    test("simple") {
      for {
        x <- Task.child("one") { ctx => 1 + 1 }.await()
        y <- Task.child("two") { ctx => 2 + 2 }.await()
      } yield x + y
    }.assert(_ == Success(6))

    test("hierarchy") {
      Task.child("workers") { ctx =>
        val one = ctx.child("one") { ctx => ctx.pause(300) }
        val two = ctx.child("two") { ctx => ctx.pause(100) }
        ctx.pause(200)
        ctx.subtasks.size
      }.await()
    }.assert(_ == Success(1))

    test("follow-on") {
      val t1 = Task.child("start") { ctx => ctx.pause(200); 3 }
      val t2 = t1.then("middle") { ctx => t => ctx.pause(200); t*5 }
      val t3 = t2.then("end") { ctx => t => ctx.pause(200); t*7 }
      t3.await()
    }.assert(_ == Success(3*5*7))
    
    test("sequence") {
      val t1 = Task.child("one") { ctx => ctx.pause(200); 3 }
      val t2 = Task.child("two") { ctx => ctx.pause(200); 5 }
      val t3 = Task.child("three") { ctx => ctx.pause(200); 7 }
      println(Task.subtasks)
      Thread.sleep(100)
      val seq = Task.sequence(List(t1, t2, t3))
      println(Task.subtasks)
      Thread.sleep(200)
      println(Task.subtasks)
      seq.await()
    }.assert(_ == Success(List(3, 5, 7)))
  }
}