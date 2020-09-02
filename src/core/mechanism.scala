package mechanism

import mercator._

import scala.concurrent._
import scala.collection.mutable.HashSet
import scala.util.control.NonFatal
import scala.util._

import java.util.function.Supplier
import java.util.concurrent._, locks._

case class Context(task: Task[_]) { thisContext =>
  def progress(percent: Int): Unit = task.completion = percent
  var aborted: Boolean = false
  
  def subtasks: Set[Task[_]] = task.subtaskSet.to[Set]

  def pause(duration: Long): Unit = try {
    Mechanism.sleep(task)
    Thread.sleep(duration)
  } catch { case _: InterruptedException => () } finally {
    Mechanism.unsleep(task)
  }

  def sequence[T](tasks: List[Task[T]]) = {
    new Task[List[T]](thisContext, s"${task.name}/sequence") { newTask =>
      val future: CompletableFuture[Try[List[T]]] = {
        task.synchronized(task.subtaskSet += newTask)
        CompletableFuture.allOf(tasks.map(_.future): _*).thenApply({ _ =>
          task.synchronized(task.subtaskSet -= this)
          tasks.map(_.future.get()).sequence
        })
      }
    }
  }

  def child[T](subtaskName: String, isolated: Boolean = false)(action: Context => T): Task[T] = {
    new Task[T](thisContext, s"${task.name}/$subtaskName") { newTask =>
      val future: CompletableFuture[Try[T]] = CompletableFuture.supplyAsync({ () =>
        val thread = Thread.currentThread
        task.synchronized(task.subtaskSet += newTask)
        val oldName: String = thread.getName
        thread.setName(name)
        val ctx = Context(newTask)
        try Success(action(ctx)) catch { case NonFatal(e) => Failure(e) } finally {
          task.synchronized(task.subtaskSet -= this)
          thread.setName(oldName)
        }
      }, Mechanism.executor)
    }
  }
}

abstract class Task[T](parentContext: Context, val name: String) { task =>
  protected[mechanism] var completion: Int = 0
  protected[mechanism] var interrupted: Boolean = false
  protected[mechanism] def future: java.util.concurrent.CompletableFuture[Try[T]]
  protected[mechanism] val subtaskSet: HashSet[Task[_]] = new HashSet()
  override def toString: String = name
  def await(): Try[T] = future.join()
  def shutdown(): Unit = synchronized(subtaskSet.foreach(_.shutdown()))

  def then[S](name: String, isolated: Boolean = false)(action: Context => T => S): Task[S] = {
    val newTask = new Task[S](parentContext, s"${parentContext.task.name}/$name") { newTask =>
      val future: CompletableFuture[Try[S]] = task.future.thenApplyAsync({ t =>
        val thread = Thread.currentThread
        val oldName: String = thread.getName
        thread.setName(name)
        task.synchronized(parentContext.task.subtaskSet += newTask)
        val ctx = Context(newTask)
        try t.map(action(ctx)) catch { case NonFatal(e) => Failure(e) } finally {
          task.synchronized(parentContext.task.subtaskSet -= newTask)
          thread.setName(oldName)
        }
      }, Mechanism.executor)
    }

    newTask
  }
}

object Mechanism extends Task[Unit](null, "mechanism:/") {

  sealed trait Isolation
  case object Awaiting extends Isolation
  case object Isolated extends Isolation
  case object Parallel extends Isolation

  private[mechanism] def enqueueIsolated(task: Task[_]): Unit = synchronized {
    if(status != Isolated) status = Awaiting
    isolationQueue :+= task
  }

  private[mechanism] var status: Isolation = Parallel
  private[mechanism] def ready: Boolean = synchronized(status == Awaiting || status == Isolated)

  private[mechanism] var isolationQueue: Vector[Task[_]] = Vector()

  private[mechanism] val executor: ExecutorService = Executors.newWorkStealingPool(100)
  private[mechanism] var sleeping: Set[Task[_]] = Set()

  private[mechanism] def sleep(task: Task[_]) = synchronized(sleeping += task)
  private[mechanism] def unsleep(task: Task[_]) = synchronized(sleeping -= task)

  val future: CompletableFuture[Try[Unit]] = new CompletableFuture()
}

object Task extends Context(Mechanism)
