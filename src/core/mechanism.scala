package mechanism

import mercator._

import scala.concurrent._
import scala.collection.mutable.HashSet
import scala.util.control.NonFatal
import scala.util._
import scala.collection.generic.CanBuildFrom

import java.util.function.Supplier
import java.util.concurrent._, locks._, atomic._

object `package` {
  implicit class IterableExtensions[Coll[S] <: Iterable[S], T](xs: Coll[Task[T]]) {
    def sequence(implicit cbf: CanBuildFrom[Nothing, T, Coll[T]]): Task[Coll[T]] = Mechanism.sequence(xs)(cbf)
  }
}

case class Context(task: Task[_]) { thisContext =>
  def progress(value: Double): Unit = {
    if((task.completion*100).toInt < (value*100).toInt) task.listener(Progress((value*100).toInt))
    task.completion = value
  }
  
  def subtasks: Set[Task[_]] = Mechanism.getSubtasks(task)

  def pause(duration: Long): Unit = try {
    Task.sleep(task)
    LockSupport.parkNanos(duration*1000000L)
  } catch { case _: InterruptedException => () } finally Task.unsleep(task)

  def spawn[T](id: String)(action: Context => T): Task[T] =
    Mechanism.spawn(task, id) { ctx => Try(action(ctx)) }

  def abort(): Task[Unit] = {
    Mechanism.cancel(task)
    //throw CancelException()
  }

  def defaultHandler: PartialFunction[ProcessAction, Unit] = {
    case Terminate => throw CancelException()
    case Suspend   => LockSupport.park()
  }

  def juncture(pf: PartialFunction[ProcessAction, Unit]): Unit =
    pf.orElse(defaultHandler).lift {
      if(task.cancelled) Terminate else Continue
    }

}

case class CancelException() extends RuntimeException()

sealed trait ProcessAction
case object Terminate extends ProcessAction
case object Suspend extends ProcessAction
case object Continue extends ProcessAction

abstract class Task[T](val parent: Option[Task[_]], val id: String, val listener: Update => Unit) { task =>
  def name: String = s"${parent.fold("mechanism:/")(_.name)}/$id"
  private[mechanism] var completion: Double = 0
  private[mechanism] var cancelled: Boolean = false
  private[mechanism] val future: java.util.concurrent.CompletableFuture[Try[T]]
  override def toString: String = name
  def await(): Try[T] = future.join()
  
  def then[S](subName: String)(action: Context => T => S): Task[S] =
    Mechanism.spawnPeer(this, subName) { ctx => future.join().map(action(ctx)(_)) }
  
  def map[S](fn: T => S): Task[S] = new Task[S](parent, id, listener) {
    private[mechanism] val future: java.util.concurrent.CompletableFuture[Try[S]] =
      task.future.thenApply { value => value.map(fn) }
  }

  def flatMap[S](fn: T => Task[S]): Task[S] = new Task[S](parent, id, listener) {
    private[mechanism] val future: CompletableFuture[Try[S]] =
      task.future.thenComposeAsync {
        case Success(value) =>
          fn(value).future
        case Failure(e) =>
          val future = new CompletableFuture[Try[S]]
          future.completeExceptionally(e)
          future
      }
  }
}

object Task extends Task[Unit](None, "mechanism:/", _ => ()) {
  private[mechanism] var sleeping: Set[Task[_]] = Set()

  private[mechanism] def sleep(task: Task[_]) = sleeping += task
  private[mechanism] def unsleep(task: Task[_]) = sleeping -= task

  val future: CompletableFuture[Try[Unit]] = new CompletableFuture()
}

sealed trait Update
case class Progress(v: Int) extends Update
case object Complete extends Update
case object Aborted extends Update

object Mechanism extends Context(Task) {
  private val executor: ExecutorService = Executors.newWorkStealingPool(100)
  
  private var children: Map[Task[_], Set[Task[_]]] = Map().withDefault { _ => Set() }
  private var parents: Map[Task[_], Task[_]] = Map(Task -> Task)

  private val counter: AtomicInteger = new AtomicInteger()
  private[mechanism] def nextId(): Int = counter.incrementAndGet()

  private[mechanism] def register(name: String, parent: Task[_], child: Task[_]): Unit = synchronized {
    Thread.currentThread.setName(name)
    children = children.updated(parent, children.getOrElse(parent, Set()) + child)
    parents = parents.updated(child, parent)
  }

  private[mechanism] def unregister(task: Task[_]): Unit = synchronized {
    val parent: Task[_] = parents.getOrElse(task, Task)
    Thread.currentThread.setName(s"mechanism://worker-${Mechanism.nextId()}")
    children = children.updated(parent, children.getOrElse(parent, Set()) - task)
    if(task != Task) parents = parents - task
  }

  private[mechanism] def getSubtasks(task: Task[_]): Set[Task[_]] = children.getOrElse(task, Set())

  private[mechanism] def cancel(task: Task[_]): Task[Unit] = {
    val subtasks = children(task)
    task.cancelled = true
    (subtasks.map(cancel(_)) + (task.map(_ => ()))).sequence.map { _ => () }
  }

  def spawnPeer[T]
               (current: Task[_], id: String, listener: Update => Unit = { _ => () })
               (action: Context => Try[T])
               : Task[T] = {
    val parent = synchronized(parents(current))
    spawn(parent, id, listener)(action)
  }

  private[mechanism] def spawn[T]
                              (parentTask: Task[_], id: String, listener: Update => Unit = { _ => () })
                              (action: Context => Try[T])
                              : Task[T] =
    new Task[T](Some(parentTask), id, listener) { newTask =>
      val future: CompletableFuture[Try[T]] = CompletableFuture.supplyAsync({ () =>
        register(name, parentTask, newTask)
        try action(Context(newTask)) catch {
          case NonFatal(e) => Failure(e)
        } finally unregister(newTask)
      }, executor)
    }

  def sequence[Coll[S] <: Iterable[S], T]
              (tasks: Coll[Task[T]])
              (implicit cbf: CanBuildFrom[Nothing, T, Coll[T]])
              : Task[Coll[T]] = {
    new Task[Coll[T]](None, s"sequence-${Mechanism.nextId()}", { _ => () }) { newTask =>
      val future: CompletableFuture[Try[Coll[T]]] = {
        register(name, Task, newTask)
        CompletableFuture.allOf(tasks.to[List].map(_.future): _*).thenApply { _ =>
          unregister(newTask)
          tasks.to[List].map(_.future.get()).sequence.map(_.to[Coll])
        }
      }
    }
  }
}
