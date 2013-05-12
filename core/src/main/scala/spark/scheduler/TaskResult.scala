package spark.scheduler

import java.io._

import scala.collection.mutable.Map
import spark.executor.TaskMetrics

private[spark]
sealed abstract class TaskResult[T]

/** A reference to a TaskResult that has been pushed to the driver's BlockManager. */
private[spark]
case class IndirectTaskResult[T](val blockId: String) extends TaskResult[T] with Serializable

/** A TaskResult that contains the task's return value and accumulator updates. */
private[spark]
class DirectTaskResult[T](var value: T, var accumUpdates: Map[Long, Any], var metrics: TaskMetrics)
  extends TaskResult[T] with Externalizable {

  def this() = this(null.asInstanceOf[T], null, null)

  override def writeExternal(out: ObjectOutput) {
    out.writeObject(value)
    out.writeInt(accumUpdates.size)
    for ((key, value) <- accumUpdates) {
      out.writeLong(key)
      out.writeObject(value)
    }
    out.writeObject(metrics)
  }

  override def readExternal(in: ObjectInput) {
    value = in.readObject().asInstanceOf[T]
    val numUpdates = in.readInt
    accumUpdates = Map()
    for (i <- 0 until numUpdates) {
      accumUpdates(in.readLong()) = in.readObject()
    }
    metrics = in.readObject().asInstanceOf[TaskMetrics]
  }
}
