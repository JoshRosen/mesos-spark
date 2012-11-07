package spark

import com.clearspring.analytics.stream.cardinality.HyperLogLog


abstract class StatsAccumulator[-T, R] extends Serializable {
  def initialValue: R
  def accumulate(currentValue: R, item: T): R
  def merge(value1: R, value2: R): R

  private[spark] def accumulateUntyped(currentValue: Any, item: T): R =
    accumulate(currentValue.asInstanceOf[R], item)
}


abstract class GlobalStatsAccumulator[-T, R] extends StatsAccumulator[T, R] {

  def serialize(stats: R): Array[Byte] = SparkEnv.get.serializer.newInstance.serialize(stats).array

  def deserialize(bytes: Array[Byte]): R =
    SparkEnv.get.serializer.newInstance.deserialize(java.nio.ByteBuffer.wrap(bytes))

  private[spark] def serializeUntyped(stats: Any): Array[Byte] =
    serialize(stats.asInstanceOf[R])
}


/**
 * A datatype for accumulating per-partition statistics.
 *
 * @param initialValue initial value of the accumulator
 * @tparam T the type of object over which statistics are computed
 * @tparam R the type of the statistic
 */
abstract class PartitionStatsAccumulator[-T, R: ClassManifest] extends StatsAccumulator[T, R] {

  def allocateBuffer(size: Int): Array[R] = Array.fill(size)(initialValue)

  def serialize(stats: Array[R]): Array[Byte] =
    SparkEnv.get.serializer.newInstance.serialize(stats).array

  def deserialize(bytes: Array[Byte]): Array[R] =
    SparkEnv.get.serializer.newInstance.deserialize(java.nio.ByteBuffer.wrap(bytes))

  private[spark] def accumulateFromUntypedArray(arr: Object, index: Int, item: T) {
    arr.asInstanceOf[Array[R]](index) = accumulate(
      arr.asInstanceOf[Array[R]](index).asInstanceOf[R], item)
  }

  private[spark] def serializeUntyped(stats: Object): Array[Byte] =
    serialize(stats.asInstanceOf[Array[R]])
}


object CountPartitionStatAccumulator extends PartitionStatsAccumulator[Any, Int] {
  override def initialValue: Int = 0
  override def accumulate(currentValue: Int, item: Any): Int = currentValue + 1
  override def merge(value1: Int, value2: Int): Int = value1 + value2
}


object CardinalityGlobalStatAccumulator extends GlobalStatsAccumulator[Any, HyperLogLog] {

  val LOG2M = 8

  override def initialValue: HyperLogLog = new HyperLogLog(LOG2M)

  override def accumulate(currentValue: HyperLogLog, item: Any): HyperLogLog = {
    currentValue.offer(item)
    currentValue
  }

  override def merge(value1: HyperLogLog, value2: HyperLogLog): HyperLogLog =
    value1.merge(value2).asInstanceOf[HyperLogLog]

  override def serialize(stats: HyperLogLog): Array[Byte] = stats.getBytes

  override def deserialize(bytes: Array[Byte]): HyperLogLog =
    HyperLogLog.Builder.build(bytes).asInstanceOf[HyperLogLog]
}


object EmptyGlobalStatAccumulator extends GlobalStatsAccumulator[Any, Int] {
  override def initialValue: Int = 0
  override def accumulate(currentValue: Int, item: Any): Int = 0
  override def merge(value1: Int, value2: Int): Int = 0
  override def serialize(stats: Int): Array[Byte] = Array.empty
  override def deserialize(bytes: Array[Byte]): Int = 0
}


object PartitionStatsAccumulator {

  def compose[T, R1: ClassManifest, R2: ClassManifest](
    s1: PartitionStatsAccumulator[T, R1],
    s2: PartitionStatsAccumulator[T, R2]): PartitionStatsAccumulator[T, (R1, R2)] = {

    new PartitionStatsAccumulator[T, (R1, R2)] {

      override def initialValue: (R1, R2) = (s1.initialValue, s2.initialValue)

      override def accumulate(currentValue: (R1, R2), item: T): (R1, R2) =
        (s1.accumulate(currentValue._1, item), s2.accumulate(currentValue._2, item))

      override def merge(value1: (R1, R2), value2: (R1, R2)): (R1, R2) =
        (s1.merge(value1._1, value1._1), s2.merge(value1._2, value1._2))
    }

  }
}

