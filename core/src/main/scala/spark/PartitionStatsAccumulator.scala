package spark

// TODO: add serialization / compression of statistics.

/**
 * A datatype for accumulating per-partition statistics.
 *
 * @param initialValue initial value of the accumulator
 * @tparam T the type of object over which statistics are computed
 * @tparam R the type of the statistic
 * @tparam Repr used for self-referencing type
 */
abstract class PartitionStatsAccumulator[T, R: ClassManifest,
  Repr <: PartitionStatsAccumulator[T, R,Repr]](
    @transient val initialValue: R,
    val numPartitions: Int)
  extends Serializable {
  val stats = Array.fill[R](numPartitions)(initialValue)

  def accumulate(partition: Int, value: T)
  def addInPlace(acc: Repr)
  def getStats(partition: Int) : R = stats(partition)
}

class CountPartitionStatAccumulator[T](numPartitions: Int)
  extends PartitionStatsAccumulator[T, Int, CountPartitionStatAccumulator[T]](0, numPartitions) {
  def accumulate(partition: Int, value: T) {
    stats(partition) += 1
  }
  def addInPlace(acc: CountPartitionStatAccumulator[T]) {
    acc.stats.zipWithIndex.foreach(x => stats(x._2) += x._1)
  }
}
