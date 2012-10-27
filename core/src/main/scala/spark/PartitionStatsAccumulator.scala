package spark

// TODO: add serialization / compression of statistics.

/**
 * A datatype for accumulating per-partition statistics.
 *
 * @param initialValue initial value of the accumulator
 * @tparam T the type of object over which statistics are computed
 * @tparam R the type of the statistic
 */
abstract class PartitionStatsAccumulator[-T, R: ClassManifest](
    @transient val initialValue: R,
    val numPartitions: Int)
  extends Serializable {
  val stats = Array.fill[R](numPartitions)(initialValue)

  def accumulate(partition: Int, value: T)
  def mergeStats(acc: R, stat: R) : R
  def getStats(partition: Int) : R = stats(partition)
}

class CountPartitionStatAccumulator(numPartitions: Int)
  extends PartitionStatsAccumulator[Any, Int](0, numPartitions) {
  def accumulate(partition: Int, value: Any) {
    stats(partition) += 1
  }
  def mergeStats(acc: Int, stat: Int) : Int = acc + stat
}
