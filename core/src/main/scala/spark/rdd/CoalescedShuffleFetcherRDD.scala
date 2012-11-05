package spark.rdd

import spark.{RDD, ShuffleDependency, SparkEnv, Split}


// TODO(rxin): set partitioner.

private[spark] class CoalescedShuffleSplit(val index: Int, val partitions: Array[Int]) extends Split


class CoalescedShuffleFetcherRDD[K, V](
    prev: RDD[(K, V)],
    groups: Array[Array[Int]],
    dep: ShuffleDependency[K, V])
  extends RDD[(K, V)](prev.context) {

  val shuffleId = dep.shuffleId

  override val partitioner = Some(dep.partitioner)

  override def splits = groups.zipWithIndex.map { case (group, index) =>
    new CoalescedShuffleSplit(index, group)
  }

  override val dependencies = List(dep)

  override def compute(split: Split) = {
    val fetcher = SparkEnv.get.shuffleFetcher
    val reduceIds = split.asInstanceOf[CoalescedShuffleSplit].partitions
    fetcher.fetchMultiple(shuffleId, reduceIds)
  }

  override def preferredLocations(split: Split) = Nil
}

