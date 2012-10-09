package spark.rdd

import spark._
import collection.mutable.ArrayBuffer

class CoalescedShuffleSplit(
    val index: Int,
    val partitions: Array[Int])
  extends Split

class CoalescedShuffleFetcherRDD[K, V, C](
    prev: RDD[(K, C)],
    dep: ShuffleDependency[K, V, C],
    coalescedPartitions: Array[CoalescedShuffleSplit])
    extends RDD[(K, C)](prev.context) {

    override def splits = coalescedPartitions.asInstanceOf[Array[Split]]

    override val dependencies = List(dep)

    override def compute(split: Split) = {
      val fetcher = SparkEnv.get.shuffleFetcher
      split.asInstanceOf[CoalescedShuffleSplit].partitions.flatMap(part => {
        val buf = new ArrayBuffer[(K, C)]
        def addTupleToBuffer(k: K, c: C) = { buf += Tuple(k, c) }
        fetcher.fetch[K, C](dep.shuffleId, part, addTupleToBuffer)
        buf.iterator
      }).iterator
    }

    override def preferredLocations(split: Split) = Nil
  }

