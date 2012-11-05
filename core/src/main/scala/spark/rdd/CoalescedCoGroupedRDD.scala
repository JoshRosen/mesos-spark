package spark.rdd

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import spark.{RDD, ShuffleDependency, SparkEnv, Split}


class CoalescedCoGroupedRDD[K](
    rdds: Seq[RDD[(_, _)]],
    groups: Array[Array[Int]],
    @transient override val dependencies: List[ShuffleDependency[_, _]])
  extends RDD[(K, Array[ArrayBuffer[Any]])](rdds.head.context) {

  val shuffleIds: Array[Int] = dependencies.map(_.shuffleId).toArray

  @transient val _splits: Array[Split] = Array.tabulate(groups.size) { index =>
    new CoalescedShuffleSplit(index, groups(index)): Split
  }

  override def splits = _splits

  override def preferredLocations(s: Split) = Nil

  override def compute(s: Split): Iterator[(K, Array[ArrayBuffer[Any]])] = {
    val split = s.asInstanceOf[CoalescedShuffleSplit]
    val numRdds = shuffleIds.size
    val map = new JHashMap[K, Array[ArrayBuffer[Any]]]
    def getSeq(k: K): Array[ArrayBuffer[Any]] = {
      var values = map.get(k)
      if (values == null) {
        values = Array.fill(numRdds)(new ArrayBuffer[Any])
        map.put(k, values)
      }
      values
    }
    val fetcher = SparkEnv.get.shuffleFetcher
    shuffleIds.zipWithIndex.foreach { case (shuffleId, rddIndex) =>
      fetcher.fetchMultiple[K, Any](shuffleId, split.partitions).foreach { case(k, v) =>
        getSeq(k)(rddIndex) += v
      }
    }

    map.iterator
  }
}
