package spark.examples

import spark.{ShuffleDependency, HashPartitioner, SparkContext, SparkEnv}
import spark.rdd.{CoalescedShuffleFetcherRDD, CoalescedShuffleSplit, DependencyForcerRDD}

object PartialDAGSubmissionTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: PartialDAGSubmissionTest <host>")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "PartialDAGSubmissionTest")
    // Create an RDD.  This RDD will produce skewed hash partitions:
    val rdd = sc.parallelize(1 to 1000)
                .union(sc.parallelize(1.to(1000).map(x => 100)))
                .map(x => (x, x))

    // Perform the fine-grained pre-partitioning, forcing the ShuffleDependency to be computed but
    // skipping the shuffle fetching phase.
    val NUM_FINE_GRAINED_BUCKETS = 10
    val part = new HashPartitioner(NUM_FINE_GRAINED_BUCKETS)
    val dep = new ShuffleDependency[Int, Int, Int](rdd.context.newShuffleId, rdd, None, part)
    val depForcer = new DependencyForcerRDD(rdd, List(dep))
    sc.runJob(depForcer, (iter: Iterator[_]) => {})

    // Collect the partition sizes
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val partitionSizes = 0.until(NUM_FINE_GRAINED_BUCKETS).map(mapOutputTracker.getServerStatuses
      (dep.shuffleId, _).map(_._2).sum)
    println("Computed shuffle partitions with sizes: " + partitionSizes)

    // At this point, we would make a partitioning decision based on statistics.
    // For now, we'll just coalesce a fixed number of shuffle partitions into larger partitions:
    val groupedSplits = 0.until(NUM_FINE_GRAINED_BUCKETS).grouped(5).zipWithIndex
       .map(x => new CoalescedShuffleSplit(x._2, x._1.toArray)).toArray

    // This RDD will fetch the coalesced partitions
    val coalesced = new CoalescedShuffleFetcherRDD(rdd, dep, groupedSplits)

    // It functions like a regular RDD:
    println(coalesced.count())
    println(coalesced.take(10))
    System.exit(0)
  }
}
