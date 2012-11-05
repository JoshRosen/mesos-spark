package spark.examples

import spark._
import SparkContext._
import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import spark.rdd.CoalescedCoGroupedRDD
import spark.rdd.CoalescedShuffleFetcherRDD

/**
 * Transitive closure on a graph.
 */
object SparkTC extends Logging {

  def generateGraph(randSeed: Int, numVertices: Int, numEdges: Int) = {
    val rand = new Random(42 + randSeed)
    val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
    while (edges.size < numEdges) {
      val from = rand.nextInt(numVertices)
      val to = rand.nextInt(numVertices)
      if (from != to) edges.+=((from, to))
    }
    edges.iterator
  }

  def linearTC(dataset: RDD[(Int, Int)]): ArrayBuffer[Long] = {
    val times = new ArrayBuffer[Long](10)
    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).
    var tc = dataset

    // Because join() joins on keys, the edges are stored in reversed order.
    val edges = tc.map(x => (x._2, x._1))

    // This join is iterated until a fixed point is reached.
    var numIterations = 0
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      val startTime = System.currentTimeMillis

      logInfo("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))
      numIterations += 1
      oldCount = nextCount
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache();
      nextCount = tc.count()

      val endTime = System.currentTimeMillis
      times += (endTime - startTime)

    } while (nextCount != oldCount)

    logInfo("TC has " + tc.count() + " edges, done in " + numIterations + " linear TC iterations.")
    times
  }

  def recursiveDoublingTC(dataset: RDD[(Int, Int)]): ArrayBuffer[Long] = {

    val times = new ArrayBuffer[Long](10)

    var tc = dataset

    var numIterations = 0
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      val startTime = System.currentTimeMillis
      logInfo("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))
      numIterations += 1
      oldCount = nextCount

      val reversedTc = tc.map(x => (x._2, x._1))
      val doubledTc = tc.join(reversedTc, DEFAULT_PARALLELISM).map(x => (x._2._2, x._2._1))
      tc = tc.union(doubledTc).distinct(DEFAULT_PARALLELISM).cache()
      nextCount = tc.count()

      val endTime = System.currentTimeMillis
      times += (endTime - startTime)

    } while (nextCount != oldCount)

    logInfo("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))

    logInfo("TC has " + tc.count() + " edges, done in " + numIterations +
      " recursive doubling TC iterations.")

    times
  }

  val NUM_FINE_GRAINED_BUCKETS = 1024
  var MAX_NUM_EDGES_PER_REDUCER_COGROUP = 1000 * 1000
  var MAX_NUM_EDGES_PER_REDUCER_DISTINCT = 1000 * 1000
  var DEFAULT_PARALLELISM = 200

  def cogroup[K: ClassManifest, V: ClassManifest, W: ClassManifest](
    r1: RDD[(K, V)], r2: RDD[(K, W)]): RDD[(K, Array[ArrayBuffer[Any]])] = {

    val part = new HashPartitioner(NUM_FINE_GRAINED_BUCKETS)
    val preshuffleResult1 = r1.preshuffle(part, CountPartitionStatAccumulator)
    val preshuffleResult2 = r2.preshuffle(part, CountPartitionStatAccumulator)

    val numEdges1 = preshuffleResult1.customStats.sum
    val numEdges2 = preshuffleResult2.customStats.sum
    val totalEdges = numEdges1 + numEdges2

    val numCoalescedPartitions = totalEdges / MAX_NUM_EDGES_PER_REDUCER_COGROUP
    val groups = Utils.groupArray(0 until NUM_FINE_GRAINED_BUCKETS, numCoalescedPartitions)

    logInfo("cogroup: %d + %d edges, %d reducers".format(numEdges1, numEdges2, groups.size))

    new CoalescedCoGroupedRDD(
      Seq(r1.asInstanceOf[RDD[(Any, Any)]], r2.asInstanceOf[RDD[(Any, Any)]]),
      groups,
      List(preshuffleResult1.dep, preshuffleResult2.dep))
  }

  def join[K: ClassManifest, V: ClassManifest, W: ClassManifest](
    r1: RDD[(K, V)], r2: RDD[(K, W)]): RDD[(K, (V, W))] = {

    val valueManifest = ClassManifest.fromClass(classOf[ArrayBuffer[Any]]).arrayManifest

    (new PairRDDFunctions(cogroup(r1, r2))(classManifest[K], valueManifest)).flatMapValues {
      g: Array[ArrayBuffer[Any]] =>
        for (v <- g(0).iterator; w <- g(1).iterator) yield (v.asInstanceOf[V], w.asInstanceOf[W])
    }
  }

  def distinct[T: ClassManifest](rdd: RDD[T]): RDD[T] = {
    val part = new HashPartitioner(NUM_FINE_GRAINED_BUCKETS)
    val kvRdd: RDD[(T, Null)] = rdd.map(x => (x, null))
    val preshuffleResult = kvRdd.preshuffle(part, CountPartitionStatAccumulator)

    val totalEdges = preshuffleResult.customStats.sum
    val numCoalescedPartitions = totalEdges / MAX_NUM_EDGES_PER_REDUCER_DISTINCT
    val groups = Utils.groupArray(0 until NUM_FINE_GRAINED_BUCKETS, numCoalescedPartitions)

    logInfo("distinct: %d edges, %d reducers".format(totalEdges, groups.size))

    val shuffledRdd = new CoalescedShuffleFetcherRDD(kvRdd, groups, preshuffleResult.dep)
    shuffledRdd.mapPartitions { part =>
      val hashset = new java.util.HashSet[T]
      part.foreach(x => hashset += x._1)
      hashset.iterator
    }
  }

  def recursiveDoublingTCPartialDag(dataset: RDD[(Int, Int)]): ArrayBuffer[Long] = {

    val times = new ArrayBuffer[Long](10)

    var tc = dataset

    var numIterations = 0
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      val startTime = System.currentTimeMillis
      logInfo("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))
      numIterations += 1
      oldCount = nextCount

      // Join to double edge size.
      val reversedTc = tc.map(x => (x._2, x._1))
      val doubledTc = join(tc, reversedTc).map(x => (x._2._2, x._2._1)).mapPartitions { part =>
        val hashset = new java.util.HashSet[(Int, Int)]
        part.foreach(x => hashset += x)
        hashset.iterator
      }

      // Distinct to remove duplicated reachable vertex pairs.
      tc = distinct(tc.union(doubledTc)).cache()

      nextCount = tc.count()
      val endTime = System.currentTimeMillis
      times += (endTime - startTime)

    } while (nextCount != oldCount)

    logInfo("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))

    logInfo("TC has " + tc.count() + " edges, done in " + numIterations +
      " recursive doubling TC iterations.")

    times
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkTC <master> <slices> <numVertices> <numEdges> <method>")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "SparkTC")
    val slices = args(1).toInt
    val numVertices = args(2).toInt
    val numEdges = args(3).toInt / slices
    val method = args(4).toInt

    // Generate the dataset.
    var dataset = spark.parallelize(0 until slices, slices).flatMap { part =>
      generateGraph(part, numVertices, numEdges)
    }.cache

    val startTime = System.currentTimeMillis()

    val times: ArrayBuffer[Long] = method match {
      case 0 => linearTC(dataset)
      case 1 => {
        DEFAULT_PARALLELISM = args(5).toInt
        recursiveDoublingTC(dataset)
      }
      case 2 => {
        MAX_NUM_EDGES_PER_REDUCER_COGROUP = args(5).toInt
        MAX_NUM_EDGES_PER_REDUCER_DISTINCT = args(6).toInt
        recursiveDoublingTCPartialDag(dataset)
      }
    }

    times.zipWithIndex.foreach { case(t, i) => logInfo("#%d: %.4f".format(i, t.toDouble / 1000)) }

    val endTime = System.currentTimeMillis()
    logInfo("Elapsed time: %.4f s".format((endTime - startTime).toDouble / 1000))
  }
}
