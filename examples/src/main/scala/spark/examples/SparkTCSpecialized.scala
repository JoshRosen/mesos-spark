package spark.examples

import spark._
import SparkContext._
import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import spark.rdd.CoalescedCoGroupedRDD
import spark.rdd.CoalescedShuffleFetcherRDD
import spark.rdd.ShuffledRDD

import it.unimi.dsi.fastutil.longs.LongOpenHashSet

/**
 * Transitive closure on a graph.
 */
object SparkTCSpecialized extends Logging {

  var sc: SparkContext = null

  def removeShuffleData(numTasks: Int) {
    sc.parallelize(0 until numTasks, numTasks).foreach { task =>
      SparkEnv.get.shuffleBlocks.synchronized {
        val shuffleBlocks = SparkEnv.get.shuffleBlocks
        val blockManager = SparkEnv.get.blockManager

        shuffleBlocks.foreach { case(shuffleId, info) =>
          val numMapOutputs: Int = info._1
          val mapIds: ArrayBuffer[Int] = info._2

          var i = 0
          while (i < mapIds.size) {
            var j = 0
            while (j < numMapOutputs) {
              blockManager.drop("shuffle_" + shuffleId + "_" + mapIds(i) + "_" + j)
              j += 1
            }
            i += 1
          }
        }

        shuffleBlocks.clear()
      }
    }
  }

  // Packs two ints into a long.
  def edge(src: Int, dst: Int): Long = {
    val srcPacked: Long = src.toLong << 32
    val dstPacked: Long = dst & 0xFFFFFFFFL
    srcPacked | dstPacked
  }

  def src(edge: Long): Int = (edge >> 32).toInt

  def dst(edge: Long): Int = (edge & 0xFFFFFFFFL).toInt

  def distinctOnEachPartition(rdd: RDD[Long]): RDD[Long] = {
    rdd.mapPartitions { part =>
      val hashset = new LongOpenHashSet
      part foreach(x => hashset.add(x))
      hashset.iterator.asInstanceOf[java.util.Iterator[Long]]
    }
  }

  def distinctOnEachPartitionAndReturnArray(rdd: RDD[Long]): RDD[Array[Long]] = {
    rdd.mapPartitions { part =>
      val hashset = new LongOpenHashSet
      part.foreach(x => hashset.add(x))
      Iterator(hashset.toLongArray)
    }
  }

  def unpackArray(rdd: RDD[Array[Long]]): RDD[Long] = rdd.mapPartitions(_.next.iterator)

  def generateGraph(randSeed: Int, numVertices: Int, numEdges: Int): Array[Long] = {
    val rand = new Random(42 + randSeed)
    val edges = new LongOpenHashSet(numEdges)

    while (edges.size < numEdges) {
      val src = rand.nextInt(numVertices)
      val dst = rand.nextInt(numVertices)
      if (src != dst) edges.add(edge(src, dst))
    }

    edges.toLongArray
  }

  def recursiveDoublingTCPartialDag(dataset: RDD[Array[Long]]): ArrayBuffer[Long] = {
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
      val tcKv: RDD[(Int, Int)] = unpackArray(tc).map(x => (src(x), dst(x)))
      val reversedTc: RDD[(Int, Int)] = unpackArray(tc).map(x => (dst(x), src(x)))
      val joined: RDD[Long] = distinctOnEachPartition(
        join(tcKv, reversedTc).map(x => edge(x._2._2, x._2._1)))

      // Distinct to remove duplicated reachable vertex pairs.
      tc = distinctOnEachPartitionAndReturnArray(
        hashPartitionPde(unpackArray(tc).union(joined))).cache()

      nextCount = tc.mapPartitions(part => Iterator(part.next.size)).reduce(_+_)
      removeShuffleData(SLICES)

      val endTime = System.currentTimeMillis
      times += (endTime - startTime)

    } while (nextCount != oldCount)

    logInfo("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))
    logInfo("TC has " + nextCount + " edges, done in " + numIterations +
      " recursive doubling TC iterations.")
    times
  }

  def recursiveDoublingTC(dataset: RDD[Array[Long]]): ArrayBuffer[Long] = {
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

      val tcKv: RDD[(Int, Int)] = unpackArray(tc).map(x => (src(x), dst(x)))
      val reversedTc: RDD[(Int, Int)] = unpackArray(tc).map(x => (dst(x), src(x)))
      val joined: RDD[Long] = distinctOnEachPartition(
        tcKv.join(reversedTc, DEFAULT_PARALLELISM).map(x => edge(x._2._2, x._2._1)))

      tc = distinctOnEachPartitionAndReturnArray(
        hashPartitionNoPde(unpackArray(tc).union(joined), DEFAULT_PARALLELISM)).cache()

      nextCount = tc.mapPartitions(part => Iterator(part.next.size)).reduce(_+_)
      removeShuffleData(SLICES)

      val endTime = System.currentTimeMillis
      times += (endTime - startTime)

    } while (nextCount != oldCount)

    logInfo("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))
    logInfo("TC has " + nextCount + " edges, done in " + numIterations +
      " recursive doubling TC iterations.")
    times
  }

  val NUM_FINE_GRAINED_BUCKETS = 1024
  var MAX_NUM_EDGES_PER_REDUCER_COGROUP = 1000 * 1000
  var MAX_NUM_EDGES_PER_REDUCER_DISTINCT = 1000 * 1000
  var DEFAULT_PARALLELISM = 200
  var SLICES = 80

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

  def hashPartitionNoPde[T: ClassManifest](rdd: RDD[T], partitions: Int): RDD[T] = {
    val part = new HashPartitioner(partitions)
    val kvRdd: RDD[(T, Null)] = rdd.map(x => (x, null))
    (new ShuffledRDD[T, Null](kvRdd, part)).map(x => x._1)
  }

  def hashPartitionPde[T: ClassManifest](rdd: RDD[T]): RDD[T] = {
    val part = new HashPartitioner(NUM_FINE_GRAINED_BUCKETS)
    val kvRdd: RDD[(T, Null)] = rdd.map(x => (x, null))
    val preshuffleResult = kvRdd.preshuffle(part, CountPartitionStatAccumulator)

    val totalEdges = preshuffleResult.customStats.sum
    val numCoalescedPartitions = totalEdges / MAX_NUM_EDGES_PER_REDUCER_DISTINCT
    val groups = Utils.groupArray(0 until NUM_FINE_GRAINED_BUCKETS, numCoalescedPartitions)

    logInfo("distinct: %d edges, %d reducers".format(totalEdges, groups.size))

    (new CoalescedShuffleFetcherRDD(kvRdd, groups, preshuffleResult.dep)).map(x => x._1)
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println(
        "Usage: SparkTCSpecialized <master> <slices> <numVertices> <numEdges> <method>")
      System.exit(1)
    }
    sc = new SparkContext(args(0), "SparkTC")
    val slices = args(1).toInt
    val numVertices = args(2).toInt
    val numEdges = args(3).toInt / slices
    val method = args(4).toInt

    SLICES = slices

    // Generate the dataset.
    var dataset: RDD[Array[Long]] = sc.parallelize(0 until slices, slices).mapPartitions {
      part => Iterator(generateGraph(part.next, numVertices, numEdges))
    }.cache

    val startTime = System.currentTimeMillis()

    val times: ArrayBuffer[Long] = method match {
      //case 0 => linearTC(dataset)
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

  // def linearTC(dataset: RDD[(Int, Int)]): ArrayBuffer[Long] = {
  //   val times = new ArrayBuffer[Long](10)
  //   // Linear transitive closure: each round grows paths by one edge,
  //   // by joining the graph's edges with the already-discovered paths.
  //   // e.g. join the path (y, z) from the TC with the edge (x, y) from
  //   // the graph to obtain the path (x, z).
  //   var tc = dataset

  //   // Because join() joins on keys, the edges are stored in reversed order.
  //   val edges = tc.map(x => (x._2, x._1))

  //   // This join is iterated until a fixed point is reached.
  //   var numIterations = 0
  //   var oldCount = 0L
  //   var nextCount = tc.count()
  //   do {
  //     val startTime = System.currentTimeMillis

  //     logInfo("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))
  //     numIterations += 1
  //     oldCount = nextCount
  //     // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
  //     // then project the result to obtain the new (x, z) paths.
  //     tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache();
  //     nextCount = tc.count()

  //     val endTime = System.currentTimeMillis
  //     times += (endTime - startTime)

  //   } while (nextCount != oldCount)

  //   logInfo("TC has " + tc.count() + " edges, done in " + numIterations + " linear TC iterations.")
  //   times
  // }
}
