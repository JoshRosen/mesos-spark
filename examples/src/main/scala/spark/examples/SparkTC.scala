package spark.examples

import spark._
import SparkContext._
import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Transitive closure on a graph.
 */
object SparkTC {

  val rand = new Random(42)

  def generateGraph(numVertices: Int, numEdges: Int) = {
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

      println("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))
      numIterations += 1
      oldCount = nextCount
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache();
      nextCount = tc.count()

      val endTime = System.currentTimeMillis
      times += (endTime - startTime)

    } while (nextCount != oldCount)

    println("TC has " + tc.count() + " edges, done in " + numIterations + " linear TC iterations.")
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

      println("iteration %d: %d -> %d".format(numIterations, oldCount, nextCount))
      numIterations += 1
      oldCount = nextCount

      val reversedTc = tc.map(x => (x._2, x._1))
      tc = tc.union(tc.join(reversedTc).map(x => (x._2._2, x._2._1))).distinct().cache()
      nextCount = tc.count()

      val endTime = System.currentTimeMillis
      times += (endTime - startTime)

    } while (nextCount != oldCount)

    println("TC has " + tc.count() + " edges, done in " + numIterations +
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
    var dataset = spark.parallelize(0 until slices, slices).mapPartitions { part =>
      generateGraph(numVertices, numEdges)
    }.cache

    val startTime = System.currentTimeMillis()

    val times = if (method == 0) linearTC(dataset) else recursiveDoublingTC(dataset)
    times.zipWithIndex.foreach { case(t, i) => println("#%d: %d".format(i, t / 1000)) }

    val endTime = System.currentTimeMillis()
    println("Elapsed time: " + (startTime - endTime))
  }
}
