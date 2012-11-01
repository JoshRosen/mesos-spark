package spark.benchmark

import scala.collection.mutable.ArrayBuffer
import scala.testing.Benchmark
import scala.util.Random

import spark._
import spark.storage._


// 100
// 8304, 7589, 7634, 7772, 7611 - mem
// 8098, 7216, 7266, 7416, 7282 - disk
// 1000
// 9014, 7980, 7857, 8051, 7579
// 10000
// 10496, 7963, 7579, 7837, 7622


object OutputSplitBenchmark extends Benchmark {

  // total size: 200MB
  val NUM_TUPLES = 2 * 1000 * 1000

  val rand = new Random

  def generateData(): Array[(Array[Byte], Array[Byte])] = {
    // 100 byte tuples (16B key + 84B value)
    Array.fill(NUM_TUPLES) {
      val key = new Array[Byte](16)
      val value = new Array[Byte](84)
      rand.nextBytes(key)
      rand.nextBytes(value)
      (key, value)
    }
  }

  val data = generateData()

  var partitioner = new HashPartitioner(100)

  def runBenchmark(numOutputSplits: Int, noTimes: Int) = {
    partitioner = new HashPartitioner(numOutputSplits)
    super.runBenchmark(noTimes)
  }

  def run = {

    val numOutputSplits = partitioner.numPartitions

    // Partition the map output.
    val buckets = Array.fill(numOutputSplits)(new ArrayBuffer[(Any, Any)])
    for (elem <- data.iterator) {
      val pair = elem.asInstanceOf[(Any, Any)]
      val bucketId = partitioner.getPartition(pair._1)
      buckets(bucketId) += pair
    }
    val bucketIterators = buckets.map(_.iterator)

    val compressedSizes = new Array[Byte](numOutputSplits)

    val blockManager = SparkEnv.get.blockManager
    for (i <- 0 until numOutputSplits) {
      val blockId = "shuffle_" + rand.nextInt() + "_" + 0 + "_" + i
      // Get a Scala iterator from Java map
      val iter: Iterator[(Any, Any)] = bucketIterators(i)
      val size = blockManager.put(blockId, iter, StorageLevel.DISK_ONLY, false)
      //compressedSizes(i) = MapOutputTracker.compressSize(size)
    }

    println("one more run done")
  }

}

