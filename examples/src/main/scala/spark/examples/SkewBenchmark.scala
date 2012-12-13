package spark.examples

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import spark.{SparkEnv, RDD, HashPartitioner, SparkContext}
import spark.SparkContext._
import spark.rdd.CoalescedShuffleFetcherRDD

import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap => OLMap}


object SkewBenchmark {
  val alphas = Seq(1.1, 1.2, 1.3, 1.4, 1.5)
  val taskCountMultipliers = Seq(1, 2, 4, 8)
  val bucketCounts = Seq(10, 100, 1000)
  val POINTS_PER_TASK = 1000000
  val NUM_REPETITONS = 5

  val someLock = new Object

  var sc : SparkContext = null

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SkewBenchmark <master> <numMachines>")
      System.exit(1)
    }
    val master = args(0)
    val numMachines = args(1).toInt

    sc = new SparkContext(master, "SkewBenchmark")

    // Generate a skewed data set following a Zipf distribution

    for (alpha <- alphas) {
      // Create the data set
      val samples : RDD[(Int, String)] = sc.parallelize(1 to numMachines,
        numMachines).flatMap(_ => generate_zipf(POINTS_PER_TASK, alpha))
      samples.cache()
      samples.count() // Force evaluation

      for (diskBasedShuffle <- Seq(true, false)) {
        sc.parallelize(0 until numMachines, numMachines).foreach { task =>
          System.setProperty("spark.diskBasedShuffle", diskBasedShuffle.toString)
        }
        for (
          numTasks <- taskCountMultipliers.map(_ * numMachines);
          numBuckets <- Seq(numTasks) ++ bucketCounts;
          repetition <- 1 to NUM_REPETITONS) {
          val isPde = numBuckets > numTasks
          val time = runQuery(samples, numTasks, numBuckets)
          removeShuffleData(numMachines)
          System.out.println("TIME: " +
            Seq(alpha, isPde, diskBasedShuffle, numTasks, numBuckets, time).mkString(","))
        }
      }
      removeCachedRdd(samples, numMachines)
    }
    System.exit(0)
  }

  def runQuery(data : RDD[(Int, String)], numTasks: Int, numBuckets: Int): Long = {
    val startTime = System.currentTimeMillis()
    val partitioner = new HashPartitioner(numBuckets)
    val preshuffleResult = data.preshuffle(partitioner)
    val totalBytes = preshuffleResult.sizes.sum
    System.out.println("Total bytes: " + totalBytes)
    val groups: Array[Array[Int]] = {
      val nonEmptyPartitions = preshuffleResult.sizes.zipWithIndex.filter(_._1 != 0)
      val bins = packBins[Int](numTasks, nonEmptyPartitions)
      System.out.println("Group costs are " + bins.map(_._1).sorted.toIndexedSeq)
      bins.map(_._2.toArray).toArray
    }
    val coalesced = new CoalescedShuffleFetcherRDD(data, groups, preshuffleResult.dep)
    coalesced.count // Force evaluation.  TODO: actually do something in the reduce phase.
    val time = System.currentTimeMillis() - startTime
    time
  }

  /** Returns a pseudorandomly generated String drawing upon
    * only ASCII characters between 33 and 126.
    */
  def nextASCIIString(length: Int) = {
    val (min, max) = (33, 126)
    def nextDigit = Random.nextInt(max - min) + min
    new String(Array.fill(length)(nextDigit.toByte), "ASCII")
  }

  def generate_zipf(numSamples: Int, alpha: Double, minKey: Int = 1, maxKey: Int = 1000000,
    seed: Int = 42) : Seq[(Int, String)] = {
    val rand = new Random(seed)
    // Shuffle the keys randomly to prevent the largest partitions from being
    // hashed into adjacent buckets:
    val keyMappingTable = rand.shuffle(0.to(maxKey).toSeq).toArray
    val g = new org.apache.hadoop.io.file.tfile.RandomDistribution.Zipf(
      new java.util.Random, minKey, maxKey, alpha)
    val range = 0 to 9
    // Multiply by 10 and add random noise to  keep the top key from being
    // overloaded.
    (1 to numSamples).map(i => (keyMappingTable(g.nextInt) * 10 + range(Random.nextInt(10)),
      nextASCIIString(48)))
  }

  /**
   * Greedily pack bins.  In increasing order of cost, assigns each item to the bin with the lowest total cost.
   * @param nBins the number of bins
   * @param items a list of (cost, item) pairs
   * @return a list of (groupCost, groupItem) pairs, representing the grouping.
   */
  def packBins[T](nBins: Int, items: Seq[(Long, T)]): Seq[(Long, Seq[T])] = {
    val groupOrdering = Ordering.by[(Long, ArrayBuffer[T]), Long](_._1).reverse
    val groups = mutable.PriorityQueue[(Long, ArrayBuffer[T])]()(groupOrdering)
    1.to(nBins).foreach(x => groups.enqueue((0L, ArrayBuffer[T]())))
    for (partition <- items.sortBy(- _._1)) {
      val (cost, assignedItems) = groups.dequeue()
      assignedItems.append(partition._2)
      groups.enqueue((cost + partition._1, assignedItems))
    }
    groups.toSeq
  }

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

  def removeCachedRdd(rdd: RDD[_], numTasks: Int) {
    val numSplits = sc.dagScheduler.getCacheLocs(rdd).size
    val rddId = rdd.id

    sc.parallelize(0 until numTasks, numTasks).foreach { task =>
      someLock.synchronized {
        (0 until numSplits).foreach { splitIndex =>
          SparkEnv.get.blockManager.drop("rdd_%d_%d".format(rddId, splitIndex))
        }
      }
    }
  }
}
