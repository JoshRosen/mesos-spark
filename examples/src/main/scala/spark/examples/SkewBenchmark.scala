package spark.examples

import scala.util.Random
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import spark.{SparkEnv, RDD, HashPartitioner, SparkContext}
import spark.SparkContext._
import spark.rdd.CoalescedShuffleFetcherRDD

import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap => OLMap}
import org.apache.hadoop.io.file.tfile.RandomDistribution


object SkewBenchmark {
  val alphas = Seq(1.1, 1.2, 1.3, 1.4, 1.5)
  val taskCountMultipliers = Seq(1, 2, 4, 8, 16)
  val bucketCountMultipliers = Seq(1, 10, 100, 1000)
  val POINTS_PER_TASK = 1000000
  val NUM_REPETITONS = 5
  val RAND_SEED = 42


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

    val blockManagerMaster = SparkEnv.get.blockManager.master

    // Generate a skewed data set following a Zipf distribution

    for (alpha <- alphas) {
      // Create the data set.  To ensure that partitions are deterministically recomputed,
      // we will choose the random seeds here:
      val rand = new Random(RAND_SEED)
      val randomSeeds = sc.parallelize(1.to(numMachines).map(rand.nextInt), numMachines)
      val samples : RDD[(Int, String)] =
        randomSeeds.flatMap(seed => generate_zipf(POINTS_PER_TASK, alpha, genSeed=seed))
      samples.cache()
      samples.count() // Force evaluation

      for (diskBasedShuffle <- Seq(true, false)) {
        for (
          numTasks <- taskCountMultipliers.map(_ * numMachines);
          numBuckets <- bucketCountMultipliers.map(_ * numTasks);
          repetition <- 1 to NUM_REPETITONS) {
          val isPde = numBuckets > numTasks
          val time = runQuery(samples, numTasks, numBuckets, diskBasedShuffle)
          blockManagerMaster.removeShuffleBlocks()
          sc.parallelize(1 to numMachines, numMachines).foreach { _ => System.gc() }
          System.out.println("TIME: " +
            Seq(alpha, diskBasedShuffle, isPde, numTasks, numBuckets, time).mkString(","))
        }
      }
      removeCachedRdd(samples, numMachines)
      sc.parallelize(1 to numMachines, numMachines).foreach { _ => System.gc() }
    }
    System.exit(0)
  }

  def runQuery(data : RDD[(Int, String)], numTasks: Int, numBuckets: Int,
    diskBasedShuffle: Boolean): Long = {
    val startTime = System.currentTimeMillis()
    val partitioner = new HashPartitioner(numBuckets)
    val preshuffleResult = data.preshuffle(partitioner, diskBasedShuffle)
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
    shuffleSeed: Int = 42, genSeed: Int = 42) : Seq[(Int, String)] = {
    val shuffleRand = new Random(shuffleSeed)
    // Shuffle the keys randomly to prevent the largest partitions from being
    // hashed into adjacent buckets:
    val keyMappingTable = shuffleRand.shuffle(0.to(maxKey).toSeq).toArray
    val genRand = new java.util.Random(genSeed)
    val g = new RandomDistribution.Zipf(genRand, minKey, maxKey, alpha)
    val range = 0 to 9
    // Multiply by 10 and add random noise to  keep the top key from being
    // overloaded.
    (1 to numSamples).map(i => (keyMappingTable(g.nextInt) * 10 + range(genRand.nextInt(10)),
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

  def removeCachedRdd(rdd: RDD[_], numTasks: Int) {
    val numSplits = sc.dagScheduler.getCacheLocs(rdd).size
    val rddId = rdd.id
    val blockManagerMaster = SparkEnv.get.blockManager.master

    (0 until numSplits).foreach { splitIndex =>
      blockManagerMaster.removeBlock("rdd_%d_%d".format(rddId, splitIndex))
    }
  }
}
