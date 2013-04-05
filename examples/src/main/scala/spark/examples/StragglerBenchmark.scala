package spark.examples

import scala.util.Random

import spark._
import spark.SparkContext._

import org.apache.hadoop.io.file.tfile.RandomDistribution
import java.io.FileWriter


object StragglerBenchmark {
  val taskCountMultipliers = Seq(1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34,
    36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 128, 256)
  val POINTS_PER_TASK = 1000000
  val NUM_REPETITONS = 10
  val RAND_SEED = 42

  var sc: SparkContext = null

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: StragglerBenchmark <master> <numMachines> <logFile>")
      System.exit(1)
    }
    val master = args(0)
    val numMachines = args(1).toInt  // Needed to pick a balanced number of partitions
    val logFilename = args(2)        // CSV file to log results
    val log = new FileWriter(logFilename, true)

    sc = new SparkContext(master, "StragglerBenchmark")

    val blockManagerMaster = SparkEnv.get.blockManager.master

    // Create the data set.  To ensure that partitions are deterministically recomputed,
    // we will choose the random seeds here:
    val rand = new Random(RAND_SEED)
    val randomSeeds = sc.parallelize(1.to(numMachines).map(rand.nextInt), numMachines)
    val samples: RDD[(Int, String)] =
      randomSeeds.flatMap(seed => generate_data(POINTS_PER_TASK, genSeed=seed))
    samples.cache()
    samples.count() // Force evaluation

    for (numTasks <- taskCountMultipliers.map(_ * numMachines);
         rep <- 1 to NUM_REPETITONS) {
      val time = runQuery(samples, numTasks)
      // Clean up before next run:
      blockManagerMaster.removeShuffleBlocks()
      sc.parallelize(1 to numMachines, numMachines).foreach { _ => System.gc() }
      // Log results:
      val csvRow = Seq(numTasks, time).mkString(",") + "\n"
      log.write(csvRow)
      log.flush()
      System.out.println("TIME: " + csvRow)
    }

    System.exit(0)
  }

  def runQuery(data : RDD[(Int, String)], numTasks: Int): Long = {
    val startTime = System.currentTimeMillis()
    val partitioner = new HashPartitioner(numTasks)
    data.groupByKey(partitioner).count()
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

  def generate_data(numSamples: Int, minKey: Int = 1, maxKey: Int = 1000000,
    genSeed: Int = 42) : Seq[(Int, String)] = {
    val genRand = new java.util.Random(genSeed)
    val g = new RandomDistribution.Flat(genRand, minKey, maxKey)
    (1 to numSamples).map(i => (g.nextInt, nextASCIIString(48)))
  }
}