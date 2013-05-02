package spark.examples

import java.util.Random
import scala.math.exp
import spark.util.Vector
import spark._
import spark.SparkContext._
import scala.Array

class LRPartition(i: Int) extends Partition { def index: Int = i }

/**
 * Fetches the fragments of the model's weight vector, re-assembles them, and joins the re-assembled
 * model with the data points to produce a net gradient, which is split back into fragments.
 */
class LRWeightsMapRDD[T: ClassManifest](
   sc: SparkContext,
   splitSize: Int,
   f: (Vector, Iterator[T]) => Iterator[Vector],
   var model: RDD[(Int, Vector)],
   var rdd: RDD[T])
  extends RDD[(Int, Vector)](sc, Nil) /* Nil because we implement getDependencies */ {

  val rddPartitions = rdd.partitions
  val modelPartitions = model.partitions

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new ShuffleDependency(model, new HashPartitioner(rdd.partitions.length)), new OneToOneDependency(rdd))
  }

  override def getPartitions: Array[Partition] = {
    rdd.partitions.map(x => new LRPartition(x.index))
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(Int, Vector)] = {
    val currentModel = modelPartitions.flatMap(model.iterator(_, context)).sortBy(_._1).flatMap(_._2.elements)
    val gradient = f(new Vector(currentModel), rdd.iterator(rddPartitions(s.index), context)).reduce(_ + _)
    SparkLRCollectiveCommunication.splitVector(gradient, splitSize)
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd = null
    model = null
  }
}

/**
 * Logistic regression based classification.
 */
object SparkLRCollectiveCommunication {
  val N = 10000  // Number of data points
  val D = 10   // Number of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def generateData = {
    def generatePoint(i: Int) = {
      val y = if(i % 2 == 0) -1 else 1
      val x = Vector(D, _ => rand.nextGaussian + y * R)
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }

  def splitVector(vec: Vector, numFragments: Int): Iterator[(Int, Vector)] =
    vec.elements.grouped(numFragments).map(new Vector(_)).zipWithIndex.map(_.swap)

  def joinVectors(vecs: Seq[(Int, Vector)]): Vector =
    new Vector(vecs.sortBy(_._1).flatMap(_._2.elements).toArray)

  def computeGradients(w: Vector, points: Iterator[DataPoint]): Iterator[Vector] =
    points.map(p => (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y * p.x)

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkLRCollectiveCommunication <master> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkLRCollectiveCommunication",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
    val numSlices = if (args.length > 1) args(1).toInt else 2
    val points = sc.parallelize(generateData, numSlices).cache()
    val splitSize = D / numSlices

    // Initialize w to a random value
    val initialW = Vector(D, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + initialW)

    // Break out weight vector into fragments:
    var weightFragments: RDD[(Int, Vector)] =
      sc.parallelize(splitVector(initialW, splitSize).toSeq, numSlices).cache()

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)

      // Fetch every weight vector fragment at every machine, then map over the data points
      // with the re-assembled weight vector to compute the gradients:
      val gradientFragments = new LRWeightsMapRDD(sc, splitSize, computeGradients,
        weightFragments, points)

      // Add these gradient fragments to produce a net gradient:
      val netGradientFragments = gradientFragments.reduceByKey(_ + _)

      // Join this net gradient with the weight vectors to compute the new eights:
      weightFragments = weightFragments.join(netGradientFragments).mapValues { x => x._1 - x._2 }
    }

    println("Final w: " + joinVectors(weightFragments.collect()))
    System.exit(0)
  }
}
