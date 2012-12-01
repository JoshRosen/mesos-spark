package spark

import scala.collection.mutable.HashMap
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import spark.rdd.CoalescedRDD
import SparkContext._

class StatisticsCollectorSuite extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = _

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  test("cardinality") {
    sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(1 to 1000, 2).map(x => (x, x))
    val preShuffled =
      nums.preshuffle(new HashPartitioner(2), Some(CountPartitionStatAccumulator), None)
    assert(preShuffled.customStats.get.sum === 1000)
  }
}
