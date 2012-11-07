package spark.benchmark

import scala.util.Random

import com.clearspring.analytics.stream.cardinality.{AdaptiveCounting, HyperLogLog}
import it.unimi.dsi.fastutil.longs.LongOpenHashSet


object Cardinality {

  def main(args: Array[String]) {

    val numDistinct = args(0).toInt

    val rand = new Random
    val hashset = new LongOpenHashSet(numDistinct)
    val hyperloglog = new HyperLogLog(4)
    val adaptive = new AdaptiveCounting(1)

    while (hashset.size < numDistinct) {
      val next = rand.nextLong()
      hashset.add(next)
      hyperloglog.offer(next)
    }

    println("Num distinct: " + hashset.size)

    println("HyperLogLog: %d using %d %d with error %d".format(
      hyperloglog.cardinality,
      hyperloglog.getBytes.size, hyperloglog.sizeof,
      math.abs(hyperloglog.cardinality - numDistinct) * 100 / numDistinct))

    println("Adaptive: %d using %d with error %d".format(
      adaptive.cardinality,
      adaptive.getBytes.size,
      math.abs(adaptive.cardinality - numDistinct) * 100 / numDistinct))

  }

}
