package spark

import org.scalatest.FunSuite

/**
 * Tests related to collecting results from workers to the master
 * (e.g. accumulator updates or reduce() results).
 */
class ResultCollectionSuite extends FunSuite with LocalSparkContext {

  test("collecting results larger than Akka frame size") {
    // We need to use local-cluster mode since results are returned differently
    // when running under LocalScheduler:
    sc = new SparkContext("local-cluster[1,1,512]", "test")
    val akkaFrameSize = sc.env.actorSystem.settings.config.getBytes("akka.remote.netty.message-frame-size").toInt
    val result = sc.parallelize(Seq(1)).map{x => 1.to(akkaFrameSize).toArray }.reduce((x, y) => x)
    assert (result === 1.to(akkaFrameSize).toArray)
  }

  test("collecting small results") {
    // We need to use local-cluster mode since results are returned differently
    // when running under LocalScheduler:
    sc = new SparkContext("local-cluster[1,1,512]", "test")
    val result = sc.parallelize(Seq(1)).map{x => 2 * x }.reduce((x, y) => x)
    assert (result === 2)
  }
}