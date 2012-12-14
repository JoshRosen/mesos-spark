package spark.rdd

import spark.Partitioner
import spark.RDD
import spark.ShuffleDependency
import spark.SparkEnv
import spark.Split
import spark.TaskContext


class ShuffleDependencyForcerRDD[K: ClassManifest, V: ClassManifest](
    prev: RDD[(K, V)], dep: ShuffleDependency[K, V])
  extends RDD[(K, V)](prev.context) {

  override def splits = Array(new ShuffledRDDSplit(1))
  override val dependencies = List(dep)
  override def compute(split: Split, taskContext: TaskContext) = null
  override def preferredLocations(split: Split) = Nil
}
