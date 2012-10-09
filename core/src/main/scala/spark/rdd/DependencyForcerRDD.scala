package spark.rdd

import spark.Dependency
import spark.RDD
import spark.Split

/**
 * Dummy RDD whose purpose is to force evaluation of its dependencies.
 */
class DependencyForcerRDD[T: ClassManifest](prev: RDD[T], deps: List[Dependency[T]])
  extends RDD[T](prev.context) {

  override def splits = Array(new ShuffledRDDSplit(1))
  override val dependencies = deps
  override def compute(split: Split) = null
  override def preferredLocations(split: Split) = Nil
}
