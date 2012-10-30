package spark

private[spark] abstract class ShuffleFetcher {
  /**
   * Fetch the shuffle outputs for a given ShuffleDependency.
   * @return An iterator over the elements of the fetched shuffle outputs.
   */
  def fetch[K, V](shuffleId: Int, reduceId: Int) : Iterator[(K, V)] =
    fetchMultiple(shuffleId, Array(reduceId))

  /**
   * Fetch multiple sets of shuffle outputs for a given ShuffleDependency.
   * @return An iterator over the elements of the fetched shuffle outputs.
   */
  def fetchMultiple[K, V](shuffleId: Int, reduceIds: Seq[Int]) : Iterator[(K, V)]

  /** Stop the fetcher */
  def stop() {}
}
