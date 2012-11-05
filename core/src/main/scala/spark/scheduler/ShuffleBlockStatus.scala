package spark.scheduler

import spark.storage.BlockManagerId

/**
 * Describes a single block produced by a ShuffleMapTask.
 *
 * @param shuffleId: the shuffle phase that produced this block
 * @param mapId: the mapper partition that produced this block
 * @param reduceId: the reducer partition that contains this block
 * @param address the block manager address for the machine containing this block
 * @param size the size of the block, in bytes
 * @param customStats optional custom statistics
 */
case class ShuffleBlockStatus(
  val shuffleId: Int,
  val mapId: Int,
  val reduceId : Int,
  val address: BlockManagerId,
  val size: Long,
  val customStats: Array[Byte])