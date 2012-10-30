package spark

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import scheduler.ShuffleBlockStatus
import spark.storage.BlockManagerId

private[spark] class BlockStoreShuffleFetcher extends ShuffleFetcher with Logging {
  override def fetchMultiple[K, V](shuffleId: Int, reduceIds: Seq[Int]) = {
    val reduceIdsString = reduceIds.toString().dropWhile(_ != '(')
    logDebug("Fetching outputs for shuffle %d, reduceIds %s".format(shuffleId, reduceIdsString))
    val blockManager = SparkEnv.get.blockManager
    
    val startTime = System.currentTimeMillis
    val statuses: Seq[ShuffleBlockStatus] = reduceIds.flatMap { reduceId =>
      SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    }
    logDebug("Fetching map output location for shuffle %d, reduceIds %s took %d ms".format(
      shuffleId, reduceIdsString, System.currentTimeMillis - startTime))

    // Map from block manager to the list of blocks we need to fetch from that block manager.
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[ShuffleBlockStatus]]
    for (status <- statuses) {
      splitsByAddress.getOrElseUpdate(status.address, ArrayBuffer()) += status
    }

    // Generate the sequence of blocks to fetch for each block manager, represented as a list of
    // (address, size) pairs.  Filters out empty blocks so that they aren't fetched.
    val blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])] = for {
      (address, splits) <- splitsByAddress.toSeq
      nonEmptySplits = splits.filter(_.size != 0)
      blocks = nonEmptySplits.map(s => ("shuffle_%d_%d_%d".format(shuffleId, s.mapId, s.reduceId),
        s.size))
    } yield (address, blocks)

    def unpackBlock(blockPair: (String, Option[Iterator[Any]])) : Iterator[(K, V)] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Some(block) => {
          block.asInstanceOf[Iterator[(K, V)]]
        }
        case None => {
          val regex = "shuffle_([0-9]*)_([0-9]*)_([0-9]*)".r
          blockId match {
            case regex(shufId, mapId, reduceId) =>
              val address = statuses(mapId.toInt).address
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId.toInt,
                null)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block")
          }
        }
      }
    }
    blockManager.getMultiple(blocksByAddress).flatMap(unpackBlock)
  }
}
