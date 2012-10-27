package spark.scheduler

import spark.storage.BlockManagerId
import java.io.{ObjectOutput, ObjectInput, Externalizable}
import spark.PartitionStatsAccumulator

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 * The map output sizes are compressed using MapOutputTracker.compressSize.
 */
private[spark] class MapStatus(var address: BlockManagerId, var compressedSizes: Array[Byte],
  var customStats : Option[PartitionStatsAccumulator[_, _]])
  extends Externalizable {

  def this() = this(null, null, null)  // For deserialization only

  def writeExternal(out: ObjectOutput) {
    address.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
    out.writeObject(customStats)
  }

  def readExternal(in: ObjectInput) {
    address = new BlockManagerId(in)
    compressedSizes = new Array[Byte](in.readInt())
    in.readFully(compressedSizes)
    customStats = in.readObject().asInstanceOf[Option[PartitionStatsAccumulator[_, _]]]
  }
}
