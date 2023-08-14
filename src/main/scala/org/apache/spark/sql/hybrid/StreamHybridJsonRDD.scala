package org.apache.spark.sql.hybrid

import org.apache.spark.Partition
import org.apache.spark.sql.types.StructType

final class StreamHybridJsonRDD(dataType: StructType, partitions: Array[HybridJsonPartition])
    extends HybridJsonRDD(dataType, Array.empty) {

  override protected def getPartitions: Array[Partition] = partitions.asInstanceOf[Array[Partition]]
}
