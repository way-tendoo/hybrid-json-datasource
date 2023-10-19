package org.apache.spark.sql.hybrid.rdd

import org.apache.spark.Partition
import org.apache.spark.sql.hybrid.HybridJsonPartition
import org.apache.spark.sql.types.StructType

class StreamHybridJsonRDD(dataType: StructType, partitions: Array[HybridJsonPartition])
    extends HybridJsonRDD(dataType, Array.empty) {

  override protected def getPartitions: Array[Partition] = partitions.asInstanceOf[Array[Partition]]
}
