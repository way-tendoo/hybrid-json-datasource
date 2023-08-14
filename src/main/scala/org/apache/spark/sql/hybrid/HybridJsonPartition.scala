package org.apache.spark.sql.hybrid

import org.apache.spark.Partition

final case class HybridJsonPartition(
  index: Int,
  filepath: String,
  commitMillis: Long,
  columnStats: Map[String, (Int, Int)])
    extends Partition
