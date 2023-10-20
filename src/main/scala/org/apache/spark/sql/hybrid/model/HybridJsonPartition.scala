package org.apache.spark.sql.hybrid.model

import org.apache.spark.Partition

case class HybridJsonPartition(index: Int, filepath: String, commitMillis: Long, columnStats: Map[String, (Int, Int)])
    extends Partition
