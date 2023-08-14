package org.apache.spark.sql.hybrid

import org.apache.spark.sql.execution.streaming.Offset

final case class HybridJsonOffset (commitMillis: Long) extends Offset {
  override def json(): String = commitMillis.toString
}
