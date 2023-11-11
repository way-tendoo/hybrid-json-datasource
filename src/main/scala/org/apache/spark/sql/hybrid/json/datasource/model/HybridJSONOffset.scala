package org.apache.spark.sql.hybrid.json.datasource.model

import org.apache.spark.sql.execution.streaming.Offset

case class HybridJSONOffset(commitMillis: Long) extends Offset {
  override def json(): String = commitMillis.toString
}
