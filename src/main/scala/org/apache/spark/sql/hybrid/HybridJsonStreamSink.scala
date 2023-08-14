package org.apache.spark.sql.hybrid

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

class HybridJsonStreamSink(ctx: HybridJsonContext) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = new HybridJsonSink().write(data, ctx)
}
