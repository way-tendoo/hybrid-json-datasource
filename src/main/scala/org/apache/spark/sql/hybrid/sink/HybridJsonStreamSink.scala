package org.apache.spark.sql.hybrid.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.hybrid.HybridJsonContext

class HybridJsonStreamSink(ctx: HybridJsonContext) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = new HybridJsonSink(ctx).write(data)
}