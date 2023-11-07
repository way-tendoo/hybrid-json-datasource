package org.apache.spark.sql.hybrid

import org.apache.spark.sql.execution.streaming.{ Sink, Source }
import org.apache.spark.sql.hybrid.sink.HybridJSONSink
import org.apache.spark.sql.hybrid.source.{ HybridJSONRelation, HybridJSONStreamSource }
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode }

class HybridJSONProvider
    extends CreatableRelationProvider
    with RelationProvider
    with DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with SchemaRelationProvider {

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    params: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val ctx = HybridJSONContext(params)
    new HybridJSONSink(ctx).write(data)
    EmptyRelation()
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): BaseRelation = {
    val ctx = HybridJSONContext(params)
    HybridJSONRelation(Schema.infer(ctx))(ctx)
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): BaseRelation = {
    val ctx = HybridJSONContext(params)
    HybridJSONRelation(schema)(ctx)
  }

  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    params: Map[String, String]
  ): (String, StructType) = {
    val ctx = HybridJSONContext(params)
    providerName -> schema.getOrElse(Schema.infer(ctx))
  }

  override def createSink(
    sqlContext: SQLContext,
    params: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode
  ): Sink = {
    val ctx = HybridJSONContext(params)
    new Sink {
      override def addBatch(batchId: Long, data: DataFrame): Unit = new HybridJSONSink(ctx).write(data)
    }
  }

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    params: Map[String, String]
  ): Source = {
    val ctx = HybridJSONContext(params)
    new HybridJSONStreamSource(schema.getOrElse(Schema.infer(ctx)))(ctx)
  }

  override def shortName(): String = "hybrid-json"
}
