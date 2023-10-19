package org.apache.spark.sql.hybrid

import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.hybrid.Const.FieldsName._
import org.apache.spark.sql.hybrid.Const.TablesName.SchemaIndex
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.mongodb.scala._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}

class HybridJsonProvider
    extends CreatableRelationProvider
    with RelationProvider
    with DataSourceRegister
    with StreamSinkProvider
    with StreamSourceProvider
    with SchemaRelationProvider
    with Serializable {

  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    params: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val ctx = HybridJsonContext(params)
    new HybridJsonSink().write(data, ctx)
    EmptyRelation()
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): BaseRelation = {
    val ctx    = HybridJsonContext(params)
    val schema = inferSchema(ctx)
    new JsonRelation(schema, ctx)
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): BaseRelation = {
    val ctx = HybridJsonContext(params)
    new JsonRelation(schema, ctx)
  }

  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    params: Map[String, String]
  ): (String, StructType) = {
    val ctx = HybridJsonContext(params)
    providerName -> schema.getOrElse(inferSchema(ctx))
  }

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    params: Map[String, String]
  ): Source = {
    val ctx = HybridJsonContext(params)
    new HybridJsonStreamSource(schema.getOrElse(inferSchema(ctx)), ctx)
  }

  override def createSink(
    sqlContext: SQLContext,
    params: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode
  ): Sink = {
    val ctx = HybridJsonContext(params)
    new HybridJsonStreamSink(ctx)
  }

  private def inferSchema(context: HybridJsonContext): StructType = {
    val schemaRefs = FileIO
      .withClosable(MongoClient(context.mongoUri()))(_.find(SchemaIndex, Document(ObjectName -> context.objectName())))
      .map(_.get(SchemaRef).map(_.asString().getValue))
      .toFuture()
      .map(_.flatMap(_.toSeq))
    val schema = new StructType().add(s"__$CommitMillis", LongType)
    Await.result(schemaRefs, 10.seconds).map(StructType.fromString).foldLeft(schema) {
      case (acc, schema) => acc.merge(schema)
    }
  }

  override def shortName(): String = "hybrid-json"
}
