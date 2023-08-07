package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.Syntax._
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  RelationProvider,
  SchemaRelationProvider
}
import org.apache.spark.sql.types.{ LongType, StructType }
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode }
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.{ Document, MongoClient }

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, ExecutionContextExecutor, Future }
import scala.util.Try

class HybridJsonProvider
    extends CreatableRelationProvider
    with RelationProvider
    with DataSourceRegister
    with SchemaRelationProvider
    with Serializable {

  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    params: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val mongoUri   = getMongoUri
    val path       = params.getMandatory("path")
    val objectName = params.getMandatory("objectName")
    val schema     = data.schema.asNullable.json
    val converter  = new RowConverter(data.schema)
    FileIO.createDirectoryIfNotExist(path)
    data.queryExecution.toRdd.foreachPartition { ir =>
      processPartition(converter)(ir, mongoUri, objectName, path)
    }
    for {
      schemaRef <- FileIO.withClosable(MongoClient(mongoUri))(
                    _.find("schema_index", Document("objectName" -> objectName))
                      .filter(equal("schemaRef", schema))
                      .headOption()
                  )
      _ <- if (schemaRef.isEmpty) {
            FileIO.withClosable(MongoClient(mongoUri))(
              _.insertOne(
                "schema_index",
                Document(
                  "objectName"   -> objectName,
                  "schemaRef"    -> schema,
                  "commitMillis" -> System.currentTimeMillis()
                )
              )
            )
          } else Future.successful()
    } yield {}

    EmptyRelation()
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): BaseRelation = {
    val objectName = params.getMandatory("objectName")
    val schema     = inferSchema(getMongoUri, objectName)
    new JsonRelation(schema, objectName, getMongoUri)
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): BaseRelation = {
    val objectName = params.getMandatory("objectName")
    new JsonRelation(schema, objectName, getMongoUri)
  }

  private def getMongoUri: String = {
    Option(System.getenv("MONGO_URI"))
      .getOrElse(throw new IllegalArgumentException(s"System env: `MONGO_URI` must be specified"))
  }

  private def inferSchema(mongoUri: String, objectName: String): StructType = {
    val schemaRefs = FileIO
      .withClosable(MongoClient(mongoUri))(_.find("schema_index", Document("objectName" -> objectName)))
      .map(_.get("schemaRef").map(_.asString().getValue))
      .toFuture()
      .map(_.flatMap(_.toSeq))
    val schema = new StructType().add("__commitMillis", LongType)
    Await.result(schemaRefs, 10.seconds).map(StructType.fromString).foldLeft(schema) {
      case (acc, schema) => acc.merge(schema)
    }
  }

  private def processPartition(
    converter: RowConverter
  )(
    ir: Iterator[InternalRow],
    mongoUri: String,
    objectName: String,
    path: String
  ): Unit = {
    val json     = converter.toJsonString(ir)
    val fullPath = Paths.get(path, s"${UUID.randomUUID().toString}.json").toString
    Try {
      FileIO.write(fullPath, json)
    }.foreach { _ =>
      FileIO.withClosable(MongoClient(mongoUri))(
        _.insertOne(
          "file_index",
          Document(
            "objectName"   -> objectName,
            "path"         -> fullPath,
            "commitMillis" -> System.currentTimeMillis()
          )
        )
      )
    }
  }

  override def shortName(): String = "hybrid-json"
}
