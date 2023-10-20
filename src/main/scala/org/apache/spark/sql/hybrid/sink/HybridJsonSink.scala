package org.apache.spark.sql.hybrid.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.Const.FieldsName._
import org.apache.spark.sql.hybrid.Const.TablesName.{FileIndex, SchemaIndex}
import org.apache.spark.sql.hybrid.{HybridJsonContext, MongoClient}
import org.apache.spark.sql.hybrid.parser.RowParser
import org.apache.spark.sql.hybrid.util.{FileIO, Utils}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.mongodb.scala.Document
import org.mongodb.scala.model.Filters.equal

import java.nio.file.Paths
import java.util.UUID
import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

class HybridJsonSink(ctx: HybridJsonContext) {

  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def write(data: DataFrame): Unit = {
    val schema    = data.schema.asNullable.json
    val converter = new RowParser(data.schema)
    FileIO.createDirectoryIfNotExist(ctx.path())
    data.queryExecution.toRdd.foreachPartition(writePartition(converter, data.schema))
    for {
      schemaRef <- Utils.withClosable(MongoClient(ctx.mongoUri))(
                    _.find(SchemaIndex, Document(ObjectName -> ctx.objectName()))
                      .filter(equal(SchemaRef, schema))
                      .headOption()
                  )
      _ <- if (schemaRef.isEmpty) {
            Utils.withClosable(MongoClient(ctx.mongoUri))(
              _.insertOne(
                SchemaIndex,
                Document(
                  ObjectName   -> ctx.objectName(),
                  SchemaRef    -> schema,
                  CommitMillis -> System.currentTimeMillis()
                )
              )
            )
          } else Future.successful()
    } yield {}
  }

  private def writePartition(converter: RowParser, schema: StructType)(ir: Iterator[InternalRow]): Unit = {
    val rows     = ir.map(_.toSeq(schema)).toSeq
    val json     = converter.toJsonString(rows.map(InternalRow.fromSeq).iterator)
    val fullPath = Paths.get(ctx.path(), s"${UUID.randomUUID().toString}.json").toString
    Try {
      FileIO.unsafeWriteFile(fullPath, json)
    }.foreach { _ =>
      val fieldsName = schema.filter(_.dataType == IntegerType).map(_.name)
      val columnStats =
        fieldsName.zip(formatColumnStats(rows)).map {
          case (name, (max, min)) => Document(Name -> name, Min -> min, Max -> max)
        }
      Utils.withClosable(MongoClient(ctx.mongoUri))(
        _.insertOne(
          FileIndex,
          Document(
            ObjectName   -> ctx.objectName(),
            Filepath     -> fullPath,
            CommitMillis -> System.currentTimeMillis(),
            ColumnStats  -> columnStats
          )
        )
      )
    }
  }

  private def formatColumnStats(ir: Seq[Seq[Any]]): immutable.Seq[(Int, Int)] = {
    val rows         = ir.map(_.filter(_.isInstanceOf[Int]).map(_.asInstanceOf[Int]))
    val buffersCount = rows.headOption.map(_.length).getOrElse(0)
    val buffers = (0 until buffersCount).map { _ =>
      new ArrayBuffer[Int]
    }
    rows.foreach {
      _.zipWithIndex.foreach { case (value, i) => buffers(i).append(value) }
    }
    buffers.map { buffer =>
      (buffer.max, buffer.min)
    }
  }
}
