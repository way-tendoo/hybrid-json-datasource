package org.apache.spark.sql.hybrid.sink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.Const.FieldsName._
import org.apache.spark.sql.hybrid.Const.TablesName.FileIndex
import org.apache.spark.sql.hybrid.parser.RowParser
import org.apache.spark.sql.hybrid.util.FileIO
import org.apache.spark.sql.hybrid.{HybridJSONContext, MongoClient}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.util.Utils
import org.mongodb.scala.bson.Document

import java.nio.file.Paths
import java.util.UUID
import scala.util.{Failure, Success, Try}

case class HybridJSONPartitionWriter(schema: StructType)(ctx: HybridJSONContext) extends Logging {

  def write(partition: Iterator[InternalRow]): Unit = {
    val (forParsing, forStats) = partition.duplicate
    val parser                 = new RowParser(schema)
    val json                   = parser.toJsonString(forParsing)
    val fullOutputPath         = Paths.get(ctx.path(), s"${UUID.randomUUID().toString}.json").toString
    Try {
      FileIO.unsafeWriteFile(fullOutputPath, json)
    } match {
      case Failure(ex) =>
        log.error(s"Write partition: $fullOutputPath finished with exception: ${ex.getMessage}")
        throw ex
      case Success(_) =>
        val columnsStatsFieldsName = schema.filter(_.dataType == IntegerType).map(_.name)
        val columnStats = forStats
          .map(_.toSeq(schema))
          .map(_.filter(_.isInstanceOf[Int]).map(_.asInstanceOf[Int]))
          .toSeq
          .transpose
          .map(col => (col.min, col.max))
          .zip(columnsStatsFieldsName)
          .map { case ((min, max), name) => Document(Name -> name, Min -> min, Max -> max) }
        Utils.tryWithResource(MongoClient(ctx.mongoUri))(
          _.insertOne(
            FileIndex,
            Document(
              ObjectName   -> ctx.objectName(),
              Filepath     -> fullOutputPath,
              CommitMillis -> System.currentTimeMillis(),
              ColumnStats  -> columnStats
            )
          )
        )
    }
  }
}
