package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.hybrid.Const.FieldsName.{CommitMillis, ObjectName}
import org.apache.spark.sql.hybrid.Const.TablesName.FileIndex
import org.apache.spark.sql.hybrid.Syntax.MongoOps
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.Document
import org.mongodb.scala.model.Filters.{and, gt, lte}
import org.mongodb.scala.model.Sorts.{descending, orderBy}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}

final class HybridJsonStreamSource(dataType: StructType, ctx: HybridJsonContext) extends Source with Logging {

  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val spark: SparkSession = SparkSession.active

  override def schema: StructType = dataType

  override def getOffset: Option[Offset] = {
    val lastOffset = FileIO
      .withClosable(MongoClient(ctx.mongoUri()))(_.find(FileIndex, Document(ObjectName -> ctx.objectName())))
      .sort(orderBy(descending(CommitMillis)))
      .map(_.get(CommitMillis).map(_.asNumber().longValue()))
      .head()
    Await.result(lastOffset, 10.seconds).map(HybridJsonOffset)
  }
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val endOffsetCommitMillis = end.json().toLong
    val files = start match {
      case Some(start) =>
        val startOffsetCommitMillis = start.json().toLong
        FileIO
          .withClosable(MongoClient(ctx.mongoUri()))(
            _.find(FileIndex, Document(ObjectName -> ctx.objectName()))
              .filter(and(gt(CommitMillis, startOffsetCommitMillis), lte(CommitMillis, endOffsetCommitMillis)))
          )
      case _ =>
        FileIO.withClosable(MongoClient(ctx.mongoUri()))(_.find(FileIndex, Document(ObjectName -> ctx.objectName())))
    }
    val paths = files
      .map(HybridJsonRDD.parseFileIndexDoc)
      .toFuture()
      .map(_.flatMap(_.toSeq))
    val partitions = Await
      .result(paths, 10.seconds)
      .zipWithIndex
      .map {
        case ((path, commitMillis, _), index) =>
          HybridJsonPartition(index, path, commitMillis, Map.empty)
      }
      .toArray
    val rdd = new StreamHybridJsonRDD(dataType, partitions)
    spark.internalCreateDataFrame(rdd, dataType, isStreaming = true)
  }

  override def stop(): Unit = log.info(s"Stop HybridJson stream source")
}
