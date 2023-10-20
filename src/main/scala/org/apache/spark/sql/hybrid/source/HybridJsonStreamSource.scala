package org.apache.spark.sql.hybrid.source

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.hybrid.Const.FieldsName.{CommitMillis, ObjectName}
import org.apache.spark.sql.hybrid.Const.TablesName.FileIndex
import org.apache.spark.sql.hybrid.{HybridJsonContext, MongoClient}
import org.apache.spark.sql.hybrid.model.{HybridJsonOffset, HybridJsonPartition}
import org.apache.spark.sql.hybrid.rdd.{HybridJsonRDD, StreamHybridJsonRDD}
import org.apache.spark.sql.hybrid.util.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mongodb.scala.bson.Document
import org.mongodb.scala.model.Filters.{and, gt, lte}
import org.mongodb.scala.model.Sorts.{descending, orderBy}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}

class HybridJsonStreamSource(dataType: StructType, ctx: HybridJsonContext) extends Source with Logging {

  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val spark: SparkSession = SparkSession.active

  override def schema: StructType = dataType

  override def getOffset: Option[Offset] = {
    val lastOffset = Utils
      .withClosable(MongoClient(ctx.mongoUri))(_.find(FileIndex, Document(ObjectName -> ctx.objectName())))
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
        Utils
          .withClosable(MongoClient(ctx.mongoUri))(
            _.find(FileIndex, Document(ObjectName -> ctx.objectName()))
              .filter(and(gt(CommitMillis, startOffsetCommitMillis), lte(CommitMillis, endOffsetCommitMillis)))
          )
      case _ =>
        Utils.withClosable(MongoClient(ctx.mongoUri))(_.find(FileIndex, Document(ObjectName -> ctx.objectName())))
    }
    val paths = files
      .map(HybridJsonRDD.parseFileIndex)
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