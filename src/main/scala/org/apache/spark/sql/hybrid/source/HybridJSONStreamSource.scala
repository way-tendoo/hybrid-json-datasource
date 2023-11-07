package org.apache.spark.sql.hybrid.source

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{ Offset, Source }
import org.apache.spark.sql.hybrid.Const.Database.IndexStore
import org.apache.spark.sql.hybrid.Const.FieldsName._
import org.apache.spark.sql.hybrid.Const.TablesName.FileIndex
import org.apache.spark.sql.hybrid.{ HybridJSONContext, MongoClient }
import org.apache.spark.sql.hybrid.Syntax._
import org.apache.spark.sql.hybrid.model.{ HybridJSONOffset, HybridJSONPartition }
import org.apache.spark.sql.hybrid.rdd.HybridJSONStreamRDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.util.Utils
import org.mongodb.scala.model.Filters.{ and, gt, lte }
import org.mongodb.scala.model.Sorts.{ descending, orderBy }
import org.mongodb.scala.Document

class HybridJSONStreamSource(val schema: StructType)(ctx: HybridJSONContext) extends Source with Logging {

  val spark: SparkSession = SparkSession.active

  override def getOffset: Option[Offset] = {
    Utils
      .tryWithResource(MongoClient(ctx.mongoUri))(
        _.find(FileIndex, Document(ObjectName -> ctx.objectName()))
          .sort(orderBy(descending(CommitMillis)))
          .map(_.get(CommitMillis).map(_.asNumber().longValue()))
          .head()
          .await()
      )
      .map(HybridJSONOffset)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val endOffsetCommitMillis = end.json().toLong // unsafe casting, can throw NumberFormatException
    val files = start match {
      case Some(start) =>
        val startOffsetCommitMillis = start.json().toLong // unsafe casting, can throw NumberFormatException
        Utils
          .tryWithResource(MongoClient(ctx.mongoUri))(
            _.find(FileIndex, Document(ObjectName -> ctx.objectName()))
              .filter(and(gt(CommitMillis, startOffsetCommitMillis), lte(CommitMillis, endOffsetCommitMillis)))
              .toFuture()
              .await()
          )
      case _ =>
        Utils.tryWithResource(MongoClient(ctx.mongoUri))(
          _.find(FileIndex, Document(ObjectName -> ctx.objectName()))
            .toFuture()
            .await()
        )
    }
    val partitions = files.zipWithIndex.map { case (fileIndex, index) => HybridJSONPartition.from(index, fileIndex) }
      .flatMap(_.toSeq)
      .toArray
    val rdd = new HybridJSONStreamRDD(partitions, schema)
    spark.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def stop(): Unit = log.info(s"Stop hybrid-json stream source")
}
