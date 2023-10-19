package org.apache.spark.sql.hybrid.rdd

import org.apache.spark.Partition
import org.apache.spark.sql.hybrid.{FileIO, HybridJsonContext, HybridJsonPartition}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.Document

import scala.concurrent.Await
import scala.concurrent.duration._

final class BatchHybridJsonRDD(dataType: StructType, filters: Array[Filter], ctx: HybridJsonContext)
    extends HybridJsonRDD(dataType, filters) {

  override protected def getPartitions: Array[Partition] = {
    val paths = FileIO
      .withClosable(MongoClient(ctx.mongoUri()))(_.find(FileIndex, Document(ObjectName -> ctx.objectName())))
      .map(HybridJsonRDD.parseFileIndexDoc)
      .toFuture()
      .map(_.flatMap(_.toSeq))
    Await
      .result(paths, 10.seconds)
      .zipWithIndex
      .map {
        case ((path, commitMillis, columnStats), index) =>
          HybridJsonPartition(index, path, commitMillis, columnStats)
      }
      .toArray
  }
}
