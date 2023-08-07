package org.apache.spark.sql.hybrid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.Syntax.MongoOps
import org.apache.spark.sql.types.StructType
import org.apache.spark.{ Partition, TaskContext }
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.Document

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, ExecutionContextExecutor }
import scala.io.Source.fromFile
import scala.util.parsing.json.JSONObject

class JsonRDD(dataType: StructType, objectName: String, mongoUri: String)
    extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {

  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val jsonPartition = split.asInstanceOf[JsonPartition]
    val filepath      = jsonPartition.filepath
    val fileRef       = fromFile(filepath)
    val parser        = new JsonParser(dataType)
    parser.toRow(fileRef.getLines()).map { ir =>
      ir.setLong(0, jsonPartition.commitMillis); ir
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val paths = FileIO
      .withClosable(MongoClient(mongoUri))(_.find("file_index", Document("objectName" -> objectName)))
      .map { doc =>
        for {
          path         <- doc.get("path").map(_.asString().getValue)
          commitMillis <- doc.get("commitMillis").map(_.asNumber().longValue())
        } yield (path, commitMillis)
      }
      .toFuture()
      .map(_.flatMap(_.toSeq))
    Await
      .result(paths, 10.seconds)
      .zipWithIndex
      .map {
        case ((path, commitMillis), index) =>
          JsonPartition(index, path, commitMillis)
      }
      .toArray
  }
}

case class JsonPartition(index: Int, filepath: String, commitMillis: Long) extends Partition
