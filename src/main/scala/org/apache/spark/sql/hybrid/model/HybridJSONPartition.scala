package org.apache.spark.sql.hybrid.model

import org.apache.spark.Partition
import org.apache.spark.sql.hybrid.Const.FieldsName.{ ColumnStats, CommitMillis, Filepath, Max, Min, Name }
import org.mongodb.scala.Document

import scala.collection.JavaConverters._

case class HybridJSONPartition(index: Int, filepath: String, commitMillis: Long, columnStats: Map[String, (Int, Int)])
    extends Partition

object HybridJSONPartition {

  def from(index: Int, fileIndex: Document): Option[HybridJSONPartition] = {
    for {
      path         <- fileIndex.get(Filepath).map(_.asString().getValue)
      commitMillis <- fileIndex.get(CommitMillis).map(_.asNumber().longValue())
      columnStats <- fileIndex
                      .get(ColumnStats)
                      .map(_.asArray())
                      .map(_.getValues.asScala.map(_.asDocument()).flatMap(parseColumnStats(_)))
    } yield HybridJSONPartition(index, path, commitMillis, columnStats.toMap)
  }

  private def parseColumnStats(doc: Document): Option[(String, (Int, Int))] = {
    for {
      name <- doc.get(Name).map(_.asString().getValue)
      max  <- doc.get(Max).map(_.asNumber().intValue())
      min  <- doc.get(Min).map(_.asNumber().intValue())
    } yield {
      name -> (max, min)
    }
  }
}
