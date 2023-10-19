package org.apache.spark.sql.hybrid.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.Const.FieldsName._
import org.apache.spark.sql.hybrid.{HybridJsonPartition, JsonParser}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, TaskContext}
import org.mongodb.scala.bson.Document

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.io.Source.fromFile

abstract class HybridJsonRDD(dataType: StructType, filters: Array[Filter])
    extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {

  @transient implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val jsonPartition = split.asInstanceOf[HybridJsonPartition]
    val columnStats   = jsonPartition.columnStats
    val maybeProcessedPartition = filters.forall {
      case EqualTo(attribute, value) =>
        columnStats.get(attribute).exists {
          case (max, min) => value.asInstanceOf[Int] <= max && value.asInstanceOf[Int] >= min
        }
      case GreaterThan(attribute, value) =>
        columnStats.get(attribute).exists {
          case (max, _) => value.asInstanceOf[Int] < max
        }
      case GreaterThanOrEqual(attribute, value) =>
        columnStats.get(attribute).exists {
          case (max, _) => value.asInstanceOf[Int] <= max
        }
      case LessThan(attribute, value) =>
        columnStats.get(attribute).exists {
          case (_, min) => value.asInstanceOf[Int] > min
        }
      case LessThanOrEqual(attribute, value) =>
        columnStats.get(attribute).exists {
          case (_, min) => value.asInstanceOf[Int] >= min
        }
      case _ => true
    }
    if (maybeProcessedPartition) {
      val filepath = jsonPartition.filepath
      val fileRef  = fromFile(filepath)
      val parser   = new JsonParser(dataType)
      val lines    = fileRef.getLines()
      parser.toRow(lines).map { ir =>
        dataType.headOption
          .filter(_.name == s"__$CommitMillis")
          .foreach(_ => ir.setLong(0, jsonPartition.commitMillis))
        ir
      }
    } else Iterator.empty
  }
}

object HybridJsonRDD {
  
  def parseFileIndexDoc(doc: Document): Option[(String, Long, Map[String, (Int, Int)])] = {
    for {
      path         <- doc.get(Filepath).map(_.asString().getValue)
      commitMillis <- doc.get(CommitMillis).map(_.asNumber().longValue())
      columnStats <- doc
                      .get(ColumnStats)
                      .map(_.asArray())
                      .map(_.getValues.asScala.map(_.asDocument()).flatMap(HybridJsonRDD.parseColumnStats(_)))
    } yield (path, commitMillis, columnStats.toMap)
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
