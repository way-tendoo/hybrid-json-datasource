package org.apache.spark.sql.hybrid.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.Const.Database.IndexStore
import org.apache.spark.sql.hybrid.Const.FieldsName
import org.apache.spark.sql.hybrid.Const.TablesName.FileIndex
import org.apache.spark.sql.hybrid.{ HybridJSONContext, MongoClient }
import org.apache.spark.sql.hybrid.Syntax._
import org.apache.spark.sql.hybrid.model.HybridJSONPartition
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.{ Partition, TaskContext }
import org.mongodb.scala.Document

case class HybridJSONBatchRDD(schema: StructType, filters: Array[Filter])(ctx: HybridJSONContext)
    extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    HybridJSONPartitionEvaluator(schema, filters).eval(split)
  }

  override def getPartitions: Array[Partition] = {
    Utils
      .tryWithResource(MongoClient(ctx.mongoUri))(
        _.find(FileIndex, Document(FieldsName.ObjectName -> ctx.objectName()))
          .toFuture()
          .await()
      )
      .zipWithIndex
      .map { case (fileIndex, index) => HybridJSONPartition.from(index, fileIndex) }
      .flatMap(_.toSeq)
      .toArray
  }
}
