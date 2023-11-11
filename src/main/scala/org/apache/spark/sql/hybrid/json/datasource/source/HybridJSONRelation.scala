package org.apache.spark.sql.hybrid.json.datasource.source

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hybrid.json.datasource.HybridJSONContext
import org.apache.spark.sql.hybrid.json.datasource.rdd.HybridJSONBatchRDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

case class HybridJSONRelation(schema: StructType)(ctx: HybridJSONContext) extends BaseRelation with PrunedFilteredScan {
  override def sqlContext: SQLContext = SparkSession.active.sqlContext

  override def needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredColumnsSchema = schema.filter(field => requiredColumns.contains(field.name))
    HybridJSONBatchRDD(StructType.apply(requiredColumnsSchema), filters)(ctx).asInstanceOf[RDD[Row]]
  }
}
