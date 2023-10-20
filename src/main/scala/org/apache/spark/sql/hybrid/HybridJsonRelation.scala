package org.apache.spark.sql.hybrid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hybrid.rdd.BatchHybridJsonRDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

class HybridJsonRelation(dataType: StructType, ctx: HybridJsonContext)
    extends BaseRelation
    with PrunedFilteredScan {

  override def sqlContext: SQLContext = SparkSession.active.sqlContext

  override def schema: StructType = dataType

  override def needConversion: Boolean = false

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredColumnsSchema = dataType.filter { sf =>
      requiredColumns.contains(sf.name)
    }
    new BatchHybridJsonRDD(StructType.apply(requiredColumnsSchema), filters, ctx).asInstanceOf[RDD[Row]]
  }
}

