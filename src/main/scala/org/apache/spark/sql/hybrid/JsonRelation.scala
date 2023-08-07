package org.apache.spark.sql.hybrid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

class JsonRelation(dataType: StructType, objectName: String, mongoUri: String) extends BaseRelation with TableScan {

  override def sqlContext: SQLContext = SparkSession.active.sqlContext

  override def schema: StructType = dataType

  override def buildScan(): RDD[Row] = new JsonRDD(dataType, objectName, mongoUri).asInstanceOf[RDD[Row]]

  override def needConversion: Boolean = false
}
