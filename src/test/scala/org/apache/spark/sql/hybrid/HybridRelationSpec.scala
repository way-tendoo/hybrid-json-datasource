package org.apache.spark.sql.hybrid

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class HybridRelationSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
  spark
    .range(10, 15, 1, 1)
    .write
    .format("hybrid-json")
    .option("path", "/Users/tendoo/Desktop/tmp")
    .option("objectName", "test01")
    .save()

  "Hybrid JSON" should "work" in {
    val df = spark.read.format("hybrid-json").option("objectName", "test01").load()
    df.printSchema
    df.show(1000, truncate = false)
  }
}
