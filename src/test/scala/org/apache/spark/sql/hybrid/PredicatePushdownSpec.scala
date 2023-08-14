package org.apache.spark.sql.hybrid

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class PredicatePushdownSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()

  import spark.implicits._

  "Predicate Pushdown" should "work" in {
    spark
      .range(0, 10, 1, 1)
      .withColumn("id", 'id.cast("int"))
      .write.format("hybrid-json")
      .option("path", "/Users/tendoo/Desktop/tmp")
      .option("objectName", "test01")
      .save()

    val df: DataFrame = spark.read.format("hybrid-json").option("objectName", "test01").load()
    val filtered: Dataset[Row] = df.filter(col("id") <= 3)
    filtered.explain(extended = true)
    filtered.show(20, truncate = false)
  }
}
