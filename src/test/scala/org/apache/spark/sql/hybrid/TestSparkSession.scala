package org.apache.spark.sql.hybrid

import org.apache.spark.sql.SparkSession

trait TestSparkSession {
  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("test")
    .getOrCreate()
}
