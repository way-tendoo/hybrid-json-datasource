package org.apache.spark.sql.hybrid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hybrid.parser.RowParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class RowParserSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

  "Converter" should "work" in {
    val data      = spark.range(1)
    val rows      = data.queryExecution.toRdd.collect().toIterator
    val converter = new RowParser(data.schema)
    val out       = converter.toJsonString(rows)
    out.next() shouldBe "{\"id\":0}"
  }
}
