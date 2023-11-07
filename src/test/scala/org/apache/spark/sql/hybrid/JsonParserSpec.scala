package org.apache.spark.sql.hybrid

import org.apache.spark.sql.hybrid.parser.JsonParser
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class JsonParserSpec extends AnyFlatSpec with should.Matchers with TestSparkSession {

  val Schema: StructType =
    StructType {
      StructField("firstName", StringType) :: StructField("lastName", StringType) :: StructField("age", IntegerType) :: Nil
    }

  val RawUserJsonString: String = """ { "firstName": "Igor", "lastName": "Ivanov", "age": 31 } """

  "JsonParser" should s"parse $RawUserJsonString" in {
    val parser = new JsonParser(Schema)
    val row    = parser.toRow(Iterator(RawUserJsonString))
    val rdd    = spark.sparkContext.parallelize(row.toList)
    val user   = spark.internalCreateDataFrame(rdd, Schema, isStreaming = false).collect().head.toSeq

    user.head shouldBe "Igor"
    user(1) shouldBe "Ivanov"
    user(2) shouldBe 31
  }
}
