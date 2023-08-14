package org.apache.spark.sql.hybrid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class JsonParserSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

  val schema: StructType =
    StructType {
      StructField("foo", IntegerType) :: StructField("bar", StringType) :: Nil
    }
  val jsonParser: JsonParser = new JsonParser(schema)
  val rawString: String      = """ { "foo": 0, "bar" : "hello world" } """

  "Parser" should s"parse $rawString" in {
    val IsStreaming: Boolean       = false
    val row: Iterator[InternalRow] = jsonParser.toRow(Iterator(rawString))
    val rowsRdd: RDD[InternalRow]  = spark.sparkContext.parallelize(row.toList)
    val df: DataFrame              = spark.internalCreateDataFrame(rowsRdd, schema, IsStreaming)
    df.show
    df.printSchema
  }
}
