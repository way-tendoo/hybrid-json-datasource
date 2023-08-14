package org.apache.spark.sql.hybrid

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ StreamingQuery, Trigger }

class ReadStreamSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()

  "Hybrid JSON Streaming" should "work" in {
    /*spark
      .range(0, 5, 1, 1)
      .write.format("hybrid-json")
      .option("path", "/Users/tendoo/Desktop/tmp")
      .option("objectName", "test01")
      .save()*/

    val df: DataFrame = spark.readStream.format("hybrid-json").option("objectName", "test01").load()
    df.printSchema
    val sq: StreamingQuery =
      df.writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("checkpointLocation", "/Users/tendoo/Desktop/tmp/chk0")
        .start()

    sq.awaitTermination(60000)

    /*spark
      .range(5, 10, 1, 1)
      .write.format("hybrid-json")
      .option("path", "/Users/tendoo/Desktop/tmp")
      .option("objectName", "test01")
      .save()

    sq.awaitTermination(15000)

    sq.stop()*/
  }
}
