package org.apache.spark.sql.hybrid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ReadStreamSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()

  "Hybrid JSON Streaming" should "work" in {
    val df = spark.readStream.format("hybrid-json").option("objectName", "test01").load()

    df.printSchema

    val sq =
      df.writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("checkpointLocation", "/Users/tendoo/Desktop/tmp/chk0")
        .start()

    sq.awaitTermination(60000)

    sq.stop()
  }
}
