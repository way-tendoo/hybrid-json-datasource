package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.sql.hybrid.json.datasource.parser.RowParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class RowParserSpec extends AnyFlatSpec with should.Matchers with TestSparkSession {

  "RowParser" should "work" in {
    val data   = spark.range(1)
    val rows   = data.queryExecution.toRdd.collect().toIterator
    val parser = new RowParser(data.schema)
    val out    = parser.toJsonString(rows)
    out.next() shouldBe """{"id":0}"""
  }
}
