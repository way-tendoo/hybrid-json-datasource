package org.apache.spark.sql.hybrid.json.datasource

import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.io.File
import java.nio.file.Paths

class BatchReadSpec extends AnyFlatSpec with should.Matchers with TestSparkSession {

  private val BaseDir: String = System.getProperty("user.dir")

  "hybrid-json batch write 1 partition" should "work" in {
    val fullOutputPath = Paths.get(BaseDir, "test01")
    val file           = new File(fullOutputPath.toString)
    FileUtils.deleteDirectory(file)

    spark
      .range(10, 15, 1, 1)
      .write
      .format("hybrid-json")
      .mode("overwrite")
      .option("objectName", "test01")
      .save(fullOutputPath.toString)

    val rows = spark.read.format("hybrid-json").option("objectName", "test01").load(fullOutputPath.toString).collect()

    rows.length shouldBe 5
  }
}
