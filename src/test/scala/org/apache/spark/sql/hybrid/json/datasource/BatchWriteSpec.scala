package org.apache.spark.sql.hybrid.json.datasource

import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.io.File
import java.nio.file.Paths
import scala.io.Source.fromFile

class BatchWriteSpec extends AnyFlatSpec with should.Matchers with TestSparkSession {

  private val BaseDir: String = System.getProperty("user.dir")

  "hybrid-json batch write one partition" should "work" in {
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

    file.listFiles().length shouldBe 1
    val fileRef = fromFile(file.listFiles().head.getAbsolutePath)
    fileRef.getLines().length shouldBe 5
  }

  "hybrid-json batch write two partitions" should "work" in {
    val fullOutputPath = Paths.get(BaseDir, "test01")
    val file           = new File(fullOutputPath.toString)
    FileUtils.deleteDirectory(file)

    spark
      .range(10, 15, 1, 2)
      .write
      .format("hybrid-json")
      .mode("overwrite")
      .option("objectName", "test01")
      .save(fullOutputPath.toString)

    file.listFiles().length shouldBe 2
  }
}
