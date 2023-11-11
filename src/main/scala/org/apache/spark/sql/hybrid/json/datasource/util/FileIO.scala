package org.apache.spark.sql.hybrid.json.datasource.util

import org.apache.spark.util.Utils

import java.io.{BufferedWriter, File, FileWriter}

object FileIO {

  def makeDirectoryIfNotExist(path: String): Unit = new File(path).mkdirs()

  def unsafeWriteFile(path: String, data: Iterator[String]): Unit = {
    val output = data.mkString("\n")
    val file   = new File(path)
    val writer = new BufferedWriter(new FileWriter(file))
    Utils.tryWithResource(writer)(_.write(output))
  }
}
