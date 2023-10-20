package org.apache.spark.sql.hybrid.util

import java.io.{ BufferedWriter, File, FileWriter }

object FileIO {

  def createDirectoryIfNotExist(path: String): Unit = new File(path).mkdirs()

  def unsafeWriteFile(path: String, data: Iterator[String]): Unit = {
    val output = data.mkString("\n")
    val file   = new File(path)
    val writer = new BufferedWriter(new FileWriter(file))
    Utils.withClosable(writer)(_.write(output))
  }
}
