package org.apache.spark.sql.hybrid

import java.io.{ BufferedWriter, File, FileWriter }

object FileIO {

  def withClosable[A <: AutoCloseable, B](resource: => A)(usage: A => B): B = {
    try {
      usage(resource)
    } finally {
      resource.close()
    }
  }

  def createDirectoryIfNotExist(path: String): Unit = new File(path).mkdirs()

  def write(path: String, data: Iterator[String]): Unit = {
    val file = new File(path)
    val bw   = new BufferedWriter(new FileWriter(file))
    while (data.hasNext) {
      bw.write(data.next())
      bw.write("\n")
    }
    bw.close()
  }
}
