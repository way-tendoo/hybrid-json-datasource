package org.apache.spark.sql.hybrid.util

object Utils {

  def withClosable[A <: AutoCloseable, B](resource: => A)(usage: A => B): B = {
    try {
      usage(resource)
    } finally {
      resource.close()
    }
  }
}
