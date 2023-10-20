package org.apache.spark.sql.hybrid

import org.apache.spark.sql.hybrid.Const.EnvVars
import org.apache.spark.sql.hybrid.Const.FieldsName._

case class HybridJsonContext private (private val params: Map[String, String], mongoUri: String) {

  def get(option: String): String =
    params.getOrElse(option, throw new IllegalArgumentException(s"Parameter: `$option` must be specified"))

  def objectName(): String = get(ObjectName)

  def path(): String = get(Filepath)
}

object HybridJsonContext {

  private val MongoLocalhostUri = "mongodb://localhost:27017"

  def apply(params: Map[String, String]): HybridJsonContext = {
    val mongoUri = Option(System.getenv(EnvVars.MongoUri)).getOrElse(MongoLocalhostUri)
    new HybridJsonContext(params, mongoUri)
  }
}
