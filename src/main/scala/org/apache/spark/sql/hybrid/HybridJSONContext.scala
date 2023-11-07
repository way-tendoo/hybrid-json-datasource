package org.apache.spark.sql.hybrid

import org.apache.spark.sql.hybrid.Const.EnvVars
import org.apache.spark.sql.hybrid.Const.FieldsName._

case class HybridJSONContext private (private val params: Map[String, String], mongoUri: String) {

  def get(option: String): String =
    params.getOrElse(option, throw new IllegalArgumentException(s"Parameter: `$option` must be specified"))

  def objectName(): String = get(ObjectName)

  def path(): String = get(Filepath)
}

object HybridJSONContext {

  private lazy val MongoLocalhostUri = "mongodb://localhost:27017"

  def apply(params: Map[String, String]): HybridJSONContext = {
    val mongoUri = Option(System.getenv(EnvVars.MongoUri)).getOrElse(MongoLocalhostUri)
    new HybridJSONContext(params, mongoUri)
  }
}
