package org.apache.spark.sql.hybrid

import org.apache.spark.sql.hybrid.Const.FieldsName._
final case class HybridJsonContext private(private val data: Map[String, String], fake: Option[AnyRef]) {
  def get(option: String): String =
    data.getOrElse(option, throw new IllegalArgumentException(s"Parameter: `$option` must be specified"))

  def objectName(): String = get(ObjectName)

  def path(): String = get(Filepath)

  def mongoUri(): String = get(MongoUri)
}

object HybridJsonContext {
  def apply(params: Map[String, String]): HybridJsonContext = {
    val context = scala.collection.mutable.HashMap[String, String]()
    Option(System.getenv("MONGO_URI")).foreach { uri =>
      context.put(MongoUri, uri)
    }
    params.get(ObjectName).foreach { objectName =>
      context.put(ObjectName, objectName)
    }
    params.get(Filepath).foreach { path =>
      context.put(Filepath, path)
    }
    new HybridJsonContext(context.toMap, None)
  }
}
