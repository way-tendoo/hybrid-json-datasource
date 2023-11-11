package org.apache.spark.sql.hybrid.json.datasource

import Const.FieldsName._
import Const.TablesName.SchemaIndex
import Syntax._
import org.apache.spark.sql.types.{ LongType, StructType }
import org.apache.spark.util.Utils
import org.mongodb.scala.Document

object Schema {

  def infer(ctx: HybridJSONContext): StructType = {
    val schemaRefs = Utils
      .tryWithResource(MongoClient(ctx.mongoUri))(
        _.find(SchemaIndex, Document(ObjectName -> ctx.objectName()))
          .toFuture()
          .await()
      )
      .map(_.get(SchemaRef).map(_.asString().getValue))
      .flatMap(_.toSeq)
    val initSchema = new StructType().add(s"__$CommitMillis", LongType)
    schemaRefs.map(StructType.fromString).foldLeft(initSchema) {
      case (acc, schema) => acc.merge(schema)
    }
  }
}
