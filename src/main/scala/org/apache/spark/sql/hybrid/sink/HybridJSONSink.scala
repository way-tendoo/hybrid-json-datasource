package org.apache.spark.sql.hybrid.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hybrid.Const.FieldsName._
import org.apache.spark.sql.hybrid.Const.TablesName.SchemaIndex
import org.apache.spark.sql.hybrid.Syntax._
import org.apache.spark.sql.hybrid.util.FileIO
import org.apache.spark.sql.hybrid.{HybridJSONContext, MongoClient}
import org.apache.spark.util.Utils
import org.mongodb.scala.bson.Document
import org.mongodb.scala.model.Filters.equal

class HybridJSONSink(ctx: HybridJSONContext) {

  def write(data: DataFrame): Unit = {
    val schema = data.schema
    FileIO.makeDirectoryIfNotExist(ctx.path())
    val writer = HybridJSONPartitionWriter(schema)(ctx)
    data.queryExecution.toRdd.foreachPartition(writer.write)
    val schemaRef = Utils.tryWithResource(MongoClient(ctx.mongoUri))(
      _.find(SchemaIndex, Document(ObjectName -> ctx.objectName()))
        .filter(equal(SchemaRef, schema.asNullable.json))
        .headOption()
        .await()
    )
    if (schemaRef.isEmpty) {
      Utils.tryWithResource(MongoClient(ctx.mongoUri))(
        _.insertOne(
          SchemaIndex,
          Document(
            ObjectName   -> ctx.objectName(),
            SchemaRef    -> schema.asNullable.json,
            CommitMillis -> System.currentTimeMillis()
          )
        )
      )
    }
  }
}
