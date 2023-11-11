package org.apache.spark.sql.hybrid.json.datasource

import Const.Database.IndexStore
import org.mongodb.scala.{ Document, FindObservable, MongoClient => NativeMongoClient }
import Syntax._

import java.io.Closeable

case class MongoClient(private val native: NativeMongoClient) extends Closeable {

  def insertOne(table: String, entity: Document): Unit = {
    native
      .getDatabase(IndexStore)
      .getCollection(table)
      .insertOne(entity)
      .head()
      .await()
  }

  def find(table: String, expr: Document): FindObservable[Document] = {
    native
      .getDatabase(IndexStore)
      .getCollection(table)
      .find(expr)
  }

  override def close(): Unit = {
    native.close()
  }
}

object MongoClient {
  def apply(mongoUri: String): MongoClient = MongoClient(NativeMongoClient(mongoUri))
}
