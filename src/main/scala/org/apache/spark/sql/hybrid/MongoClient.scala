package org.apache.spark.sql.hybrid

import org.apache.spark.sql.hybrid.Const.Database.IndexStore
import org.mongodb.scala.{Completed, Document, FindObservable, MongoClient => NativeMongoClient }

import java.io.Closeable
import scala.concurrent.Future

case class MongoClient(private val native: NativeMongoClient) extends Closeable {

  def insertOne(table: String, document: Document, database: String = IndexStore): Future[Completed] = {
    native
      .getDatabase(database)
      .getCollection(table)
      .insertOne(document)
      .head()
  }

  def find(table: String, expr: Document, database: String = IndexStore): FindObservable[Document] = {
    native
      .getDatabase(database)
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


