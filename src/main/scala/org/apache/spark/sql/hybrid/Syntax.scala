package org.apache.spark.sql.hybrid

import org.apache.spark.sql.hybrid.Const.Database.IndexStore
import org.mongodb.scala.{Completed, Document, FindObservable, MongoClient}

import scala.concurrent.Future

object Syntax {

  implicit final class MongoOps(private val client: MongoClient) extends AnyVal {

    def insertOne(table: String, document: Document, database: String = IndexStore): Future[Completed] = {
      client
        .getDatabase(database)
        .getCollection(table)
        .insertOne(document)
        .head()
    }

    def find(table: String, expr: Document, database: String = IndexStore): FindObservable[Document] = {
      client
        .getDatabase(database)
        .getCollection(table)
        .find(expr)
    }
  }
}
