package org.apache.spark.sql.hybrid

import org.mongodb.scala.{ Completed, Document, FindObservable, MongoClient }

import scala.concurrent.Future

object Syntax {

  implicit final class MapOps[K, V](private val m: Map[K, V]) extends AnyVal {
    def getMandatory(key: K): V =
      m.getOrElse(key, throw new IllegalArgumentException(s"Parameter: `$key` must be specified"))
  }

  implicit final class MongoOps(private val client: MongoClient) extends AnyVal {

    def insertOne(table: String, document: Document, database: String = "index_store"): Future[Completed] = {
      client
        .getDatabase(database)
        .getCollection(table)
        .insertOne(document)
        .head()
    }

    def find(table: String, expr: Document, database: String = "index_store"): FindObservable[Document] = {
      client
        .getDatabase(database)
        .getCollection(table)
        .find(expr)
    }
  }
}
