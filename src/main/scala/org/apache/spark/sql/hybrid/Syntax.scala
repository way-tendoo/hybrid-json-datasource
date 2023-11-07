package org.apache.spark.sql.hybrid

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

object Syntax {

  implicit final class FutureOps[T](private val f: Future[T]) extends AnyVal {
    // Don't use that in production code
    def await(atMost: Duration = 10.seconds): T = Await.result(f, atMost)
  }
}
