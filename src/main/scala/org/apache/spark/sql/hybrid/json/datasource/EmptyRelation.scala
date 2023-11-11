package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

object EmptyRelation {
  def apply(): BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = null
      override def schema: StructType     = null
    }
  }
}
