package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import scala.util.parsing.json.JSONObject

class RowConverter(dataType: StructType) {

  def toJsonString(input: Iterator[InternalRow]): Iterator[String] = {
    input.map { ir =>
      val row = new GenericRowWithSchema(ir.toSeq(dataType).toArray, dataType)
      JSONObject(row.getValuesMap(row.schema.fieldNames)).toString()
    }
  }
}