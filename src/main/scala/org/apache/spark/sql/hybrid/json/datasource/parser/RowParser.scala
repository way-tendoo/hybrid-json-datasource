package org.apache.spark.sql.hybrid.json.datasource.parser

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JacksonGenerator
import org.apache.spark.sql.hybrid.json.datasource.util.JSONOptions
import org.apache.spark.sql.types.StructType

import java.io.CharArrayWriter

class RowParser(schema: StructType) {

  def toJsonString(input: Iterator[InternalRow]): Iterator[String] = {
    val writer     = new CharArrayWriter()
    val jacksonGen = new JacksonGenerator(schema, writer, JSONOptions.empty)

    new Iterator[String] {
      override def hasNext: Boolean = input.hasNext

      override def next(): String = {
        jacksonGen.write(input.next())
        jacksonGen.flush()

        val json = writer.toString
        if (hasNext) writer.reset() else jacksonGen.close()
        json
      }
    }
  }
}
