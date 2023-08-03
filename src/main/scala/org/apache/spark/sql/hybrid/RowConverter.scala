package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{ JSONOptions, JacksonGenerator }
import org.apache.spark.sql.catalyst.util.{ resourceToString, CaseInsensitiveMap }
import org.apache.spark.sql.types.StructType

import java.io.CharArrayWriter
import java.util.TimeZone

class RowConverter(dataType: StructType) {

  private val emptyOptions = new JSONOptions(CaseInsensitiveMap(Map.empty), TimeZone.getTimeZone("UTC").getID)

  def toJsonString(input: Iterator[InternalRow]): Iterator[String] = {
    val writer     = new CharArrayWriter()
    val jacksonGen = new JacksonGenerator(dataType, writer, emptyOptions)

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
