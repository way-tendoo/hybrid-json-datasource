package org.apache.spark.sql.hybrid

import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JacksonParser}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.FailureSafeParser
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.util.TimeZone

final class JsonParser(schema: StructType) extends Serializable {

  private val jsonParser = {
    val emptyOptions = new JSONOptions(CaseInsensitiveMap(Map.empty), TimeZone.getTimeZone("UTC").getID)
    new JacksonParser(schema, emptyOptions)
  }

  def toRow(input: Iterator[String]): Iterator[InternalRow] = {
    val parser = new FailureSafeParser[String](
      input =>
        jsonParser
          .parse(input, CreateJacksonParser.string(_: JsonFactory, _: String), (s: String) => UTF8String.fromString(s)),
      jsonParser.options.parseMode,
      schema,
      jsonParser.options.columnNameOfCorruptRecord
    )
    input.flatMap(parser.parse)
  }
}
