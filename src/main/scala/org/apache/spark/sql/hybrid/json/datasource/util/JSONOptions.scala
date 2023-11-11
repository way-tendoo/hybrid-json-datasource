package org.apache.spark.sql.hybrid.json.datasource.util

import org.apache.spark.sql.catalyst.json.{JSONOptions => SparkJSONOptions}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.hybrid.json.datasource.Const

import java.util.TimeZone

object JSONOptions {

  def empty = new SparkJSONOptions(CaseInsensitiveMap(Map.empty), TimeZone.getTimeZone(Const.TimeZone.UTC).getID)
}
