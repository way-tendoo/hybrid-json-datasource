package org.apache.spark.sql.hybrid.rdd

import org.apache.spark.{ Partition, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.model.HybridJSONPartition
import org.apache.spark.sql.types.StructType

class HybridJSONStreamRDD(partitions: Array[HybridJSONPartition], schema: StructType)
    extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    HybridJSONPartitionEvaluator(schema, Array.empty).eval(split)
  }

  override def getPartitions: Array[Partition] = partitions.asInstanceOf[Array[Partition]]
}
