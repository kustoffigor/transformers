package com.antifraud

import java.time.format.DateTimeFormatter

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.types.{DataType, DataTypes}


class MinuteExtractor (override val uid: String)
  extends UnaryTransformer[java.sql.Timestamp, Int, MinuteExtractor] with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("minuteExtractor"))

  override protected def createTransformFunc: java.sql.Timestamp => Int = _.toLocalDateTime.getMinute()


  override protected def outputDataType: DataType = DataTypes.IntegerType


  override def copy(extra: ParamMap): MinuteExtractor = defaultCopy(extra)
}

object MinuteExtractor extends DefaultParamsReadable[MinuteExtractor] {
  override def load(path: String): MinuteExtractor = super.load(path)
}