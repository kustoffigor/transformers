package com.antifraud

import java.time.format.DateTimeFormatter

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.types.{DataType, DataTypes}



class MinuteExtractor (override val uid: String)
  extends UnaryTransformer[String, Int, YearExtractor] with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("yearExtractor"))

  override protected def createTransformFunc: String => Int = java.time.LocalDateTime.parse(_, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getMinute()


  override protected def outputDataType: DataType = DataTypes.IntegerType


  override def copy(extra: ParamMap): YearExtractor = defaultCopy(extra)
}

object MinuteExtractor extends DefaultParamsReadable[YearExtractor] {
  override def load(path: String): YearExtractor = super.load(path)
}