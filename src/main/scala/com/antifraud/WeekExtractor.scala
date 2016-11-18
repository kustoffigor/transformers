package com.antifraud

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.WeekFields
import java.util.Locale

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{DataType, DataTypes}

class WeekExtractor (override val uid: String)
  extends UnaryTransformer[java.sql.Timestamp, Int, WeekExtractor] with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("WeekExtractor"))

  override protected def createTransformFunc: java.sql.Timestamp => Int = {
    val weekFields: WeekFields = WeekFields.of(Locale.getDefault)
    _.toLocalDateTime.get(weekFields.weekOfWeekBasedYear)
  }



  override protected def outputDataType: DataType = DataTypes.IntegerType


  override def copy(extra: ParamMap): WeekExtractor = defaultCopy(extra)
}

object WeekExtractor extends DefaultParamsReadable[WeekExtractor] {
  override def load(path: String): WeekExtractor = super.load(path)
}