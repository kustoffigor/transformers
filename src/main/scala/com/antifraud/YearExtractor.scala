package com.antifraud

import java.time.format.DateTimeFormatter

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.types.{DataType, DataTypes}



class YearExtractor (override val uid: String)
  extends UnaryTransformer[java.sql.Timestamp, Int, YearExtractor] with DefaultParamsWritable {


  def this() = this(Identifiable.randomUID("yearExtractor"))

  override protected def createTransformFunc: java.sql.Timestamp => Int = _.toLocalDateTime.getYear()



//  override protected def validateInputType(inputType: DataType): Unit = {
//    require(inputType == Timestamp, "Input type must be string type but got $inputType.")
//  }

  override protected def outputDataType: DataType = DataTypes.IntegerType


  override def copy(extra: ParamMap): YearExtractor = defaultCopy(extra)
}

object YearExtractor extends DefaultParamsReadable[YearExtractor] {
  override def load(path: String): YearExtractor = super.load(path)
}