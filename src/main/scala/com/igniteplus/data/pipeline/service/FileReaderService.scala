package com.igniteplus.data.pipeline.service


import org.apache.spark.sql.{DataFrame, SparkSession}
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession

object FileReaderService {

  def readFile(path:String,fileFormat:String)( implicit spark :SparkSession) : DataFrame ={
    val dfViewData: DataFrame = spark.read.format(fileFormat)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    dfViewData
  }


}
