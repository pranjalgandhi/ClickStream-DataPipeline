package com.igniteplus.data.pipeline.service



import com.igniteplus.data.pipeline.exception.ExceptionHandler.EmptyFileException
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession

object FileReaderService {

  def readFile(path:String,fileFormat:String)( implicit spark :SparkSession) : DataFrame ={
    val df: DataFrame = spark.read.format(fileFormat)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    if(df.count==0){
      throw new EmptyFileException("The file is empty")
    }
    df
  }


}
