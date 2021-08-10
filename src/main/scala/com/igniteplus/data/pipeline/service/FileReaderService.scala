package com.igniteplus.data.pipeline.service
import java.nio.file.{Files, Paths}
import com.igniteplus.data.pipeline.exception.ExceptionHandler.{EmptyFileException, FileNotFoundException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession

object FileReaderService {

  def readFile(path:String,fileFormat:String)( implicit spark :SparkSession) : DataFrame ={
    if(Files.exists(Paths.get(path))==false){
      throw new FileNotFoundException("The path is invalid")
    }

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
