package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileReadException
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReaderService {
  /**
   * Reads the contents of the file

   * @param inputPath specifies the path from where the data is to read

   * @param inputPath specifies the inputPath from where the data is to read

   * @param fileFormat specifies the format of the file
   * @param spark
   * @return the contents read from the file
   */
    def readFile(inputPath:String,
                 fileFormat:String)
                (implicit spark:SparkSession): DataFrame = {

        val dfReadData: DataFrame =
          try {
            spark.read
              .option("header","true")
              .format(fileFormat)
              .load(inputPath)
          }
          catch {
            case e: Exception =>
              FileReadException("Unable to read file from the given location " + inputPath)
              spark.emptyDataFrame

          }

        val dfDataCount: Long = dfReadData.count()

        if(dfDataCount == 0) {

          throw FileReadException("The input file is empty " + inputPath)

        }

        dfReadData
  }

}
