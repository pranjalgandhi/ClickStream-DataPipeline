package com.igniteplus.data.pipeline

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{APP_NAME, MASTER}
import com.igniteplus.data.pipeline.service.PipelineService
import com.igniteplus.data.pipeline.util.ApplicationUtil
import org.apache.spark.sql.SparkSession


object ProductName {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = ApplicationUtil.createSparkSession(APP_NAME,MASTER)
    PipelineService.execute()

//    val dfItemData = readFile(ITEM_DATASET)(spark = createSparkSession("product","local"))
//    println(dfItemData.count())
//    dfItemData.printSchema()
//    val dfViewLog = readFile(CLICKSTREAM_DATASET)(spark = createSparkSession("product","local"))
//    println(dfViewLog.count())
//    dfViewLog.printSchema()
//    valconvertToDatatype
//    val dfItemData1 = separateNull(dfItemData,COL_NAME_NULLKEY_DF1)
//    print(dfItemData1.count())
//

  }

}
