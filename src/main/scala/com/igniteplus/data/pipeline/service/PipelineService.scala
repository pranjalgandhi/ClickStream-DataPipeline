package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{CLICKSTREAM_DATASET, CLICKSTREAM_DATASET_NULL, COLS_OF_CLICKSTREAM, COLS_OF_ITEMDATA, COL_NAME_DATATYPE_DF1, DATATYPE_DF1, EVENT_TIMESTAMP, FILE_FORMAT, ITEM_DATASET, ITEM_DATASET_NULL, NULLKEY_CLICKSTREAM, NULLKEY_ITEM}
import com.igniteplus.data.pipeline.util.ApplicationUtil
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import com.igniteplus.data.pipeline.cleaner.Cleanser.{FilterNullRow, dataTypeValidation, removeDuplicates, separateNotNull, trimColumn}
import org.apache.spark.sql.SparkSession

object PipelineService {

  def execute()(implicit spark: SparkSession):Unit = {
    val dfItemData = readFile(ITEM_DATASET,FILE_FORMAT)
    dfItemData.printSchema()

    val df = readFile("data/input02/clickstream/empty1.csv",FILE_FORMAT)

//    val dfViewLog = readFile(CLICKSTREAM_DATASET,FILE_FORMAT)
//    dfViewLog.printSchema()
//
//    val dfViewLogDtaTypeValidated = dataTypeValidation(dfViewLog,COL_NAME_DATATYPE_DF1,DATATYPE_DF1)
//    dfViewLogDtaTypeValidated .printSchema()
//
//    val dfViewLogTrimmed = trimColumn(dfViewLogDtaTypeValidated ,COLS_OF_CLICKSTREAM)
//    val dfItemDataTrimmed = trimColumn(dfItemData,COLS_OF_ITEMDATA)
//
//    FilterNullRow(dfViewLogTrimmed,NULLKEY_CLICKSTREAM,CLICKSTREAM_DATASET_NULL)
//    FilterNullRow(dfItemDataTrimmed,NULLKEY_ITEM,ITEM_DATASET_NULL)
//
//    val dfViewLogNotNull = separateNotNull(dfViewLogTrimmed,NULLKEY_CLICKSTREAM)
//    val dfItemDataNotNull = separateNotNull(dfItemDataTrimmed,NULLKEY_ITEM)
//
//    val dfViewRemovedDuplicates = removeDuplicates(dfViewLogNotNull,NULLKEY_CLICKSTREAM,EVENT_TIMESTAMP)
//    //    println(dfViewRemovedDuplicates.count())
//    val dfItemRemovedDuplicates = removeDuplicates(dfItemDataNotNull,NULLKEY_ITEM)
//    //    println(dfItemRemovedDuplicates.count())

    //    dfViewLog.printSchema()
    //    writeFile(dfViewLog.filter(dfViewLog("device_type") === "web"),"data/input02/item/item.csv","csv"







  }

}