package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{CLICKSTREAM_DATASET, CLICKSTREAM_DATASET_NULL, COLS_OF_CLICKSTREAM, COLS_OF_ITEMDATA, COL_NAME_DATATYPE_DF1, COL_NAME_LOWERCASE_DF1, DATATYPE_DF1, EVENT_TIMESTAMP, FILE_FORMAT, ITEM_DATASET, ITEM_DATASET_NULL, NULLKEY_CLICKSTREAM, NULLKEY_ITEM, REDIRECTION_SOURCE}
import com.igniteplus.data.pipeline.util.ApplicationUtil
import com.igniteplus.data.pipeline.service.FileReaderService.readFile
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import com.igniteplus.data.pipeline.cleaner.Cleanser.{checkNFilterNullRow, convertToLowerCase, dataTypeValidation, removeDuplicates, trimColumn}
import com.igniteplus.data.pipeline.dqchecks.DqCheckMethods.{DqDuplicateCheck, DqNullCheck}
import org.apache.spark.sql.SparkSession

object PipelineService {

  def execute()(implicit spark: SparkSession):Unit = {
    val dfItemData = readFile(ITEM_DATASET,FILE_FORMAT)
    dfItemData.printSchema()

//    val df = readFile("data/input02/clickstream/empty12.csv",FILE_FORMAT)

    val dfViewLog = readFile(CLICKSTREAM_DATASET,FILE_FORMAT)
    dfViewLog.printSchema()

    val dfViewLogDataTypeValidated = dataTypeValidation(dfViewLog,COL_NAME_DATATYPE_DF1,DATATYPE_DF1)
    dfViewLogDataTypeValidated.printSchema()

    val dfViewLogTrimmed = trimColumn(dfViewLogDataTypeValidated)
    val dfItemDataTrimmed = trimColumn(dfItemData)

    val dfViewLogNotNull = checkNFilterNullRow(dfViewLogTrimmed,NULLKEY_CLICKSTREAM,CLICKSTREAM_DATASET_NULL)
    val dfItemDataNotNull = checkNFilterNullRow(dfItemDataTrimmed,NULLKEY_ITEM,ITEM_DATASET_NULL)

    val dfViewRemovedDuplicates = removeDuplicates(dfViewLogNotNull,NULLKEY_CLICKSTREAM,EVENT_TIMESTAMP)
    val dfItemRemovedDuplicates = removeDuplicates(dfItemDataNotNull,NULLKEY_ITEM)

    val dfViewLogLower = convertToLowerCase(dfViewRemovedDuplicates,COL_NAME_LOWERCASE_DF1)

    DqNullCheck(dfItemRemovedDuplicates,NULLKEY_ITEM)
    DqNullCheck(dfViewLogLower,NULLKEY_CLICKSTREAM)

    DqDuplicateCheck(dfItemRemovedDuplicates,NULLKEY_ITEM)
    DqDuplicateCheck(dfViewLogLower,NULLKEY_CLICKSTREAM,EVENT_TIMESTAMP)

//
//    val dfViewRemovedDuplicates = removeDuplicates(dfViewLog,NULLKEY_CLICKSTREAM,EVENT_TIMESTAMP)
//    //    println(dfViewRemovedDuplicates.count())
//    dfViewRemovedDuplicates.show(false)
////    val dfItemRemovedDuplicates = removeDuplicates(dfItemDataNotNull,NULLKEY_ITEM)
////    //    println(dfItemRemovedDuplicates.count())
//
//    val dfLower = convertToLowerCase(dfViewRemovedDuplicates,COL_NAME_LOWERCASE_DF1)
//dfLower.show(false)
//    val dfViewLogDtaTypeValidated = dataTypeValidation(dfLower,COL_NAME_DATATYPE_DF1,DATATYPE_DF1)
//     dfViewLogDtaTypeValidated.show(false)
//    //    dfViewLog.printSchema()
//    //    writeFile(dfViewLog.filter(dfViewLog("device_type") === "web"),"data/input02/item/item.csv","csv"







  }

}
