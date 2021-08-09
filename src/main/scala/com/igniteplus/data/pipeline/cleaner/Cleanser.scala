package com.igniteplus.data.pipeline.cleaner


import com.igniteplus.data.pipeline.constants.ApplicationConstants.FILE_FORMAT
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.SizeBasedWindowFunction.n
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lower, row_number, trim, unix_timestamp, when}


object Cleanser {


  def FilterNullRow(df:DataFrame, columnList: Seq[String],path:String): DataFrame = {

    val columnNames:Seq[Column] = columnList.map(ex => col(ex))
    val condition:Column = columnNames.map(ex => ex.isNull).reduce(_||_)
    val dfCheckNullKeyRows:DataFrame = df.withColumn("nullFlag" , when(condition,value = "true").otherwise(value = "false"))

    val  dataSetNull : DataFrame = dfCheckNullKeyRows.filter(dfCheckNullKeyRows("nullFlag")==="true")

    if (dataSetNull.count() > 0)
      writeFile(dataSetNull,path ,FILE_FORMAT)
    dataSetNull
  }


  //separate not null
  def separateNotNull(dataset:DataFrame,
                      columnList: Seq[String]):DataFrame={
    val datasetNotNull:DataFrame= dataset.na.drop(columnList)
    datasetNotNull
  }


  //convert to lowercase
  def convertToLowerCase(df:DataFrame,colName:Seq[String]):DataFrame = {
    var dfConvertToLowerCase = df
    for(n<-colName) dfConvertToLowerCase = df.withColumn(n, lower(col(n)))
    dfConvertToLowerCase
  }



  def dataTypeValidation(df:DataFrame,colName:Seq[String], dt:Seq[String]): DataFrame = {
    var dfCrDataType = df
    for (i <- colName.indices) {
      if (dt(i) == "timestamp")
        dfCrDataType = dfCrDataType.withColumn(colName(i), unix_timestamp(col(colName(i)), "MM/dd/yyyy H:mm").cast("timestamp"))
      else
        dfCrDataType = dfCrDataType.withColumn(colName(i), col(colName(i)).cast(dt(i)))
    }
    dfCrDataType
  }


  def trimColumn(df:DataFrame,column:Seq[String]):DataFrame = {
    var trimmedDF: DataFrame = df
    for(n<-column) trimmedDF = df.withColumn(n, trim(col(n)))
    trimmedDF
  }


  //  //separate null rows
  //  def separateNull(dataset:DataFrame,
  //                   columnList: Seq[String],path:String):Unit ={
  //    var dataSetNull = dataset
  //    for (n <- columnList)  dataSetNull = dataSetNull.filter(col(n).isNull)
  //    if (dataSetNull.count() > 0)
  //      writeFile(dataSetNull,path ,FILE_FORMAT)
  //
  //  }



  def removeDuplicates (df:DataFrame, KeyColumns : Seq[String], orderByCol: String = "") : DataFrame  = {
    if( orderByCol.isEmpty)  {
      val dfDropDuplicate = df.dropDuplicates(KeyColumns)
      dfDropDuplicate
    }
    else{
      val windowSpec = Window.partitionBy(KeyColumns.map(col):_* ).orderBy(desc(orderByCol))
      val dfDropDuplicate: DataFrame = df.withColumn(colName ="row_number", row_number().over(windowSpec))
        .filter(conditionExpr = "row_number == 1" ).drop("row_number")
      dfDropDuplicate
    }
  }


  //
  //  def removeDuplicates(dataset:DataFrame):DataFrame = {
  //  val winSpec=Window.partitionBy("session_id","item_id").orderBy(desc("event_timestamp"))
  //  val primaryData:DataFrame=dataset.withColumn("row_num",row_number().over(winSpec))
  //  val output:DataFrame=primaryData.filter("row_num == 1")
  //  val out = output.drop("row_num")
  //  out
  //}


  //convert to specific datatype
  //  def convertToDatatype(dataset:DataFrame,colName:Seq[String],datatype: Seq[String]):DataFrame = {
  //    var dataModified = dataset
  //    for (n <- colName; m <- datatype) {
  //      if (m == "timestamp") dataModified.withColumn(n, unix_timestamp(col(n), "MM/dd/yyyy HH:mm").cast(m))
  //      else dataModified = dataset.withColumn(n,col(n).cast(m))
  //    }
  //    dataModified
  //  }
}
