package com.igniteplus.data.pipeline.dqchecks

import com.igniteplus.data.pipeline.constants.ApplicationConstants.ROW_NUMBER
import com.igniteplus.data.pipeline.exception.{DqDuplicateCheckException, DqNullCheckException}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, when}

object DqCheckMethods {

  def DqNullCheck(df: DataFrame, keyColumns: Seq[String]):Boolean = {
    val columnNames:Seq[Column] = keyColumns.map(ex => col(ex))
    val condition:Column = columnNames.map(ex => ex.isNull).reduce(_||_)
    val dfCheckNullKeyRows:DataFrame = df.withColumn("nullFlag" , when(condition,value = true).otherwise(value = false))

    val  nullDf:DataFrame = dfCheckNullKeyRows.filter(dfCheckNullKeyRows("nullFlag")===true)

    if (nullDf.count() > 0)
      throw DqNullCheckException("The file contains nulls")
    true
  }

  def DqDuplicateCheck (df:DataFrame, KeyColumns : Seq[String], orderByCol: String ) :Boolean   = {
    val windowSpec = Window.partitionBy(KeyColumns.map(col):_* ).orderBy(desc(orderByCol))
    val dfDropDuplicate: DataFrame = df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
      .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
    if(df.count()!=dfDropDuplicate.count())
      throw DqDuplicateCheckException("The file contains duplicate")
    true

  }

}
