package com.igniteplus.data.pipeline.dqchecks

import com.igniteplus.data.pipeline.exception.ExceptionHandler.{DqDuplicateCheckFail, DqNullCheckFail}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, desc, row_number, when}

object DqCheckMethods {

  def DqNullCheck(df: DataFrame, keyColumns: Seq[String]): Unit = {
    var nullDf: DataFrame = df
    for (i <- keyColumns)
      nullDf = df.filter(df(i).isNull)
    if (nullDf.count() > 0)
      throw new DqNullCheckFail("The file contains nulls")
  }

  def DqDuplicateCheck (df:DataFrame, KeyColumns : Seq[String], orderByCol: String = "") : Unit  = {
    if( orderByCol.isEmpty)  {
      val dfDropDuplicate = df.dropDuplicates(KeyColumns)
      if(df.count()!=dfDropDuplicate.count())
        throw new DqDuplicateCheckFail("The file contains duplicate")
    }
    else{
      val windowSpec = Window.partitionBy(KeyColumns.map(col):_* ).orderBy(desc(orderByCol))
      val dfDropDuplicate: DataFrame = df.withColumn(colName ="row_number", row_number().over(windowSpec))
        .filter(conditionExpr = "row_number == 1" ).drop("row_number")
      if(df.count()!=dfDropDuplicate.count())
        throw new DqDuplicateCheckFail("The file contains duplicate")
    }
  }


}

