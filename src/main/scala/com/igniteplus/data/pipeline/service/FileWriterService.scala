package com.igniteplus.data.pipeline.service

import org.apache.spark
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.{DataFrame, Row}

object FileWriterService {
  def writeFile(dataset:DataFrame,path:String,fileFormat:String): Unit = {
    dataset.write.format(fileFormat).save(path)
  }

}
