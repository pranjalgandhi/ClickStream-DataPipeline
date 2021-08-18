package com.igniteplus.data.pipeline.service


import org.apache.spark.sql.{DataFrame}

object FileWriterService {
  def writeFile(dataset:DataFrame,path:String,fileFormat:String): Unit = {
    dataset.write.option("header",true).format(fileFormat).save(path)
  }

}
