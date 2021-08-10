package com.igniteplus.data.pipeline.constants

object ApplicationConstants {
  val MASTER:String = "local"
  val APP_NAME:String ="Clickstream Pipeline"

  //DATASET
  val CLICKSTREAM_DATASET:String = "data/input02/clickstream/clickstream_log.csv"
  val ITEM_DATASET:String = "data/input02/item/item_data.csv"

  val COLS_OF_CLICKSTREAM: Seq[String] = Seq("event_timestamp","device_type","session_id","visitor_id","item_id","redirection_source")
  val COLS_OF_ITEMDATA: Seq[String] = Seq("item_id","item_price","product_type","department_name")

  val CLICKSTREAM_DATASET_NULL:String = "data/input02/clickstream/view_log_null"
  val ITEM_DATASET_NULL:String = "data/input02/item/item_data_null"

  val FILE_FORMAT:String = "csv"

  val SESSION_ID:String = "session_id"
  val ITEM_ID:String = "item_id"
  val NULLKEY_CLICKSTREAM: Seq[String] = Seq(SESSION_ID, ITEM_ID)
  val NULLKEY_ITEM: Seq[String] = Seq(ITEM_ID)

  val EVENT_TIMESTAMP:String = "event_timestamp"
  val COL_NAME_DATATYPE_DF1: Seq[String] = Seq(EVENT_TIMESTAMP)
  val TIME_STAMP:String = "timestamp"
  val DATATYPE_DF1: Seq[String] = Seq(TIME_STAMP)

  val REDIRECTION_SOURCE:String = "redirection_source"
  val COL_NAME_LOWERCASE_DF1: Seq[String] = Seq(REDIRECTION_SOURCE)
  val DEPARTMENT_NAME:String = "department_name"
  val COL_NAME_LOWERCASE_DF2: String = DEPARTMENT_NAME


}
