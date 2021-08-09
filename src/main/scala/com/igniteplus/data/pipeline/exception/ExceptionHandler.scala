package com.igniteplus.data.pipeline.exception

object ExceptionHandler {
  class EmptyFileException(s:String) extends Exception(s){}
}
