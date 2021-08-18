package com.igniteplus.data.pipeline.exception

object ExceptionHandler {
  class EmptyFileException(s:String) extends Exception(s){}
  class FileNotFoundException(s:String) extends Exception(s){}
  class DqDuplicateCheckFail(s:String) extends Exception(s){}
  class DqNullCheckFail(s:String) extends Exception(s){}
}
