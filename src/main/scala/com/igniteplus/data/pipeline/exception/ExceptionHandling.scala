package com.igniteplus.data.pipeline.exception

class ExceptionHandling(message: String, cause: Throwable) extends Exception(message,cause){
  def this(message: String) = this(message, None.orNull)
}
case class EmptyFileException(message: String) extends ExceptionHandling(message)

case class FileNotFoundException(message: String) extends ExceptionHandling(message)