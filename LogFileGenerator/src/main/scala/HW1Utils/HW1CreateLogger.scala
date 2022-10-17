package edu.uic.cs441
package HW1Utils

import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

// Create logger from logback.xml to begin login for mapreduce tasks

object HW1CreateLogger:
  def apply[T](class4Logger: Class[T]): Logger =
    val HW1LOGBACKXML = "logback.xml"
    val logger = LoggerFactory.getLogger("mr-logger")
    Try(getClass.getClassLoader.getResourceAsStream(HW1LOGBACKXML)) match {
      case Failure(exception) => logger.error(s"Failed to locate $HW1LOGBACKXML for reason $exception")
      case Success(inStream) => inStream.close()
    }
    logger