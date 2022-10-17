package edu.uic.cs441
package HW1Utils

import com.typesafe.config.Config
import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}
import collection.JavaConverters.asScalaBufferConverter
import collection.JavaConverters.collectionAsScalaIterableConverter
import collection.JavaConverters.iterableAsScalaIterableConverter 

// Return paramters set in HW1Configs.conf for use in the application


object HW1Params:


  private val logger = HW1CreateLogger(classOf[HW1Params.type])
  val config: Config = HW1ObtainConfigReference("MRTask") match {
    case Some(value) => value
    case None => throw new RuntimeException("cannot obtain config data")
  }

  type ConfigType2Process[T] = T match
    case Int => Int
    case Long => Long
    case String => String
    case Double => Double
    case Tuple2[Double, Double] => Tuple2[Double, Double]

  private def timeInterval(logTypeName: String): Tuple2[Double, Double] =
    val lst = Try(config.getDoubleList(s"edu.uic.cs441.HW1Utils.MapReduceProgram.logMessageType.$logTypeName").asScala.toList) match {
      case Success(value) => value.sorted
      case Failure(exception) => logger.error(s"No config paramerter is proved: $logTypeName")
        throw new IllegalArgumentException(s"No config data for $logTypeName")
    }
    if lst(0) < 0 then throw new IllegalArgumentException(s"Min log interval cannot be negative for log $logTypeName")
    if lst(0) < lst(1) then throw new IllegalArgumentException(s"Max log interval cannot be less than Min for log $logTypeName")
    (lst(0), lst(1))
  end timeInterval

  private def logMsgRange(logTypeName: String): Tuple2[Double, Double] =
    val lst = Try(config.getDoubleList(s"edu.uic.cs441.HW1Utils.MapReduceProgram.logMessageType.$logTypeName").asScala.toList) match {
      case Success(value) => value.sorted
      case Failure(exception) => logger.error(s"No config parameter is provided: $logTypeName")
        throw new IllegalArgumentException(s"No config data for $logTypeName")
    }
    if lst.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for log $logTypeName")
    (lst(0), lst(1))
  end logMsgRange

  private def func4Parameter[T](defaultVal: T, f: String => T): String => T =
    (pName: String) => Try(f(s"MRTask.$pName")) match {
      case Success(value) => value
      case Failure(exception) => logger.warn(s"No config parameter $pName is provided. Defaulting to $defaultVal")
        defaultVal
    }
  end func4Parameter

  private val compFunc: (Tuple2[Double, Double], Tuple2[Double, Double]) => Boolean = (input1, input2) => (input1._1 <= input2._1) && (input1._2 < input2._2)

  private def getParam[T](pName: String, defaultVal: T): ConfigType2Process[T] =
    defaultVal match {
      case v: Int => func4Parameter(v, config.getInt)(pName)
      case v: Long => func4Parameter(v, config.getLong)(pName)
      case v: String => func4Parameter(v, config.getString)(pName)
      case v: Double => func4Parameter(v, config.getDouble)(pName)
      case v: Tuple2[Double, Double] => logMsgRange(pName)
    }
  end getParam

  import scala.concurrent.duration.*
  val minimumInterval: ConfigType2Process[String] = getParam("minTime", "00:00:00:000")
  val maximumInterval: ConfigType2Process[String] = getParam("maxTime", "23:59:59.999")
  val inputPathTask1: ConfigType2Process[String] = getParam("inputPathTask1", "src/main/resources/input")
  val outputPathTask1: ConfigType2Process[String] = getParam("outputPathTask1", "src/main/resources/output")
  val secInterval: ConfigType2Process[String] = getParam("secInterval", "00:00:30.000")
  val inputPathTask2: ConfigType2Process[String] = getParam("inputPathTask2", "src/main/resources/input2")
  val outputPathTask2: ConfigType2Process[String] = getParam("outputPathTask2", "src/main/resources/output2")
  val inputPathTask3: ConfigType2Process[String] = getParam("inputPathTask3", "src/main/resources/input3")
  val outputPathTask3: ConfigType2Process[String] = getParam("outputPathTask3", "src/main/resources/output3")
  val inputPathTask4: ConfigType2Process[String] = getParam("inputPathTask4", "src/main/resources/input4")
  val outputPathTask4: ConfigType2Process[String] = getParam("outputPathTask4", "src/main/resources/output4")
  val sortInputTask2: ConfigType2Process[String] = getParam("sortInputTask2", "src/main/resources/output2/part-00000")
  val sortOutputTask2: ConfigType2Process[String] = getParam("sortOutputTask2", "src/main/resources/output2Sort/")

