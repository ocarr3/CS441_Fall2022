package edu.uic.cs441
package HW1Utils
import HW1Utils.{HW1CreateLogger, HW1Params, MapReduceProgram, MapReduceProgram3}

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*
import org.slf4j.{Logger, LoggerFactory}
import java.io.*
import java.util
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.sys.process.*
import scala.util.{Failure, Success, Try}

object RunMP:
  @main def runMapReduce =
    val logger = HW1CreateLogger(classOf[RunMP.type])
    logger.info("Logging for MapReduce has started...")
    val INITSTRING = "Starting the task."
    implicit val ec: ExecutionContext = ExecutionContext.global
    logger.info("Starting Task One")
    val TaskOne = MapReduceProgram(HW1Params.minimumInterval, HW1Params.maximumInterval)
    if(TaskOne.runMapReduce(HW1Params.inputPathTask1, HW1Params.outputPathTask1)){
      logger.info(s"Map and reduce for logs ${HW1Params.minimumInterval} through ${HW1Params.maximumInterval}")
    } else{
      logger.info(s"Map and reduce task one jobs have failed!")
    }
    
    logger.info("Starting Task Three")
    val TaskThree = MapReduceProgram3()
    if(TaskThree.runMapReduce(HW1Params.inputPathTask3, HW1Params.outputPathTask3)){
      logger.info(s"Map and reduce task 3 succesful")
    } else {
      logger.info(s"Map and reduce jobs for task three have failed!")
    }
    
    
    






