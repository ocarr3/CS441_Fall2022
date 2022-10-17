package edu.uic.cs441
package HW1Utils

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.mapred.lib.MultipleInputs
import org.apache.hadoop.util.*

import java.io.*
import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId}
import java.util
import java.util.{Calendar, Date, TimeZone}
import scala.concurrent.ExecutionContext.global
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.sys.process.*
import scala.util.matching.Regex


// Functions that use the millis since the epoch to calculate time intervals based on given choice by the user
// Lower bound for the interval requires rounding down and upper bound rounding upwards
def getBottomIntervalTime(logDate: Date, intervalDate: Date): Long =
  val getLog = logDate.getTime
  val getInt = intervalDate.getTime
  val intervalTimeMillis = ((getLog).toDouble / (getInt).toDouble).floor.toInt
  val intervalTime = intervalTimeMillis * intervalDate.getTime
  intervalTime

def getTopIntervalTime(logDate: Date, intervalDate: Date): Long =
  val intervalTimeMillis = (logDate.getTime.toDouble / intervalDate.getTime.toDouble).ceil.toInt
  val intervalTime = intervalTimeMillis * intervalDate.getTime
  intervalTime


// Mapper and Reducer code for Task 2: Compute time intervals in order that contained messages with ERROR and injected string


class Map2 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
  private final val one = new IntWritable(1)
  private val word = new Text()
  val pattern: Regex = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r

  @throws[IOException]
  override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    val line: String = value.toString
    val calendar = Calendar.getInstance()
    System.setProperty("user.timezone", "GMT")
    line.split("\n").foreach { token =>
      try {
        val typeString = line.substring(line.indexOf("]") + 2, line.length())
        val timeString = line.substring(0, line.indexOf(" "))
        val genString = typeString.substring((typeString.indexOf("-") + 2), typeString.length())
        val regexFound = pattern.findFirstMatchIn(genString)
        val format = new SimpleDateFormat("HH:mm:ss.SSS")
        format.setTimeZone(TimeZone.getTimeZone("GMT"))
        println("TIME STRING: " + timeString) //Parse the found date and the chosen interval from HW1Params
        val interval = HW1Params.secInterval
        val logTime = format.parse(timeString)
        val intervalTime = format.parse(interval)

        // Grab the millis for the dates that correspond to the upper and lower bound surrounding our found date for the chosen interval
        val bottomMillis = getBottomIntervalTime(logTime, intervalTime)
        val topMillis = getTopIntervalTime(logTime, intervalTime)

        // Turn these millis back into dates to grab the times
        val bottomDate = Instant.ofEpochMilli(bottomMillis)
        val topDate = Instant.ofEpochMilli(topMillis)

        val bottomDateString = bottomDate.toString
        val topDateString = topDate.toString

        // Combine the newfound times into the key for the mapper 
        val bottomTimeKey = bottomDateString.substring(bottomDateString.indexOf("T") + 1, bottomDateString.indexOf("Z"))
        val topTimeKey = topDateString.substring(topDateString.indexOf("T") + 1, topDateString.indexOf("Z"))
        val key = bottomTimeKey + " - " + topTimeKey

        if (typeString.contains("ERROR") && regexFound.isDefined) { // Only map lines with ERROR and injected string for task 2
          word.set(key)
          output.collect(word, one)
        }
      } catch { // Handle exceptions for lines that are empty or do not follow the format
        case e: StringIndexOutOfBoundsException => println("Had an out of bounds error splitting info from line: " + line)
      }
    }

class Reduce2 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
    output.collect(key, new IntWritable(sum.get()))

class MapReduceProgram2() {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def runMapReduce(inputPath: String, outputPath: String): Boolean =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("MapReduceTaskOne")
    //conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "10")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[IntWritable])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map2])
    conf.setCombinerClass(classOf[Reduce2])
    conf.setReducerClass(classOf[Reduce2])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    MultipleInputs.addInputPath(conf, new Path(inputPath), classOf[TextInputFormat])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    return true

}




