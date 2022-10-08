package edu.uic.cs441
package HW1Utils

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.mapred.lib.MultipleInputs
import org.apache.hadoop.util.*

import scala.util.matching.Regex
import java.io.*
import java.text.SimpleDateFormat
import java.util
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.sys.process.*
import scala.concurrent.ExecutionContext.global
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor


def compareTimeInterval(log : String, min: String, max:String) : Boolean =
  val format = new SimpleDateFormat("HH:mm:ss.SSS")
  val dateString = log.substring(0, log.indexOf(" "))
  val logDate = format.parse(dateString)
  val minTime = format.parse(min)
  val maxTime = format.parse(max)
  if(logDate.before(maxTime) && logDate.after(minTime)){
    return true
  }
  return false

class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
  private final val one = new IntWritable(1)
  private val word = new Text()
  val pattern: Regex = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r

  @throws[IOException]
  override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    val line: String = value.toString
    line.split("\n").foreach { token =>
      try {
        val typeString = line.substring(line.indexOf("]") + 2, line.length())
        val genString = typeString.substring((typeString.indexOf("-") + 2), typeString.length())
        val regexFound = pattern.findFirstMatchIn(genString)
        val categoryString = typeString.substring(0, typeString.indexOf(" "))
        if (compareTimeInterval(line, HW1Params.minimumInterval, HW1Params.maximumInterval) && regexFound.isDefined) { // Only map lines that have the regex pattern in their string for task 1
          word.set(HW1Params.minimumInterval + " - " + HW1Params.maximumInterval + "," + categoryString)
          output.collect(word, one)
        }
      } catch { // Handle exceptions for lines that are empty or do not follow the format
        case e: StringIndexOutOfBoundsException => println("Had an out of bounds error splitting info from line: " + line)
      }
    }

class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =

    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
    output.collect(key, new IntWritable(sum.get()))

class MapReduceProgram(val minTimeInterval : String, val maxStringInterval : String){
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  def runMapReduce(inputPath: String, outputPath : String) : Boolean =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("MapReduceTaskOne")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "10")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[IntWritable])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    MultipleInputs.addInputPath(conf, new Path(inputPath), classOf[TextInputFormat])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    return true

}




