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

class Map4 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
  private final val one = new IntWritable(1)
  private val word = new Text()
  val pattern: Regex = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r

  // Mapper and Reducer code for Task 4: Show the for each log type the highest character message with the injected string

  @throws[IOException]
  override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    val line: String = value.toString
    line.split("\n").foreach { token =>
      try {
        val typeString = line.substring(line.indexOf("]") + 2, line.length())
        val genString = typeString.substring((typeString.indexOf("- ") + 2), typeString.length())
        val regexFound = pattern.findFirstMatchIn(genString)
        val categoryString = typeString.substring(0, typeString.indexOf(" "))
        val characters = new IntWritable(genString.length)
        // For the mapper not much other than tokenizing the line into the information we need which is the log type, the reducer is doing the "heavy lifting"
        if(regexFound.isDefined) {
          word.set(categoryString)
          output.collect(word, characters)
        }
      } catch { // Handle exceptions for lines that are empty or do not follow the format
        case e: StringIndexOutOfBoundsException => println("Had an out of bounds error splitting info from line: " + line)
      }
    }

class Reduce4 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    // As values are passed to the reducer replace the previous value mapped to a log type key only if it greater than that previous key
    val sum = values.asScala.reduce((valueOne, valueTwo) => {
      if (valueOne.get() > valueTwo.get()) {
        new IntWritable(valueOne.get())
      } else {
        new IntWritable(valueTwo.get())
      }
    })
    // Result of this is the highest value left mapped to the unique log type
    output.collect(key, new IntWritable(sum.get()))

class MapReduceProgram4(){
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
    conf.setMapperClass(classOf[Map4])
    conf.setCombinerClass(classOf[Reduce4])
    conf.setReducerClass(classOf[Reduce4])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    MultipleInputs.addInputPath(conf, new Path(inputPath), classOf[TextInputFormat])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    return true

}




