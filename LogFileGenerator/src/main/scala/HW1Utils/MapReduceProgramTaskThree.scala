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
import org.apache.hadoop.mapreduce.lib.input.FileSplit


class Map3 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
  private final val one = new IntWritable(1)
  private val word = new Text()
  val pattern: Regex = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r
  @throws[IOException]
  override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    val line: String = value.toString
    line.split("\n").foreach { token =>
      val typeString = line.substring(line.indexOf("]") + 2, line.length())
      val categoryString = typeString.substring(0, typeString.indexOf(" "))
      word.set(categoryString)
      output.collect(word, one)
    }

class Reduce3 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
    output.collect(key, new IntWritable(sum.get()))

class MapReduceProgram3(){
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  def runMapReduce(inputPath: String, outputPath : String) : Boolean =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("MapReduceTaskOne")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[IntWritable])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map3])
    conf.setCombinerClass(classOf[Reduce3])
    conf.setReducerClass(classOf[Reduce3])
    conf.setInputFormat(classOf[TextInputFormat])
    MultipleInputs.addInputPath(conf, new Path(inputPath), classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    return true

}




