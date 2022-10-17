package edu.uic.cs441
package HW1Utils

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.lib.MultipleInputs
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*

import java.io.*
import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId}
import java.util
import java.util.{Calendar, Date, StringTokenizer, TimeZone}
import scala.concurrent.ExecutionContext.global
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.sys.process.*
import scala.util.matching.Regex

// Simple mapper just used to pass the output of MapReduceProgramTaskTwo so it is sorted by Value and not Key

class Sort extends MapReduceBase with Mapper[LongWritable, Text, IntWritable, Text] :
  private val word = new Text()
  private final val one = new IntWritable(1)

  @throws[IOException]
  override def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable, Text], reporter: Reporter): Unit =
    val line: String = value.toString
    line.split("\n").foreach { token =>
      val key = token.substring(0, line.indexOf(","))
      val num = token.substring(line.indexOf(",") + 1, line.length())
      word.set(key + " ")
      val number = new IntWritable(Integer.parseInt(num))
      val fKey = new Text()
      fKey.set(key)
      output.collect(number, word)
    }


class SortProgram() {
  def runMapReduce(inputPath: String, outputPath: String): Boolean =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("Sort")
    //conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[IntWritable])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[Sort])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    MultipleInputs.addInputPath(conf, new Path(inputPath), classOf[TextInputFormat])
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
    return true
}


