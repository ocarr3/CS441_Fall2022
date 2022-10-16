package Generation

import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.text.SimpleDateFormat
import java.time.Instant
import scala.util.matching.Regex
import java.util.{Date, TimeZone}

// Tests for logic used in the mappers across the tasks

class MRLogic extends AnyFlatSpec with Matchers with PrivateMethodTester{

  it should "given a date it should fall between the two intervals" in {
    val method = new Methods
    val result = method.compareTimeInterval("10:00:00.000", "09:00:00.000", "11:00:00.000")
    result should be (true)

    val result2 = method.compareTimeInterval("10:000:00.000", "11:00:00.000", "12:00:00.000")
    result2 should not be (true)

    val result3 = method.compareTimeInterval("23:00:00.000", "10:00:00.000", "22:00:00.000")
    result3 should not be (true)
  }

  it should "given a string and regex it should be determined if present in the string" in {
    val pattern: Regex = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r
    val goodString = "[et<ghDN5Je/U1we]f`ag2H6pS9rae0bg1_]N>iZ|<:~lnvcKi>Tk"
    val badString = "hPQ\rTRa-G391SxSEQFrVBtvngJX$W7Gww<MaW=69H*7(f"

    val regexFound = pattern.findFirstMatchIn(goodString)
    val regexNotFound = pattern.findFirstMatchIn(badString)

    regexFound should not be (None)
    regexNotFound should be (None)

  }

  it should "given a line from the log the string should be stripped correctly" in {
    val line = "05:53:26.074 [scala-execution-context-global-25] INFO  HelperUtils.Parameters$ - ag1C$Jw:z)30B~C><Lag2bf3ce3S5uaf0O6j&q>q=`3+Z?N.h8/IZK"
    val pattern: Regex = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r
    val typeString = line.substring(line.indexOf("]") + 2, line.length())
    val genString = typeString.substring((typeString.indexOf("-") + 2), typeString.length())
    val regexFound = pattern.findFirstMatchIn(genString)
    val categoryString = typeString.substring(0, typeString.indexOf(" "))

    typeString should be ("INFO  HelperUtils.Parameters$ - ag1C$Jw:z)30B~C><Lag2bf3ce3S5uaf0O6j&q>q=`3+Z?N.h8/IZK")
    genString should be ("ag1C$Jw:z)30B~C><Lag2bf3ce3S5uaf0O6j&q>q=`3+Z?N.h8/IZK")
    regexFound should not be (None)
    categoryString should be ("INFO")
  }

  it should "given a time in a string and a interval in a string they should be parsed into dates and get the millis" in {
    val time = "19:01:30.00"
    val interval = "00:01:00.000"
    val format = new SimpleDateFormat("HH:mm:ss.SSS")
    format.setTimeZone(TimeZone.getTimeZone("GMT"))
    val timeDate = format.parse(time)
    val intervalDate = format.parse(interval)

    val bottomInterval = (timeDate.getTime.toDouble / intervalDate.getTime.toDouble).floor.toInt
    val topInterval = (timeDate.getTime.toDouble / intervalDate.getTime.toDouble).ceil.toInt

    bottomInterval should be (1141)
    topInterval should be (1142)
  }


  it should "given a Date object from the log and the chosen interval return the correct interval key" in {
    val method = new Methods
    val time = "14:07:31.365"
    val interval = "00:00:30.000"
    val format = new SimpleDateFormat("HH:mm:ss.SSS")
    format.setTimeZone(TimeZone.getTimeZone("GMT"))
    val timeDate = format.parse(time)
    val intervalDate = format.parse(interval)

    val bottomMillis = method.getBottomIntervalTime(timeDate, intervalDate)
    val topMillis = method.getTopIntervalTime(timeDate, intervalDate)

    bottomMillis should be (50850000)
    topMillis should be (50880000)

    val bottomDate = Instant.ofEpochMilli(bottomMillis)
    val topDate = Instant.ofEpochMilli(topMillis)

    val bottomTimeKey = bottomDate.toString.substring(bottomDate.toString.indexOf("T") + 1, bottomDate.toString.indexOf("Z"))
    val topTimeKey = topDate.toString.substring(topDate.toString.indexOf("T") + 1, topDate.toString.indexOf("Z"))

    bottomTimeKey should be ("14:07:30")
    topTimeKey should be ("14:08:00")

  }
  

  class Methods {
    def compareTimeInterval(log: String, min: String, max: String): Boolean =
      val format = new SimpleDateFormat("HH:mm:ss.SSS")
      val dateString = log.substring(0, log.length())
      val logDate = format.parse(dateString)
      val minTime = format.parse(min)
      val maxTime = format.parse(max)
      if (logDate.before(maxTime) && logDate.after(minTime)) {
        return true
      }
      return false

    def getBottomIntervalTime(logDate: Date, intervalDate: Date): Long =
      //println("getting bottoms : " + logDate.toString + " " + intervalDate.toString)
      val getLog = logDate.getTime
      val getInt = intervalDate.getTime
      //println("getLog: " + getLog)
      //println("getInt: " + getInt)
      val intervalTimeMillis = ((getLog).toDouble / (getInt).toDouble).floor.toInt
      //println("getting bottom millis: " + intervalTimeMillis)
      val intervalTime = intervalTimeMillis * intervalDate.getTime
      intervalTime

    def getTopIntervalTime(logDate: Date, intervalDate: Date): Long =
      val intervalTimeMillis = (logDate.getTime.toDouble / intervalDate.getTime.toDouble).ceil.toInt
      val intervalTime = intervalTimeMillis * intervalDate.getTime
      intervalTime
  }
}



