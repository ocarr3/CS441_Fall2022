/*
 *
 *  Copyright (c) 2021. Mark Grechanik and Lone Star Consulting, Inc. All rights reserved.
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under
 *   the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *   either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 *
 */
import Generation.{LogMsgSimulator, RandomStringGenerator}
import HelperUtils.{CreateLogger, Parameters}
import org.slf4j.Logger

import scala.collection.JavaConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, duration}
import scala.util.{Failure, Success, Try}

object GenerateLogData:
  val logger: Logger = CreateLogger(classOf[GenerateLogData.type])

//this is the main starting point for the log generator
def runLogGenerator() =
  import GenerateLogData.*
  import Generation.*
  import Generation.RSGStateMachine.*
  import HelperUtils.Parameters.*

  logger.info("Log data generator started...")
  val INITSTRING = "Starting the string generation"
  val init = unit(INITSTRING)

  val logFuture = Future {
    LogMsgSimulator(init(RandomStringGenerator((Parameters.minStringLength, Parameters.maxStringLength), Parameters.randomSeed)), Parameters.maxCount)
  }
  Try(Await.result(logFuture, Parameters.runDurationInMinutes)) match {
    case Success(value) => logger.info(s"Log data generation has completed after generating ${Parameters.maxCount} records.")
    case Failure(exception) => logger.info(s"Log data generation has completed within the allocated time, ${Parameters.runDurationInMinutes}")
  }

