import HelperUtils.CreateLogger
import LogMethods.{DistributionPattern, LargestMessage, MostErrorInTime, TimeIntervalLogs}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import java.sql.Driver

class ExecutionStart

object ExecutionStart {
  val logger = CreateLogger(classOf[ExecutionStart])

  /** Main Method - Triggers the MapReduce method based on command line input
   *
   * @param args : Array[String] - command line input
   */

  def main(args: Array[String]): Unit = {
    logger.info(s"Execution Start")
    val jobType = args(args.length - 1)
    jobType match {
      case "1" => {
        logger.info(s"Arguments selected $jobType for TimeIntervalLogs Class")
        TimeIntervalLogs.Start(args)
      }
      case "2" => {
        logger.info(s"Arguments selected $jobType for MostErrorInTime Class")
        MostErrorInTime.Start(args)
      }
      case "3" => {
        logger.info(s"Arguments selected $jobType for DistributionPattern Class")
        DistributionPattern.Start(args)
      }
      case "4" => {
        logger.info(s"Arguments selected $jobType for LargestMessage Class")
        LargestMessage.Start(args)
      }
      case _ => {
        logger.info(s"No argument given so executing DistributionPattern Class")
        DistributionPattern.Start(args)
      }
    }
    logger.info(s"Execution Finish")
  }
}

