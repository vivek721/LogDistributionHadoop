import LogMethods.{DistributionPattern, LargestMessage, MostErrorInTime, TimeIntervalLogs}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import java.sql.Driver

class ExecutionStart

object ExecutionStart {


  /** Main Method - Triggers the MapReduce method based on command line input
   *
   * @param args : Array[String] - command line input
   */

  def main(args: Array[String]): Unit = {
    val jobType = args(args.length - 1)
    jobType match {
      case "1" => TimeIntervalLogs.Start(args)
      case "2" => MostErrorInTime.Start(args)
      case "3" => DistributionPattern.Start(args)
      case "4" => LargestMessage.Start(args)
      case _ => DistributionPattern.Start(args)
    }
  }
}

