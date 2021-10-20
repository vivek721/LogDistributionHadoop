package LogMethods

import HelperUtils.{CreateLogger, ObtainConfigReference}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import java.time.LocalTime
import java.util.regex.Pattern
import scala.collection.JavaConverters._

/**
 * This is a map/reduce model to find the distribution of log types from the logfile within a given time period and
 * having a given regex pattern
 *
 * {DEBUG, ERROR, INFO, WARN} these are the type of messages in logfile
 *
 * All the configuration are read from application.conf file.
 *
 * The code entry point is the ExecutionStart.scala.
 *
 * @author Vivek Mishra
 *
 */

class TimeIntervalLogs

object TimeIntervalLogs {
  val config: Config = ConfigFactory.load("application" + ".conf")
  val logger = CreateLogger(classOf[TimeIntervalLogs])

  /**
   * User-defined Mapper class that extends Mapper superclass
   */
  class TimeIntervalLogsMapper extends Mapper[Object, Text, Text, IntWritable] {
    val regEx = config.getString("Config.RegEx")
    val startTime = config.getString("Config.StartTime")
    val endTime = config.getString("Config.EndTime")

    // Key - state will be text and Value - count will be IntWritable
    val logType = new Text()
    val count = new IntWritable()

    // Override the map function
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split("\\s+")
      // Add the logtype (line[2]) to the variable state0
      logType.set(line(2))
      count.set(1);
      //check time
      val logTime = LocalTime.parse(line(0));
      val checkTime = (
        logTime.isAfter(LocalTime.parse(startTime))
          &&
          logTime.isBefore(LocalTime.parse(endTime))
        );

      //check pattern
      val pattern = Pattern.compile(regEx)
      val matcher = pattern.matcher(line(5))
      if (matcher.find() && checkTime) {
        // Write (key: Text, value: IntWritable(count)) to the context
        context.write(logType, count)
      }
    }
  }

  /**
   * User-defined Reduce class that extends Reducer superclass
   */
  class TimeIntervalLogsReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    // Override the reduce function
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // Compute sum
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      // Write (key: Text, value: IntWritable(sum)) to context
      context.write(key, new IntWritable(sum))
    }
  }

  /**
   * execution starts here this is the main function of this class
   *
   * @param arg(0) - inputfile
   * @param arg(1) - outputfile
   * @param arg(2) - 1 (program selector)
   *
   */
  def Start(args: Array[String]): Unit = {
    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration
    logger.info("Configuration created")

    configuration.set("mapred.textoutputformat.separator", ",");
    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "Time interval Log Distribution")
    logger.info("Job created")
    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[TimeIntervalLogsMapper])
    job.setReducerClass(classOf[TimeIntervalLogsReducer])

    // Set the Key and Value types of the output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    // Exit after completion
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}