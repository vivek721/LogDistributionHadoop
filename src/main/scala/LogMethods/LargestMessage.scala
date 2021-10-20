package LogMethods

import HelperUtils.{CreateLogger, ObtainConfigReference}
import LogMethods.TimeIntervalLogs.logger
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
 * This is a map/reduce model to find the largest message of each log type
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

class LargestMessage

object LargestMessage {
  val config: Config = ConfigFactory.load("application" + ".conf")
  val logger = CreateLogger(classOf[LargestMessage])

  /**
   * User-defined Mapper class that extends Mapper superclass
   *
   * @output the output of the Mapper will be a key-value pair of (logType - 1)
   */
  class LargestMessageMapper extends Mapper[Object, Text, Text, IntWritable] {
    val regEx = config.getString("Config.RegEx")
    val startTime = config.getString("Config.StartTime")
    val endTime = config.getString("Config.EndTime")

    // Key - state will be text and Value - count will be IntWritable
    val logType = new Text()
    val count = new IntWritable()

    // Override the map function
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      // Split the input line by the delimiter(spaces)
      val line = value.toString().split("\\s+")
      // Add the logtype (line[2]) to the variable state0
      logType.set(line(2))
      count.set(line(5).length);

      context.write(logType, count)
    }
  }

  /**
   * User-defined Reduce class that extends Reducer superclass
   *
   * @output the output of the Reducer will be a key-value pair of (logType - length of largest message)
   */
  class LargestMessageReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    // Override the reduce function
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // Compute sum
      val maximum = values.asScala.foldLeft(0)(_ max _.get)
      // Write (key: Text, value: IntWritable(sum)) to context
      context.write(key, new IntWritable(maximum))
    }
  }

  /**
   * execution starts here this is the main function of this class
   * @param arg(0) - inputfile
   * @param arg(1) - outputfile
   * @param arg(2) - 4 (program selector)
   */
  def Start(args: Array[String]): Unit = {
    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration
    logger.info("Configuration created")

    // output text formatter
    configuration.set("mapred.textoutputformat.separator", ",");
    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "Largest log Message")
    logger.info("Job created")

    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[LargestMessageMapper])
    job.setReducerClass(classOf[LargestMessageReducer])

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