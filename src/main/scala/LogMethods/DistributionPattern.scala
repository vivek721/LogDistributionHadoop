package LogMethods

import HelperUtils.CreateLogger
import LogMethods.TimeIntervalLogs.logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang.Iterable
import scala.collection.JavaConverters._

/**
 * This is a map/reduce model to find the distribution of log types from the logfile.
 *
 * {DEBUG, ERROR, INFO, WARN} these are the type of messages in logfile
 *
 * The code entry point is the ExecutionStart.scala.
 *
 * @author Vivek Mishra
 *
 */

class DistributionPattern

object DistributionPattern {

  val logger = CreateLogger(classOf[TimeIntervalLogs])
  /**
   * User-defined Mapper class that extends Mapper superclass
   */
  class DistibutionPatternMapper extends Mapper[Object, Text, Text, IntWritable] {

    // Key - state will be text and Value - count will be IntWritable
    val logType = new Text()
    val count = new IntWritable()

    // Override the map function
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split("\\s+")
      // Add the logtype (line[2]) to the variable state
      logType.set(line(2))
      // Add the log type count (line[1]) to the variable count
      count.set(1);
      // Write (key: Text, value: IntWritable(count)) to the context
      context.write(logType, count)
    }
  }

  /**
   * User-defined Reduce class that extends Reducer superclass
   */
  class DistibutionPatternReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

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
   */
  def Start(args: Array[String]): Unit = {
    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration
    logger.info("Configuration created")

    configuration.set("mapred.textoutputformat.separator", ",");
    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "Log Distribution")
    logger.info("Job created")

    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[DistibutionPatternMapper])
    job.setReducerClass(classOf[DistibutionPatternReducer])

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