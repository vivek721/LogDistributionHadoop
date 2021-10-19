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
 * having a given regex type
 * {DEBUG, ERROR, INFO, WARN} these are the type of messages in logfile
 *
 * All parameters are specified in the simulationIaaS.conf file.
 *
 * The code entry point is the runSimulation method in Simulation.scala.
 *
 * @author Vivek Mishra
 *
 */

class LargestMessage

object LargestMessage {
  val config: Config = ConfigFactory.load("application" + ".conf")

  /**
   * User-defined Mapper class that extends Mapper superclass
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
      // Split the input line by the delimiter
      val line = value.toString().split("\\s+")
      // Add the logtype (line[2]) to the variable state0
      logType.set(line(2))
      count.set(line(5).length);

      context.write(logType, count)
    }
  }

  /**
   * User-defined Reduce class that extends Reducer superclass
   */
  class LargestMessageReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    // Override the reduce function
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // Compute sum
      val sum = values.asScala.foldLeft(0)(_ max _.get)
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
    configuration.set("mapred.textoutputformat.separator", ",");
    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "Time interval Log Distribution")

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