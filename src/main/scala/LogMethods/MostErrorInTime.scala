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
 * This is a map/reduce model to find the error withing a predefined time interval
 * having a given regex type pattern
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

class MostErrorInTime

object MostErrorInTime {
  val config: Config = ConfigFactory.load("application" + ".conf")

  /**
   * User-defined Mapper class that extends Mapper superclass
   */
  class MostErrorInTimeMapper extends Mapper[Object, Text, Text, IntWritable] {
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
      val timeStamp = line(0).split("\\.")(0)

      logType.set(line(2))
      count.set(1);
      //check time
      val logTime = LocalTime.parse(line(0))

      //check pattern
      val pattern = Pattern.compile(regEx)
      val matcher = pattern.matcher(line(5))
      if (matcher.find() && line(2) == "ERROR") {
        // Write (key: Text, value: IntWritable(count)) to the context
        context.write(new Text(timeStamp), count)
      }
    }
  }

  /**
   * User-defined Reduce class that extends Reducer superclass
   */
  class MostErrorInTimeReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    // Override the reduce function
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // Compute sum
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      // Write (key: Text, value: IntWritable(sum)) to context
      context.write(key, new IntWritable(sum))
    }
  }


  /**
   * User-defined Mapper class that extends Mapper superclass used for sorting the output from
   * MostErrorInTimeReducer
   */
  class SortingMapper extends Mapper[Object, Text, IntWritable, Text] {

    // Override the map function
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split("\t")
      context.write(new IntWritable(line(1).toInt * -1), new Text(line(0)))
    }
  }

  /**
   * User-defined Reduce class that extends Reducer superclass
   */
  class SortingReducer extends Reducer[IntWritable, Text, Text, IntWritable] {
    // Override the reduce function
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      // Write (key: Text, value: IntWritable(sum)) to context
      values.asScala.foreach(value => {
        context.write(value , new IntWritable(key.get() * -1))
      })
    }
  }

  /**
   * execution starts here this is the main function of this class
   */
  def Start(args: Array[String]): Unit = {
    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration

    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "Time interval Log Distribution")

    // Assign the drive class to the job
    job.setJarByClass(this.getClass)

    // Assign user-defined Mapper and Reducer class
    job.setMapperClass(classOf[MostErrorInTimeMapper])
    job.setReducerClass(classOf[MostErrorInTimeReducer])

    // Set the Key and Value types of the output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.waitForCompletion(true)

    // Read the default configuration of the cluster from configuration xml files
    val configuration1 = new Configuration

    // output text formatter
    configuration1.set("mapred.textoutputformat.separator", ",");
    // Initialize the job with default configuration of the cluster
    val job1 = Job.getInstance(configuration1, "Time interval Log Distribution")

    // Assign the drive class to the job
    job1.setJarByClass(this.getClass)

    job1.setMapperClass(classOf[SortingMapper])
    job1.setReducerClass(classOf[SortingReducer])

    // Set the Key and Value types of the output
    job1.setMapOutputKeyClass(classOf[IntWritable])
    job1.setMapOutputValueClass(classOf[Text])

    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    // Add input and output path from the args
    FileInputFormat.addInputPath(job1, new Path(args(1)))
    FileOutputFormat.setOutputPath(job1, new Path(args(2)))

    // Exit after completion
    System.exit(if (job1.waitForCompletion(true)) 0 else 1)
  }
}