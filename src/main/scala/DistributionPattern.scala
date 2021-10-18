import java.lang.Iterable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.collection.JavaConverters._

class DistributionPattern

object DistributionPattern{

  // User-defined Mapper class that extends Mapper superclass
  class DistibutionPatternMapper extends Mapper[Object, Text, Text, IntWritable] {

    // Key - state will be text and Value - count will be IntWritable
    val logType = new Text()
    val caseCount = new IntWritable()

    // Override the map function
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      // Split the input line by the delimiter
      val line = value.toString().split(" ")
      // Add the logtype (line[2]) to the variable state
      logType.set(line(2))
      // Add the daily case count (line[5]) to the variable caseCount
      caseCount.set(1);
      // Write (key: Text, value: IntWritable(count)) to the context
      context.write(logType, caseCount)
    }
  }

  // User-defined Reduce class that extends Reducer superclass
  class DistibutionPatternReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    // Override the reduce function
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) : Unit = {
      // Compute sum
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      // Write (key: Text, value: IntWritable(sum)) to context
      context.write(key, new IntWritable(sum))
    }
  }

  def main(args: Array[String]) : Unit = {
    // Read the default configuration of the cluster from configuration xml files
    val configuration = new Configuration

    // Initialize the job with default configuration of the cluster
    val job = Job.getInstance(configuration, "Covid Cases")

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
    System.exit(if(job.waitForCompletion(true)) 0 else 1)
  }
}