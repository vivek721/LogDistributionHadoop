package LogMethods

import com.typesafe.config.{Config, ConfigFactory}

import java.time.LocalTime
import java.util.regex.Pattern


object Test {
  val config: Config = ConfigFactory.load("application" + ".conf")
  def Start(): Unit ={
    print(config.getString("Config.RegEx"))
    val startTime = config.getString("Config.StartTime")
    val endTime = config.getString("Config.EndTime")

    val target = LocalTime.parse( "17:07:15.035") ;
    val targetInZone = (
      target.isAfter( LocalTime.parse( startTime ) )
        &&
        target.isBefore( LocalTime.parse( endTime) )
      );
    println(targetInZone)

    val RegEx = config.getString("Config.RegEx");
//    val RegEx = ""
    val pattern = Pattern.compile(RegEx);
    val s = "G\"jo1B5!cjm\\%.4;Eanl-OEpYqm~)Z8rO7qY5icf3I9mcg2R5m##U^w'm%)LJ@(2:]n<%^V+k@\"58ks"
    val matcher = pattern.matcher(s);
    if (matcher.find()) {
      println(true)
    }



//    val line = s.split("\\s+")
    println(s.length)
  }
}
