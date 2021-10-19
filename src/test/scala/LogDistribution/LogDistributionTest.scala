package LogDistribution
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LogDistributionTest extends AnyFlatSpec with Matchers {
  val config = ConfigFactory.load("application" + ".conf")

  // Test what service is encoded for application.conf
  it should "RegEx should length > 0" in {
    val regEx = config.getString("Config.RegEx")
    assert(regEx.length > 0)
  }

  it should "Starttime should have correct format HH.mm.ss" in {
    val startTime = config.getString("Config.StartTime")
    val timeStamp = startTime.split("\\.")(0)
    assert(timeStamp.length > 0)
  }


  it should "Endtime should have correct format HH.mm.ss" in {
    val startTime = config.getString("Config.EndTime")
    val timeStamp = startTime.split("\\.")(0)
    assert(timeStamp.length > 0)
  }

  it should "Starttime should have some millisecond " in {
    val startTime = config.getString("Config.StartTime")
    val timeStamp = startTime.split("\\.")(1)
    assert(timeStamp.length > 0)
  }


  it should "Endtime should have some millisecond " in {
    val startTime = config.getString("Config.EndTime")
    val timeStamp = startTime.split("\\.")(1)
    assert(timeStamp.length > 0)
  }

}
