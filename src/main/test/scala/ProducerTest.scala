package scala

import org.scalatest._
import com.Producer._

class ProducerTest extends flatspec.AnyFlatSpec with matchers.must.Matchers {
  "getSparkSession()" should "return a SparkSession" in {
    assert(getSparkSession() != null)
  }

  "pull_cities_countries()" should "Return Array Of City and Country" in {
    val testLoc = pull_cities_countries()
    assert(testLoc.length == 2 && !testLoc.contains(null))
  }
}