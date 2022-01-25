package scala

import org.scalatest._
import com.Producer._

class ProducerTest extends flatspec.AnyFlatSpec with matchers.must.Matchers {
  "getSparkSession()" should "Return a SparkSession" in {
    assert(getSparkSession() != null)
  }
}