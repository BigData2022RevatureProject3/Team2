import org.scalatest.flatspec
import org.scalatest.matchers.should

class ProducerTest extends flatspec.AnyFlatSpec with should.Matchers {
  "getSparkSession" should "return a non-null instance of SparkSession" in {
    assert(Producer.getSparkSession() != null)
  }

  "main(Array[String])" should "successfully insert records into Kafka team2 topic" in {
    Producer.main(Array[String]())
  }
}