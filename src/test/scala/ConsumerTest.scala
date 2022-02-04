import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ConsumerTest extends AnyFlatSpec with should.Matchers {
  "getSparkSession" should "return an instance of SparkSession" in {
    assert(Consumer.getSparkSession != null)
  }

  // First run Producer
  "main" should "fetch and load records from a Kafka instance" in {
    Consumer.main(Array[String]())
  }
}