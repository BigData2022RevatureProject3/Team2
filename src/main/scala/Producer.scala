import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object Producer {
  // Create And Return Initial Spark Session For Producer
  def getSparkSession(): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkProducerConsumer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def main(args: Array[String]): Unit = {
    val rand = randomGenerator
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "ec2-44-202-112-109.compute-1.amazonaws.com:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")


    for(i <- 0 until 5){
      val batch = rand.generate_2(i*100 + 1)
      val producer = new KafkaProducer[String, String](props)
      val topic = "team2"

      try {
        var count = 0
        batch.foreach(x => {
          val record = new ProducerRecord[String, String](topic, count.toString, x)
          val metadata = producer.send(record)
          printf(s"sent record(key=%s value=%s) " +
            "meta(partition=%d, offset=%d)\n",
            record.key(), record.value(),
            metadata.get().partition(),
            metadata.get().offset())
          count += 1
        })
        Thread.sleep(1000)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        producer.close()
      }
    }

  }
}