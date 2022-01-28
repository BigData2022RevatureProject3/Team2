import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.jcraft.jsch.{Channel, JSch, Session, UserInfo}

import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern

object Consumer {
  // Create And Return Initial Spark Session For Consumer
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
    val props: Properties = new Properties()
    props.put("group.id", "test")
    props.put("bootstrap.servers", "ec2-44-202-112-109.compute-1.amazonaws.com:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(props)
    val topics: Pattern = Pattern.compile("team2")
    try {
      consumer.subscribe(topics)
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMinutes(1L))
        records.records("team2").forEach(println)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }
}
