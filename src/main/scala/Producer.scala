import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object Producer {
  private var orderID : Long = 0;

  private def getSparkSession() : SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR);
    val spark : SparkSession = SparkSession.builder().master("local[*]").appName("SparkProducerConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  private def getNextOrderID() : Long = {
    orderID += 1
    orderID
  }

  def main(args : Array[String]) : Unit = {
    val props:Properties = new Properties()
    props.put("bootstrap.servers", "[::1]:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")

    val producer = new KafkaProducer[String, String](props)
    val topic = "mytest"

    try {
      for (i <- 0 to 15) {
        val record = new ProducerRecord[String, String](topic, i.toString, "test " + i)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }
  }
}