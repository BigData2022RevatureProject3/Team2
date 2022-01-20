import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern

object Consumer {

  private def getSparkSession() : SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR);
    val spark : SparkSession = SparkSession.builder().master("local[*]").appName("SparkProducerConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def main(args : Array[String]) : Unit = {
    val props:Properties = new Properties()
    props.put("group.id", "test")
    props.put("bootstrap.servers","[::1]:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer : KafkaConsumer[String, String] = new KafkaConsumer(props)
    val topics : Pattern = Pattern.compile("mytest")
    try {
      consumer.subscribe(topics)
      while (true) {
        val records : ConsumerRecords[String, String] = consumer.poll(Duration.ofMinutes(1L))
        records.records("mytest").forEach(println)
        print("hello")
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      consumer.close()
    }

    /*val df : DataFrame = getSparkSession()
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "[::1]:9092")
      .option("subscribe", "mytest")
      .option("startingOffsets", "earliest")
      .load()
    val topicStringDF : DataFrame = df.selectExpr("CAST(value AS STRING)")
    val schema : StructType = new StructType().add("text", StringType)
    val topicDF : DataFrame = topicStringDF.select(from_json(col("value"), schema).as("data")).select("data.*")

    topicDF.writeStream.format("console").outputMode("append").start().awaitTermination()*/
  }
}