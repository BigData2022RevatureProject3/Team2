import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.concurrent.{Future, blocking}
import scala.concurrent.forkjoin._
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

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
    props.put("bootstrap.servers", "[::1]:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    val producer = new KafkaProducer[String, String](props)
    val topic = "testteam2"
    var count = 0
    for(i <- 0 until 100){
      val batch = randomGenerator.generate_2(i*1000 + 1)
      val f: Future[String] = Future {
        print(s"batch $i started")
        try{
          batch.foreach(x =>{
            val record = new ProducerRecord[String, String](topic, x.substring(0,8).toInt.toString, x)
            val metadata = producer.send(record)
            printf(s"sent record(key=%s value=%s) " +
              "meta(partition=%d, offset=%d)\n",
              record.key(), record.value(),
              metadata.get().partition(),
              metadata.get().offset())
            count += 1
          })
          s"Batch $i complete"
        }catch {
          case e: Exception => {e.printStackTrace()
            "error"}
        }

      }
      f onComplete {
        case Success(batch) => println(batch)
        case Failure(t) => println("An error has occurred: " + t.getMessage)
      }
    }
    producer.close()

  }
}