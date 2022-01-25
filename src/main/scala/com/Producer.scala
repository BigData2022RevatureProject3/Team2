package com

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util.Properties
import scala.util.Random

object Producer {
  // Create And Return Initial Spark Session
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

  // Pull DataFrame For Cities And Countries
  // Return Random City (0) / Country (1)
  def pull_cities_countries(): Array[String] = {
    val dbCon = getSparkSession()
    val locationRes = new Array[String](2)

    try {
      val df = dbCon.read.format("csv")
        .option("header", "true")
        .options(Map("inferSchema" -> "true", "delimiter" -> ","))
        .load("data\\Countries_Cities.csv")
        .cache()
        .collect()

      val rIndex = Random.nextInt(df.length)

      locationRes(0) = df(rIndex)(1).toString
      locationRes(1) = df(rIndex)(0).toString

      return locationRes
    }
    catch {
      case e => println("File Not Found")
    }

    locationRes
  }

  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties()

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
      /*
      val ranLoc = pull_cities_countries()
      println(ranLoc(0))
      println(ranLoc(1))
      val cityRecord = new ProducerRecord[String, String](topic, "City", ranLoc(0))
      val countryRecord = new ProducerRecord[String, String](topic, "Country", ranLoc(1))
      val metadata1 = producer.send(cityRecord)
      val metadata2 = producer.send(countryRecord)
      */
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      producer.close()
    }
  }
}
