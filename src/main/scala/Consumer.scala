import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.Column

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




    val spark = getSparkSession()
    import spark.implicits._
    val props: Properties = new Properties()
    props.put("group.id", "test")
    props.put("bootstrap.servers", "[::1]:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(props)
    val topics: Pattern = Pattern.compile("team2")
    var count = 0
    try {
      consumer.subscribe(topics)
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMinutes(1L))
        records.records("team2").forEach(x =>{
          var df = getSparkSession().emptyDataFrame
          var ar = Seq(x.value().mkString)
          val ar2 = getSparkSession().sparkContext.parallelize(ar)
          val data = ar2.toDF().select("*")
          var df2 = data.select(split(col("value"), "\\|").getItem(0).as("order_id"),
            split(col("value"), "\\|").getItem(1).as("customer_id"),
            split(col("value"), "\\|").getItem(2).as("customer_name"),
            split(col("value"), "\\|").getItem(3).as("product_id"),
            split(col("value"), "\\|").getItem(4).as("product_name"),
            split(col("value"), "\\|").getItem(5).as("product_category"),
            split(col("value"), "\\|").getItem(6).as("price"),
            split(col("value"), "\\|").getItem(7).as("qty"),
            split(col("value"), "\\|").getItem(8).as("payment_type"),
            split(col("value"), "\\|").getItem(9).as("datetime"),
            split(col("value"), "\\|").getItem(10).as("country"),
            split(col("value"), "\\|").getItem(11).as("city"),
            split(col("value"), "\\|").getItem(12).as("ecommerce_website_name"),
            split(col("value"), "\\|").getItem(13).as("payment_txn_id"),
            split(col("value"), "\\|").getItem(14).as("payment_txn_success"),
            split(col("value"), "\\|").getItem(15).as("failure_reason")

            ).drop("value")
          df2.show()
          if(count == 0){
            df2.write.mode("overwrite").option("header","true").csv("hdfs://localhost:9000/user/jahinojos2/test/test.csv")
          }else{
            df2.write.mode("append").option("header","true").csv("hdfs://localhost:9000/user/jahinojos2/test/test.csv")
          }
          count += 1
          //data.show()
          //df = df.union(data)
          //df.show()


        })
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }

  }
  def consume(): Unit = {
    val spark = getSparkSession()
    import spark.implicits._
    val props: Properties = new Properties()
    props.put("group.id", "test")
    props.put("bootstrap.servers", "[::1]:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer(props)
    val topics: Pattern = Pattern.compile("team2")
    try {
      consumer.subscribe(topics)

      val df2 = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "[::1]:9092")
        .option("subscribe", "team2")
        .option("startingOffsets", "earliest") // From starting
        .load()
      df2.printSchema()
      while(true){
        val personStringDF = df2.selectExpr("CAST(value AS STRING)").writeStream
          .outputMode("append")
          .format("console")
          .start().awaitTermination(2000)

      }


      /*
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMinutes(1L))
      records.records("team2").forEach(x =>{
        var df = getSparkSession().emptyDataFrame
        var ar = x.value().split("\\|")
        val ar2 = getSparkSession().sparkContext.parallelize(ar)
        val data = ar2.toDF()
        data.show()
        //df = df.union(data)
        //df.show()


      })

       */

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
