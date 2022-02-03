import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object s3coms extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR);
  val spark : SparkSession = SparkSession
    .builder
    .appName("Covid Analyze App")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  var df = spark.read.option("header","true").csv("C:/Users/ghost/Downloads/data")
  df = df.withColumnRenamed("price", "payment_types")
  df = df.withColumnRenamed("payment_type", "price").withColumnRenamed("payment_types", "payment_type")
  df.show()
  df.write.mode("overwrite").option("header","true").csv("hdfs://localhost:9000/user/jahinojos2/project3/data.csv")
  /*
  val df2 = spark.read.csv("hdfs://localhost:9000/user/jahinojos2/test/test.csv").collect().length
  println(df2)


  //df.write.mode("append").csv("hdfs://localhost:9000/user/jahinojos2/test/test.csv")
  val df3 = spark.read.option("header","true").csv("hdfs://localhost:9000/user/jahinojos2/test/test.csv").show()

   */
}
