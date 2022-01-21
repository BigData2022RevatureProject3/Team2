import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object randomGenerator extends App{
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
  var products = spark.read.option("header","true").csv("data/products.csv")
  generate()



  def generate():Unit ={
    var output = ArrayBuffer[String]()
    val products = Array(Array[String]("300", "Electronics"),
      Array("200", "Computers"),
      Array("150", "Food"),
      Array("250", "Entertainment"),
      Array("100", "Home") )
    products.foreach(x =>{
      output = gen(x(0).toInt, x(1), output)
    })
    val out = Random.shuffle(output.toList).toArray.foreach(println)
    generate()
  }
  def gen(m:Int, cat:String, output:ArrayBuffer[String]):ArrayBuffer[String] = {
    var quantity = 0
    val max = m
    val list = products.select("*").where(s"product_category = '$cat'").collect()
    while(quantity != max) {
      val i = Random.nextInt(list.length)
      var total = (Random.nextInt(max-quantity) + 1)
      output.append(list(i).mkString(",") + ","+total.toString)
      quantity += total

    }
    output
  }



}
