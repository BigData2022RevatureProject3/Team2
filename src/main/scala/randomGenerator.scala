import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object randomGenerator {
  private var orderID: Long = 0
  private var spark: SparkSession = null

  //Tested for file not found
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR);
    //System.setProperty("hadoop.home.dir", "C:\\Hadoop")

    spark = SparkSession
      .builder
      .appName("Covid Analyze App")
      .config("spark.master", "local[*]")
      //.enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      val products = spark.read.option("header", "true").csv("data/products.csv")
      generate(products)
    }
    catch {
      case e: Exception => println("File not found")
    }

  }

  private def getNextOrderID() : Long = {
    orderID += 1
    orderID
  }

  def generate(p:DataFrame):Unit ={
    var output = ArrayBuffer[String]()
    val products = Array(Array[String]("300", "Electronics"),
      Array("200", "Computers"),
      Array("150", "Food"),
      Array("250", "Entertainment"),
      Array("100", "Home") )
    products.foreach(x =>{
      output = gen(x(0).toInt, x(1), output, p)
    })
    val out = Random.shuffle(output.toList).toArray.foreach(println)
    Thread.sleep(2000)
    generate(p)

  }


  /*
  **
  **  Transaction Data
  **
  */

  private var _transactionID : Long = 0

  def getNextTransactionID : Long = {
    _transactionID += 1
    _transactionID
  }

  def getTransactionSuccess : String = {
    val isSuccess : Array[String] = Array("Y","Y","Y","Y","Y","Y","Y","Y","Y","N")
    isSuccess(Random.nextInt(isSuccess.length))
  }


  // better to init _reasons with array made by file but no spark session in randomGenerator
  // if randomGenerator gets a variable for spark session then can reconfigure
  private var _reasons : Array[String] = Array("")

  def failureReasonGenerator(spark : SparkSession) : String = {
    if (_reasons.length < 2)
      _reasons = spark.read.csv("data\\failure_list.csv").collect().map(_.getString(0))
    _reasons(Random.nextInt(_reasons.length))
  }

  /*
  **
  **  End Transaction Data
  **
  */

  // Pull DataFrame For Cities And Countries
  // Return Random City (0) / Country (1)
  def pull_cities_countries(): Array[String] = {
    val locationRes = new Array[String](2)

    try {
      val df = spark.read.format("csv")
        .option("header", "true")
        .options(Map("inferSchema" -> "true", "delimiter" -> ","))
        .load("data\\Countries_Cities.csv")
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

  // eCommerce Website Data

  def eCommWebsites(): Array[String] = {
    val websiteData = new Array[String](2)

    try {
      val df = spark.read.format("csv")
        .option("header", "true")
        .options(Map("inferSchema" -> "true", "delimiter" -> ","))
        .load("data\\ecommerceWebsites.csv")
        .collect()

      val rIndex = Random.nextInt(df.length)

      websiteData(0) = df(rIndex)(0).toString
      websiteData(1) = df(rIndex)(1).toString

      return websiteData
    }
    catch {
      case e => println("File Not Found")
    }

    websiteData
  }

  // End of eCommerce Website Data

  def gen(m: Int, cat: String, output: ArrayBuffer[String], products: DataFrame): ArrayBuffer[String] = {
    var quantity = 0
    val max = m

    val list = products.select("*").where(s"product_category = '$cat'").collect()
    while (quantity != max) {
      val i = Random.nextInt(list.length)
      var total = (Random.nextInt(max - quantity) + 1)
      val local = pull_cities_countries()
      val websites = eCommWebsites()
      output.append(list(i).mkString(",") + "," + total.toString + "," + local(0) + "," + local(1) + "," + websites(0))
      quantity += total
    }

    output
  }
}
