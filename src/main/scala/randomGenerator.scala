import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.FileNotFoundException
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object randomGenerator {
  //Tested for file not found
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark : SparkSession = SparkSession
      .builder
      .appName("Covid Analyze App")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try{
      val products = spark.read.option("header","true").csv("data/products.csv")
      val names = spark.read.option("header","true").csv("data/customer_names.csv")
      val websites = spark.read.option("header","true").csv("data/ecommerce_websites.csv")
      val failures = spark.read.option("header","true").csv("data/failure_list.csv")
      val locations = spark.read.option("header","true").csv("data/Countries_Cities.csv")
      generate(products,failures,locations,websites,names)
    }
    catch {
      case e:Exception => println("File not found")
    }
  }

  def generate(p:DataFrame,f:DataFrame,l:DataFrame,w:DataFrame,n:DataFrame):Unit ={
    var counter = 0
    var output = ArrayBuffer[String]()
    val products = Array(Array[String]("300", "Electronics"),
      Array("200", "Computers"),
      Array("150", "Food"),
      Array("250", "Entertainment"),
      Array("100", "Home") )

    products.foreach(x =>{
      counter += 1
      output = gen(x(0).toInt, x(1), output, p,f,l,w,n)
    })

    val out = Random.shuffle(output.toList).toArray//.foreach(println)

    if(counter >= 5) {
      counter = 0
      val ranIndex = Random.nextInt(out.length)
      val modStr = bad_data(out(ranIndex))
      out(ranIndex) = modStr
    }

    out.foreach(println)
    Thread.sleep(2000)
    generate(p,f,l,w,n)
  }

  def gen(m:Int, cat:String, output:ArrayBuffer[String], products:DataFrame,failures:DataFrame,locations:DataFrame,website:DataFrame,names:DataFrame):ArrayBuffer[String] = {
    var quantity = 0
    val max = m

    val list = products.select("*").where(s"product_category = '$cat'").collect()

    while(quantity != max) {
      //counter += 1
      val i = Random.nextInt(list.length)
      var total = (Random.nextInt(max-quantity) + 1)
      val local = pull_cities_countries(locations)

      output.append(list(i).mkString(",") + "," +
        total.toString + "," +
        failureReasonGenerator(failures) + "," +
        local(0) + "," + local(1) + "," +
        getNextTransactionID + paymentTypeGenerator + "," + getTransactionSuccess)

      quantity += total
    }
    output
  }

  // Routing For Bad Data
  def bad_data(modArr: String): String = {
    val ranOpt = Random.nextInt(4)
    val newArr = modArr.split(",")

    val moddedArr = ranOpt match {
      case 0 => mismatched_name(newArr)
      case 1 => false_failure(newArr, "False Failure")
      case 2 => negative_price(newArr)
      case 3 => random_null(newArr, Random.nextInt(newArr.length))
    }

    return moddedArr.mkString(",")
  }

  // Swap Customer And Product Name
  def mismatched_name(modName: Array[String]): Array[String] = {
    println("Mismatched Name")
    return modName
  }

  // False Payment Failure
  def false_failure(modFail: Array[String], errMsg: String): Array[String] = {
    val resArray = modFail

    if(modFail(modFail.length - 1) == "Y")
    {resArray(5) = ""}
    else
    {resArray(5) = errMsg}

    return resArray
  }

  // Negative Product Price
  def negative_price(modPrice: Array[String]): Array[String] = {
    val newPrice = modPrice(3).toDouble - (modPrice(3).toDouble * 2)
    modPrice(3) = newPrice.toString
    return modPrice
  }

  // Null Value In Random Field
  def random_null(modNull: Array[String], index: Int): Array[String] = {
    modNull(index) = ""
    return modNull
  }

  //Michael
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

  def failureReasonGenerator(failures:DataFrame) : String = {
    if (_reasons.length < 2) {
      _reasons = failures.collect().map(_.getString(0))
    }

    _reasons(Random.nextInt(_reasons.length))
  }

  //Tony
  def paymentTypeGenerator(): String = {
    /*
     * DO +paymentTypeGenerator() to where you want to append to string in gen() function
     * CHANGE values in if statements below to change percentage of each payment type
     */
    val i = Random.nextInt(100)
    var paymentType = " "
    if (i < 25) paymentType = ",Card"
    else if (i >= 25 && i < 50) paymentType = ",Internet Banking"
    else if (i >= 50 && i < 75) paymentType = ",UPI"
    else if (i >= 75 && i < 100) paymentType= ",Wallet"
    paymentType
  }

  //Mike
  def pull_cities_countries(locations: DataFrame): Array[String] = {
    val locationRes = new Array[String](2)

    try {
      val df = locations.collect()
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
}