import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.FileNotFoundException
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object randomGenerator{
  //Tested for file not found
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

  val products: DataFrame = spark.read.option("header","true").csv("data/products.csv")
  val names: DataFrame = spark.read.option("header","true").csv("data/customer_names.csv")
  val websites: DataFrame = spark.read.option("header","true").csv("data/ecommerce_websites.csv")
  val failures: DataFrame = spark.read.option("header","true").csv("data/failure_list.csv")
  val locations: DataFrame = spark.read.option("header","true").csv("data/Countries_Cities.csv")
  val name_cities:ArrayBuffer[String] = randomize_cities()

  var count = 1

  def main(args: Array[String]): Unit = {
    generate_2()
  }

  def generate_2(): Unit ={
    var builder = List[String]()
    val products = Array(Array[String]("300", "Electronics"),
      Array("200", "Computers"),
      Array("150", "Food"),
      Array("250", "Entertainment"),
      Array("100", "Home") )

    var counter = 0

    products.foreach(x =>{
      counter += 1
      builder = builder ++ gen_2(x(0).toInt, x(1))
    })

    val batchList = builder.toArray[String]

    if(counter >= 5) {
      counter = 0
      val ranIndex = Random.nextInt(batchList.length)
      val modStr = bad_data(batchList(ranIndex))
      batchList(ranIndex) = modStr
    }

    batchList.foreach(println)
    generate_2()
  }

  def gen_2(m:Int, cat:String):List[String] ={
    var quantity = 0
    val max = m
    val output:ListBuffer[String] = ListBuffer[String]()
    val nm = names.collect()
    val list = products.select("*").where(s"product_category = '$cat'").collect()

    while(quantity != max) {
      val i = Random.nextInt(list.length)
      val ncIndex = Random.nextInt(nm.length)
      var total = (Random.nextInt(max-quantity) + 1)
      val local = pull_cities_countries(locations)
      val website = eCommWebsites(websites)
      val succeeded = getTransactionSuccess

      output += (f"$count%08d"+","+nm(ncIndex).mkString(",") + "," + list(i).mkString(",") +","+
        name_cities(ncIndex) + ","+website(0)+","+
        getNextTransactionID + paymentTypeGenerator + "," + total.toString + "," +
        succeeded + failureReasonGenerator(succeeded))

      count += 1
      quantity += total
    }

    output.toList
  }

  /*
  def generate(p:DataFrame,f:DataFrame,l:DataFrame,w:DataFrame,n:DataFrame, count:Int):Unit ={
    var output = ArrayBuffer[String]()
    var c = count
    val products = Array(Array[String]("300", "Electronics"),
      Array("200", "Computers"),
      Array("150", "Food"),
      Array("250", "Entertainment"),
      Array("100", "Home") )
    products.foreach(x =>{
      output.append(gen(x(0).toInt, x(1), output, p,f,l,w,n))
    })
    val out = Random.shuffle(output.toList).toArray.foreach(x => {
      println(f"$c%08d,"+x.mkString)
      c += 1
    })
    Thread.sleep(2000)
    generate(p,f,l,w,n,c)
  }
  def gen(m:Int, cat:String, output:ArrayBuffer[String], products:DataFrame,failures:DataFrame,locations:DataFrame,website:DataFrame,names:DataFrame):ArrayBuffer[String] = {
    var quantity = 0
    val max = m

    val list = products.select("*").where(s"product_category = '$cat'").collect()
    while(quantity != max) {
      val i = Random.nextInt(list.length)
      var total = (Random.nextInt(max-quantity) + 1)
      val local = pull_cities_countries(locations)
      output.append(list(i).mkString(",") + ","+total.toString+failureReasonGenerator(failures)+","
        +local(0) + ","+local(1)+","+getNextTransactionID
        +paymentTypeGenerator+","+getTransactionSuccess)
      quantity += total
    }
    output
  }
  */

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

  def failureReasonGenerator(success: String) : String = {
    if (_reasons.length < 2) {
      _reasons = failures.collect().map(_.getString(0))
    }

    if(success == "N")
    {return "," + _reasons(Random.nextInt(_reasons.length))}
    else
    {return ", "}
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
  // Pull Matching Pair Of City (0) / Country (1)
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

  def randomize_cities():ArrayBuffer[String] ={
    val nm = names.collect().length
    val names_cities = ArrayBuffer[String]()

    for(i <- 0 to nm-1){
      val cit = locations.collect()
      names_cities.append(cit(Random.nextInt(cit.length)).mkString(","))
    }

    names_cities
  }

  // Routing For Bad Data
  def bad_data(modArr: String): String = {
    val ranOpt = Random.nextInt(4)
    val newArr = modArr.split(",")

    val moddedArr = ranOpt match {
      case 0 => mismatched_name(newArr)
      case 1 => false_failure(newArr, failureReasonGenerator("N"))
      case 2 => negative_price(newArr)
      case 3 => random_null(newArr, Random.nextInt(newArr.length))
    }

    return moddedArr.mkString(",")
  }

  // Swap Customer And Product Name
  def mismatched_name(modName: Array[String]): Array[String] = {
    modName(5) = modName(2)
    return modName
  }

  // False Payment Failure
  def false_failure(modFail: Array[String], errMsg: String): Array[String] = {
    val resArray = modFail

    if(modFail(modFail.length - 2) == "Y")
    {resArray(modFail.length - 1) = ""}
    else
    {resArray(modFail.length -1) = "Y" + errMsg}

    return resArray
  }

  // Negative Product Price
  def negative_price(modPrice: Array[String]): Array[String] = {
    try {
      val newPrice = modPrice(7).toDouble - (modPrice(7).toDouble * 2)
      modPrice(7) = newPrice.toString
      return modPrice
    }
    catch {
      case e => return modPrice
    }
  }

  // Null Value In Random Field
  def random_null(modNull: Array[String], index: Int): Array[String] = {
    modNull(index) = null
    return modNull
  }

  // Brian
  def eCommWebsites(websites: DataFrame): Array[String] = {
    val websiteData = new Array[String](2)

    try {
      val df = websites.collect()
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
}