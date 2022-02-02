import org.scalatest._

import java.text.{ParseException, SimpleDateFormat}

class RandomGeneratorTest extends flatspec.AnyFlatSpec with matchers.must.Matchers{
  "generate_2" should "generate an array of strings of length 100" in {
    val test = randomGenerator.generate_2(1)
    assert(test != null)
    assert(!test.isEmpty)
    assert(test.length == 100)

    for(i <- test.indices){
      val testa = test(i).split("\\|")
      assert(testa.length == 15 || testa.length == 16)
    }
  }

  "gen_2" should "generate an array of strings of set length and category" in {
    val m = 5
    val cat = List("Computers", "Electronics", "Food", "Home", "Entertainment")
    cat.foreach(x =>{
      val test = randomGenerator.gen_2(m, x)
      assert(test != null)
      assert(test.nonEmpty)
      assert(test.length == m)
    })
  }

  "getNextTransactionID" should "generate numbers sequentially" in {
    var trIdTest: Long = 1
    while (trIdTest <= 3000) {
      val trID: Long = randomGenerator.getNextTransactionID
      assert(trID != trIdTest)
      trIdTest += 1
    }
  }

  "getTransactionSuccess" should "only output 'Y' or 'N'" in {
    for (x <- 1 to 500) {
      val success = randomGenerator.getTransactionSuccess
      assert(success != null)
      assert(success == "Y" || success == "N")
    }
  }

  "failureReasonGenerator" should "only generate a reason if success is N" in {
    val yes = randomGenerator.failureReasonGenerator("Y")
    assert(yes != null)
    assert(yes == "|")
    for (x <- 1 to 200) {
      val no = randomGenerator.failureReasonGenerator("N")
      assert(no != null)
      assert(no.length > 1)
    }
  }

  "paymentTypeGenerator()" should "return a String containing the payment type, either card, Internet banking, UPI, or wallet" in {
    var card : Boolean = false;
    var internetBanking : Boolean = false;
    var upi : Boolean = false;
    var wallet : Boolean = false;
    for (i : Int <- 0 until 100) {
      val value : String = randomGenerator.paymentTypeGenerator()
      if (value.equals("|Card"))
        card = true
      else if (value.equals("|Internet Banking"))
        internetBanking = true
      else if (value.equals("|UPI"))
        upi = true
      else if (value.equals("|Wallet"))
        wallet = true
    }
    assert(card && internetBanking && upi && wallet)
  }

  it should "return around 25 of each after 100 generations" in {
    var card : Int = 0
    var internetBanking : Int = 0
    var upi : Int = 0
    var wallet : Int = 0
    for (i : Int <- 0 until 100) {
      val value : String = randomGenerator.paymentTypeGenerator()
      if (value.equals("|Card"))
        card += 1
      else if (value.equals("|Internet Banking"))
        internetBanking += 1
      else if (value.equals("|UPI"))
        upi += 1
      else if (value.equals("|Wallet"))
        wallet += 1
    }

    println(s"Card variance from 25: ${card - 25}")
    println(s"Internet Banking variance from 25: ${internetBanking - 25}")
    println(s"UPI variance from 25: ${upi - 25}")
    println(s"Wallet variance from 25: ${wallet - 25}")
  }

  "mismatched_name(Array[String])" should "set Product Name to Customer Name" in {
    var arr : Array[String] = "00000001|80000236|Mikael Jacobson|800005|Logitech PRO X GAMING HEADSET|Electronics|129.99|1|UPI|2022-01-13T21:05:06|Canada|Edmonton|blackmesa.com|1|Y|".split("\\|")
    arr = randomGenerator.mismatched_name(arr)
    assert(arr(2).equals(arr(5)))
  }

  "randomize_cities" should "return an array of city/countries associated with a customer" in {
    val test = randomGenerator.randomize_cities()
    val namesLength = randomGenerator.names.collect().length
    assert(test.nonEmpty)
    assert(test != null)
    assert(test.length == namesLength)
    val caught = intercept[IndexOutOfBoundsException]{
      test(-1)
    }
    val caught2 = intercept[IndexOutOfBoundsException]{
      test(namesLength)
    }
    assert(caught.getMessage.indexOf("-1") != -1)
    for(i <- test.indices){
      val testa= test(i).split("\\|")
      assert(testa.length == 2)
    }
  }


  "random_null()" should "Return Null And Index" in {
    val startArray = Array[String]("800050", "Kimchi", "Food", "3.99", "68", "Payment gateway unreachable", "Frankfurt", "Germany", "68", "UPI", "N")
    println(startArray.length)
    assert(randomGenerator.random_null(startArray, 3)(3) == null)
  }

  "random_null()" must "Return Same Length Array" in {
    val startArray = Array[String]("800050", "Kimchi", "Food", "3.99", "68", null, "Frankfurt", "Germany", "68", "UPI", "N")
    assert(randomGenerator.random_null(startArray, 5).length == startArray.length)
  }

  "dateGenerator()" should "Start In Year 2022 And Month January" in {
    assert(randomGenerator.dateGenerator().startsWith("|2022-01-"))
  }

  "dateGenerator()" should "Have Valid TimeStamp" in {
    println(randomGenerator.dateGenerator().length)
    val fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    var isValid = true

    try {
      fmt.parse(randomGenerator.dateGenerator().substring(1))
    } catch {
      case e : ParseException => isValid = false
    }

    assert(isValid)
  }

  "eCommWebsites" should "make an array of website names" in {
    val categories = List("Food", "Electronics", "Computers", "Entertainment", "Home")
    categories.foreach(x => {
      val webTest = randomGenerator.eCommWebsites(x)
      assert(webTest.length == 2)
      assert(webTest != null)
      assert(webTest.nonEmpty)
    })
  }
}