import org.scalatest._
class RandomGeneratorTest extends flatspec.AnyFlatSpec with matchers.must.Matchers{
  "generate_2" should "generate an array of strings of length 100" in {
    val test = randomGenerator.generate_2()
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
      assert(!test.isEmpty)
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
}
