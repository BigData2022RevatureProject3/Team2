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
}
