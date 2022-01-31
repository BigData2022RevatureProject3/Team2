import java.text.SimpleDateFormat
import scala.util.matching.{Regex, UnanchoredRegex}

object DataRegex extends App {
  //ASSUMES ALL STRINGS ARE TRIMMED OF LEADING/TRAILING SPACES
  val paymentSuccessFormat = """^Y|N|y|n$""".r
  val positiveIntegerFormat = """^\d*$""".r
  val currencyFormat = """^\d*[.]?\d{0,2}$""".r
  val paymentTypeFormat = """^card|internetbanking|upi|wallet|internet banking$""".r
  val URLFormat = """^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$""".r
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  val nullValue = """"""
  dateFormat.setLenient(false)

  def isValidOrderID(orderID: String) : Boolean = positiveIntegerFormat.pattern.matcher(orderID).matches
  def isValidCustomerID(customerID: String) : Boolean = positiveIntegerFormat.pattern.matcher(customerID).matches
  def isValidProductID(productID: String) : Boolean = positiveIntegerFormat.pattern.matcher(productID).matches
  def isValidQuantity(quantity: String) : Boolean = positiveIntegerFormat.pattern.matcher(quantity).matches
  def isValidTransactionID(transactionID:String) : Boolean =positiveIntegerFormat.pattern.matcher(transactionID).matches
  def isValidPaymentType(paymentType: String) : Boolean = paymentTypeFormat.pattern.matcher(paymentType).matches
  def isValidPaymentSuccess(paymentSuccess: String) : Boolean = paymentSuccessFormat.pattern.matcher(paymentSuccess).matches
  def isValidPrice(price: String) : Boolean = currencyFormat.pattern.matcher(price).matches

  def isValidDateTime(transactionDateTime:String) : Boolean = {
    var isValid = true
    try {
      dateFormat.parse(transactionDateTime)
    }
    catch {
      case e => isValid = false
    }
    isValid
  }
}
