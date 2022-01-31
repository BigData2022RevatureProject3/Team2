import java.text.SimpleDateFormat
import scala.util.matching.{Regex, UnanchoredRegex}
import java.util.Locale
import org.apache.commons.validator.routines.UrlValidator

object DataRegex extends App {
  //ASSUMES ALL STRINGS ARE TRIMMED OF LEADING/TRAILING SPACES
  //FOR PAYMENT SUCCESS AND TYPE FORMAT MAKE LCASE AND REMOVE ALL SPACES BEFORE VALIDATING
  //Countries IS case sensitive so make sure Countries are Capitalized and Abbreviations are all caps
  val paymentSuccessFormat = """^y|n$""".r
  val positiveIntegerFormat = """^\d*$""".r
  val currencyFormat = """^\d*[.]?\d{0,2}$""".r
  val paymentTypeFormat = """^card|internetbanking|upi|wallet$""".r
  val nullOrEmptyFormat = """^null|$""".r
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  dateFormat.setLenient(false)
  val urlValidator = new UrlValidator()
  val countryCodeISO3 = getISO3CountryCodes
  val countryCodeISO = getISOCountryCodes
  val countryNameISO = getISOCountryNames

  def isValidOrderID(orderID: String) = positiveIntegerFormat.pattern.matcher(orderID).matches
  def isValidCustomerID(customerID: String) : Boolean = positiveIntegerFormat.pattern.matcher(customerID).matches
  def isValidProductID(productID: String) : Boolean = positiveIntegerFormat.pattern.matcher(productID).matches
  def isValidQuantity(quantity: String) : Boolean = positiveIntegerFormat.pattern.matcher(quantity).matches
  def isValidTransactionID(transactionID:String) : Boolean =positiveIntegerFormat.pattern.matcher(transactionID).matches
  def isValidPaymentType(paymentType: String) : Boolean = paymentTypeFormat.pattern.matcher(paymentType).matches
  def isValidPaymentSuccess(paymentSuccess: String) : Boolean = paymentSuccessFormat.pattern.matcher(paymentSuccess).matches
  def isValidPrice(price: String) : Boolean = currencyFormat.pattern.matcher(price).matches
  def isNullOrEmpty(failureReason: String) : Boolean = nullOrEmptyFormat.pattern.matcher(failureReason).matches
  def isValidWebsite(website: String): Boolean = urlValidator.isValid(website)
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
 def isValidCountry(country:String): Boolean = {
   var isValid = false
   if(countryCodeISO.contains(country) || countryCodeISO3.contains(country) || countryNameISO.contains(country)){
     isValid = true
   }
   isValid
 }
  def getISO3CountryCodes:Array[String] = {
    val countries = Locale.getISOCountries
    val iso = new Array[String](countries.length)
    for (i <- 0 until countries.length) {
      val locale = new Locale("", countries(i))
      iso(i) = locale.getISO3Country
    }
    iso
  }
  def getISOCountryCodes:Array[String] = {
    val countries = Locale.getISOCountries
    val iso = new Array[String](countries.length)
    for (i <- 0 until countries.length) {
      val locale = new Locale("", countries(i))
      iso(i) = locale.getCountry
    }
    iso
  }
  def getISOCountryNames: Array[String] = {
    val countries = Locale.getISOCountries
    val iso = new Array[String](countries.length)
    for (i <- 0 until countries.length) {
      val locale = new Locale("", countries(i))
      iso(i) = locale.getDisplayCountry
    }
    iso
  }
}
