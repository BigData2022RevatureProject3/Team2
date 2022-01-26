package com

import scala.util.Random

object badDataGenerator {
  // Swap Customer And Product Name
  def mismatched_name(modName: String): Unit = {
    println("Mismatched Name")
  }

  // False Payment Failure
  def false_failure(modFail: Array[String], errMsg: String): Array[String] = {
    val resArray = modFail

    if(modFail(modFail.length - 1) == "Y") {
      resArray(5) = ""
    }
    else {
      resArray(5) = errMsg
    }

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
}
