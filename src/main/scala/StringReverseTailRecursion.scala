package com.sparkscala.dev

import scala.annotation.tailrec

object StringReverseTailRecursion extends App{
  def reverseString(s:String): String ={
    @tailrec
    def reverseStringHelper(str: String, r: String): String = {
      str match {
        case "" => r
        case s => reverseStringHelper(s.tail,s.head + r)
      }
    }
    reverseStringHelper(s,"")
  }
  print(reverseString("reverse"))
}
