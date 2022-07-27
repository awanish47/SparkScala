package com.sparkscala.dev

object TestProgram {
  def main(args: Array[String]): Unit = {
    val foo ="""This is
        |a multiline
        |statement""".stripMargin
    println(foo)
    val name = "Fred"
    val age = 33
    val weight =  200.00
    println(s"$name is $age years old and weights $weight pounds.")
    println(s"Age next year:${age+1}")
    println(s"You are 33 year old: ${age == 33}")
    case class Student(name:String, score:Int)
    val martin = Student("Martin",95)
    println(s"${martin.name} has a score of ${martin.score}")
    println(f"$name, You weigh $weight%.0f pounds.")
    println(raw"foo\nbar")
    println("hello world".map(c => c.toUpper))
    println("hello world".map(_.toUpper))
  }
}
