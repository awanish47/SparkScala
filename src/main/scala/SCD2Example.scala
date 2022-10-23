package com.sparkscala.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SCD2Example extends App{
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  val spark = SparkSession.builder.master("local[*]").appName("SCD Type-2 Example").getOrCreate()
  val old_customers = spark.read.option("header","true").csv("src/resources/old_customers.csv")
  val new_customers = spark.read.option("header","true").csv("src/resources/new_customer.csv")
  

}
