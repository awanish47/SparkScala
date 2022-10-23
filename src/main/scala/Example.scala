package com.sparkscala.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Example extends App{
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  val spark = SparkSession.builder.master("local[*]").appName("Example").getOrCreate()
  val productLink = "C:\\Users\\awani\\Downloads\\flatteningandnodecombination\\SSB_PRODUCT_LINKS.csv"
  val productLinkDf = spark.read.format("csv").option("header",true).load(productLink)
  productLinkDf.show(100)

}
