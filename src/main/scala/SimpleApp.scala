package com.sparkscala.dev
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object SimpleApp {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    val logFile = "C:\\apps\\opt\\spark-3.0.3-bin-hadoop2.7\\README.md" // Should be some file on your system
    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    //println(System.getenv("SPARK_CONF_DIR"))
    spark.stop()
  }
}
