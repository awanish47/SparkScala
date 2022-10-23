package com.sparkscala.dev

import org.apache.spark.sql.SparkSession

object ReadOracleTable extends App{
  val spark = SparkSession.builder().master("local").appName("Oracle Read").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  //val df = spark.read.format("jdbc").options(Map("url" -> "jdbc:oracle:thin:@localhost:1521:xe", "user" -> "hr", "password" -> "hr", "dbtable" -> "employees", "driver" -> "oracle.jdbc.driver.OracleDriver")).load()
  val empDf = spark.read.format("jdbc").option("url", "jdbc:oracle:thin:@localhost:1521:xe").option("dbtable", "employees").option("user", "hr").option("password", "hr").option("driver", "oracle.jdbc.driver.OracleDriver").load()
  empDf.show(false)

}
