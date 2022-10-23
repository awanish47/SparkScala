package com.sparkscala.dev
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, format_string, lpad}
object InterviewScenario1 extends App{

  val spark = SparkSession.builder().master("local").appName("Interview Scenario-1").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val df = Seq(
    ("Aksah",20),
    ("Aman",8),
    ("Raju",75),
    ("Manoj",123),
    ("Arif",30)
  ).toDF("name","score")

  //method-1
  val df1 = df.withColumn("score_000",lpad(col("score"),3,"0"))
  df1.show()

  //method-2
  val df2 = df.withColumn("score_000",format_string("%03d",col("score")))
  df2.show()
}
