package com.sparkscala.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object JoinRejectTest extends App {
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  val spark = SparkSession.builder.master("local[*]").appName("Join Reject Application").getOrCreate()

  import spark.implicits._
  val A = spark.sparkContext.parallelize(List(("id1", 1234),("id1", 1234),("id3", 5678))).toDF("id1", "number")
  val B = spark.sparkContext.parallelize(List(("id1", "Hello"),("id2", "world"))).toDF("id2", "text")
  val joined = udf((id: String) => id match {
    case null => "No"
    case _ => "Yes"
  })

  val C = A
    .distinct
    .join(B, 'id1 === 'id2, "left_outer")
    .withColumn("joined",joined('id2))
  C.show()
  val result=C.filter("joined=='Yes'").drop('joined)
  val reject=C.filter("joined!='Yes'").drop('joined).drop('text).drop('id2)
  result.show()
  reject.show()
}
