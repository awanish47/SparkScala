package com.sparkscala.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DropDuplicate extends App {
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  val spark = SparkSession.builder.master("local[*]").appName("Drop duplicate spark").getOrCreate()

  import spark.implicits._

  val df = Seq(
    ("Fruit","Apple",100),
    ("Fruit","Banana",80),
    ("Fruit","Orange",80),
    ("Fruit","Mango",90),
    ("Vegetable","Carrot",70),
    ("Vegetable","Potato",50),
    ("Vegetable","Brinjal",70),
    ("Vegetable","Carrot",70),
    ("Dairy","Milk",40),
    ("Dairy","Curd",40),
    ("Dairy","Cheese",50),
    ("Dairy","Milk",40),
    ("Dairy","Ghee",70)
  ).toDF("item_group","item_name","price")

  //removing duplicate rows
  df.distinct().show()

  //removing duplicates based on column
  df.dropDuplicates(("price")).show()

  //keeping first occurrence
  val window = Window.partitionBy("item_group","item_name","price").orderBy("price")
  val driver_df = df.withColumn("row_num",row_number.over(window))
  driver_df.filter(col("row_num")===1).show()

  //keeping second occurrence
  driver_df.groupBy("item_group","item_name","price").max("row_num").show()

  //df.groupBy("item_group").agg(max("price"),min("price")).show()
}
