package com.sparkscala.dev
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit}

object Process {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Process").getOrCreate()
    val df_uci = spark.read.option("header",true).csv("C:\\Users\\awani\\IdeaProjects\\SparkScala\\src\\resources\\UCI_Hierarchy.csv")
    val df_ifs = spark.read.option("header",true).csv("C:\\Users\\awani\\IdeaProjects\\SparkScala\\src\\resources\\IFS_Hierarchy.csv")
    val df_product = spark.read.option("header",true).csv("C:\\Users\\awani\\IdeaProjects\\SparkScala\\src\\resources\\Product_Hierarchy.csv")


    val pre_final_df = df_uci.select(col("CHILD_CODE").alias("CC1"),col("UCI_PATH"),col("UCI_FLAG"))
      .crossJoin(df_ifs.select(col("CHILD_CODE").as("CC2"),col("IFS_PATH"),col("IFS_FLAG")))
      .crossJoin(df_product.select(col("CHILD_CODE").as("CC3"),col("PRODUCT_PATH"),col("PRODUCT_FLAG")))
    val final_df = pre_final_df.withColumn("NODE_COMBINATION",concat(col("CC1"),lit("|"),col("CC2"),lit("|"),col("CC3"))).select(col("NODE_COMBINATION"),col("UCI_PATH"),col("IFS_PATH"),col("PRODUCT_PATH"),col("UCI_FLAG"),col("IFS_FLAG"),col("PRODUCT_FLAG"))
    final_df.show(1000,false)
    //df_ifs.show()
    //df_product.show()
    //df_uci.show()
  }

}
