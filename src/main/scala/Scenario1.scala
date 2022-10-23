package com.sparkscala.dev
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Scenario1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Graph Scenario-1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val data = Seq(
      ("A","E","PROXY"),
      ("B","E","AGGR"),
      ("C","E","PROXY"),
      ("D","E","AGGR"),
      ("P","E","PROXY"),
      ("B","F","AGGR"),
      ("D","F","AGGR"),
      ("A","G","PROXY"),
      ("C","G","PROXY")
    ).toDF("Child","Parent","Parent_Type")

    val calculation = data.groupBy("Parent","Parent_Type").agg(count("Parent_Type").as("Parent_Type_Count"))
                           .groupBy("Parent").agg(count("Parent_Type").as("Parent_Count"))
    val final_data = data.join(calculation,"Parent").filter(!($"Parent_Count">1 && $"Parent_Type"==="AGGR")).drop("Parent_Count")
    final_data.show(false)
  }

}
