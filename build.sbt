name := "SparkScala"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.sparkscala.dev")

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "3.0.3",
//  "org.apache.spark" %% "spark-sql" % "3.0.3"
//)
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.3"% "compile"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.3" % "compile"
libraryDependencies += "org.apache.spark" %%"spark-graphx" % "3.0.3" % "compile"