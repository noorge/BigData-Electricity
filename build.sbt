ThisBuild / scalaVersion := "2.12.18"

name := "BigData-Electricity"
version := "0.1.0"

// Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.8",
  "org.apache.spark" %% "spark-sql" % "3.5.8"

)
