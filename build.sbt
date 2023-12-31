name := "MagniteTakeHome"

version := "0.1"

scalaVersion := "2.13.12"
fork := true
libraryDependencies += "com.typesafe" % "config" % "1.4.1"
libraryDependencies +=  "org.apache.hadoop" % "hadoop-client" % "3.3.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.2.9",
)