
scalaVersion := "2.11.12"
name := "scalaSpark_project"
organization := "ch.epfl.scala"
version := "1.0"

val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)


