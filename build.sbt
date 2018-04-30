
name := "spark-add-columns"

organization := "LansaloLtd"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.3.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.scalatest" %% "scalatest" % "3.0.1" % Test
  )
}
