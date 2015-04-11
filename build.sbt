import _root_.sbt.Keys._

name := "nosql OLAP with spark"

version := "1.0"



scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.1",
  "org.apache.spark" % "spark-core_2.10" % "1.2.1",
  "org.apache.cassandra" % "cassandra-all" % "2.1.0"
)