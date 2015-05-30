import _root_.sbt.Keys._
import AssemblyKeys._


name := "nosql OLAP with spark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.2.1" ,
//  "org.apache.spark" % "spark-core_2.10" % "1.2.1" % "provided" ,
  "com.datastax.spark" %% "spark-cassandra-connector-java" % "1.1.1"
)

assemblySettings

// Configure JAR used with the assembly plug-in
jarName in assembly := "my-project-assembly.jar"

// A special option to exclude Scala itself form our assembly JAR, since Spark
// already bundles Scala.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

