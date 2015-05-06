package app

import domain._
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._


/**
 * Created by vipmax on 27.04.2015.
 */
object SimpleOut {
  System.setProperty("hadoop.home.dir","C:\\Java\\spark-1.3.1-bin-hadoop2.6\\winutil")

  val conf = new SparkConf().setAppName("Show Data").setMaster("local[4]").set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    println("Hello. This is SimpleOut")
    showAllData()
  }

  def showAllData() {
    sc.cassandraTable[Project]("monitoring", "projects").foreach(println)
    sc.cassandraTable[Instance]("monitoring", "instances").foreach(println)
    sc.cassandraTable[Parameter]("monitoring", "parameters").foreach(println)
    sc.cassandraTable[RawData]("monitoring", "raw_data").foreach(println)
    sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").foreach(println)
  }

}
