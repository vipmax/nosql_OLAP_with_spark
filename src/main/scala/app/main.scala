package app
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime


/**
  * Created by max on 10.03.2015.
  */
object main {
   def main(args: Array[String]) {
     val conf = new SparkConf().setMaster("local[4]").setAppName("tests").set("spark.cassandra.connection.host", "127.0.0.1")
     val sc = new SparkContext(conf)

     sc.cassandraTable("monitoring", "rawdata").
       select("instanceid", "parameterid", "time", "timeperiod", "value").
       groupBy(row => (row.get[Int]("instanceid"), row.get[Int]("parameterid"), row.get[DateTime]("time").getHourOfDay)).
       map(r => (r._1, r._2.fold(0.0)((acc, cr) => acc.asInstanceOf[Double] + cr.asInstanceOf[CassandraRow].get[Double]("value")))).
       foreach(r => println("end " + r))
   }
 }
