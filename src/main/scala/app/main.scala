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
       select("instanceid", "parameterid","time", "timeperiod", "value").
       where("timeperiod = ?", "1m").map(cr => {println("select " + cr); cr}).
       groupBy(row => (row.get[Int]("instanceid"),row.get[Int]("parameterid"), row.get[DateTime]("time").getHourOfDay)).map(r=> { println("groupby "+r); r}).
       map(r => {
         val key = r._1  //instance id, parameter id,time hour
         val sum = r._2.fold(0.0)((acc, cr) => acc.asInstanceOf[Double] + cr.asInstanceOf[CassandraRow].get[Double]("value"))
         val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[CassandraRow].get[Double]("value")))
         val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[CassandraRow].get[Double]("value")))
         val avg = (sum.asInstanceOf[Double])/(r._2.size)
       (key, sum, max, min, avg)
       }).
       foreach(r => println("end " + r))
   }
 }
