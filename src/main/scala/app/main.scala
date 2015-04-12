package app
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object main {

  val projectsInstancesMap = Map(1 -> List(1,2))

  def getProjectId(instanceId: Int) = projectsInstancesMap.filter(_._2.contains(instanceId)).head._1


  def main(args: Array[String]) {

     val conf = new SparkConf().setMaster("local[4]").setAppName("tests").set("spark.cassandra.connection.host", "127.0.0.1")
     val sc = new SparkContext(conf)

     sc.cassandraTable("monitoring", "raw_data").
       select("instance_id", "parameter_id","time", "time_period", "value").
       where("time_period = ?", "1m").map(cr => {println(cr); cr}).
       groupBy(row => (row.get[Int]("instance_id"),row.get[Int]("parameter_id"), row.get[DateTime]("time").getHourOfDay)).map(r=> { println("groupby "+r); r}).
       map(r => {
         val instanceId = r._1._1
         val parameterId = r._1._2
         val time = r._2.head.get[DateTime]("time").withMinuteOfHour(0)
         val timePeriod = "1h"
         val projectId = getProjectId(instanceId)
         val sum = r._2.fold(0.0)((acc, cr) => acc.asInstanceOf[Double] + cr.asInstanceOf[CassandraRow].get[Double]("value"))
         val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[CassandraRow].get[Double]("value")))
         val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[CassandraRow].get[Double]("value")))
         val avg = (sum.asInstanceOf[Double])/(r._2.size)
       (projectId,instanceId,parameterId,time,timePeriod, max, min, sum, avg)
       }).
       foreach(r => println("end " + r))
   }
 }
