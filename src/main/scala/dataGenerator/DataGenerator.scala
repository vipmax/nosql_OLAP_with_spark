package dataGenerator

import java.util.Date

import akka.actor.ActorSystem
import com.datastax.spark.connector._
import domain._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTimeZone, DateTime}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random
import org.apache.spark.SparkConf

/**
 * Created by i00 on 4/12/15.
 */
object DataGenerator {


  val conf = new SparkConf().setAppName("Generator").setMaster("local[4]").set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)

  var projects = sc.cassandraTable[Project]("monitoring", "projects").collect()
  var instances = sc.cassandraTable[Instance]("monitoring", "instances").collect()
  var parameters = sc.cassandraTable[Parameter]("monitoring", "parameters").collect()

  println("projects = " + projects)
  println("instances = " + instances)
  println("parameters = " + parameters)

  def main(args: Array[String]) {
//    System.setProperty("hadoop.home.dir","C:\\Java\\spark-1.3.1-bin-hadoop2.6\\winutil")

    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    implicit val executor = actorSystem.dispatcher


    scheduler.schedule(
      initialDelay = 0 seconds,
      interval = 1 minute,
      runnable = new GeneratorTask
    )

    scheduler.schedule(
      initialDelay = 0 seconds,
      interval = 5 minute,
      runnable = new UpdateMetaDataTask
    )
//    new GeneratorTask().run()

  }




  class GeneratorTask extends Runnable{

    override def run() {
      val datas = mutable.MutableList[RawData]()

      var from = DateTime.now.minusDays(5).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      val to = DateTime.now.minusDays(4).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      while (from.getMillis < to.getMillis) {
        projects.foreach(pr =>
          instances.filter(i => pr.instances.contains(i.instanceId)).
            foreach(i =>
            parameters.filter(p => i.parameters.contains(p.parameterId)).
              foreach(p => {

              datas += new RawData(i.instanceId, p.parameterId,
                from,
                "1m", Random.nextInt(p.maxValue.toInt))
            })
            )
        )
        from = from.plusMinutes(1)
        println("dateTime = " + from)
      }

      println("datas = " + datas)
      sc.parallelize(datas.toSeq).saveToCassandra("monitoring", "raw_data")

    }
  }
  
  class UpdateMetaDataTask extends Runnable{

    override def run() {
      projects = sc.cassandraTable[Project]("monitoring", "projects").collect()
      instances = sc.cassandraTable[Instance]("monitoring", "instances").collect()
      parameters = sc.cassandraTable[Parameter]("monitoring", "parameters").collect()
    }
  }
}

