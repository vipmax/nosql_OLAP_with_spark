package dataGenerator

import akka.actor.ActorSystem
import com.datastax.spark.connector._
import domain._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.util.Random

/**
 * Created by i00 on 4/12/15.
 */
object DataGenerator {
  val conf = new SparkConf().setAppName("Generator").set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)

  val projects = sc.cassandraTable[Project]("monitoring", "projects").collect()
  val instances = sc.cassandraTable[Instance]("monitoring", "instances").collect()
  val parameters = sc.cassandraTable[Parameter]("monitoring", "parameters").collect()


  def main(args: Array[String]) {
//
//    val actorSystem = ActorSystem()
//    val scheduler = actorSystem.scheduler
//    implicit val executor = actorSystem.dispatcher
//
//
//    scheduler.schedule(
//      initialDelay = 0 seconds,
//      interval = 1 hour,
//      runnable = new GeneratorTask
//    )

    new GeneratorTask().run()
  }

  def showAllData() {
    sc.cassandraTable[RawData]("monitoring", "raw_data").foreach(println)
    sc.cassandraTable[Project]("monitoring", "projects").foreach(println)
    sc.cassandraTable[Instance]("monitoring", "instances").foreach(println)
    sc.cassandraTable[Parameter]("monitoring", "parameters").foreach(println)
    sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").foreach(println)
  }



  class GeneratorTask extends Runnable{

    override def run() {

      projects.foreach(pr=>
        instances.filter(i => pr.Instances.contains(i.InstanceId)).
          foreach(i =>
          parameters.filter(p => i.parameters.contains(p.ParameterId)).
            foreach(p => {
              print("pr = " + pr)
              print(", i = " + i)
              println(", p = " + p)
              val newRawDataSeq = 0.until(60).map(m => new RawData(i.InstanceId, p.ParameterId, DateTime.now().minusHours(1).withMinuteOfHour(m).withSecondOfMinute(0).withMillisOfSecond(0), "1m", Random.nextInt(p.Max_Value.toInt))).toSeq
              sc.parallelize(newRawDataSeq).saveToCassandra("monitoring", "raw_data")
            })
          )
      )
    }
  }
}

