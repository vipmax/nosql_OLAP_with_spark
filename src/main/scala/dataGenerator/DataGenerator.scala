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


  val conf = new SparkConf().setMaster("local[4]").setAppName("Generator").set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)

  var projects = sc.cassandraTable[Project]("monitoring", "projects").collect()
  var instances = sc.cassandraTable[Instance]("monitoring", "instances").collect()
  var parameters = sc.cassandraTable[Parameter]("monitoring", "parameters").collect()

  println("projects = " + projects.foreach(println))
  println("instances = " + instances.foreach(println))
  println("parameters = " + parameters.foreach(println))

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Java\\spark-1.3.1-bin-hadoop2.6\\winutil")

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
      val datas = mutable.MutableList[RawData]()

      projects.foreach(pr=>
        instances.filter(i => pr.Instances.contains(i.InstanceId)).
          foreach(i =>
            parameters.filter(p => i.parameters.contains(p.ParameterId)).
              foreach(p => {

                datas +=  new RawData(i.InstanceId, p.ParameterId,
                                                 new DateTime(new Date()).withSecondOfMinute(0).withMillisOfSecond(0),
                                                 "1m", Random.nextInt(p.Max_Value.toInt))
              })
            )
      )

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

