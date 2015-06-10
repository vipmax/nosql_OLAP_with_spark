package app

import java.util.{Calendar, GregorianCalendar}
import akka.actor.ActorSystem
import com.datastax.spark.connector._
import domain._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import scala.concurrent.duration._

object Aggregator {

  val conf = new SparkConf().setAppName("Aggregator").setMaster("local[4]").set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)

  var projects = sc.cassandraTable[Project]("monitoring", "projects").collect()
  var instances = sc.cassandraTable[Instance]("monitoring", "instances").collect()
  var parameters = sc.cassandraTable[Parameter]("monitoring", "parameters").collect()

  def getProjectId(instanceId: Int) = projects.find(_.instances.contains(instanceId)).get.projectId

  def main(args: Array[String]) {
    // see http://qnalist.com/questions/4994960/run-spark-unit-test-on-windows-7
    System.setProperty("hadoop.home.dir", "C:\\Java\\spark-1.3.1-bin-hadoop2.6\\winutil")

    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    implicit val executor = actorSystem.dispatcher

    println("Waiting  " +((60 - DateTime.now.getMinuteOfHour) +" min"))
    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour) minute,
      interval = 1 hour,
      runnable = new HourTask
    )

    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour + 5) minute,
      interval = 1 hour,
      runnable = new DayTask
    )

    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour + 10) minute,
      interval = 1 hour,
      runnable = new WeekTask
    )

    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour + 15) minute,
      interval = 1 hour,
      runnable = new MonthTask
    )

    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour + 20) minute,
      interval = 1 hour,
      runnable = new YearTask
    )

    scheduler.schedule(
      initialDelay = 0 seconds,
      interval = 55 minute,
      runnable = new UpdateMetaDataTask
    )
  }

  class HourTask (minusHours: Int = 1) extends Runnable {
    override def run() {
      println("HourTask")

      val dateTime = DateTime.now.minusHours(minusHours).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)

      sc.cassandraTable[RawData]("monitoring", "raw_data").
        where("time > ? ",  dateTime.getMillis).
        groupBy(rd => (rd.instanceId, rd.parameterId, rd.timePeriod, rd.time.getDayOfYear, rd.time.getHourOfDay)).
        map(r => {
        val instanceId = r._1._1
        val parameterId = r._1._2
        val time = r._2.head.time.withMinuteOfHour(0)
        val timePeriod = "1h"
        val projectId = getProjectId(instanceId)
        val sum = r._2.fold(0.0)((acc, cr) => acc.asInstanceOf[Double] + cr.asInstanceOf[RawData].value).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[RawData].value)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[RawData].value)).asInstanceOf[Double]
        val avg = sum / 60
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("HourTask Data = " + aggregatedData)
        aggregatedData
      }).
        saveToCassandra("monitoring", "aggregated_data")
    }
  }

  class DayTask (minusDays: Int = 1) extends Runnable {
    override def run() {
      println("DayTask")

      val dateTime = DateTime.now.minusDays(minusDays).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)

      sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").
        where("time > ? ",   dateTime.getMillis).filter(_.timePeriod.equals("1h")).
        groupBy(ad => (ad.projectId, ad.instanceId, ad.parameterId, ad.time.getDayOfYear, ad.time.getDayOfWeek)).
        map(r => {
        val projectId = r._1._1
        val instanceId = r._1._2
        val parameterId = r._1._3
        val time = r._2.head.time.withHourOfDay(0).withMinuteOfHour(0)
        val timePeriod = "1d"
        val sum = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].sumValue).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].maxValue)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].minValue)).asInstanceOf[Double]
        val avg = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].avgValue).asInstanceOf[Double] / 24
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("DayTask Data = " + aggregatedData)
        aggregatedData
      }).
        saveToCassandra("monitoring", "aggregated_data")
    }
  }

  class WeekTask(minusWeeks: Int = 0) extends Runnable {
    override def run() {
      println("WeekTask")
      val dateTime = DateTime.now.minusWeeks(minusWeeks).withDayOfWeek(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      println("dateTime = " + dateTime)
      sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").
        where("time > ? ", dateTime.getMillis).filter(_.timePeriod.equals("1d")).
        groupBy(ad => {
          val c = new GregorianCalendar()
          c.setTime(ad.time.toDate)
          val weekOfMonth = c.get(Calendar.WEEK_OF_MONTH)
          (ad.projectId, ad.instanceId, ad.parameterId, weekOfMonth)
        }).map(ad => { println("ad = " + ad); ad }).
        map(r => {
        val projectId = r._1._1
        val instanceId = r._1._2
        val parameterId = r._1._3
        val time = r._2.head.time.withDayOfWeek(1).withHourOfDay(0).withMinuteOfHour(0)
        val timePeriod = "1w"
        val sum = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].sumValue).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].maxValue)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].minValue)).asInstanceOf[Double]
        val avg = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].avgValue).asInstanceOf[Double] / 7
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("calcWeekAggregates aggregatedData = " + aggregatedData)
        aggregatedData
      })
        .saveToCassandra("monitoring", "aggregated_data")
    }
  }

  class MonthTask(minusMonths: Int = 1) extends Runnable {
    override def run() {
      println("MonthTask")
      val dateTime = DateTime.now.minusMonths(minusMonths).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)

      sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").
        where("time > ? ", dateTime.getMillis).filter(_.timePeriod.equals("1d")).
        groupBy(ad => (ad.projectId, ad.instanceId, ad.parameterId, ad.time.monthOfYear.get())).
        map(r => {
        val projectId = r._1._1
        val instanceId = r._1._2
        val parameterId = r._1._3
        val time = r._2.head.time.withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0)
        val timePeriod = "1M"
        val sum = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].sumValue).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].maxValue)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].minValue)).asInstanceOf[Double]
        val avg = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].avgValue).asInstanceOf[Double] / time.dayOfMonth().getMaximumValue()
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("MonthTask Data = " + aggregatedData)
        aggregatedData
      }).
        saveToCassandra("monitoring", "aggregated_data")
    }
  }

  class YearTask(minusYears: Int = 1) extends Runnable {
    override def run() {
      println("YearTask")
      val dateTime = DateTime.now.minusYears(minusYears).withDayOfYear(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)

      sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").
        where("time > ? ", dateTime.getMillis).filter(_.timePeriod.equals("1M")).
        groupBy(ad => (ad.projectId, ad.instanceId, ad.parameterId, ad.time.year().get())).
        map(r => {
        val projectId = r._1._1
        val instanceId = r._1._2
        val parameterId = r._1._3
        val time = r._2.head.time.withDayOfYear(1).withHourOfDay(0).withMinuteOfHour(0)
        val timePeriod = "1Y"
        val sum = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].sumValue).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].maxValue)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].minValue)).asInstanceOf[Double]
        val avg = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].avgValue).asInstanceOf[Double] / 12
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("YearTask Data = " + aggregatedData)
        aggregatedData
      }).
        saveToCassandra("monitoring", "aggregated_data")
    }
  }

  class UpdateMetaDataTask extends Runnable {
    override def run() {
      projects = sc.cassandraTable[Project]("monitoring", "projects").collect()
      instances = sc.cassandraTable[Instance]("monitoring", "instances").collect()
      parameters = sc.cassandraTable[Parameter]("monitoring", "parameters").collect()

      println("projects = " + projects.mkString("[",", ","]"))
      println("instances = " + instances.mkString("[",", ","]"))
      println("parameters = " + parameters.mkString("[",", ","]"))
    }
  }

}
