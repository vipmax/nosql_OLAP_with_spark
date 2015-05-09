package app

import java.util.{Calendar, GregorianCalendar}

import akka.actor.ActorSystem
import com.datastax.spark.connector._
import domain._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import scala.concurrent.duration._




object Aggregator {



  val conf = new SparkConf().setMaster("local[4]").setAppName("Aggregator").set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)

  def getProjectId(instanceId: Int) = projects.find(_.Instances.contains(instanceId)).get.ProjectId
  var projects = sc.cassandraTable[Project]("monitoring", "projects").collect()
  var instances = sc.cassandraTable[Instance]("monitoring", "instances").collect()
  var parameters = sc.cassandraTable[Parameter]("monitoring", "parameters").collect()

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Java\\spark-1.3.1-bin-hadoop2.6\\winutil")

    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    implicit val executor = actorSystem.dispatcher


    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour) minute,
      interval = 1 hour,
      runnable = new HourTask(2)
    )

    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour) minute,
      interval = 1 hour,
      runnable = new DayTask
    )

    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour) minute,
      interval = 1 hour,
      runnable = new WeekTask
    )


    scheduler.schedule(
      initialDelay = (60 - DateTime.now.getMinuteOfHour) minute,
      interval = 1 hour,
      runnable = new MonthTask
    )

    new HourTask(24).run()
    new DayTask().run()
    new WeekTask().run()
    new MonthTask().run()
    new YearTask().run()
  }

  class HourTask (minusHours: Int) extends Runnable {

    override def run() {
      println("HourTask")

            val dateTime = DateTime.now.minusHours(minusHours).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      sc.cassandraTable[RawData]("monitoring", "raw_data").
        where("time > ? and time_period = ?", dateTime.getMillis, "1m").map(cr => { println(cr);  cr }).
        groupBy(rd => (rd.InstanceId, rd.ParameterId,rd.Time.getDayOfYear, rd.Time.getHourOfDay)).map(r => {println("groupby " + r); r }).
        map(r => {
        val instanceId = r._1._1
        val parameterId = r._1._2
        val time = r._2.head.Time.withMinuteOfHour(0)
        val timePeriod = "1h"
        val projectId = getProjectId(instanceId)
        val sum = r._2.fold(0.0)((acc, cr) => acc.asInstanceOf[Double] + cr.asInstanceOf[RawData].Value).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[RawData].Value)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[RawData].Value)).asInstanceOf[Double]
        val avg = sum / 60
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("calcHourAggregates aggregatedData = " + aggregatedData)
        aggregatedData
      }).saveToCassandra("monitoring", "aggregated_data")
    }
  }

  class DayTask  extends Runnable {

    override def run() {
      println("DayTask")

      val dateTime = DateTime.now.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)

      sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").
        where("time > ? and time_period = ?", dateTime.getMillis, "1h").
        groupBy(ad => (ad.ProjectId, ad.InstanceId, ad.ParameterId, ad.Time.getDayOfWeek)).map(ad => { println("ad = " + ad); ad }).
        map(r => {
        val projectId = r._1._1
        val instanceId = r._1._2
        val parameterId = r._1._3
        val time = r._2.head.Time.withHourOfDay(0).withMinuteOfHour(0)
        val timePeriod = "1d"
        val sum = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].Sum_Value).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].Max_Value)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].Min_Value)).asInstanceOf[Double]
        val avg = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].Avg_Value).asInstanceOf[Double] / 24
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("calcDayAggregates aggregatedData = " + aggregatedData)
        aggregatedData
      }).saveToCassandra("monitoring", "aggregated_data")
    }
  }

  class WeekTask extends Runnable {

    override def run() {
      println("WeekTask")
      val dateTime = DateTime.now.withDayOfWeek(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      println("dateTime = " + dateTime)
      sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").
        where("time >= ? and time_period = ?", dateTime.getMillis, "1d").
        groupBy(ad => {
          val c = new GregorianCalendar()
          c.setTime(ad.Time.toDate)
          val weekOfMonth = c.get(Calendar.WEEK_OF_MONTH)
          (ad.ProjectId, ad.InstanceId, ad.ParameterId, weekOfMonth)
        }).map(ad => { println("ad = " + ad); ad }).
        map(r => {
        val projectId = r._1._1
        val instanceId = r._1._2
        val parameterId = r._1._3
        val time = r._2.head.Time.withDayOfWeek(1).withHourOfDay(0).withMinuteOfHour(0)
        val timePeriod = "1w"
        val sum = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].Sum_Value).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].Max_Value)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].Min_Value)).asInstanceOf[Double]
        val avg = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].Avg_Value).asInstanceOf[Double] / 7
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("calcWeekAggregates aggregatedData = " + aggregatedData)
        aggregatedData
      })
        .saveToCassandra("monitoring", "aggregated_data")
    }
  }

  //use DayTask output data
  class MonthTask extends Runnable {

    override def run() {
      println("MonthTask")
      val dateTime = DateTime.now.withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      println("dateTime = " + dateTime)
      sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").
        where("time >= ? and time_period = ?", dateTime.getMillis, "1d").
        groupBy(ad => (ad.ProjectId, ad.InstanceId, ad.ParameterId, ad.Time.monthOfYear.get())).map(ad => { println("MonthTask ad = " + ad); ad }).
        map(r => {
        val projectId = r._1._1
        val instanceId = r._1._2
        val parameterId = r._1._3
        val time = r._2.head.Time.withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0)
        val timePeriod = "1M"
        val sum = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].Sum_Value).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].Max_Value)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].Min_Value)).asInstanceOf[Double]
        val avg = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].Avg_Value).asInstanceOf[Double] / time.dayOfMonth().getMaximumValue()
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("MonthTask aggregatedData = " + aggregatedData)
        aggregatedData
      })
        .saveToCassandra("monitoring", "aggregated_data")
    }
  }

  class YearTask extends Runnable {

    override def run() {
      println("YearTask")
      val dateTime = DateTime.now.withDayOfYear(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
      println("dateTime = " + dateTime)
      sc.cassandraTable[AggregatedData]("monitoring", "aggregated_data").
        where("time >= ? and time_period = ?", dateTime.getMillis, "1M").
        groupBy(ad => (ad.ProjectId, ad.InstanceId, ad.ParameterId, ad.Time.year().get())).map(ad => { println("MonthTask ad = " + ad); ad }).
        map(r => {
        val projectId = r._1._1
        val instanceId = r._1._2
        val parameterId = r._1._3
        val time = r._2.head.Time.withDayOfYear(1).withHourOfDay(0).withMinuteOfHour(0)
        val timePeriod = "1Y"
        val sum = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].Sum_Value).asInstanceOf[Double]
        val max = r._2.fold(Double.MinValue)((max, cr) => Math.max(max.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].Max_Value)).asInstanceOf[Double]
        val min = r._2.fold(Double.MaxValue)((min, cr) => Math.min(min.asInstanceOf[Double], cr.asInstanceOf[AggregatedData].Min_Value)).asInstanceOf[Double]
        val avg = r._2.fold(0.0)((acc, ad) => acc.asInstanceOf[Double] + ad.asInstanceOf[AggregatedData].Avg_Value).asInstanceOf[Double] / 12
        val aggregatedData = new AggregatedData(projectId, instanceId, parameterId, time, timePeriod, max, min, avg, sum)
        println("YearTask aggregatedData = " + aggregatedData)
        aggregatedData
      })
        .saveToCassandra("monitoring", "aggregated_data")
    }
  }

}
