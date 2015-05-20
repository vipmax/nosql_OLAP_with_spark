package domain

import org.joda.time.DateTime

/**
 * Created by i00 on 4/12/15.
 */

case class AggregatedData (projectId: Integer, instanceId: Integer, parameterId: Integer, time: DateTime, timePeriod: String,
                           maxValue: Double, minValue: Double, avgValue: Double, sumValue: Double)

case class RawData (instanceId: Integer, parameterId: Integer, time: DateTime, timePeriod: String, value: Double)
case class Project (projectId: Integer, name: String, instances: Set[Integer])
case class Instance (instanceId: Integer, name: String, parameters: Set[Integer])
case class Parameter (parameterId: Integer, name: String, unit: String, maxValue: Double, minValue: Double)