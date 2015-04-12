package domain

import org.joda.time.DateTime

/**
 * Created by i00 on 4/12/15.
 */

case class AggregatedData (ProjectId: Integer, InstanceId: Integer, ParameterId: Integer, Time: DateTime, TimePeriod: String,
                           Max_Value: Double, Min_Value: Double, Avg_Value: Double, Sum_Value: Double)

case class RawData (InstanceId: Integer, ParameterId: Integer, Time: DateTime, TimePeriod: String, Value: Double)
case class Project (ProjectId: Integer, Name: String, Instances: Set[Integer])
case class Instance (InstanceId: Integer, Name: String, parameters: Set[Integer])
case class Parameter (ParameterId: Integer, Name: String, unit: String, Max_Value: Double, Min_Value: Double)