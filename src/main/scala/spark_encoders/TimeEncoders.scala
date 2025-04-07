package spark_encoders

import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Multiply}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, IntervalUtils}
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, Period, ZoneId, ZoneOffset, ZonedDateTime, Duration => JDuration}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag

abstract class BaseTimeEncoder[A: ClassTag](
    override val catalystRepr: DataType,
    to: String,
    from: String)
    extends TypedEncoder[A] {

  override def toCatalyst(path: Expression): Expression =
    StaticInvoke(
      staticObject = DateTimeUtils.getClass,
      dataType = catalystRepr,
      functionName = to,
      arguments = path :: Nil,
      returnNullable = false)

  override def fromCatalyst(path: Expression): Expression =
    StaticInvoke(
      staticObject = DateTimeUtils.getClass,
      dataType = jvmRepr,
      functionName = from,
      arguments = path :: Nil,
      returnNullable = false)
}

case object TimestampEncoder
    extends BaseTimeEncoder[Timestamp](TimestampType, "fromJavaTimestamp", "toJavaTimestamp")

case object InstantEncoder
    extends BaseTimeEncoder[Instant](TimestampType, "instantToMicros", "microsToInstant")

object LocalDateTimeEncoder
    extends BaseTimeEncoder[LocalDateTime](
      TimestampNTZType,
      "localDateTimeToMicros",
      "microsToLocalDateTime")

object DateEncoder extends BaseTimeEncoder[Date](DateType, "fromJavaDate", "toJavaDate")

object LocalDateEncoder
    extends BaseTimeEncoder[LocalDate](DateType, "localDateToDays", "daysToLocalDate")

case class OffsetDateTimeStruct(time: LocalDateTime, offset: Int)

case object OffsetDateTimeInvariant extends Invariant[OffsetDateTime, OffsetDateTimeStruct] {
  override def map(in: OffsetDateTime): OffsetDateTimeStruct =
    OffsetDateTimeStruct(in.toLocalDateTime, in.getOffset.getTotalSeconds)
  override def contrMap(out: OffsetDateTimeStruct): OffsetDateTime =
    OffsetDateTime.of(out.time, ZoneOffset.ofTotalSeconds(out.offset))
}

case class ZonedDateTimeStruct(time: LocalDateTime, offset: Int, zoneId: String)

case object ZonedDateTimeInvariant extends Invariant[ZonedDateTime, ZonedDateTimeStruct] {
  override def map(in: ZonedDateTime): ZonedDateTimeStruct =
    ZonedDateTimeStruct(in.toLocalDateTime, in.getOffset.getTotalSeconds, in.getZone.getId)
  override def contrMap(out: ZonedDateTimeStruct): ZonedDateTime =
    ZonedDateTime.ofStrict(out.time, ZoneOffset.ofTotalSeconds(out.offset), ZoneId.of(out.zoneId))
}

object JDurationEncoder extends TypedEncoder[JDuration] {

  override def catalystRepr: DataType = DayTimeIntervalType.DEFAULT

  override def toCatalyst(path: Expression): Expression =
    StaticInvoke(
      staticObject = IntervalUtils.getClass,
      dataType = catalystRepr,
      functionName = "durationToMicros",
      arguments = path :: Nil,
      returnNullable = false)

  override def fromCatalyst(path: Expression): Expression =
    StaticInvoke(
      staticObject = IntervalUtils.getClass,
      dataType = jvmRepr,
      functionName = "microsToDuration",
      arguments = path :: Nil,
      returnNullable = false)
}

object FiniteDurationEncoder extends TypedEncoder[FiniteDuration] {

  override def catalystRepr: DataType = DayTimeIntervalType.DEFAULT

  override def toCatalyst(path: Expression): Expression =
    Invoke(
      targetObject = path,
      functionName = "toMicros",
      dataType = catalystRepr,
      returnNullable = false)

  override def fromCatalyst(path: Expression): Expression = {
    StaticInvoke(
      classOf[Duration],
      jvmRepr,
      "fromNanos",
      Multiply(path, Literal(1000)) :: Nil,
      returnNullable = false)
  }
}

object PeriodEncoder extends TypedEncoder[Period] {

  override def catalystRepr: DataType = YearMonthIntervalType.DEFAULT

  override def toCatalyst(path: Expression): Expression =
    StaticInvoke(
      staticObject = IntervalUtils.getClass,
      dataType = catalystRepr,
      functionName = "periodToMonths",
      arguments = path :: Nil,
      returnNullable = false)

  override def fromCatalyst(path: Expression): Expression =
    StaticInvoke(
      staticObject = IntervalUtils.getClass,
      dataType = jvmRepr,
      functionName = "monthsToPeriod",
      arguments = path :: Nil,
      returnNullable = false)
}
