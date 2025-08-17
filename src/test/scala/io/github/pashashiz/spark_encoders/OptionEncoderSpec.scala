package io.github.pashashiz.spark_encoders

import org.apache.spark.sql.types.DecimalType.SYSTEM_DEFAULT
import org.apache.spark.sql.types.{Decimal, DecimalType}

import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInt}
import java.sql.{Date, Timestamp}
import java.time._
import java.util.UUID
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

class OptionEncoderSpec extends SparkAnyWordSpec() with TypedEncoderMatchers with TypedEncoderImplicits {

  "OptionEncoder" when {

    "used with primitive types" should {

      "support String wrapped in Option" in {
        Option("Hey!") should haveTypedEncoder[Option[String]]()
        Option("") should haveTypedEncoder[Option[String]]()
        Option(Random.alphanumeric.take(10000).mkString("")) should haveTypedEncoder[Option[String]]()
        Option.empty[String] should haveTypedEncoder[Option[String]]()
      }

      "support Boolean wrapped in Option" in {
        Option(true) should haveTypedEncoder[Option[Boolean]]()
        Option(false) should haveTypedEncoder[Option[Boolean]]()
        Option.empty[Boolean] should haveTypedEncoder[Option[Boolean]]()
      }

      "support Byte wrapped in Option" in {
        Option(Byte.MinValue) should haveTypedEncoder[Option[Byte]]()
        Option((-35).toByte) should haveTypedEncoder[Option[Byte]]()
        Option(0.toByte) should haveTypedEncoder[Option[Byte]]()
        Option(35.toByte) should haveTypedEncoder[Option[Byte]]()
        Option(Byte.MaxValue) should haveTypedEncoder[Option[Byte]]()
        Option.empty[Byte] should haveTypedEncoder[Option[Byte]]()
      }

      "support Short wrapped in Option" in {
        Option(Short.MinValue) should haveTypedEncoder[Option[Short]]()
        Option((-543).toShort) should haveTypedEncoder[Option[Short]]()
        Option(0.toShort) should haveTypedEncoder[Option[Short]]()
        Option(543.toShort) should haveTypedEncoder[Option[Short]]()
        Option(Short.MaxValue) should haveTypedEncoder[Option[Short]]()
        Option.empty[Short] should haveTypedEncoder[Option[Short]]()
      }

      "support Int wrapped in Option" in {
        Option(Int.MinValue) should haveTypedEncoder[Option[Int]]()
        Option(-13656545) should haveTypedEncoder[Option[Int]]()
        Option(0) should haveTypedEncoder[Option[Int]]()
        Option(13656545) should haveTypedEncoder[Option[Int]]()
        Option(Int.MaxValue) should haveTypedEncoder[Option[Int]]()
        Option.empty[Int] should haveTypedEncoder[Option[Int]]()
      }

      "support Long wrapped in Option" in {
        Option(Long.MinValue) should haveTypedEncoder[Option[Long]]()
        Option(-1343095043994444L) should haveTypedEncoder[Option[Long]]()
        Option(0L) should haveTypedEncoder[Option[Long]]()
        Option(1343095043994444L) should haveTypedEncoder[Option[Long]]()
        Option(Long.MaxValue) should haveTypedEncoder[Option[Long]]()
        Option.empty[Long] should haveTypedEncoder[Option[Long]]()
      }

      "support Float wrapped in Option" in {
        Option(Float.MinValue) should haveTypedEncoder[Option[Float]]()
        Option(-34.643f) should haveTypedEncoder[Option[Float]]()
        Option(-0.0f) should haveTypedEncoder[Option[Float]]()
        Option(34.643f) should haveTypedEncoder[Option[Float]]()
        Option(Float.MaxValue) should haveTypedEncoder[Option[Float]]()
        Option.empty[Float] should haveTypedEncoder[Option[Float]]()
      }

      "support Double wrapped in Option" in {
        Option(Double.MinValue) should haveTypedEncoder[Option[Double]]()
        Option(-34.643) should haveTypedEncoder[Option[Double]]()
        Option(0.0) should haveTypedEncoder[Option[Double]]()
        Option(34.643) should haveTypedEncoder[Option[Double]]()
        Option(Double.MaxValue) should haveTypedEncoder[Option[Double]]()
        Option.empty[Double] should haveTypedEncoder[Option[Double]]()
      }

      "support BigDecimal wrapped in Option" in {
        Option(BigDecimal("1943784783.989793879489340000")) should haveTypedEncoder[Option[BigDecimal]]()

        // this is to make sure we test with a sufficiently big value
        // note that we can't go over the max Decimal precision of Spark (38),
        // unless we specify the schema explicitly
        val tooBigDecimal = BigDecimal(Long.MaxValue) * 3
        noException should be thrownBy Decimal(tooBigDecimal, SYSTEM_DEFAULT.precision, SYSTEM_DEFAULT.scale)
        BigDecimal(tooBigDecimal.longValue) should not equal tooBigDecimal
        Option(tooBigDecimal) should haveTypedEncoder[Option[BigDecimal]]()

        Option.empty[BigDecimal] should haveTypedEncoder[Option[BigDecimal]]()
      }

      "support Java BigDecimal wrapped in Option" in {
        Option(new JBigDecimal("1943784783.989793879489340000")) should haveTypedEncoder[Option[JBigDecimal]]()

        // this is to make sure we test with a sufficiently big value
        // note that we can't go over the maximum precision of DecimalType (38),
        // unless we specify the schema explicitly
        val tooBigDecimal = new JBigDecimal(Long.MaxValue)
          .setScale(DecimalType.DEFAULT_SCALE)
          .multiply(new JBigDecimal(3))
        noException should be thrownBy Decimal(tooBigDecimal, SYSTEM_DEFAULT.precision, SYSTEM_DEFAULT.scale)
        new JBigDecimal(tooBigDecimal.longValue) should not equal tooBigDecimal
        Option(tooBigDecimal) should haveTypedEncoder[Option[JBigDecimal]]()

        Option.empty[JBigDecimal] should haveTypedEncoder[Option[JBigDecimal]]()
      }

      "support BigInt wrapped in Option" in {
        Option(BigInt("1943784783")) should haveTypedEncoder[Option[BigInt]]()
        Option(BigInt("19437847830000000000000000000000")) should haveTypedEncoder[Option[BigInt]]()
        Option.empty[BigInt] should haveTypedEncoder[Option[BigInt]]()
      }

      "support Java BigInteger wrapped in Option" in {
        Option(new JBigInt("1943784783")) should haveTypedEncoder[Option[JBigInt]]()
        Option(new JBigInt("19437847830000000000000000000000")) should haveTypedEncoder[Option[JBigInt]]()
        Option.empty[JBigInt] should haveTypedEncoder[Option[JBigInt]]()
      }

      "support UUID wrapped in Option" in {
        Option(UUID.fromString("e5ee0a5d-75f5-42de-adbf-c9f19df26475")) should haveTypedEncoder[Option[UUID]]()
        Option.empty[UUID] should haveTypedEncoder[Option[UUID]]()
      }
    }

    "used with time types" should {

      "support Timestamp wrapped in Option" in {
        Option(Timestamp.valueOf("2025-04-02 19:09:42.657")) should haveTypedEncoder[Option[Timestamp]]()
        Option.empty[Timestamp] should haveTypedEncoder[Option[Timestamp]]()
      }

      "support Instant wrapped in Option" in {
        Option(Instant.parse("2025-04-02T19:09:42.657Z")) should haveTypedEncoder[Option[Instant]]()
        Option.empty[Instant] should haveTypedEncoder[Option[Instant]]()
      }

      "support LocalDateTime wrapped in Option" in {
        Option(LocalDateTime.parse("2025-04-02T19:09:42.657")) should haveTypedEncoder[Option[LocalDateTime]]()
        Option.empty[LocalDateTime] should haveTypedEncoder[Option[LocalDateTime]]()
      }

      "support Date wrapped in Option" in {
        Option(Date.valueOf("2025-04-02")) should haveTypedEncoder[Option[Date]]()
        Option.empty[Date] should haveTypedEncoder[Option[Date]]()
      }

      "support LocalDate wrapped in Option" in {
        Option(LocalDate.parse("2025-04-02")) should haveTypedEncoder[Option[LocalDate]]()
        Option.empty[LocalDate] should haveTypedEncoder[Option[LocalDate]]()
      }

      "support OffsetDateTime wrapped in Option" in {
        Option(OffsetDateTime.parse("2025-04-02T22:08:01.855+03:00")) should haveTypedEncoder[Option[OffsetDateTime]]()
        Option.empty[OffsetDateTime] should haveTypedEncoder[Option[OffsetDateTime]]()
      }

      "support ZonedDateTime wrapped in Option" in {
        Option(ZonedDateTime.parse("2025-04-02T22:19:30.498+03:00[Europe/Kiev]")) should haveTypedEncoder[Option[ZonedDateTime]]()
        Option.empty[ZonedDateTime] should haveTypedEncoder[Option[ZonedDateTime]]()
      }

      "support Java Duration wrapped in Option" in {
        Option(Duration.ofHours(15)) should haveTypedEncoder[Option[Duration]]()
        Option.empty[Duration] should haveTypedEncoder[Option[Duration]]()
      }

      "support FiniteDuration wrapped in Option" in {
        Option(15.hours) should haveTypedEncoder[Option[FiniteDuration]]()
        Option.empty[FiniteDuration] should haveTypedEncoder[Option[FiniteDuration]]()
      }

      "support Period wrapped in Option" in {
        Option(Period.ofMonths(5)) should haveTypedEncoder[Option[Period]]()
        Option.empty[Period] should haveTypedEncoder[Option[Period]]()
      }
    }
  }
}
