package io.github.pashashiz.spark_encoders.testutils

import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInt}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, Period, ZonedDateTime, Duration => JDuration}
import java.util.UUID
import scala.collection.{Map => ScalaMap, Seq => ScalaSeq, Set => ScalaSet}
import scala.concurrent.duration.FiniteDuration

case class ComplexWrapper(
  // Primitive types
  booleanField: Boolean,
  byteField: Byte,
  shortField: Short,
  intField: Int,
  longField: Long,
  floatField: Float,
  doubleField: Double,

  // String
  stringField: String,

  // Big numeric types
  bigDecimalField: BigDecimal,
  javaBigDecimalField: JBigDecimal,
  bigIntField: BigInt,
  javaBigIntField: JBigInt,

  // UUID (uses invariant encoder)
  uuidField: UUID,

  // Date/Time types
  timestampField: Timestamp,
  instantField: Instant,
  localDateTimeField: LocalDateTime,
  dateField: Date,
  localDateField: LocalDate,
  offsetDateTimeField: OffsetDateTime,
  zonedDateTimeField: ZonedDateTime,

  // Duration/Period types
  javaDurationField: JDuration,
  finiteDurationField: FiniteDuration,
  periodField: Period,

  // Option types
  optionalInt: Option[Int],
  optionalString: Option[String],

  // Either types
  eitherField: Either[String, Int],

  // Collection types
  arrayField: Array[String],
  seqField: ScalaSeq[Int],
  listField: List[String],
  vectorField: Vector[Int],
  setField: ScalaSet[String],
  mapField: ScalaMap[String, Int],

  // Nested case class
  nestedField: SimpleType,

  // Optional nested
  optionalNested: Option[SimpleType],

  // Collections of nested
  listOfNested: List[SimpleType],
  mapOfNested: Map[String, SimpleType],
  
  // Complex nested combinations
  arrayOfOptions: Array[Option[String]],
  optionOfArray: Option[Array[Int]],
  optionOfList: Option[List[SimpleType]],
  optionOfMap: Option[Map[String, UUID]],
  
  // Maps with complex values
  mapOfOptions: Map[String, Option[Int]],
  mapOfArrays: Map[String, Array[Boolean]],
  mapOfLists: Map[String, List[LocalDate]],
  mapOfEithers: Map[String, Either[UUID, BigDecimal]],
  
  // Arrays/Lists with complex types
  arrayOfMaps: Array[Map[String, Int]],
  listOfMaps: List[Map[UUID, OffsetDateTime]],
  listOfEithers: List[Either[String, SimpleType]],
  arrayOfTimestamps: Array[Instant],
  
  // Deeply nested options
  optionOfOptionOfString: Option[Option[String]],
  optionOfEither: Option[Either[List[String], Map[Int, Boolean]]],
  
  // Sets with complex types
  setOfOptions: Set[Option[Int]],
  setOfUUIDs: Set[UUID],
  setOfDurations: Set[FiniteDuration],
  
  // Either with complex types
  eitherOfCollections: Either[List[String], Set[Int]],
  eitherOfMaps: Either[Map[String, Int], Map[UUID, LocalDateTime]],
  eitherOfOptions: Either[Option[String], Option[BigDecimal]],
  
  // Triple nested complexity
  mapOfListsOfOptions: Map[String, List[Option[SimpleType]]],
  optionOfMapOfArrays: Option[Map[UUID, Array[Either[String, Int]]]],
  arrayOfMapsOfLists: Array[Map[String, List[LocalDate]]],
  
  // Collections of time types with options
  listOfOptionalTimestamps: List[Option[Instant]],
  mapOfOptionalDurations: Map[String, Option[JDuration]]
)

object ComplexWrapper {
  import java.time.ZoneOffset
  import scala.concurrent.duration._
  
  val defaultValue: ComplexWrapper = ComplexWrapper(
    // Primitive types
    booleanField = true,
    byteField = 1.toByte,
    shortField = 2.toShort,
    intField = 42,
    longField = 1000L,
    floatField = 3.14f,
    doubleField = 2.718,
    
    // String
    stringField = "default string",
    
    // Big numeric types
    bigDecimalField = BigDecimal("123.45"),
    javaBigDecimalField = new JBigDecimal("678.90"),
    bigIntField = BigInt("123456789"),
    javaBigIntField = new JBigInt("987654321"),
    
    // UUID
    uuidField = UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
    
    // Date/Time types
    timestampField = Timestamp.valueOf("2023-01-01 12:00:00"),
    instantField = Instant.parse("2023-01-01T12:00:00Z"),
    localDateTimeField = LocalDateTime.of(2023, 1, 1, 12, 0),
    dateField = Date.valueOf("2023-01-01"),
    localDateField = LocalDate.of(2023, 1, 1),
    offsetDateTimeField = OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
    zonedDateTimeField = ZonedDateTime.of(2023, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC),
    
    // Duration/Period types
    javaDurationField = JDuration.ofHours(2),
    finiteDurationField = 30.seconds,
    periodField = Period.ofDays(7),
    
    // Option types
    optionalInt = Some(100),
    optionalString = Some("optional value"),
    
    // Either types
    eitherField = Right(200),
    
    // Collection types
    arrayField = Array("item1", "item2"),
    seqField = ScalaSeq(1, 2, 3),
    listField = List("a", "b", "c"),
    vectorField = Vector(10, 20, 30),
    setField = ScalaSet("x", "y", "z"),
    mapField = ScalaMap("key1" -> 1, "key2" -> 2),
    
    // Nested case class
    nestedField = SimpleType.defaultValue,
    
    // Optional nested
    optionalNested = Some(SimpleTypeB.defaultValue),
    
    // Collections of nested
    listOfNested = List(SimpleTypeA.defaultValue, SimpleTypeB.defaultValue, SimpleTypeC),
    mapOfNested = Map("nested1" -> SimpleTypeA.defaultValue, "nested2" -> SimpleTypeB.defaultValue, "nested3" -> SimpleTypeC),
    
    // Complex nested combinations
    arrayOfOptions = Array(Some("opt1"), None, Some("opt2")),
    optionOfArray = Some(Array(1, 2, 3)),
    optionOfList = Some(List(SimpleTypeB.defaultValue, SimpleTypeC)),
    optionOfMap = Some(Map("uuid1" -> UUID.randomUUID())),
    
    // Maps with complex values
    mapOfOptions = Map("key1" -> Some(1), "key2" -> None),
    mapOfArrays = Map("bools" -> Array(true, false)),
    mapOfLists = Map("dates" -> List(LocalDate.of(2023, 1, 1))),
    mapOfEithers = Map("either1" -> Left(UUID.randomUUID()), "either2" -> Right(BigDecimal("100"))),
    
    // Arrays/Lists with complex types
    arrayOfMaps = Array(Map("inner" -> 1)),
    listOfMaps = List(Map(UUID.randomUUID() -> OffsetDateTime.now())),
    listOfEithers = List(Left("left"), Right(SimpleTypeB.defaultValue)),
    arrayOfTimestamps = Array(Instant.now()),
    
    // Deeply nested options
    optionOfOptionOfString = Some(Some("deeply nested")),
    optionOfEither = Some(Left(List("a", "b"))),
    
    // Sets with complex types
    setOfOptions = Set(Some(1), None, Some(2)),
    setOfUUIDs = Set(UUID.randomUUID()),
    setOfDurations = Set(1.second, 2.minutes),
    
    // Either with complex types
    eitherOfCollections = Left(List("collection")),
    eitherOfMaps = Right(Map(UUID.randomUUID() -> LocalDateTime.now())),
    eitherOfOptions = Left(Some("either option")),
    
    // Triple nested complexity
    mapOfListsOfOptions = Map("complex" -> List(Some(SimpleTypeA.defaultValue), Some(SimpleTypeB.defaultValue), None)),
    optionOfMapOfArrays = Some(Map(UUID.randomUUID() -> Array(Right(1)))),
    arrayOfMapsOfLists = Array(Map("dates" -> List(LocalDate.now()))),
    
    // Collections of time types with options
    listOfOptionalTimestamps = List(Some(Instant.now()), None),
    mapOfOptionalDurations = Map("short" -> Some(JDuration.ofMinutes(5)), "none" -> None)
  )
}

