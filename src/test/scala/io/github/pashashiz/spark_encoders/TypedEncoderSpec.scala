package io.github.pashashiz.spark_encoders

import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.Inside.inside

import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInt}
import java.sql.{Date, Timestamp}
import java.time._
import java.util.UUID
import scala.collection.{immutable, mutable}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Random, Try}

class TypedEncoderSpec extends SparkAnyWordSpec() with TypedEncoderMatchers with SampleEncoders
    with TypedEncoderImplicits {

  "TypedEncoder" when {

    "used with primitive type" should {

      "support String" in {
        "Hey!" should haveTypedEncoder[String]()
        "" should haveTypedEncoder[String]()
        Random.alphanumeric.take(10000).mkString("") should haveTypedEncoder[String]()
      }

      "support Boolean" in {
        true should haveTypedEncoder[Boolean]()
        false should haveTypedEncoder[Boolean]()
      }

      "support Byte" in {
        Byte.MinValue should haveTypedEncoder[Byte]()
        -35.toByte should haveTypedEncoder[Byte]()
        0.toByte should haveTypedEncoder[Byte]()
        35.toByte should haveTypedEncoder[Byte]()
        Byte.MaxValue should haveTypedEncoder[Byte]()
      }

      "support Short" in {
        Short.MinValue should haveTypedEncoder[Short]()
        -543.toShort should haveTypedEncoder[Short]()
        0.toShort should haveTypedEncoder[Short]()
        543.toShort should haveTypedEncoder[Short]()
        Short.MaxValue should haveTypedEncoder[Short]()
      }

      "support Int" in {
        Int.MinValue should haveTypedEncoder[Int]()
        -13656545 should haveTypedEncoder[Int]()
        0 should haveTypedEncoder[Int]()
        13656545 should haveTypedEncoder[Int]()
        Int.MaxValue should haveTypedEncoder[Int]()
      }

      "support Long" in {
        Long.MinValue should haveTypedEncoder[Long]()
        -1343095043994444L should haveTypedEncoder[Long]()
        0L should haveTypedEncoder[Long]()
        1343095043994444L should haveTypedEncoder[Long]()
        Long.MaxValue should haveTypedEncoder[Long]()
      }

      "support Float" in {
        Float.MinValue should haveTypedEncoder[Float]()
        -34.643f should haveTypedEncoder[Float]()
        -0.0f should haveTypedEncoder[Float]()
        34.643f should haveTypedEncoder[Float]()
        Float.MaxValue should haveTypedEncoder[Float]()
      }

      "support Double" in {
        Double.MinValue should haveTypedEncoder[Double]()
        -34.643 should haveTypedEncoder[Double]()
        0.0 should haveTypedEncoder[Double]()
        34.643 should haveTypedEncoder[Double]()
        Double.MaxValue should haveTypedEncoder[Double]()
      }

      "support BigDecimal" in {
        BigDecimal("1943784783.989793879489340000") should haveTypedEncoder[BigDecimal]()
      }

      "support Java BigDecimal" in {
        new JBigDecimal("1943784783.989793879489340000") should haveTypedEncoder[JBigDecimal]()
      }

      "support BigInt" in {
        BigInt("1943784783") should haveTypedEncoder[BigInt]()
      }

      "support Java BigInt" in {
        new JBigInt("1943784783") should haveTypedEncoder[JBigInt]()
      }

      "support UUID" in {
        UUID.fromString("e5ee0a5d-75f5-42de-adbf-c9f19df26475") should haveTypedEncoder[UUID]()
      }
    }

    "used with time" should {

      "support Timestamp" in {
        Timestamp.valueOf("2025-04-02 19:09:42.657") should haveTypedEncoder[Timestamp]()
      }

      "support Instant" in {
        Instant.parse("2025-04-02T19:09:42.657Z") should haveTypedEncoder[Instant]()
      }

      "support LocalDateTime" in {
        LocalDateTime.parse("2025-04-02T19:09:42.657") should haveTypedEncoder[LocalDateTime]()
      }

      "support Date" in {
        Date.valueOf("2025-04-02") should haveTypedEncoder[Date]()
      }

      "support LocalDate" in {
        LocalDate.parse("2025-04-02") should haveTypedEncoder[LocalDate]()
      }

      "support OffsetDateTime" in {
        OffsetDateTime.parse("2025-04-02T22:08:01.855+03:00") should
          haveTypedEncoder[OffsetDateTime]()
      }

      "support ZonedDateTime" in {
        ZonedDateTime.parse("2025-04-02T22:19:30.498+03:00[Europe/Kiev]") should
          haveTypedEncoder[ZonedDateTime]()
      }

      "support Java Duration" in {
        Duration.ofHours(15) should haveTypedEncoder[Duration]()
      }

      "support FiniteDuration" in {
        15.hours should haveTypedEncoder[FiniteDuration]()
      }

      "support Period" in {
        Period.ofMonths(5) should haveTypedEncoder[Period]()
      }
    }

    "used with product type" should {

      "support all required fields" in {
        val schema = StructType(Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false)))
        TypedEncoder[SimpleUser].catalystRepr shouldBe schema
        SimpleUser("Pablo", 34) should haveTypedEncoder[SimpleUser]()
      }

      "support some optional fields" in {
        val schema = StructType(Seq(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = true)))
        TypedEncoder[UserOptAge].catalystRepr shouldBe schema
        UserOptAge("Pavlo", Some(34)) should haveTypedEncoder[UserOptAge]()
        UserOptAge("Pavlo", None) should haveTypedEncoder[UserOptAge]()
        UserOptName(Some("Pavlo"), 34) should haveTypedEncoder[UserOptName]()
        UserOptName(None, 34) should haveTypedEncoder[UserOptName]()
      }

      "support all optional fields" in {
        val schema = StructType(Seq(
          StructField("name", StringType, nullable = true),
          StructField("age", IntegerType, nullable = true)))
        TypedEncoder[UserOptBoth].catalystRepr shouldBe schema
        UserOptBoth(Some("Pavlo"), Some(34)) should haveTypedEncoder[UserOptBoth]()
        UserOptBoth(None, None) should haveTypedEncoder[UserOptBoth]()
      }

      "gracefully fail when null values used in required fields" in {
        UserOptAge("Pavlo", null) should failToSerializeWith[UserOptAge](
          "Error while encoding: java.lang.NullPointerException: Null value appeared in non-nullable field")
      }

      "support required nested product" in {
        val schema = StructType(Seq(
          StructField("name", StringType, nullable = false),
          StructField(
            "user",
            StructType(Seq(
              StructField("name", StringType, nullable = false),
              StructField("age", IntegerType, nullable = false))),
            nullable = false)))
        TypedEncoder[SimpleTask].catalystRepr shouldBe schema
        SimpleTask("t1", SimpleUser("Pablo", 34)) should haveTypedEncoder[SimpleTask]()
      }

      "support optional nested product" in {
        val schema = StructType(Seq(
          StructField("name", StringType, nullable = false),
          StructField(
            "user",
            StructType(Seq(
              StructField("name", StringType, nullable = false),
              StructField("age", IntegerType, nullable = false))),
            nullable = true)))
        TypedEncoder[SimpleTaskOptUser].catalystRepr shouldBe schema
        SimpleTaskOptUser("t1", Some(SimpleUser("Pablo", 34))) should
          haveTypedEncoder[SimpleTaskOptUser]()
        SimpleTaskOptUser("t1", None) should haveTypedEncoder[SimpleTaskOptUser]()
      }

      "gracefully fail when null value used as nested product" in {
        SimpleTaskOptUser("t1", null) should failToSerializeWith[SimpleTaskOptUser](
          "Error while encoding: java.lang.NullPointerException: Null value appeared in non-nullable field")
      }

      "support remapping via dataframe map function" in {
        val result = spark.createDataset(List(("Pavlo", 34)))
          .map { case (name, age) => SimpleUser(name, age + 1) }
          .collect()
          .head
        result shouldBe SimpleUser("Pavlo", 35)
      }

      "support casting to dataset with different order" in {
        val result = spark.createDataset(List(("Pavlo", 34)))
          .toDF("name", "age")
          .select((col("age") + 1).as("age"), col("name"))
          .as[SimpleUser]
          .collect()
          .head
        result shouldBe SimpleUser("Pavlo", 35)
      }

      "support empty case class" in {
        val ds = spark.createDataset(List(EmptyCaseClass()))
        ds.schema shouldBe StructType(Seq.empty)
        ds.collect().head shouldBe EmptyCaseClass()
      }

      "support case object" in {
        val ds = spark.createDataset(List(EmptyCaseObject))
        ds.schema shouldBe StructType(Seq.empty)
        ds.collect().head shouldBe EmptyCaseObject
      }
    }

    "used with ADT type" should {

      "support sub types with 1 same field and 1 different" in {
        val schema = StructType(Seq(
          StructField("_type", StringType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("points", IntegerType, nullable = true),
          StructField("priority", IntegerType, nullable = true),
          StructField("size", IntegerType, nullable = true)))
        TypedEncoder[WorkItem].catalystRepr shouldBe schema
        (WorkItem.Defect("My defect", 10): WorkItem) should haveTypedEncoder[WorkItem]()
        (WorkItem.Story("My story", 5): WorkItem) should haveTypedEncoder[WorkItem]()
        (WorkItem.Feature("My feature", 1): WorkItem) should haveTypedEncoder[WorkItem]()
      }

      "support sub types with all different fields" in {
        val schema = StructType(Seq(
          StructField("_type", StringType, nullable = false),
          StructField("description", StringType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("priority", IntegerType, nullable = true),
          StructField("size", IntegerType, nullable = true)))
        TypedEncoder[WorkItemDiffFields].catalystRepr shouldBe schema
        val sample1: WorkItemDiffFields =
          WorkItemDiffFields.Defect("My defect", 10): WorkItemDiffFields
        sample1 should haveTypedEncoder[WorkItemDiffFields]()
        val sample2: WorkItemDiffFields = WorkItemDiffFields.Feature("My feature", 1)
        sample2 should haveTypedEncoder[WorkItemDiffFields]()
      }

      "support sub types with same field but one optional and another required" in {
        val schema = StructType(Seq(
          StructField("_type", StringType, nullable = false),
          StructField("name", StringType, nullable = true),
          StructField("priority", IntegerType, nullable = true),
          StructField("size", IntegerType, nullable = true)))
        TypedEncoder[WorkItemOpt].catalystRepr shouldBe schema
        val sample1: WorkItemOpt = WorkItemOpt.Defect(Some("My defect"), 10)
        sample1 should haveTypedEncoder[WorkItemOpt]()
        val sample2: WorkItemOpt = WorkItemOpt.Feature(Some("My feature"), 1)
        sample2 should haveTypedEncoder[WorkItemOpt]()
      }

      "support nested product and ADT types" in {
        val schema = StructType(Seq(
          StructField("_type", StringType, nullable = false),
          StructField("description", StringType, nullable = false),
          StructField(
            "item",
            StructType(Seq(
              StructField("_type", StringType, nullable = false),
              StructField("name", StringType, nullable = false),
              StructField("points", IntegerType, nullable = true),
              StructField("priority", IntegerType, nullable = true),
              StructField("size", IntegerType, nullable = true))),
            nullable = false),
          StructField(
            "reviewer",
            StructType(Seq(
              StructField("name", StringType, nullable = false),
              StructField("age", IntegerType, nullable = false))),
            nullable = true)))
        TypedEncoder[PR].catalystRepr shouldBe schema
        val sample1: PR = PR.Draft(WorkItem.Story("s1", 5), "d1")
        sample1 should haveTypedEncoder[PR]()
        val sample2: PR = PR.InProgress(WorkItem.Story("s1", 5), "d1", SimpleUser("u1", 23))
        sample2 should haveTypedEncoder[PR]()
      }

      "support nested optional product and ADT types" in {
        val schema = StructType(Seq(
          StructField("_type", StringType, nullable = false),
          StructField("description", StringType, nullable = false),
          StructField(
            "item",
            StructType(Seq(
              StructField("_type", StringType, nullable = false),
              StructField("name", StringType, nullable = false),
              StructField("points", IntegerType, nullable = true),
              StructField("priority", IntegerType, nullable = true),
              StructField("size", IntegerType, nullable = true))),
            nullable = true),
          StructField(
            "reviewer",
            StructType(Seq(
              StructField("name", StringType, nullable = false),
              StructField("age", IntegerType, nullable = false))),
            nullable = true)))
        TypedEncoder[PROpt].catalystRepr shouldBe schema
        val sample1: PROpt = PROpt.Draft(Some(WorkItem.Story("s1", 5)), "d1")
        sample1 should haveTypedEncoder[PROpt]()
        val sample2: PROpt = PROpt.Draft(None, "d1")
        sample2 should haveTypedEncoder[PROpt]()
        val sample3: PROpt =
          PROpt.InProgress(Some(WorkItem.Story("s1", 5)), "d1", Some(SimpleUser("u1", 23)))
        sample3 should haveTypedEncoder[PROpt]()
        val sample4: PROpt = PROpt.InProgress(None, "d1", None)
        sample4 should haveTypedEncoder[PROpt]()
      }

      "fail with sub types that have same field of different type" in {
        // note: ideally code should not compile with this error, need to write out own macro
        the[SparkException].thrownBy(TypedEncoder[WorkItemDiffType]).getMessage shouldBe
          "[INTERNAL_ERROR] Standard ADT encoder does not support subtypes that have same field names with different types. Field 'size' has conflicting types: IntegerType, FloatType"
      }

      "support nested enums via case objects encoded as string" in {
        TypedEncoder[NestedEnum].catalystRepr shouldBe StringType
        val case1: NestedEnum = NestedEnum.Case1
        case1 should haveTypedEncoder[NestedEnum]()
        val case2: NestedEnum = NestedEnum.Case2
        case2 should haveTypedEncoder[NestedEnum]()
      }

      "support flat enums via case objects encoded as string" in {
        TypedEncoder[FlatEnum].catalystRepr shouldBe StringType
        val case1: FlatEnum = FlatEnumCase1
        case1 should haveTypedEncoder[FlatEnum]()
        val case2: FlatEnum = FlatEnumCase2
        case2 should haveTypedEncoder[FlatEnum]()
      }

      "support mixed case classes and objects encoded as classes" in {
        val schema = StructType(Seq(
          StructField("_type", StringType, nullable = false),
          StructField("name", StringType, nullable = true),
          StructField("value", IntegerType, nullable = true)))
        TypedEncoder[UserAttribute].catalystRepr shouldBe schema
        val case1: UserAttribute = UserAttribute.Name("Pavlo")
        case1 should haveTypedEncoder[UserAttribute]()
        val case2: UserAttribute = UserAttribute.Age(34)
        case2 should haveTypedEncoder[UserAttribute]()
        val case3: UserAttribute = UserAttribute.Unknown
        case3 should haveTypedEncoder[UserAttribute]()
      }

      /**
       * 1. [[Option[Instant]] gets processed by [[OptionEncoder.toCatalyst()]]
       * 2. [[org.apache.spark.sql.catalyst.expressions.objects.UnwrapOption]] extracts the [[Instant]] from the [[Option]]
       * 3. [[Primitive.unbox(unwrapped, catalystRepr)]] is called with:
       *    - unwrapped = the [[Instant]] object
       *    - catalystRepr = [[TimestampType]] (which is a primitive in Spark)
       * 4. [[Primitive.isPrimitive(TimestampType)]] returns true
       * 5. [[org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.javaType]] converts [[TimestampType]] to [[org.apache.spark.sql.catalyst.types.PhysicalLongType]] and returns "long"
       * 6. The method name becomes "longValue" 
       * 7. [[org.apache.spark.sql.catalyst.expressions.objects.Invoke(instant, "longValue", TimestampType)]] is created
       * 
       * Note: Spark internally represents timestamps as microseconds since epoch (long values)
       * via the PhysicalDataType mapping: TimestampType => PhysicalLongType
       */
      "support Instant wrapped in Option" in {
        Option(Instant.now()) should haveTypedEncoder[Option[Instant]]()
      }
    }

    "used with unsupported types" should {

      "support serializing unsupported object by converting them to supported and back" in {
        LightException("Oops!") should haveTypedEncoder[Throwable]()(lightExceptionEncoder)
      }

      "support Try via LightException" in {
        implicit val leEncoder: TypedEncoder[Throwable] = lightExceptionEncoder
        implicit val tryEncoder: TypedEncoder[Try[SimpleUser]] = derive[Try[SimpleUser]]
        Try(SimpleUser("Pavlo", 35)) should haveTypedEncoder[Try[SimpleUser]]()
        Try[SimpleUser](throw new RuntimeException("Oops!")) should haveTypedEncoder[
          Try[SimpleUser]](assertion = (_, deserialized) => {
          inside(deserialized) {
            case Failure(e) => e.getMessage shouldBe "Oops!"
          }
        })
      }

      "support kryo" in {
        implicit val errorEncoder: TypedEncoder[RuntimeException] = kryo[RuntimeException]
        ExceptionWrapper(new RuntimeException("Oops!")) should haveTypedEncoder[ExceptionWrapper](
          assertion = (_, deserialized) => {
            deserialized.value.getMessage shouldBe "Oops!"
          })
      }

      "support Either" in {
        val schema = StructType(Seq(
          StructField("_type", StringType, nullable = false),
          StructField(
            "left",
            StructType(Seq(StructField("message", StringType, nullable = false))),
            nullable = true),
          StructField(
            "right",
            StructType(Seq(
              StructField("name", StringType, nullable = false),
              StructField("age", IntegerType, nullable = false))),
            nullable = true)))
        TypedEncoder[Either[Error, SimpleUser]].catalystRepr shouldBe schema
        val sample1: Either[Error, SimpleUser] = Left(Error("Oops!"))
        sample1 should haveTypedEncoder[Either[Error, SimpleUser]]()
        val sample2: Either[Error, SimpleUser] = Right(SimpleUser("Pavlo", 35))
        sample2 should haveTypedEncoder[Either[Error, SimpleUser]]()
      }

      "support UDT" in {
        implicit val pointUdt: PointUDT = new PointUDT()
        val ds = spark.createDataset(Seq(Point(1, 2), Point(3, 4)))
        ds.collect().toList shouldBe List(Point(1, 2), Point(3, 4))
      }
    }

    "used with sequences" should {

      "support generic Seq" in {
        val schema = StructType(Seq(StructField(
          "value",
          ArrayType(
            StructType(Seq(
              StructField("name", StringType, nullable = false),
              StructField("age", IntegerType, nullable = false))),
            containsNull = false),
          nullable = false)))
        TypedEncoder[Container[collection.Seq[SimpleUser]]].catalystRepr shouldBe schema
        val sample = Container(collection.Seq(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[collection.Seq[SimpleUser]]]()
      }

      "support generic Seq with option" in {
        val schema = StructType(Seq(StructField(
          "value",
          ArrayType(
            StructType(Seq(
              StructField("name", StringType, nullable = false),
              StructField("age", IntegerType, nullable = false))),
            containsNull = true),
          nullable = false)))
        TypedEncoder[Container[collection.Seq[Option[SimpleUser]]]].catalystRepr shouldBe schema
        val sample = Container(collection.Seq(Some(SimpleUser("Pablo", 34)), None))
        sample should haveTypedEncoder[Container[collection.Seq[Option[SimpleUser]]]]()
      }

      "support generic Seq with primitive" in {
        val schema = StructType(Seq(StructField(
          "value",
          ArrayType(IntegerType, containsNull = false),
          nullable = false)))
        TypedEncoder[Container[collection.Seq[Int]]].catalystRepr shouldBe schema
        val sample = Container(collection.Seq(1, 2, 3))
        sample should haveTypedEncoder[Container[collection.Seq[Int]]]()
      }

      "support List" in {
        val sample = Container(List(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[List[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[List[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support Vector" in {
        val sample = Container(Vector(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[Vector[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[Vector[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support Stream" in {
        val sample = Container(Stream(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[Stream[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[Stream[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable Queue" in {
        val sample = Container(immutable.Queue(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[immutable.Queue[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.Queue[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable Seq" in {
        implicitly[CollectionFactory[String, immutable.Seq[String]]]
        val sample = Container(immutable.Seq(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        TypedEncoder[immutable.Seq[String]]
        sample should haveTypedEncoder[Container[immutable.Seq[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.Seq[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable ArrayBuffer" in {
        val sample = Container(mutable.ArrayBuffer(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.ArrayBuffer[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.ArrayBuffer[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable ListBuffer" in {
        val sample = Container(mutable.ListBuffer(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.ListBuffer[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.ListBuffer[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable ArraySeq" in {
        val sample = Container(mutable.ArraySeq(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.ArraySeq[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.ArraySeq[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable Queue" in {
        val sample = Container(mutable.Queue(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.Queue[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.Queue[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable ArrayStack" in {
        val sample = Container(mutable.ArrayStack(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.ArrayStack[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.ArrayStack[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable Seq" in {
        val sample = Container(mutable.Seq(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.Seq[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.Seq[_]]
            original.value shouldBe deserialized.value
          })
      }
    }

    "used with sets" should {

      "support generic Set" in {
        val sample = Container(collection.Set(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[collection.Set[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[collection.Set[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable Set" in {
        val sample = Container(immutable.Set(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[immutable.Set[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.Set[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable HashSet" in {
        val sample = Container(immutable.HashSet(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[immutable.HashSet[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.HashSet[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable ListSet" in {
        val sample = Container(immutable.ListSet(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[immutable.ListSet[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.ListSet[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable TreeSet" in {
        val sample = Container(immutable.TreeSet("Pavlo", "Bob"))
        sample should haveTypedEncoder[Container[immutable.TreeSet[String]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.TreeSet[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable Set" in {
        val sample = Container(mutable.Set(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.Set[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.Set[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable HashSet" in {
        val sample = Container(mutable.HashSet(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.HashSet[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.HashSet[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable LinkedHashSet" in {
        val sample =
          Container(mutable.LinkedHashSet(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.LinkedHashSet[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.LinkedHashSet[_]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable TreeSet" in {
        val sample = Container(mutable.TreeSet("Pavlo", "Bob"))
        sample should haveTypedEncoder[Container[mutable.TreeSet[String]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.TreeSet[_]]
            original.value shouldBe deserialized.value
          })
      }
    }

    "used with arrays" should {

      "support Array of objects" in {
        val sample = Container(Array(SimpleUser("Pablo", 34), SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[Array[SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value.asInstanceOf[Array[Object]] should beInstanceOf[Array[Object]]
            original.value shouldBe deserialized.value
          })
      }

      "support Array of primitives" in {
        val sample = Container(Array(1, 2, 3))
        sample should haveTypedEncoder[Container[Array[Int]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[Array[Int]]
            original.value shouldBe deserialized.value
          })
      }

      "support Array of bytes" in {
        val sample = Container(Array(1.toByte, 2.toByte, 3.toByte))
        sample should haveTypedEncoder[Container[Array[Byte]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[Array[Byte]]
            original.value shouldBe deserialized.value
          })
      }
    }

    "used with maps" should {

      "support generic Map" in {
        val schema = StructType(Seq(
          StructField(
            name = "value",
            dataType = MapType(
              keyType = StringType,
              valueType = StructType(Seq(
                StructField("name", StringType, nullable = false),
                StructField("age", IntegerType, nullable = false))),
              valueContainsNull = false),
            nullable = false)))
        TypedEncoder[Container[collection.Map[String, SimpleUser]]].catalystRepr shouldBe schema
        val sample = Container(collection.Map(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[collection.Map[String, SimpleUser]]]()
      }

      "support generic Map with option values" in {
        val schema = StructType(Seq(
          StructField(
            name = "value",
            dataType = MapType(
              keyType = StringType,
              valueType = StructType(Seq(
                StructField("name", StringType, nullable = false),
                StructField("age", IntegerType, nullable = false))),
              valueContainsNull = true),
            nullable = false)))
        TypedEncoder[
          Container[collection.Map[String, Option[SimpleUser]]]].catalystRepr shouldBe schema
        val sample = Container(collection.Map(
          "Pavlo" -> Some(SimpleUser("Pablo", 34)),
          "Bob" -> None))
        sample should haveTypedEncoder[Container[collection.Map[String, Option[SimpleUser]]]]()
      }

      "support immutable Map" in {
        val sample = Container(immutable.Map(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[immutable.Map[String, SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.Map[_, _]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable HashMap" in {
        val sample = Container(immutable.HashMap(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[immutable.HashMap[String, SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.HashMap[_, _]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable ListMap" in {
        val sample = Container(immutable.ListMap(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[immutable.ListMap[String, SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.ListMap[_, _]]
            original.value shouldBe deserialized.value
          })
      }

      "support immutable TreeMap" in {
        val sample = Container(immutable.TreeMap(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[immutable.TreeMap[String, SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[immutable.TreeMap[_, _]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable Map" in {
        val sample = Container(mutable.Map(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.Map[String, SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.Map[_, _]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable HashMap" in {
        val sample = Container(mutable.HashMap(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.HashMap[String, SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.HashMap[_, _]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable LinkedHashMap" in {
        val sample = Container(mutable.LinkedHashMap(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.LinkedHashMap[String, SimpleUser]]](
          assertion =
            (original, deserialized) => {
              deserialized.value should beInstanceOf[mutable.LinkedHashMap[_, _]]
              original.value shouldBe deserialized.value
            })
      }

      "support mutable ListMap" in {
        val sample = Container(mutable.ListMap(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.ListMap[String, SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.ListMap[_, _]]
            original.value shouldBe deserialized.value
          })
      }

      "support mutable TreeMap" in {
        val sample = Container(mutable.TreeMap(
          "Pavlo" -> SimpleUser("Pablo", 34),
          "Bob" -> SimpleUser("Bob", 18)))
        sample should haveTypedEncoder[Container[mutable.TreeMap[String, SimpleUser]]](assertion =
          (original, deserialized) => {
            deserialized.value should beInstanceOf[mutable.TreeMap[_, _]]
            original.value shouldBe deserialized.value
          })
      }
    }
  }
}
