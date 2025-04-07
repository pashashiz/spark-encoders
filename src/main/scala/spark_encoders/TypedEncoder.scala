package spark_encoders

import magnolia1.{CaseClass, Magnolia, SealedTrait}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.types._
import org.apache.spark.util.PrivateClosureCleaner

import java.io.Serializable
import java.math.{BigDecimal => JBigDecimal, BigInteger => JBigInt}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, Period, ZonedDateTime, Duration => JDuration}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.{ClassTag, classTag}

abstract class TypedEncoder[A](implicit val classTag: ClassTag[A]) extends Serializable {

  def runtimeClass: Class[_] = classTag.runtimeClass

  def nullable: Boolean = false

  // used to reconstruct type from catalyst
  def jvmRepr: DataType = ObjectType(runtimeClass)

  // type inside catalyst
  def catalystRepr: DataType

  def toCatalyst(path: Expression): Expression

  def fromCatalyst(path: Expression): Expression

  def encoder: ExpressionEncoder[A] = {
    // input is always BoundReference with single element
    val in = BoundReference(0, jvmRepr, nullable)
    // output is always GetColumnByOrdinal with single element
    val out = GetColumnByOrdinal(0, catalystRepr)
    new ExpressionEncoder[A](
      objSerializer = toCatalyst(in),
      objDeserializer = fromCatalyst(out),
      clsTag = classTag)
  }

  def encoderResolved = {
    val instance = encoder
    instance.resolveAndBind(toAttributes(instance.schema))
  }

  override def toString = getClass.getSimpleName.replace("$", "")
}

object TypedEncoder {

  def apply[A: TypedEncoder: ClassTag]: TypedEncoder[A] = implicitly[TypedEncoder[A]]

  def xmap[A: ClassTag, B: ClassTag: TypedEncoder](mapVia: A => B)(
      contrmapVia: B => A): TypedEncoder[A] = {
    PrivateClosureCleaner.clean(mapVia)
    PrivateClosureCleaner.clean(contrmapVia)
    InvariantEncoder(new Invariant[A, B] {
      override def map(in: A): B = mapVia(in)
      override def contrMap(out: B): A = contrmapVia(out)
    })
  }

  trait Derivation {
    type Typeclass[T] = TypedEncoder[T]
    def join[A: ClassTag](ctx: CaseClass[TypedEncoder, A]): TypedEncoder[A] = ProductEncoder[A](ctx)
    def split[A: ClassTag](ctx: SealedTrait[TypedEncoder, A]): TypedEncoder[A] = ADTEncoder[A](ctx)
    implicit def genTypedEncoder[T]: TypedEncoder[T] = macro Magnolia.gen[T]
  }

  trait Implicits extends Derivation {

    implicit def typedEncoderToEncoder[A](implicit te: TypedEncoder[A]): ExpressionEncoder[A] =
      te.encoder

    implicit val booleanEncoder: TypedEncoder[Boolean] = BooleanEncoder
    implicit val byteEncoder: TypedEncoder[Byte] = ByteEncoder
    implicit val shortEncoder: TypedEncoder[Short] = ShortEncoder
    implicit val intEncoder: TypedEncoder[Int] = IntEncoder
    implicit val longEncoder: TypedEncoder[Long] = LongEncoder
    implicit val floatEncoder: TypedEncoder[Float] = FloatEncoder
    implicit val doubleEncoder: TypedEncoder[Double] = DoubleEncoder

    implicit val stringEncoder: TypedEncoder[String] = StringEncoder
    // note: Char and Array[Char] is not yet supported, needs lots of extra work (special encoders)
    // since unsupported by Spark natively yet difficult to use any sort of Invariant
    // since Spark will generate char and char[] data types

    implicit val bigDecimal: TypedEncoder[BigDecimal] = BigDecimalEncoder
    implicit val jBigDecimal: TypedEncoder[JBigDecimal] = JBigDecimalEncoder
    implicit val bigInt: TypedEncoder[BigInt] = BigIntEncoder
    implicit val jBigInt: TypedEncoder[JBigInt] = JBigIntEncoder

    implicit val uuidEncoder: TypedEncoder[UUID] = InvariantEncoder(UUIDInvariant)

    implicit val timestampEncoder: TypedEncoder[Timestamp] = TimestampEncoder
    implicit val instantEncoder: TypedEncoder[Instant] = InstantEncoder
    implicit val localDateTimeEncoder: TypedEncoder[LocalDateTime] = LocalDateTimeEncoder
    implicit val dateEncoder: TypedEncoder[Date] = DateEncoder
    implicit val localDateEncoder: TypedEncoder[LocalDate] = LocalDateEncoder
    implicit val offsetDateTimeEncoder: TypedEncoder[OffsetDateTime] =
      InvariantEncoder(OffsetDateTimeInvariant)
    implicit val zonedDateTimeEncoder: TypedEncoder[ZonedDateTime] =
      InvariantEncoder(ZonedDateTimeInvariant)

    implicit val jDurationEncoder: TypedEncoder[JDuration] = JDurationEncoder
    implicit val finiteDurationEncoder: TypedEncoder[FiniteDuration] = FiniteDurationEncoder
    implicit val periodEncoder: TypedEncoder[Period] = PeriodEncoder

    implicit def optionEncoder[A: TypedEncoder]: TypedEncoder[Option[A]] =
      OptionEncoder()

    def kryo[A: ClassTag]: TypedEncoder[A] = {
      val external = Encoders.kryo[A].asInstanceOf[ExpressionEncoder[A]]
      ExternalEncoder(external)
    }

    implicit def udt[A >: Null: ClassTag](implicit instance: UserDefinedType[A]): TypedEncoder[A] =
      UDTEncoder(instance)

    val lightExceptionEncoder: TypedEncoder[Throwable] =
      TypedEncoder.xmap[Throwable, String](_.getMessage)(LightException(_))

    implicit def eitherEncoder[A: TypedEncoder, B: TypedEncoder]: TypedEncoder[Either[A, B]] =
      InvariantEncoder(new EitherInvariant())

    implicit def seqEncoder[C[_] <: collection.Seq[_], A](implicit
        seqTag: ClassTag[C[A]],
        elementEncoder: TypedEncoder[A],
        factory: CollectionFactory[A, C[A]]): TypedEncoder[C[A]] = {
      // we give a hint to Spark to build out custom collection when deserializing,
      // yet that is not guaranteed we might have to convert collection back ourselves
      implicit val targetSeqEncoder: TypedEncoder[collection.Seq[A]] = SeqEncoder[C, A]()
      InvariantEncoder(new SeqInvariant(factory))
    }

    implicit def setEncoder[C[_] <: collection.Set[_], A](implicit
        setTag: ClassTag[C[A]],
        elementEncoder: TypedEncoder[A],
        factory: CollectionFactory[A, C[A]]): TypedEncoder[C[A]] = {
      implicit val genericSeqEncoder: TypedEncoder[collection.Seq[A]] =
        SeqEncoder[collection.Seq, A]()
      InvariantEncoder(new SetInvariant(factory))
    }

    implicit def arrayEncoder[A](implicit elementEncoder: TypedEncoder[A]): TypedEncoder[Array[A]] =
      ArrayEncoder[A](elementEncoder.classTag, elementEncoder)

    implicit def mapEncoder[C[_, _] <: collection.Map[_, _], A, B](implicit
        mapTag: ClassTag[C[A, B]],
        keyEncoder: TypedEncoder[A],
        valueEncoder: TypedEncoder[B],
        factory: CollectionFactory[(A, B), C[A, B]]): TypedEncoder[C[A, B]] = {
      // in case we need concrete immutable implementation
      // or any mutable one we have to convert it back ourselves
      implicit val geneticMapEncoder: TypedEncoder[collection.Map[A, B]] = MapEncoder[C, A, B]()
      InvariantEncoder(new MapInvariant(factory))
    }

  }
}

object auto extends TypedEncoder.Implicits
