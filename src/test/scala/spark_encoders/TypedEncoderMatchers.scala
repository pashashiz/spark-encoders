package spark_encoders

import TypedEncoder.typedEncoderToEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{Assertion, Suite}
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.reflect.{classTag, ClassTag}
import scala.util.Try

trait TypedEncoderMatchers extends SharedSpark with Matchers { self: Suite =>

  def haveTypedEncoder[A](
      pure: Boolean = false,
      dataset: Boolean = true,
      assertion: (A, A) => Assertion = (l: A, r: A) => l shouldBe r)(
      implicit enc: TypedEncoder[A]): Matcher[A] = {

    lazy val resolved = enc.encoderResolved

    def pureSerialize(input: A): Either[String, InternalRow] =
      Try {
        val serializer = resolved.createSerializer()
        serializer(input)
      }.toEither.left.map { e =>
        e.printStackTrace()
        s"Encoder failed to serialize $input: ${e.getMessage}"
      }

    def pureDeserialize(output: InternalRow): Either[String, A] =
      Try {
        val deserializer = resolved.createDeserializer()
        deserializer(output)
      }.toEither.left.map { e =>
        e.printStackTrace()
        s"Encoder failed to deserialize $output: ${e.getMessage}"
      }

    def workInDataset(input: A): Either[String, A] =
      Try {
        val ds = spark.createDataset(List(input))
        ds.printSchema()
        ds.show(truncate = false)
        ds.map(identity).collect().head
      }.toEither.left.map { e =>
        e.printStackTrace()
        s"Encoder failed to serialize $input: ${e.getMessage}"
      }

    def checkEquals(original: A, deserialized: A): Either[String, Unit] =
      Try(assertion(original, deserialized)).toEither.left.map(_.getMessage).map(_ => ())

    input => {
      def verify(codegen: CodegenObjectFactoryMode.Value) = {
        SQLConf.get.setConfString(
          SQLConf.CODEGEN_FACTORY_MODE.key,
          codegen.toString)
        def checkPure = for {
          bytes        <- pureSerialize(input)
          deserialized <- pureDeserialize(bytes)
          result       <- checkEquals(input, deserialized)
        } yield result
        def checkDataset = for {
          deserialized <- workInDataset(input)
          result       <- checkEquals(input, deserialized)
        } yield result
        def result = for {
          _ <- if (pure) checkPure.left.map(err => s"MODE=PURE: $err") else Right(())
          _ <- if (dataset) checkDataset.left.map(err => s"MODE=DATASET: $err") else Right(())
        } yield ()
        result.left.map(err => s"EXEC=${codegen.toString}: $err")
      }

      val result = for {
        _ <- verify(CodegenObjectFactoryMode.CODEGEN_ONLY)
        _ <- verify(CodegenObjectFactoryMode.NO_CODEGEN)
      } yield ()

      val message = result.fold(identity, _ => s"The object $input has type encoder")
      MatchResult(result.isRight, message, message)
    }
  }

  def failToSerializeWith[A](errorPrefix: String)(implicit encoder: TypedEncoder[A]): Matcher[A] = {

    val resolved = encoder.encoderResolved

    def serialize(input: A): Either[String, InternalRow] =
      Try {
        val serializer = resolved.createSerializer()
        serializer(input)
      }.toEither.left.map { e =>
        e.getMessage + Option(e.getCause).map(e => ". " + e.getMessage).getOrElse("")
      }

    input => {
      val result = serialize(input).fold(identity, _ => "Serialization was successful")
      MatchResult(
        matches = result.startsWith(errorPrefix),
        rawFailureMessage = s"""""$result" did not start with "$errorPrefix"""",
        rawNegatedFailureMessage = s"""""$result" did started with "$errorPrefix"""")
    }
  }

  def beInstanceOf[A: ClassTag]: Matcher[A] =
    input => {
      MatchResult(
        matches = classTag[A].runtimeClass.isAssignableFrom(input.getClass),
        rawFailureMessage =
          s"Class ${input.getClass.getCanonicalName} is not instance of ${classTag[A].runtimeClass.getCanonicalName}",
        rawNegatedFailureMessage =
          s"Class ${input.getClass.getCanonicalName} is instance of ${classTag[A].runtimeClass.getCanonicalName}")
    }
}
