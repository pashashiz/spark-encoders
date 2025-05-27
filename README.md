# Spark Encoders

## Intro

This is a light-weight Scala library for compile-time derivation of Spark `org.apache.spark.sql.Encoder`. It offers
comprehensive support for standard Scala data types (ADTs, Enums, Either, Try, collections, durations, etc.) and is
designed for easy extension with custom types.

The library supports Scala `2.12`, `2.13`, and Scala `3`. The Scala 2 version has a single external dependency
on [magnolia](https://github.com/softwaremill/magnolia/tree/scala2); the Scala 3 version has no external dependencies.
We plan to remove the Magnolia dependency in the future to make the library even lighter.

Spark requires an `Encoder[A]` for `Dataset[A]` operations and when parallelizing collections. Encoders handle the
crucial serialization/deserialization between Scala objects and Spark's internal `Row` format (used by Catalyst),
enabling the movement between the Scala type system and Spark's untyped data representation.

Spark's built-in `Encoder`s, typically imported via `import spark.implicits._`, are generated using
`ExpressionEncoder[T]()`. This process relies on Scala reflection and a `TypeTag[T]`, which works well for many standard
types. However, this approach has significant limitations:

* **Runtime errors:** Spark's reflection-based derivation fails at runtime if an `Encoder` cannot be found for any
  nested type within your data structure.
* **Difficult to extend:** Unlike typical Scala implicit derivation, providing an `implicit Encoder` for a custom type
  is ignored by Spark's built-in mechanism. Writing a custom `Encoder` is non-trivial, requiring deep knowledge of the
  Spark Catalyst Expression API and involving significant boilerplate. The only official extension point,
  `UserDefinedType`, is also complex and requires Catalyst expertise.
* **Incorrect nullability handling:** Spark derives schemas based on Java conventions, not Scala's `Option`. For
  example, a `String` field in a case class will be treated as nullable (`String | Null`), even though `Option` is used
  to explicitly model optionality. This leads to schemas where fields expected to be required are marked as nullable.
* **Limited and fragile collection support:** Spark's built-in encoders primarily support generic `Array`, `Seq` (often
  backed by `List`), and `Map` (backed by `HashMap`). Using other collection types like `Set` or `TreeMap` often results
  in runtime errors. Furthermore, the behavior can be inconsistent depending on Spark's execution mode (codegen vs.
  interpreted), potentially leading to `ClassCastException`s.
* **Lack of Algebraic Data Type (ADT) support:** Spark's built-in encoders only support `Product` types (like case
  classes). This is a major limitation in Scala, where ADTs (sealed hierarchies, enums, `Either`, `Option`, `Try`) are
  fundamental for modeling domains. For example, directly encoding `Either[Error, Result]` to represent success/failure
  within a `Dataset` is not supported. Common workarounds like Kryo serialization (`Array[Byte]` schema, losing
  structure) or manually mapping to a cumbersome product type (
  `case class EitherCompat(left: Option[Error], right: Option[Result])`) are slow, lose type safety, or complicate
  schema interaction.
* **No direct support for Scala `enum`:** There's no built-in mechanism to directly encode Scala enums as Spark strings.
* **Limited Date/Time types:** Support is missing for types like Scala `Duration`, `Period`, and Java `OffsetDateTime`,
  `ZonedDateTime`.
* **Lack of fine-grained control:** There's no built-in way to specify that only *certain* types within a complex
  structure should be serialized using an alternative method like Kryo, while others use Catalyst.

## User Guide

### Adding the Library

Add the following library dependency using your build tool (e.g., sbt):

```scala
libraryDependencies += "com.github.pashashiz" %% "spark-encoders" % "0.1.0" // Check latest version
```

### Deriving Your First Product Encoder

Let's start with a simple case class:

```scala
case class User(name: String, age: Int)
```

To enable `Encoder` derivation for `User`, you need the necessary imports.

**Scala 2:**

Add the following import:

```scala
import com.github.pashashiz.spark_encoders.TypedEncoder._
```

You can then derive an `Encoder[User]` in several ways:

```scala
val enc1 = implicitly[TypedEncoder[User]]
val enc2 = TypedEncoder[User]
val enc3 = derive[User]
```

**Important:** When defining an `implicit val`, use `derive` to avoid infinite recursion during compilation:

```scala
implicit val enc4: TypedEncoder[User] = derive[User] // Use derive here
```

If you encounter derivation errors, explicitly using the `derive` method can provide more specific error messages.

**Scala 3:**

In Scala 3, you can use the same implicit derivation pattern as in Scala 2 by importing `TypedEncoder.given`:

```scala 3
import com.github.pashashiz.spark_encoders.TypedEncoder.*
import com.github.pashashiz.spark_encoders.TypedEncoder.given
```

Alternatively, you can use the `derives` keyword directly on your `case class` or `sealed trait`:

```scala 3
case class User(name: String, age: Int)derives TypedEncoder
```

Using the derived encoder, you can create a Dataset:

```scala
spark.createDataset(Seq(User("Pavlo", 35), User("Randy", 45))).show(false)
```

This will produce the expected output without relying on Spark's reflection-based encoders (`spark.implicits._`):

```
+-----+---+
|name |age|
+-----+---+
|Pavlo|35 |
|Randy|45 |
+-----+---+
```

### ADT Encoder

The library natively supports Algebraic Data Types (ADTs), such as sealed hierarchies. Given the following ADT:

```scala
sealed trait Item {
  def name: String
}

object Item {
  case class Defect(name: String, priority: Int) extends Item

  case class Feature(name: String, size: Int) extends Item

  case class Story(name: String, points: Int) extends Item
}
```

You can use it directly in Spark:

```scala
spark.createDataset[Item](
    Seq(Item.Defect("d1", 100), Item.Feature("f1", 1), Item.Story("s1", 3)))
  .show(false)
```

The result encodes the type information in a special `_type` column using a type discriminator strategy:

```
+-------+----+------+--------+----+
|_type  |name|points|priority|size|
+-------+----+------+--------+----+
|Defect |d1  |NULL  |100     |NULL|
|Feature|f1  |NULL  |NULL    |1   |
|Story  |s1  |3     |NULL    |NULL|
+-------+----+------+--------+----+
```

Currently, only the type discriminator strategy is available for ADTs. Future versions may offer more customization
options.

### Enum Encoder

The library supports deriving encoders for Scala enums (Scala 3 `enum` or Scala 2 `sealed trait` with case objects).
Given the following enum:

```scala
sealed trait Color // Or `enum Color` in Scala 3

object Color {
  case object Red extends Color

  case object Green extends Color

  case object Blue extends Color
}
```

You can use it in a dataset:

```scala
spark.createDataset[Color](
    Seq(Color.Red, Color.Green, Color.Blue))
  .show(false)
```

The enum values are automatically encoded as strings:

```
+-----+
|value|
+-----+
|Red  |
|Green|
|Blue |
+-----+
```

### Either Encoder

Just like ADTs, standard Scala `Either` is supported out-of-the-box.

```scala
spark.createDataset[Either[String, User]](
    Seq(Right(User("Pavlo", 35)), Left("Oops")))
  .show()
```

This results in a structure similar to ADTs, clearly separating the `Left` and `Right` cases:

```
+-----+----+-----------+
|_type|left|      right|
+-----+----+-----------+
|Right|NULL|{Pavlo, 35}|
| Left|Oops|       NULL|
+-----+----+-----------+
```

### Try Encoder

Deriving an encoder for `Try[A]` requires an implicit encoder for `Throwable`, as the library needs to know how to
handle the potential exception.

You can provide an implicit encoder for `Throwable`. For instance, one that only preserves the error message:

```scala
import com.github.pashashiz.spark_encoders.encoders.ThrowableEncoders._ // lightExceptionEncoder is here

implicit val leEncoder: TypedEncoder[Throwable] = lightExceptionEncoder
implicit val tryEncoder: TypedEncoder[Try[SimpleUser]] = derive[Try[SimpleUser]]
```

If you need to preserve full exception information, you can use Kryo serialization for `Throwable`:

```scala
import com.github.pashashiz.spark_encoders.encoders.PrimitiveEncoders._ // kryo is here

implicit val errorEncoder: TypedEncoder[Throwable] = kryo[Throwable]
implicit val tryEncoder: TypedEncoder[Try[SimpleUser]] = derive[Try[SimpleUser]] // Try will now use Kryo for Throwable
```

### Custom Encoders with xmap

For types that cannot be automatically derived or for which you want a custom encoding format (e.g., encoding a case
class as a primitive), you can use the `xmap` helper. This maps your type `T` to an existing encodable type `U` using
transformation functions.

Let's say you have `case class Altitude(value: Double)` and want to encode it directly as a `Double` instead of a
struct:

```scala
// Define the mapping
implicit val altTE: TypedEncoder[Altitude] =
  xmap[Altitude, Double](_.value)(Altitude.apply) // map: Altitude => Double, contrMap: Double => Altitude

// Use the custom encoder
spark.createDataset(Seq(Altitude(100), Altitude(132))).show(false)
```

This will produce the desired output:

```
+-----+
|value|
+-----+
|100.0|
|132.0|
+-----+
```

`xmap` is a powerful tool for defining encoders for types that don't have a direct Spark-compatible structure or for
customizing the schema of types that do.

### Integrating with Spark UDTs

If you have an existing Spark User Defined Type (UDT), you can easily derive an `Encoder` for its user class. Simply
place an instance of the `UserDefinedType` in the implicit scope.

Given the standard Spark UDT definition:

```scala
@SQLUserDefinedType(udt = classOf[PointUDT])
case class Point(x: Double, y: Double)

class PointUDT extends UserDefinedType[Point] {
  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)

  override def serialize(point: Point): Any = new GenericArrayData(Array[Double](point.x, point.y))

  override def deserialize(datum: Any): Point = datum match {
    case values: ArrayData => Point(values.getDouble(0), values.getDouble(1))
  }

  override def userClass: Class[Point] = classOf[Point]
  // ... other UDT methods
}
```

To use `Point` with the library's encoder derivation:

```scala
import org.apache.spark.sql.types.{UserDefinedType, DataType, ArrayType, DoubleType} // Spark imports
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData} // Spark imports
import org.apache.spark.annotation.SQLUserDefinedType // Spark import for annotation

// Place UDT instance in implicit scope
implicit val pointUdt: PointUDT = new PointUDT()

// Encoder for Point will now be derivable automatically
spark.createDataset(Seq(Point(1, 2), Point(3, 4))).show(false)
```

The output will reflect the UDT's serialization format:

```
+--------------+
|value         |
+--------------+
|[1.0, 2.0]    |
|[3.0, 4.0]    |
+--------------+
```

### Optimizing Compilation Time and JAR Size

Relying *solely* on the wildcard import (`import com.github.pashashiz.spark_encoders.TypedEncoder._`) for fully
automatic encoder derivation in large or complex Spark applications can sometimes impact compilation time and the size
of the generated code/JAR.

This is because the compiler's implicit search might re-derive encoders for the same types multiple times within
different contexts, leading to redundant generated code.

For better performance and smaller artifacts, especially in applications with many `Dataset` operations, it's
recommended to use semi-auto derivation. This involves explicitly defining your top-level or frequently used encoders in
a dedicated location and then importing or mixing in that object/trait.

Here's the recommended pattern:

Define encoders explicitly in a trait or object:

```scala
import com.github.pashashiz.spark_encoders.TypedEncoder._
import org.apache.spark.sql.Encoder 

trait MyAppEncoders {
  // Explicitly derive encoders for your main types
  implicit val userEncoder: Encoder[User] = derive[User]
  implicit val itemEncoder: Encoder[Item] = derive[Item]
  // ... define encoders for other types used in Datasets
}

object MyAppEncoders extends MyAppEncoders // Create an object extending the trait for easy import
```

Then, import these explicit encoders where needed:

```scala
import org.apache.spark.sql.SparkSession
import MyAppEncoders._ // Import your explicitly defined encoders

object MyApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    // Now User encoder is available via MyAppEncoders._
    spark.createDataset(Seq(User("Pavlo", 35), User("Randy", 45))).show(false)
    // Same for Item, etc.
    spark.createDataset[Item](...).show(false) // Item encoder is also available
  }
}
```

This approach ensures that each required encoder is derived only once, reducing compiler workload and generated code
duplication.

## Future Work:

- Cross spark versions build
- Support DBR (Databricks)
- Support the rest of Scala collections and data types
- Support of different ADT encoding strategies
- Benchmarks
- Get rid of magnolia for Scala 2, use macro instead to have 0 dependency library