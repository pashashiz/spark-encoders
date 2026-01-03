# Testing

## Testing Spark OOS

Run tests with Spark `3.3.4` + Scala (`2.12`, `2.13`, `3.3`) + Java `1.8`

```shell
sdk use java 8.0.452.fx-zulu
SPARK_VERSION=3.3.4 sbt clean +test
```

Run tests with Spark `3.4.4` + Scala (`2.12`, `2.13`, `3.3`) + Java `1.8`

```shell
sdk use java 8.0.452.fx-zulu
SPARK_VERSION=3.4.4 sbt clean +test
```

Run tests with Spark `3.5.7` + Scala (`2.12`, `2.13`, `3.3`) + Java `1.8`

```shell
sdk use java 8.0.452.fx-zulu
SPARK_VERSION=3.5.7 sbt clean +test
```

Run tests with latest Spark `4.0.1` + Scala (`2.13`, `3.3`) + Java `1.17+`

```shell
sdk use java 17.0.14.fx-zulu
SPARK_VERSION=4.0.1 sbt clean +test
```

## Testing Spark Databricks

While working on the snapshot version we can just build an uber jar locally given a spark/scala/java version we need
via:

```shell
sdk use java 17.0.14.fx-zulu
SPARK_VERSION=4.0.1 sbt "++ 2.13.20" clean "Test / assembly"
```

Then copy resulting jar like `target/scala-2.13/spark4-encoders-0.2.0+1-9d1ab208+20260103-1531-SNAPSHOT-all-tests.jar`
to Databricks Volume. Then start the Databricks Runtime you are interested in and run the following code in the
notebook:

```scala
import io.github.pashashiz.spark_encoders._

TestSuites.verify()
```

You might also deploy jar as a Job using `io.github.pashashiz.spark_encoders.TestSuites` as the main class.