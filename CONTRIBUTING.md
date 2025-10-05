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

TBD