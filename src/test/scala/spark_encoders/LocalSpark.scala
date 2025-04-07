package spark_encoders

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf._

import java.util.TimeZone

case class LocalSpark(
    parallelism: Int = 4,
    shufflePartitions: Int = 4,
    sparkUI: Boolean = false,
    java8Api: Boolean = true,
    appID: String =
      getClass.getName + math.floor(math.random * 10e4).toLong.toString,
    conf: Map[String, String] = Map.empty) {

  def create: SparkSession = {
    val configs = Map(
      "spark.ui.enabled" -> sparkUI.toString,
      "spark.ui.showConsoleProgress" -> sparkUI.toString,
      "spark.app.id" -> appID,
      "spark.driver.host" -> "localhost",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
      SHUFFLE_PARTITIONS.key -> shufflePartitions.toString,
      "spark.sql.sources.parallelPartitionDiscovery.parallelism" -> 2.toString,
      // for some reason parquet vectorized reader cannot handle UDT inside arrays
      // however, that works on databricks, looks like they have custom implementation
      "spark.sql.parquet.enableNestedColumnVectorizedReader" -> "false")
    val sparkConf = new SparkConf().setMaster(s"local[$parallelism]")
      .setAppName("test")
      .setAll(configs ++ conf)
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    SparkSession.builder().config(sparkConf).getOrCreate()
  }
}
