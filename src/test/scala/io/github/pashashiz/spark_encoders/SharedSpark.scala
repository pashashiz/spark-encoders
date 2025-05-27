package io.github.pashashiz.spark_encoders

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSpark extends BeforeAndAfterAll { self: Suite =>

  @transient private var _spark: SparkSession = _

  lazy val spark: SparkSession = _spark
  lazy val sparkContext: SparkContext = _spark.sparkContext
  implicit def sqlContext: SQLContext = spark.sqlContext


  protected def reuseContext: Boolean = false
  protected def parallelism: Int = 2
  protected def shufflePartitions: Int = 4
  protected def sparkUI: Boolean = false
  protected def java8Api: Boolean = true
  protected def appID: String =
    getClass.getName + math.floor(math.random * 10e4).toLong.toString

  def conf: Map[String, String] = Map.empty

  override protected def beforeAll(): Unit = {
    _spark = LocalSpark(
      parallelism = parallelism,
      shufflePartitions = shufflePartitions,
      sparkUI = sparkUI,
      java8Api = java8Api,
      appID = appID,
      conf = conf)
      .create
    super.beforeAll()
  }

  override protected def afterAll(): Unit =
    try {
      if (!reuseContext) stop()
    } finally {
      super.afterAll()
    }

  def stop(): Unit =
    Option(_spark).foreach { session =>
      session.stop()
      _spark = null
      System.clearProperty("spark.driver.port")
    }
}
