package io.github.pashashiz.spark_encoders

import org.scalatest.wordspec.AnyWordSpec

object SparkAnyWordSpec {
  // Auto-detect Databricks Runtime
  private def isDatabricks: Boolean = {
    System.getenv("DATABRICKS_RUNTIME_VERSION") != null
  }
  
  private val defaultReuseContext = isDatabricks
}

abstract class SparkAnyWordSpec(
    override val reuseContext: Boolean = SparkAnyWordSpec.defaultReuseContext,
    override val parallelism: Int = 2,
    override val shufflePartitions: Int = 4,
    override val sparkUI: Boolean = false,
    override val java8Api: Boolean = true)
    extends AnyWordSpec
    with SharedSpark
