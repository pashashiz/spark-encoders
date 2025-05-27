package com.github.pashashiz.spark_encoders

import org.scalatest.wordspec.AnyWordSpec

abstract class SparkAnyWordSpec(
    override val reuseContext: Boolean = false,
    override val parallelism: Int = 2,
    override val shufflePartitions: Int = 4,
    override val sparkUI: Boolean = false,
    override val java8Api: Boolean = true)
    extends AnyWordSpec
    with SharedSpark
