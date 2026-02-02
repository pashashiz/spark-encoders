object V {
  val sparkAll: Seq[String] = Seq("3.3.4", "3.4.4", "3.5.7", "4.0.1")
  val spark: String = sys.env.getOrElse("SPARK_VERSION", "3.3.4")
  if (!sparkAll.contains(spark))
    sys.error(s"Unsupported SPARK_VERSION: $spark. Supported versions: ${sparkAll.mkString(", ")}")
  val sparkMajor: String = spark.split('.').head
  val java: Int = sys.props("java.version").split("\\.")(0).toInt
  val scala_2_12 = "2.12.20"
  val scala_2_13 = "2.13.16"
  val scala_3 = "3.3.6"
  val scalaAll: Seq[String] = (if (spark != "4.0.1") Seq(scala_2_12, scala_2_13) else Seq(scala_2_13)) :+ scala_3
  val scala: String = scalaAll.head

  val magnolia = "1.1.10"
  val scalaTest = "3.2.19"
}
