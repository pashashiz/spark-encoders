ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"
ThisBuild / crossScalaVersions := Seq("2.12.20", "2.13.16", "3.3.6")

lazy val root = (project in file("."))
  .settings(
    name := "spark-encoders",
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % "3.5.5").cross(CrossVersion.for3Use2_13),
      "org.scalatest" %% "scalatest" % "3.2.19" % Test exclude (
        "org.scala-lang.modules",
        "scala-xml_3")),
    libraryDependencies ++= (
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => Seq("com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.10")
        case _            => Seq.empty
      }),
      Test / parallelExecution := false)
