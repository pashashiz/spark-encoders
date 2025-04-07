ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"
ThisBuild / crossScalaVersions := Seq("2.12.20", "2.13.16")

lazy val root = (project in file("."))
  .settings(
    name := "spark-encoders",
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % "3.5.5").cross(CrossVersion.for3Use2_13),
      "org.scalatest" %% "scalatest" % "3.2.19" % Test),
    libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => Seq("com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.10")
      case Some((3, _)) => Seq("com.softwaremill.magnolia1_3" %% "magnolia" % "1.3.16")
      case _ => Seq.empty
    }))
