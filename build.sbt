inThisBuild(List(
  scalaVersion := "2.12.20",
  crossScalaVersions := Seq("2.12.20", "2.13.16", "3.3.6"),
  organization := "io.github.pashashiz",
  homepage := Some(url("https://github.com/pashashiz")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      id = "pashashiz",
      name = "Pavlo Pohrebnyi",
      email = "your@pogrebnij@gmail.com",
      url = url("https://github.com/pashashiz"))),
  sonatypeCredentialHost := "s01.oss.sonatype.org",
  sonatypeRepository := "https://s01.oss.sonatype.org/service/local"))

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
