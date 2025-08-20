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
      email = "pogrebnij@gmail.com",
      url = url("https://github.com/pashashiz"))),
  sonatypeCredentialHost := "s01.oss.sonatype.org",
  sonatypeRepository := "https://s01.oss.sonatype.org/service/local"))

lazy val providedAsRunnable = Seq(
  Compile / run := Defaults
    .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
    .evaluated,
  Compile / runMain := Defaults
    .runMainTask(Compile / fullClasspath, Compile / run / runner)
    .evaluated)

lazy val root = (project in file("."))
  .settings(
    name := "spark-encoders",
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % "3.5.5" % Provided).cross(CrossVersion.for3Use2_13),
      "org.scalatest" %% "scalatest" % "3.2.19" % Test exclude (
        "org.scala-lang.modules",
        "scala-xml_3")),
    libraryDependencies ++= (
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => Seq("com.softwaremill.magnolia1_2" %% "magnolia" % "1.1.10")
        case _            => Seq.empty
      }),
    Test / parallelExecution := false,
    providedAsRunnable,
    
    // Shared assembly merge strategy
    ThisBuild / assemblyMergeStrategy := {
      case PathList("META-INF", _) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    
    // Assembly settings - production uber JAR (compile scope only)
    assembly / fullClasspath := (Compile / fullClasspath).value,
    assembly / assemblyJarName := s"${name.value}-${version.value}-all.jar",
    
    // Enable Test configuration for assembly
    inConfig(Test)(baseAssemblySettings),
    
    // Test assembly settings - includes test dependencies as well for running tests on Databricks
    Test / assembly / fullClasspath := (Test / fullClasspath).value,
    Test / assembly / assemblyJarName := s"${name.value}-${version.value}-all-tests.jar",
  )
