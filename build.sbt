inThisBuild(List(
  ThisBuild / scalaVersion := V.scala,
  ThisBuild / crossScalaVersions := V.scalaAll,
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

lazy val javaOpens = {
  if (V.java >= 17) {
    // borrowed from https://github.com/apache/spark/blob/master/project/SparkBuild.scala
    Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED")
  } else {
    Seq.empty
  }
}

lazy val root = (project in file("."))
  .settings(
    name := s"spark${V.sparkMajor}-encoders",
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % V.spark % Provided).cross(CrossVersion.for3Use2_13),
      "org.scalatest" %% "scalatest" % V.scalaTest % Test exclude (
        "org.scala-lang.modules",
        "scala-xml_3")),
    libraryDependencies ++= (
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => Seq("com.softwaremill.magnolia1_2" %% "magnolia" % V.magnolia)
        case _            => Seq.empty
      }),
    Compile / unmanagedSourceDirectories += {
      baseDirectory.value / "src/main" / s"scala-spark${V.sparkMajor}"
    },
    Test / parallelExecution := false,
    Test / fork := true,
    Test / javaOptions ++= javaOpens,
    providedAsRunnable,

    // Shared assembly merge strategy
    ThisBuild / assemblyMergeStrategy := {
      case PathList("META-INF", _) => MergeStrategy.discard
      case _                       => MergeStrategy.first
    },

    // Assembly settings - production uber JAR (runtime scope only, excludes provided deps)
    assembly / fullClasspath := (Runtime / fullClasspath).value,
    assembly / assemblyJarName := s"${name.value}-${version.value}-all.jar",
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false),

    // Enable Test configuration for assembly
    inConfig(Test)(baseAssemblySettings),

    // Test assembly settings - includes test dependencies but excludes provided deps (DBR provides Spark)
    Test / assembly / fullClasspath := {
      val exported = (Test / exportedProducts).value
      val deps = (Test / dependencyClasspath).value
      val providedFiles = update.value.select(configurationFilter("provided")).toSet
      val filteredDeps = deps.filterNot(entry => providedFiles.contains(entry.data))
      exported ++ filteredDeps
    },
    Test / assembly / assemblyJarName := s"${name.value}-${version.value}-all-tests.jar",
    Test / assembly / assemblyOption := (Test / assembly / assemblyOption).value.withIncludeScala(
      false))
