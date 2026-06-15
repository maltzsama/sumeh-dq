// ============================================================================
// Global Settings
// ============================================================================
ThisBuild / organization := "io.galieostudio"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"
ThisBuild / resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Maven Central" at "https://repo1.maven.org/maven2",
  "Spark Packages" at "https://repos.spark-packages.org"
)

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xsource:3",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:privates"
)

ThisBuild / javacOptions ++= Seq(
  "-source", "11",
  "-target", "11",
  "-Xlint:deprecation",
  "-Xlint:unchecked"
)

// Test settings
ThisBuild / Test / parallelExecution := false
ThisBuild / Test / fork := true
ThisBuild / Test / javaOptions ++= Seq("-Xmx2g", "-XX:+UseG1GC")

// Publishing settings
ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
ThisBuild / licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/maltzsama/sumeh-scala"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/maltzsama/sumeh-scala"),
    "scm:git@github.com:maltzsama/sumeh-scala.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "maltzsama",
    name = "Demetrius Albuquerque",
    email = "demetrius@sumeh.dev",
    url = url("https://github.com/maltzsama")
  )
)

// ============================================================================
// Root Project (Aggregator)
// ============================================================================
lazy val root = project
  .in(file("."))
  .aggregate(
    core,
    spark,
    flink
  )
  .settings(
    name := "sumeh-scala",
    publish / skip := true,
    publishLocal / skip := true
  )

// ============================================================================
// CORE (Shared domain models, no engine dependencies)
// ============================================================================
lazy val core = project
  .in(file("core"))
  .settings(
    name := "sumeh-core",
    crossScalaVersions := Seq("2.12.18", "2.13.12"),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalacheck" %% "scalacheck" % "1.17.0" % Test,
      "com.lihaoyi" %% "upickle" % "3.1.0",
      "org.slf4j" % "slf4j-api" % "2.0.9",
      "ch.qos.logback" % "logback-classic" % "1.4.12" % Test
    ),
    description := "Sumeh Core - Shared data quality validation models and rules"
  )


lazy val spark = project
  .in(file("spark"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "sumeh-spark",

    scalaVersion := "2.13.12",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.0.0" % Provided,
      "org.apache.spark" %% "spark-core" % "4.0.0" % Provided,

      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalacheck" %% "scalacheck" % "1.17.0" % Test
    )
  )

lazy val flink = project
  .in(file("flink"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "sumeh-flink",

    scalaVersion := "2.13.12",

    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-table-api-java" % "2.1.0" % Provided,

      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalacheck" %% "scalacheck" % "1.17.0" % Test
    )
  )