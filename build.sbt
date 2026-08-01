ThisBuild / organization := "io.galileostd"
ThisBuild / version      := sys.props.getOrElse("version", "0.1.0-SNAPSHOT")
ThisBuild / description := "Declarative data-quality validation for Spark and Flink: one rule catalog, single-pass bifurcation."
ThisBuild / homepage := Some(url("https://github.com/maltzsama/sumeh-dq"))
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/maltzsama/sumeh-dq"),
    "scm:git:https://github.com/maltzsama/sumeh-dq.git"
  )
)
ThisBuild / developers := List(
  Developer(
    "maltzsama",
    "Demetrius Albuquerque",
    "demetrius.albuquerque@yahoo.com.br",
    url("https://github.com/maltzsama")
  )
)

ThisBuild / Test / parallelExecution := false

// Dependencies (e.g. upickle 4.x) require scala-library 2.13.16, so we compile with it too.
val scalatestVersion = "3.2.20"

ThisBuild / versionScheme := Some("early-semver")

// ---------------------------------------------------------------------------
// Publishing.
//
// Artifacts are published to GitHub Packages in CI on release events, gated by
// the GITHUB_PACKAGES env var (set in the publish job). Publishing to Maven
// Central / Sonatype requires the repo owner to add the SONATYPE_USERNAME,
// SONATYPE_PASSWORD, PGP_SECRET and PGP_PASSPHRASE repository secrets — see the
// publishing PR for the exact checklist.
//
// TODO(owner): enable Maven Central by adding the Sonatype resolver here and
// wiring the missing secrets in .github/workflows/ci.yml.
// ---------------------------------------------------------------------------
ThisBuild / publishTo := {
  if (sys.env.contains("GITHUB_PACKAGES"))
    Some("GitHub Packages".at("https://maven.pkg.github.com/maltzsama/sumeh-dq"))
  else None
}

ThisBuild / credentials ++= sys.env
  .get("GITHUB_TOKEN")
  .map(token => Credentials("GitHub Package Registry", "maven.pkg.github.com", "maltzsama", token))
  .toSeq

lazy val core = (project in file("core"))
  .settings(
    name                     := "sumeh-core",
    scalaVersion             := "2.13.16",
    crossScalaVersions       := Seq("2.12.18", "2.13.16"),
    coverageMinimumStmtTotal := 90,
    coverageFailOnMinimum    := true,
    libraryDependencies ++= Seq(
      "com.lihaoyi"   %% "upickle"   % "4.4.3",
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )

lazy val sparkVersion = settingKey[String]("Spark version")

lazy val spark = (project in file("spark"))
  .dependsOn(core)
  .settings(
    sparkVersion := sys.props.getOrElse("spark.version", "4.1.2"),
    name         := s"sumeh-spark${sparkVersion.value.takeWhile(_ != '.')}", // sumeh-spark4 / sumeh-spark3
    scalaVersion := "2.13.16",
    crossScalaVersions := {
      if (sparkVersion.value.startsWith("3.")) Seq("2.12.18", "2.13.16")
      else Seq("2.13.16")
    },
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion.value % Provided,
      "org.scalatest"    %% "scalatest"  % scalatestVersion   % Test
    ),
    coverageMinimumStmtTotal := 80,
    coverageFailOnMinimum    := true,
    Test / fork              := true,
    Test / javaOptions ++= Seq(
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
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  )

lazy val flinkVersion = settingKey[String]("Flink version")

lazy val flink = (project in file("flink"))
  .dependsOn(core)
  .settings(
    flinkVersion       := sys.props.getOrElse("flink.version", "2.2.0"),
    name               := s"sumeh-flink${flinkVersion.value.takeWhile(_ != '.')}", // sumeh-flink2 / sumeh-flink1
    scalaVersion       := "2.13.16",
    crossScalaVersions := Seq("2.12.18", "2.13.16"),
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java"        % flinkVersion.value % Provided,
      "org.apache.flink" % "flink-table-api-java"        % flinkVersion.value % Provided,
      "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion.value % Provided,
      "org.apache.flink" % "flink-clients"               % flinkVersion.value % Test,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0",
      "org.scalatest"   %% "scalatest"                   % scalatestVersion   % Test
    ),
    coverageMinimumStmtTotal := 80,
    coverageFailOnMinimum    := true,
    Test / fork              := true,
    Test / javaOptions ++= Seq(
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
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  )

lazy val root = (project in file("."))
  .aggregate(core, spark, flink)
  .settings(
    name           := "sumeh-dq",
    publish / skip := true
  )
