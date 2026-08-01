ThisBuild / organization := "io.galileostd"
ThisBuild / version      := sys.props.getOrElse("version", "0.1.0-SNAPSHOT")
ThisBuild / description  := "Declarative data-quality validation for Spark and Flink: one rule catalog, single-pass bifurcation."
ThisBuild / homepage     := Some(url("https://github.com/maltzsama/sumeh-dq"))
ThisBuild / licenses     := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / scmInfo      := Some(
  ScmInfo(
    url("https://github.com/maltzsama/sumeh-dq"),
    "scm:git:https://github.com/maltzsama/sumeh-dq.git"
  )
)
ThisBuild / developers := List(
  Developer("maltzsama", "Demetrius Albuquerque", "demetrius.albuquerque@yahoo.com.br", url("https://github.com/maltzsama"))
)

ThisBuild / Test / parallelExecution := false

// Dependencies (e.g. upickle 4.x) require scala-library 2.13.16, so we compile with it too.

// Publishing to GitHub Packages (https://maven.pkg.github.com/maltzsama/sumeh-dq).
// Only used in CI on release events; the version is set via -Dversion=X.Y.Z from the release tag.
ThisBuild / publishTo := Some("GitHub Packages" at "https://maven.pkg.github.com/maltzsama/sumeh-dq")
ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "maltzsama",
  sys.env.getOrElse("GITHUB_TOKEN", "")
)

lazy val core = (project in file("core"))
  .settings(
    name               := "sumeh-core",
    scalaVersion       := "2.13.16",
    crossScalaVersions := Seq("2.12.18", "2.13.16"),
    coverageMinimumStmtTotal := 90,
    coverageFailOnMinimum    := true,
    libraryDependencies ++= Seq(
      "com.lihaoyi"   %% "upickle"   % "4.4.3",
      "org.slf4j"      % "slf4j-api" % "2.0.18",
      "org.scalatest" %% "scalatest" % "3.2.20" % Test
    )
  )

lazy val sparkVersion = settingKey[String]("Spark version")

lazy val spark = (project in file("spark"))
  .dependsOn(core)
  .settings(
    name         := "sumeh-spark",
    scalaVersion := "2.13.16",
    sparkVersion := sys.props.getOrElse("spark.version", "4.1.2"),
    crossScalaVersions := {
      if (sparkVersion.value.startsWith("3.")) Seq("2.12.18", "2.13.16")
      else Seq("2.13.16")
    },
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion.value % Provided,
      "org.scalatest"    %% "scalatest"  % "3.2.17"           % Test
    ),
    Test / fork := true,
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
    name               := "sumeh-flink",
    scalaVersion       := "2.13.16",
    crossScalaVersions := Seq("2.12.18", "2.13.16"),
    flinkVersion       := sys.props.getOrElse("flink.version", "2.2.0"),
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % flinkVersion.value % Provided,
      "org.apache.flink" % "flink-table-api-java"  % flinkVersion.value % Provided,
      "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion.value % Provided,
      "org.apache.flink" % "flink-clients"        % flinkVersion.value % Test,
      "org.scalatest"   %% "scalatest"            % "3.2.17" % Test
    ),
    Test / fork := true,
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
