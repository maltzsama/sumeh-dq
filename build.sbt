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

def docOptions(moduleTitle: String) = Def.setting {
  Seq(
    "-doc-title",
    moduleTitle,
    "-doc-version",
    version.value,
    "-doc-footer",
    "© 2026 GalileoStd.io · Apache 2.0",
    "-implicits",
    "-groups",
    "-sourcepath",
    (ThisBuild / baseDirectory).value.getAbsolutePath,
    "-doc-source-url",
    s"https://github.com/maltzsama/sumeh-dq/tree/main/€{FILE_PATH_EXT}#L€{FILE_LINE}"
  )
}

// ---------------------------------------------------------------------------
// PGP signing.
//
// Signs via the system `gpg` binary — sbt-pgp defaults to this already
// (Bouncy Castle mode is deprecated, so `useGpg` doesn't need to be set).
// `pgpPassphrase` is read from PGP_PASSPHRASE in CI; locally it falls back
// to gpg-agent/pinentry.
// ---------------------------------------------------------------------------
ThisBuild / pgpSigningKey := sys.env.get("PGP_SECRET")
ThisBuild / pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)
Global / excludeLintKeys ++= Set(pgpPassphrase, pgpSigningKey)

// ---------------------------------------------------------------------------
// PUBLISHING:
//
// Two independent destinations, each gated by its own env var:
//   - GITHUB_PACKAGES → GitHub Packages (existing CI job)
//   - CENTRAL_PORTAL → Maven Central via Sonatype Central Portal (native sbt 1.11.0+ support)
//
// IMPORTANT:
//   - DO NOT use sbt-sonatype (deprecated, legacy API retired 2025-06-30)
//   - Use native sbt support: publishSigned + sonaRelease
//   - Credentials: SONATYPE_USERNAME and SONATYPE_PASSWORD (Central Portal User Token)
//   - Namespace io.galileostd must be verified (DNS TXT record)
//
// If no env var is set → publishTo = None (safe no-op)
// ---------------------------------------------------------------------------
ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (sys.env.contains("GITHUB_PACKAGES"))
    Some("GitHub Packages".at("https://maven.pkg.github.com/maltzsama/sumeh-dq"))
  else if (sys.env.contains("CENTRAL_PORTAL")) {
    if (isSnapshot.value) Some("central-snapshots".at(centralSnapshots))
    else localStaging.value
  } else None
}

lazy val publishSkip = Def.setting(
  !sys.env.contains("GITHUB_PACKAGES") && !sys.env.contains("CENTRAL_PORTAL")
)

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
    ),
    publish / skip      := publishSkip.value,
    publishLocal / skip := false,
    Compile / doc / scalacOptions ++= docOptions("Sumeh Core").value
  )

lazy val sparkVersion = settingKey[String]("Spark version")

lazy val spark = (project in file("spark"))
  .dependsOn(core)
  .settings(
    sparkVersion := sys.props.getOrElse("spark.version", "3.5.0"),
    name := s"sumeh-spark-${sparkVersion.value.split('.').take(2).mkString(".")}", // sumeh-spark-3.5, sumeh-spark-4.0, ...
    moduleName   := name.value, // keep the "." in the artifactId (sbt would otherwise turn it into "-")
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
    publish / skip           := publishSkip.value,
    publishLocal / skip      := false,
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
    ),
    Compile / doc / scalacOptions ++= docOptions("Sumeh Spark").value
  )

lazy val flinkVersion = settingKey[String]("Flink version")

lazy val flink = (project in file("flink"))
  .dependsOn(core)
  .settings(
    flinkVersion := sys.props.getOrElse("flink.version", "1.20.0"),
    name := s"sumeh-flink-${flinkVersion.value.split('.').take(2).mkString(".")}", // sumeh-flink-1.20, sumeh-flink-2.2, ...
    moduleName         := name.value, // keep the "." in the artifactId (sbt would otherwise turn it into "-")
    scalaVersion       := "2.13.16",
    crossScalaVersions := Seq("2.12.18", "2.13.16"),
    libraryDependencies ++= Seq(
      "org.apache.flink"        % "flink-streaming-java"        % flinkVersion.value % Provided,
      "org.apache.flink"        % "flink-table-api-java"        % flinkVersion.value % Provided,
      "org.apache.flink"        % "flink-table-api-java-bridge" % flinkVersion.value % Provided,
      "org.apache.flink"        % "flink-clients"               % flinkVersion.value % Test,
      "org.scala-lang.modules" %% "scala-collection-compat"     % "2.12.0",
      "org.scalatest"          %% "scalatest"                   % scalatestVersion   % Test
    ),
    publish / skip           := publishSkip.value,
    publishLocal / skip      := false,
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
    ),
    Compile / doc / scalacOptions ++= docOptions("Sumeh Flink").value
  )

lazy val root = (project in file("."))
  .aggregate(core, spark, flink)
  .settings(
    name           := "sumeh-dq",
    publish / skip := true
  )
