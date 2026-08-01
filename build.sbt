ThisBuild / organization := "io.galileostd"
ThisBuild / version      := "0.1.0-SNAPSHOT"

ThisBuild / Test / parallelExecution := false

lazy val core = (project in file("core"))
  .settings(
    name               := "sumeh-core",
    scalaVersion       := "2.13.14",
    crossScalaVersions := Seq("2.12.18", "2.13.14"),
    libraryDependencies ++= Seq(
      "com.lihaoyi"   %% "upickle"   % "3.1.0",
      "org.slf4j"      % "slf4j-api" % "2.0.9",
      "org.scalatest" %% "scalatest" % "3.2.17" % Test
    )
  )

lazy val sparkVersion = settingKey[String]("Spark version")

lazy val spark = (project in file("spark"))
  .dependsOn(core)
  .settings(
    name         := "sumeh-spark",
    scalaVersion := "2.13.14",
    sparkVersion := sys.props.getOrElse("spark.version", "4.1.2"),
    crossScalaVersions := {
      if (sparkVersion.value.startsWith("3.")) Seq("2.12.18", "2.13.14")
      else Seq("2.13.14")
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
    scalaVersion       := "2.13.14",
    crossScalaVersions := Seq("2.12.18", "2.13.14"),
    flinkVersion       := sys.props.getOrElse("flink.version", "2.2.0"),
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % flinkVersion.value % Provided,
      "org.apache.flink" % "flink-table-api-java"  % flinkVersion.value % Provided,
      "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion.value % Provided,
      "org.scalatest"   %% "scalatest"            % "3.2.17" % Test
    )
  )

lazy val root = (project in file("."))
  .aggregate(core, spark, flink)
  .settings(
    name           := "sumeh-dq",
    publish / skip := true
  )
