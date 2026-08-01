// Code coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")

// Formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")

// Linting
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")

// Git versioning (optional but nice)
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.0.1")

// Plugin GPG
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")

// Publishing to Maven Central (Sonatype staging)
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
