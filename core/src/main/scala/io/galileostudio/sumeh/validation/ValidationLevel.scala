package io.galileostudio.sumeh.validation

// Enums

sealed trait ValidationLevel
object ValidationLevel {
  case object ROW   extends ValidationLevel { override def toString = "ROW"   }
  case object TABLE extends ValidationLevel { override def toString = "TABLE" }

  def fromString(s: String): ValidationLevel = s.toUpperCase match {
    case "ROW"   => ROW
    case "TABLE" => TABLE
    case other   => throw new IllegalArgumentException(s"Unknown ValidationLevel: $other")
  }
}
