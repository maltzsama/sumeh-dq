package io.galileostd.sumeh.validation

sealed trait ValidationStatus
object ValidationStatus {
  case object PASS extends ValidationStatus {
    override def toString = "PASS"
  }

  case object FAIL extends ValidationStatus {
    override def toString = "FAIL"
  }

  case object ERROR extends ValidationStatus {
    override def toString = "ERROR"
  }

  case object SKIPPED extends ValidationStatus {
    override def toString = "SKIPPED"
  }

}
