package io.galileostd.sumeh.engine

trait Splittable[DF] {
  def split(df: DF): (DF, DF)
}
