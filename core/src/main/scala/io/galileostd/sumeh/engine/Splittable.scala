package io.galileostd.sumeh.engine

/**
 * Splits a validated dataset into good and bad rows (the Bifurcation Pattern).
 *
 * Each engine provides its own implicit instance, so the split is performed by the same engine that validated the data,
 * with no reprocessing or extra scans.
 *
 * @param DF
 *   The engine-specific validated data type (e.g. `ValidatedSparkDataFrame`).
 */
trait Splittable[DF] {

  /**
   * Splits a validated dataset into good and bad rows.
   *
   * @param df
   *   The validated dataset.
   * @return
   *   A `(good, bad)` tuple; bad rows carry the quality errors.
   */
  def split(df: DF): (DF, DF)
}
