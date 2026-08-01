package io.galileostd.sumeh.exception

/**
 * Base exception for Sumeh runtime errors.
 *
 * Used for config-level failures not tied to a specific engine (e.g. an unknown `check_type`), so callers can catch a
 * single type regardless of the Spark/Flink runtime.
 *
 * Args: message: Human-readable error description. cause: Optional underlying cause.
 */
class SumehException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
