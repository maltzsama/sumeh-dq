package org.apache.spark

/**
 * Test-only bridge to `SparkContext.listenerBus`, which is `private[spark]` and therefore unreachable from a spec
 * package outside `org.apache.spark`.
 *
 * `SparkListener` events are delivered over an asynchronous bus, so an action returning does not guarantee a listener
 * has seen its events yet. Job-counting specs must drain the bus before reading their counter, or the count races the
 * bus thread and flakes on slow CI.
 */
object SparkListenerBusTestSupport {

  /**
   * Blocks until every event posted so far has been delivered to the listeners.
   *
   * @param sc The spark context whose listener bus to drain.
   * @param timeoutMillis How long to wait before giving up.
   */
  def waitUntilEmpty(sc: SparkContext, timeoutMillis: Long): Unit =
    sc.listenerBus.waitUntilEmpty(timeoutMillis)
}
