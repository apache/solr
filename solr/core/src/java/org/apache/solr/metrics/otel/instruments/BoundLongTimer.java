package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import java.util.concurrent.TimeUnit;

/**
 * A timer built on top of LongHistogram. Internally, we only keep a longTimer of “start time
 * (nanos)” instead of a TimingContext object. We also register an InheritableThreadLocalProvider so
 * that if you submit work to a Solr Executor, the start‐time is inherited into the worker thread.
 */
public class BoundLongTimer extends BoundLongHistogram {

  /** ThreadLocal that holds the startTime (System.nanoTime()) for each thread. */
  private final ThreadLocal<Long> startTimeNanos = new ThreadLocal<>();

  public BoundLongTimer(LongHistogram histogram, Attributes attributes) {
    super(histogram, attributes);
  }

  /** Record the current System.nanoTime() under this thread’s ThreadLocal. */
  public void start() {
    if (startTimeNanos.get() != null) {
      throw new IllegalStateException("Timer already started on this thread");
    }
    startTimeNanos.set(System.nanoTime());
  }

  /**
   * Reads startTimeNanos, computes elapsed ms, then records into histogram. Must have called
   * start() first.
   */
  public void stop() {
    Long start = startTimeNanos.get();
    if (start == null) {
      throw new IllegalStateException("Must call start() before stop()");
    }

    try {
      long elapsedNanos = System.nanoTime() - startTimeNanos.get();
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
      histogram.record(elapsedMs, attributes);
    } finally {
      startTimeNanos.remove();
    }
  }
}
