package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import org.apache.solr.metrics.otel.OtelLongMetric;

public class BoundLongUpDownCounter implements OtelLongMetric {

  private final LongUpDownCounter upDownCounter;
  private final Attributes attributes;

  public BoundLongUpDownCounter(LongUpDownCounter upDownCounter, Attributes attributes) {
    this.upDownCounter = upDownCounter;
    this.attributes = attributes;
  }

  public void inc() {
    record(1L);
  }

  public void dec() {
    record(-1L);
  }

  @Override
  public void record(Long value) {
    upDownCounter.add(value, attributes);
  }
}
