package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import org.apache.solr.metrics.otel.OtelLongMetric;

public class BoundLongCounter implements OtelLongMetric {

  private final LongCounter baseCounter;
  private final io.opentelemetry.api.common.Attributes attributes;

  public BoundLongCounter(LongCounter baseCounter, Attributes attributes) {
    this.baseCounter = baseCounter;
    this.attributes = attributes;
  }

  public void inc() {
    record(1L);
  }

  @Override
  public void record(Long value) {
    baseCounter.add(value, attributes);
  }
}
