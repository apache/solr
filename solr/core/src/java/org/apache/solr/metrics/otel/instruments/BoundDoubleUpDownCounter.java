package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import org.apache.solr.metrics.otel.OtelDoubleMetric;

public class BoundDoubleUpDownCounter implements OtelDoubleMetric {

  private final DoubleUpDownCounter upDownCounter;
  private final Attributes attributes;

  public BoundDoubleUpDownCounter(DoubleUpDownCounter upDownCounter, Attributes attributes) {
    this.upDownCounter = upDownCounter;
    this.attributes = attributes;
  }

  public void inc() {
    record(1.0);
  }

  public void dec() {
    record(-1.0);
  }

  @Override
  public void record(Double value) {
    upDownCounter.add(value, attributes);
  }
}
