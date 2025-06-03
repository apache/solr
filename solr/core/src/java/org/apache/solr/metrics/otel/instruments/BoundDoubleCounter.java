package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleCounter;
import org.apache.solr.metrics.otel.OtelDoubleMetric;

public class BoundDoubleCounter implements OtelDoubleMetric {

  private final DoubleCounter counter;
  private final Attributes attributes;

  public BoundDoubleCounter(DoubleCounter counter, Attributes attributes) {
    this.counter = counter;
    this.attributes = attributes;
  }

  public void inc() {
    record(1.0);
  }

  @Override
  public void record(Double value) {
    counter.add(value, attributes);
  }
}
