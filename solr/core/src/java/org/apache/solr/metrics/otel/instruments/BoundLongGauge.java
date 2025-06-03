package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongGauge;
import org.apache.solr.metrics.otel.OtelLongMetric;

public class BoundLongGauge implements OtelLongMetric {

  private final LongGauge gauge;
  private final Attributes attributes;

  public BoundLongGauge(LongGauge gauge, Attributes attributes) {
    this.gauge = gauge;
    this.attributes = attributes;
  }

  @Override
  public void record(Long value) {
    gauge.set(value, attributes);
  }
}
