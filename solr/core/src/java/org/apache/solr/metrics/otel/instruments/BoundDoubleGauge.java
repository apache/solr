package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGauge;
import org.apache.solr.metrics.otel.OtelDoubleMetric;

public class BoundDoubleGauge implements OtelDoubleMetric {
  private final DoubleGauge gauge;
  private final Attributes attributes;

  public BoundDoubleGauge(DoubleGauge gauge, Attributes attributes) {
    this.gauge = gauge;
    this.attributes = attributes;
  }

  @Override
  public void record(Double value) {
    gauge.set(value, attributes);
  }
}
