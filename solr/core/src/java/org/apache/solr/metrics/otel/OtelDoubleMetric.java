package org.apache.solr.metrics.otel;

@FunctionalInterface
public interface OtelDoubleMetric {
  void record(Double value);
}
