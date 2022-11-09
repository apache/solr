package org.apache.solr.otel;

import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class OtlpGrpcTracerConfigurator extends BaseOtelTracerConfigurator {
  private static final SpanExporter exporter = OtlpGrpcSpanExporter.getDefault();

  @Override
  protected SpanExporter getExporter() {
    return exporter;
  }
}
