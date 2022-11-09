package org.apache.solr.otel;

import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentracing.Tracer;
import org.apache.solr.core.TracerConfigurator;

public class OtelTracerConfigurator extends TracerConfigurator {
  @Override
  public Tracer getTracer() {
    // TODO: Find better way to set defaults
    System.setProperty("OTEL_SERVICE_NAME", "solr");
    System.setProperty("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    System.setProperty("OTEL_TRACES_SAMPLER", "parentbased_always_on"); //
    System.setProperty("OTEL_TRACES_EXPORTER", "otlp"); // zipkin, jaeger
    return OpenTracingShim.createTracerShim(
        AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk());
  }
}
