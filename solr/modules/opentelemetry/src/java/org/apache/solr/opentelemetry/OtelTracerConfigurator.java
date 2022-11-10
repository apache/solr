package org.apache.solr.opentelemetry;

import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentracing.Tracer;
import org.apache.solr.core.TracerConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class OtelTracerConfigurator extends TracerConfigurator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public Tracer getTracer() {
    // TODO: Find better way to set defaults
    System.setProperty("OTEL_SERVICE_NAME", "solr");
    System.setProperty("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    System.setProperty("OTEL_TRACES_SAMPLER", "parentbased_always_on");
    System.setProperty("OTEL_TRACES_EXPORTER", "otlp");
    // Need to disable the exporters for metrics and logs
    System.setProperty("OTEL_METRICS_EXPORTER", "none");
    System.setProperty("OTEL_LOGS_EXPORTER", "none");
    log.info("Configuring tracer {}...", getClass().getName());
    return OpenTracingShim.createTracerShim(
        AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk());
  }
}
