package org.apache.solr.opentelemetry;

import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentracing.Tracer;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Objects;
import org.apache.solr.core.TracerConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtelTracerConfigurator extends TracerConfigurator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public Tracer getTracer() {
    log.info("Configuring tracer {}...", getClass().getName());

    setDefaultIfNotConfigured("OTEL_SERVICE_NAME", "solr");
    setDefaultIfNotConfigured("OTEL_TRACES_EXPORTER", "otlp");
    setDefaultIfNotConfigured("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    setDefaultIfNotConfigured("OTEL_TRACES_SAMPLER", "parentbased_always_on");

    System.getenv().entrySet().stream()
        .filter(e -> e.getKey().startsWith("OTEL_"))
        .forEach(entry -> log.info("Environment {}={}", entry.getKey(), entry.getValue()));

    // Need to disable the exporters for metrics and logs
    String metricsExporter = getEnvOrSysprop("OTEL_METRICS_EXPORTER");
    String logsExporter = getEnvOrSysprop("OTEL_LOGS_EXPORTER");
    if ((metricsExporter != null && !Objects.equals(metricsExporter, "none"))
        || (logsExporter != null && !Objects.equals(logsExporter, "none"))) {
      log.warn(
          "The opentelemetry module does not support METRICS or LOGS. Ignoring faulty environment setting");
    }
    System.setProperty("otel.metrics.exporter", "none");
    System.setProperty("otel.logs.exporter", "none");

    return OpenTracingShim.createTracerShim(
        AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk());
  }

  private String getEnvOrSysprop(String envName) {
    String value = System.getenv(envName);
    return value != null ? value : System.getProperty(envNameToSyspropName(envName));
  }

  private String envNameToSyspropName(String envName) {
    return envName.toLowerCase(Locale.ROOT).replace("_", ".");
  }

  private void setDefaultIfNotConfigured(String envName, String defaultValue) {
    String incomingValue = getEnvOrSysprop(envName);
    if (incomingValue == null) {
      System.setProperty(envNameToSyspropName(envName), defaultValue);
      if (log.isDebugEnabled()) {
        log.debug("Using default setting {}={}", envName, getEnvOrSysprop(envName));
      }
    }
  }
}
