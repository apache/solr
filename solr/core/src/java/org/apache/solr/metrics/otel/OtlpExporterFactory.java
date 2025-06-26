package org.apache.solr.metrics.otel;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.util.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OtlpExporterFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final Boolean OTLP_EXPORTER_ENABLED =
      Boolean.parseBoolean(EnvUtils.getProperty("solr.otlpMetricExporterEnabled", "false"));

  public static final String OTLP_EXPORTER_PROTOCOL =
      EnvUtils.getProperty("solr.otlpMetricExporterProtocol", "grpc");

  public static final int OTLP_EXPORTER_INTERVAL =
      Integer.parseInt(EnvUtils.getProperty("solr.otlpMetricExporterInterval", "60000"));

  public static MetricExporter getExporter() {
    if (!OTLP_EXPORTER_ENABLED) {
      log.info("OTLP metric exporter is disabled.");
      return new NoopMetricExporter();
    }

    return switch (OTLP_EXPORTER_PROTOCOL) {
      case "grpc" -> OtlpGrpcMetricExporter.getDefault();
      case "http" -> OtlpHttpMetricExporter.getDefault();
      case "none" -> new NoopMetricExporter();
      default -> {
        log.warn(
            "Unknown OTLP exporter type: {}. Defaulting to NO-OP exporter.",
            OTLP_EXPORTER_PROTOCOL);
        yield new NoopMetricExporter();
      }
    };
  }
}
