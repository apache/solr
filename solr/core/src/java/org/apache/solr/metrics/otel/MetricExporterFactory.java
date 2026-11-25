/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.metrics.otel;

import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.util.EnvUtils;
import org.slf4j.LoggerFactory;

/**
 * Configuration of Solr's OTLP metric exporter. Each setting is resolved from its Solr-specific
 * property first, then from the equivalent standard OpenTelemetry environment variable (or {@code
 * otel.*} system property), then from a default.
 */
public interface MetricExporterFactory {

  int DEFAULT_INTERVAL_MS = 60000;

  Boolean OTLP_EXPORTER_ENABLED = resolveEnabled();

  String OTLP_EXPORTER_PROTOCOL = resolveProtocol();

  int OTLP_EXPORTER_INTERVAL = resolveInterval();

  String OTLP_EXPORTER_GRPC_ENDPOINT =
      resolveEndpoint("solr.metrics.otlpGrpcExporterEndpoint", false);

  String OTLP_EXPORTER_HTTP_ENDPOINT =
      resolveEndpoint("solr.metrics.otlpHttpExporterEndpoint", true);

  MetricExporter getExporter();

  /**
   * Also enabled by {@code OTEL_METRICS_EXPORTER} naming {@code otlp}, the only one Solr exports,
   * but only while the Solr property is unset, so an explicit {@code false} still disables export.
   */
  private static boolean resolveEnabled() {
    String solrEnabled = EnvUtils.getProperty("solr.metrics.otlpExporterEnabled");
    if (solrEnabled != null) return Boolean.parseBoolean(solrEnabled);
    String configured = otelConfig("OTEL_METRICS_EXPORTER");
    return configured != null && configured.contains("otlp");
  }

  private static String resolveProtocol() {
    String solrProtocol = EnvUtils.getProperty("solr.metrics.otlpExporterProtocol");
    if (solrProtocol != null) return solrProtocol;
    String otelProtocol = otelConfig("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL");
    if (otelProtocol == null) otelProtocol = otelConfig("OTEL_EXPORTER_OTLP_PROTOCOL");
    if (otelProtocol == null) return "http";
    // Solr's http is protobuf; any other encoding is left for the exporter factory to reject
    return otelProtocol.equals("http/protobuf") ? "http" : otelProtocol;
  }

  private static int resolveInterval() {
    String solrInterval = EnvUtils.getProperty("solr.metrics.otlpExporterInterval");
    if (solrInterval != null) {
      return parseInterval("solr.metrics.otlpExporterInterval", solrInterval);
    }
    String otelInterval = otelConfig("OTEL_METRIC_EXPORT_INTERVAL");
    return otelInterval != null
        ? parseInterval("OTEL_METRIC_EXPORT_INTERVAL", otelInterval)
        : DEFAULT_INTERVAL_MS;
  }

  /** Milliseconds, or as the OTEL SDK also accepts, a number with a ms, s, m, h or d suffix. */
  private static int parseInterval(String name, String value) {
    String trimmed = value.strip();
    int digits = 0;
    while (digits < trimmed.length() && Character.isDigit(trimmed.charAt(digits))) digits++;
    TimeUnit unit =
        switch (trimmed.substring(digits).strip()) {
          case "", "ms" -> TimeUnit.MILLISECONDS;
          case "s" -> TimeUnit.SECONDS;
          case "m" -> TimeUnit.MINUTES;
          case "h" -> TimeUnit.HOURS;
          case "d" -> TimeUnit.DAYS;
          default -> null;
        };
    try {
      if (unit != null) {
        int millis = Math.toIntExact(unit.toMillis(Long.parseLong(trimmed.substring(0, digits))));
        if (millis > 0) return millis;
      }
    } catch (NumberFormatException | ArithmeticException e) {
      // falls through to the default
    }
    LoggerFactory.getLogger(MetricExporterFactory.class)
        .warn("Ignoring invalid {}={}, using {} ms", name, value, DEFAULT_INTERVAL_MS);
    return DEFAULT_INTERVAL_MS;
  }

  /**
   * @param solrProperty the Solr-specific property for this transport, which wins if set
   * @param http whether this is the HTTP endpoint, which carries the signal path
   */
  private static String resolveEndpoint(String solrProperty, boolean http) {
    String solrEndpoint = EnvUtils.getProperty(solrProperty);
    if (solrEndpoint != null) return solrEndpoint;
    // The per-signal variable is a complete endpoint; the general one is a base to append to
    String signalEndpoint = otelConfig("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
    if (signalEndpoint != null) return signalEndpoint;
    String baseEndpoint = otelConfig("OTEL_EXPORTER_OTLP_ENDPOINT");
    if (baseEndpoint != null) {
      return http ? baseEndpoint.replaceAll("/+$", "") + "/v1/metrics" : baseEndpoint;
    }
    return http ? "http://localhost:4318/v1/metrics" : "http://localhost:4317";
  }

  /**
   * Standard OTEL configuration, as a system property if present, else an environment variable. A
   * blank value counts as unset, as it does for the SDK.
   */
  private static String otelConfig(String envName) {
    String value = System.getProperty(envName.toLowerCase(Locale.ROOT).replace('_', '.'));
    if (value == null) value = System.getenv(envName);
    return value == null || value.isBlank() ? null : value;
  }
}
