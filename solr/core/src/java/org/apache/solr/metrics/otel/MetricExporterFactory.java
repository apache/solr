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
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.core.OpenTelemetryConfigurator;

/**
 * Configuration of Solr's OTLP metric exporter. Each setting is resolved from its Solr-specific
 * property first, then from the equivalent standard OpenTelemetry environment variable (or {@code
 * otel.*} system property), then from a default.
 */
public interface MetricExporterFactory {

  Boolean OTLP_EXPORTER_ENABLED = resolveEnabled();

  String OTLP_EXPORTER_PROTOCOL = resolveProtocol();

  int OTLP_EXPORTER_INTERVAL = resolveInterval();

  String OTLP_EXPORTER_GRPC_ENDPOINT =
      resolveEndpoint("solr.metrics.otlpGrpcExporterEndpoint", false);

  String OTLP_EXPORTER_HTTP_ENDPOINT =
      resolveEndpoint("solr.metrics.otlpHttpExporterEndpoint", true);

  MetricExporter getExporter();

  /**
   * Also enabled by {@code OTEL_METRICS_EXPORTER} naming {@code otlp}, the only one Solr exports.
   */
  private static boolean resolveEnabled() {
    if (Boolean.parseBoolean(EnvUtils.getProperty("solr.metrics.otlpExporterEnabled", "false"))) {
      return true;
    }
    // As the operator set it: the opentelemetry module overwrites the system property with "none"
    // to keep the autoconfigured SDK from running a metrics pipeline of its own.
    String configured = OpenTelemetryConfigurator.getConfiguredMetricsExporter();
    return configured != null && configured.contains("otlp");
  }

  private static String resolveProtocol() {
    String solrProtocol = EnvUtils.getProperty("solr.metrics.otlpExporterProtocol");
    if (solrProtocol != null) return solrProtocol;
    String otelProtocol = otelConfig("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL");
    if (otelProtocol == null) otelProtocol = otelConfig("OTEL_EXPORTER_OTLP_PROTOCOL");
    if (otelProtocol == null) return "http";
    // The OTEL spec spells the HTTP transports http/protobuf and http/json
    return otelProtocol.startsWith("http") ? "http" : otelProtocol;
  }

  private static int resolveInterval() {
    String solrInterval = EnvUtils.getProperty("solr.metrics.otlpExporterInterval");
    if (solrInterval != null) return Integer.parseInt(solrInterval);
    String otelInterval = otelConfig("OTEL_METRIC_EXPORT_INTERVAL");
    return otelInterval != null ? Integer.parseInt(otelInterval) : 60000;
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

  /** Standard OTEL configuration, as a system property if present, else an environment variable. */
  private static String otelConfig(String envName) {
    String fromProperty = System.getProperty(envName.toLowerCase(Locale.ROOT).replace('_', '.'));
    return fromProperty != null ? fromProperty : System.getenv(envName);
  }
}
