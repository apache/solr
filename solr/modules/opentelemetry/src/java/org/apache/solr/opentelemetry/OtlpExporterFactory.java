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
package org.apache.solr.opentelemetry;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.lang.invoke.MethodHandles;
import org.apache.solr.metrics.otel.MetricExporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating OpenTelemetry OTLP metric exporters and its configuration properties.
 *
 * @see io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter
 * @see io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
 */
public class OtlpExporterFactory implements MetricExporterFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public MetricExporter getExporter() {
    if (!OTLP_EXPORTER_ENABLED) {
      log.info("OTLP metric exporter is disabled.");
      return null;
    }

    return switch (OTLP_EXPORTER_PROTOCOL) {
      case "grpc" -> OtlpGrpcMetricExporter.getDefault().toBuilder()
          .setEndpoint(OTLP_EXPORTER_GRPC_ENDPOINT)
          .build();
      case "http" -> OtlpHttpMetricExporter.getDefault().toBuilder()
          .setEndpoint(OTLP_EXPORTER_HTTP_ENDPOINT)
          .build();
      case "none" -> null;
      default -> {
        log.warn(
            "Unknown OTLP exporter type: {}. Disabling metric exporter", OTLP_EXPORTER_PROTOCOL);
        yield null;
      }
    };
  }
}
