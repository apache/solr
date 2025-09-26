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
import org.apache.solr.metrics.otel.NoopMetricExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating OpenTelemetry OTLP metric exporters and its configuration properties.
 *
 * @see io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter
 * @see io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
 * @see NoopMetricExporter
 */
public class OtlpExporterFactory implements MetricExporterFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public MetricExporter getExporter() {
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
