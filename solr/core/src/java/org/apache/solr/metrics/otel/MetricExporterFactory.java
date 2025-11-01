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
import org.apache.solr.common.util.EnvUtils;

public interface MetricExporterFactory {

  public static final Boolean OTLP_EXPORTER_ENABLED =
      Boolean.parseBoolean(EnvUtils.getProperty("solr.metrics.otlpExporterEnabled", "false"));

  public static final String OTLP_EXPORTER_PROTOCOL =
      EnvUtils.getProperty("solr.metrics.otlpExporterProtocol", "grpc");

  public static final int OTLP_EXPORTER_INTERVAL =
      Integer.parseInt(EnvUtils.getProperty("solr.metrics.otlpExporterInterval", "60000"));

  public static final String OTLP_EXPORTER_GRPC_ENDPOINT =
      EnvUtils.getProperty("solr.metrics.otlpGrpcExporterEndpoint", "http://localhost:4317");

  public static final String OTLP_EXPORTER_HTTP_ENDPOINT =
      EnvUtils.getProperty(
          "solr.metrics.otlpHttpExporterEndpoint", "http://localhost:4318/v1/metrics");

  MetricExporter getExporter();
}
