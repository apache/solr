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
package org.apache.solr.response;

import static org.apache.solr.handler.admin.MetricsHandler.OPEN_METRICS_WT;
import static org.apache.solr.handler.admin.MetricsHandler.PROMETHEUS_METRICS_WT;

import java.util.Map;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.handler.admin.api.ReplicationAPIBase;

/**
 * Essential response writers always available regardless of core configuration.
 *
 * <p>Used by node/container-level requests that have no associated {@link
 * org.apache.solr.core.SolrCore}.
 *
 * <p>For the full set of response writers (csv, geojson, graphml, smile, etc.), use {@link
 * org.apache.solr.core.SolrCore}'s response writer registry.
 */
public class BuiltInResponseWriters {

  private BuiltInResponseWriters() {
    // Prevent instantiation
  }

  /**
   * Built-in response writers that are always available.
   *
   * <p>Contains only essential formats needed by admin APIs and core functionality:
   *
   * <ul>
   *   <li><b>javabin</b> - Binary format, efficient for SolrJ clients
   *   <li><b>json/standard</b> - JSON format, default for most requests
   *   <li><b>xml</b> - XML format, provides backward compatibility
   *   <li><b>prometheus/openmetrics</b> - Required by metrics endpoints
   *   <li><b>filestream</b> - File streaming for replication and exports
   * </ul>
   */
  private static final Map<String, QueryResponseWriter> BUILTIN_WRITERS;

  static {
    // Initialize built-in writers that are always available
    JacksonJsonWriter jsonWriter = new JacksonJsonWriter();
    PrometheusResponseWriter prometheusWriter = new PrometheusResponseWriter();

    BUILTIN_WRITERS =
        Map.of(
            CommonParams.JAVABIN,
            new JavaBinResponseWriter(),
            CommonParams.JSON,
            jsonWriter,
            "standard",
            jsonWriter, // Alias for JSON
            "xml",
            new XMLResponseWriter(),
            PROMETHEUS_METRICS_WT,
            prometheusWriter,
            OPEN_METRICS_WT,
            prometheusWriter,
            ReplicationAPIBase.FILE_STREAM,
            new FileStreamResponseWriter());
  }

  /**
   * Gets a built-in response writer.
   *
   * <p>Built-in writers are always available and provide essential formats needed by admin APIs and
   * core functionality. They do not depend on core configuration or ImplicitPlugins.json settings.
   *
   * <p>If the requested writer is not available, returns the "standard" (JSON) writer as a
   * fallback. This ensures requests always get a valid response format.
   *
   * @param writerName the writer name (e.g., "json", "xml", "javabin"), or null for default
   * @return the response writer, never null (returns "standard"/JSON if not found)
   */
  public static QueryResponseWriter getWriter(String writerName) {
    if (writerName == null || writerName.isEmpty()) {
      return BUILTIN_WRITERS.get("standard");
    }
    return BUILTIN_WRITERS.getOrDefault(writerName, BUILTIN_WRITERS.get("standard"));
  }
}
