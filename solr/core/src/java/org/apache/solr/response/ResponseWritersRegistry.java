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

import static org.apache.solr.util.stats.MetricUtils.OPEN_METRICS_WT;
import static org.apache.solr.util.stats.MetricUtils.PROMETHEUS_METRICS_WT;

import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.api.ReplicationAPIBase;

/**
 * Essential response writers always available regardless of core configuration.
 *
 * <p>Used by node/container-level requests that have no associated {@link
 * org.apache.solr.core.SolrCore}.
 *
 * <p>For the full set of response writers see {@link org.apache.solr.core.SolrCore}'s response
 * writer registry.
 */
public class ResponseWritersRegistry {

  private ResponseWritersRegistry() {
    // Prevent instantiation
  }

  private static final Map<String, QueryResponseWriter> BUILTIN_WRITERS;

  static {
    // Initialize built-in writers that are always available
    JacksonJsonWriter jsonWriter = new JacksonJsonWriter();
    PrometheusResponseWriter prometheusWriter = new PrometheusResponseWriter();

    BUILTIN_WRITERS =
        Map.ofEntries(
            Map.entry(CommonParams.JAVABIN, new JavaBinResponseWriter()),
            Map.entry(CommonParams.JSON, jsonWriter),
            Map.entry("xml", new XMLResponseWriter()),
            Map.entry("raw", new RawResponseWriter()),
            Map.entry(PROMETHEUS_METRICS_WT, prometheusWriter),
            Map.entry(OPEN_METRICS_WT, prometheusWriter),
            Map.entry(ReplicationAPIBase.FILE_STREAM, new FileStreamResponseWriter()));
  }

  /**
   * Gets a built-in response writer.
   *
   * <p>Built-in writers are always available and provide essential formats needed by admin APIs and
   * core functionality. They do not depend on core configuration or ImplicitPlugins.json settings.
   *
   * <p>If the requested writer is not available, returns the JSON writer as a fallback. This
   * ensures requests always get a valid response format.
   *
   * @param writerName the writer name (e.g., "json", "xml", "javabin"), or null for default
   * @return the response writer, never null (returns JSON if not found)
   */
  public static QueryResponseWriter getWriter(String writerName) {
    if (writerName == null || writerName.isEmpty()) {
      writerName = CommonParams.JSON;
    }
    return BUILTIN_WRITERS.get(writerName);
  }

  /**
   * Gets a response writer, trying the core's registry first, then falling back to built-in
   * writers. This is the unified entry point for all writer resolution.
   *
   * <p>Resolution order:
   *
   * <ol>
   *   <li>If core is provided, check core's writer registry
   *   <li>If not found in core (or no core), check built-in writers
   *   <li>If writer name is explicitly specified but not found anywhere, throw exception
   *   <li>If writer name is null/empty, return default (JSON)
   * </ol>
   *
   * @param writerName the writer name (e.g., "json", "xml", "javabin"), or null for default
   * @param core the SolrCore to check first, or null for node-level requests
   * @return the response writer, never null
   * @throws SolrException if an explicitly requested writer type is not found
   */
  public static QueryResponseWriter getWriter(String writerName, SolrCore core) {
    QueryResponseWriter writer = null;

    // Try core registry first if available
    if (core != null) {
      writer = core.getQueryResponseWriter(writerName);
    }

    // If not found and writer is explicitly requested, validate it exists in built-in
    if (writer == null) {
      if (!hasWriter(writerName)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Unknown response writer type: " + writerName);
      } else {
        writer = getWriter(writerName);
      }
    }
    return writer;
  }

  /**
   * Checks if a writer with the given name exists in the built-in writers.
   *
   * @param writerName the writer name to check
   * @return true if the writer exists, false otherwise
   */
  public static boolean hasWriter(String writerName) {
    if (writerName == null || writerName.isEmpty()) {
      return true; // null/empty is valid, will use default
    }
    return BUILTIN_WRITERS.containsKey(writerName);
  }

  /**
   * Gets all built-in response writers.
   *
   * @return immutable map of all built-in writers
   */
  public static Map<String, QueryResponseWriter> getAllWriters() {
    return BUILTIN_WRITERS;
  }
}
