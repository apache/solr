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

package org.apache.solr.client.solrj.impl;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

/**
 * This class is responsible for fetching metrics and other attributes from a given node in Solr
 * cluster. This is a helper class that is used by {@link SolrClientNodeStateProvider}
 */
public class NodeValueFetcher {
  // well known tags
  public static final String NODE = "node";
  public static final String PORT = "port";
  public static final String HOST = "host";
  public static final String CORES = "cores";
  public static final String SYSPROP = "sysprop.";
  public static final Set<String> tags = Set.of(NODE, PORT, HOST, CORES);
  public static final Pattern hostAndPortPattern = Pattern.compile("(?:https?://)?([^:]+):(\\d+)");
  public static final String METRICS_PREFIX = "metrics:";

  /** Various well known tags that can be fetched from a node */
  public enum Metrics {
    FREEDISK("freedisk", "solr_cores_filesystem_disk_space", "type", "usable_space"),
    TOTALDISK("totaldisk", "solr_cores_filesystem_disk_space", "type", "total_space"),
    CORES("cores", "solr_cores_loaded") {
      @Override
      public Object extractResult(NamedList<Object> root) {
        Object metrics = root.get("stream");
        if (metrics == null || metricName == null) return null;

        try (InputStream in = (InputStream) metrics) {
          String[] lines = parsePrometheusOutput(in);
          int count = 0;

          for (String line : lines) {
            if (shouldSkipPrometheusLine(line) || !line.startsWith(metricName)) continue;
            count += (int) extractPrometheusValue(line);
          }
          return count;
        } catch (Exception e) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR, "Unable to read prometheus metrics output", e);
        }
      }
    },
    SYSLOADAVG("sysLoadAvg", "jvm_system_cpu_utilization_ratio");

    public final String tagName;
    public final String metricName;
    public final String labelKey;
    public final String labelValue;

    Metrics(String name, String metricName) {
      this(name, metricName, null, null);
    }

    Metrics(String name, String metricName, String labelKey, String labelValue) {
      this.tagName = name;
      this.metricName = metricName;
      this.labelKey = labelKey;
      this.labelValue = labelValue;
    }

    public Object extractResult(NamedList<Object> root) {
      return extractFromPrometheusResponse(root, metricName, labelKey, labelValue);
    }

    /**
     * Extract metric value from Prometheus response, optionally filtering by label. This
     * consolidated method handles both labeled and unlabeled metrics.
     */
    private static Long extractFromPrometheusResponse(
        NamedList<Object> root, String metricName, String labelKey, String labelValue) {
      Object metrics = root.get("stream");

      if (metrics == null || metricName == null) {
        return null;
      }

      try (InputStream in = (InputStream) metrics) {
        String[] lines = parsePrometheusOutput(in);

        for (String line : lines) {
          if (shouldSkipPrometheusLine(line) || !line.startsWith(metricName)) continue;

          // If metric with specific labels were requested, then return the metric with that label
          // and skip others
          if (labelKey != null && labelValue != null) {
            String expectedLabel = labelKey + "=\"" + labelValue + "\"";
            if (!line.contains(expectedLabel)) {
              continue;
            }
          }

          return extractPrometheusValue(line);
        }
      } catch (Exception e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Unable to read prometheus metrics output", e);
      }

      return null;
    }

    public static long extractPrometheusValue(String line) {
      line = line.trim();
      String actualValue;
      if (line.contains("}")) {
        actualValue = line.substring(line.lastIndexOf("} ") + 1);
      } else {
        actualValue = line.split(" ")[1];
      }
      return (long) Double.parseDouble(actualValue);
    }

    public static String[] parsePrometheusOutput(InputStream inputStream) throws Exception {
      String output = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
      return output.split("\n");
    }
  }

  /** Retrieve values of well known tags, as defined in {@link Metrics}. */
  private void getRemoteMetricsFromTags(
      Set<String> requestedTagNames, SolrClientNodeStateProvider.RemoteCallCtx ctx) {

    // First resolve names into actual Tags instances
    EnumSet<Metrics> requestedMetricNames = EnumSet.noneOf(Metrics.class);
    for (Metrics t : Metrics.values()) {
      if (requestedTagNames.contains(t.tagName)) {
        requestedMetricNames.add(t);
      }
    }

    if (requestedMetricNames.isEmpty()) {
      return;
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("wt", "prometheus");

    // Collect unique metric names
    Set<String> uniqueMetricNames = new HashSet<>();
    for (Metrics t : requestedMetricNames) {
      uniqueMetricNames.add(t.metricName);
    }

    // Use metric name filtering to get only the metrics we need
    params.add("name", StrUtils.join(uniqueMetricNames, ','));

    try {
      var req = new GenericSolrRequest(METHOD.GET, "/admin/metrics", SolrRequestType.ADMIN, params);
      req.setResponseParser(new InputStreamResponseParser("prometheus"));

      String baseUrl =
          ctx.zkClientClusterStateProvider.getZkStateReader().getBaseUrlForNodeName(ctx.getNode());
      SimpleSolrResponse rsp =
          ctx.cloudSolrClient.getHttpClient().requestWithBaseUrl(baseUrl, req::process);

      for (Metrics t : requestedMetricNames) {
        Object value = t.extractResult(rsp.getResponse());
        if (value != null) {
          ctx.tags.put(t.tagName, value);
        }
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void getTags(Set<String> requestedTags, SolrClientNodeStateProvider.RemoteCallCtx ctx) {
    Set<String> requestsProperties = new HashSet<>();
    Set<String> requestedMetrics = new HashSet<>();
    Set<String> requestedMetricTags = new HashSet<>();

    try {
      if (requestedTags.contains(NODE)) ctx.tags.put(NODE, ctx.getNode());
      if (requestedTags.contains(HOST)) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(ctx.getNode());
        if (hostAndPortMatcher.find()) ctx.tags.put(HOST, hostAndPortMatcher.group(1));
      }
      if (requestedTags.contains(PORT)) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(ctx.getNode());
        if (hostAndPortMatcher.find()) ctx.tags.put(PORT, hostAndPortMatcher.group(2));
      }

      if (!ctx.isNodeAlive(ctx.getNode())) {
        // Don't try to reach out to the node if we already know it is down
        return;
      }

      // Categorize requested system properties or metrics
      requestedTags.forEach(
          tag -> {
            if (tag.startsWith(SYSPROP)) {
              requestsProperties.add(tag);
            } else if (tag.startsWith(METRICS_PREFIX)) {
              requestedMetrics.add(tag);
            } else {
              requestedMetricTags.add(tag);
            }
          });

      getRemoteSystemProps(requestsProperties, ctx);
      getRemoteMetrics(requestedMetrics, ctx);
      getRemoteMetricsFromTags(requestedMetricTags, ctx);

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private void getRemoteSystemProps(
      Set<String> requestedTagNames, SolrClientNodeStateProvider.RemoteCallCtx ctx) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    try {
      SimpleSolrResponse rsp = ctx.invokeWithRetry(ctx.getNode(), "/admin/info/properties", params);
      NamedList<?> systemPropsRsp = (NamedList<?>) rsp.getResponse().get("system.properties");
      for (String requestedProperty : requestedTagNames) {
        Object property = systemPropsRsp.get(requestedProperty.substring(SYSPROP.length()));
        if (property != null) ctx.tags.put(requestedProperty, property.toString());
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error getting remote info", e);
    }
  }

  /**
   * Retrieve values that match metrics. Metrics names are structured like below:
   *
   * <p>"metrics:solr_cores_filesystem_disk_space_bytes:type=usable_space" or
   * "metrics:jvm_cpu_count" Metrics are fetched from /admin/metrics and parsed using shared utility
   * methods.
   */
  private void getRemoteMetrics(
      Set<String> requestedTagNames, SolrClientNodeStateProvider.RemoteCallCtx ctx) {
    Set<MetricRequest> metricRequests = new HashSet<>();
    Set<String> requestedMetricNames = new HashSet<>();
    Set<String> labelsFilter = new HashSet<>();

    // Parse metric tags into structured MetricRequest objects
    for (String tag : requestedTagNames) {
      try {
        MetricRequest request = MetricRequest.fromTag(tag);
        metricRequests.add(request);
        requestedMetricNames.add(request.metricName());

        if (request.hasLabelFilter()) {
          labelsFilter.add(request.kvLabel());
        }

        // Pre-populate the map tag key to match its corresponding prometheus metrics
        ctx.tags.put(tag, null);
      } catch (IllegalArgumentException e) {
        // Skip invalid metric tags
        continue;
      }
    }

    if (requestedMetricNames.isEmpty()) {
      return;
    }

    // Fetch all prometheus metrics from requested prometheus metric names
    String[] lines =
        SolrClientNodeStateProvider.fetchBatchedMetric(ctx.getNode(), ctx, requestedMetricNames);

    // Process prometheus response using structured MetricRequest objects
    for (String line : lines) {
      if (shouldSkipPrometheusLine(line)) continue;

      String lineMetricName = extractMetricNameFromLine(line);
      Long value = Metrics.extractPrometheusValue(line);

      // Find matching MetricRequest(s) for this line
      for (MetricRequest request : metricRequests) {
        if (!request.metricName().equals(lineMetricName)) {
          continue; // Metric name doesn't match
        }

        // Skip metric if it does not contain requested label
        if (request.hasLabelFilter() && !line.contains(request.kvLabel())) {
          continue;
        }

        // Found a match - store the value using the original tag
        ctx.tags.put(request.originalTag(), value);
        break; // Move to next line since we found our match
      }
    }
  }

  public static String extractMetricNameFromLine(String line) {
    if (line.contains("{")) {
      return line.substring(0, line.indexOf("{"));
    } else {
      return line.split(" ")[0];
    }
  }

  public static String extractLabelValueFromLine(String line, String labelKey) {
    String labelPattern = labelKey + "=\"";
    if (!line.contains(labelPattern)) return null;

    int startIdx = line.indexOf(labelPattern) + labelPattern.length();
    int endIdx = line.indexOf("\"", startIdx);
    return endIdx > startIdx ? line.substring(startIdx, endIdx) : null;
  }

  /** Helper method to check if a Prometheus line should be skipped (comments or empty lines). */
  public static boolean shouldSkipPrometheusLine(String line) {
    return line.startsWith("#") || line.trim().isEmpty();
  }

  /**
   * Represents a structured metric request instead of using string parsing. This eliminates the
   * need for complex string manipulation and parsing.
   */
  record MetricRequest(String metricName, String kvLabel, String originalTag) {

    /**
     * Create a MetricRequest from a metric tag string like "metrics:jvm_cpu_count" or
     * "metrics:solr_cores_filesystem_disk_space_bytes:type=usable_space"
     */
    public static MetricRequest fromTag(String tag) {
      String[] parts = tag.split(":");
      if (parts.length < 2) {
        throw new IllegalArgumentException("Invalid metric tag format: " + tag);
      }

      String metricName = parts[1];
      String labelFilter = parts.length > 2 ? parts[2] : null;

      return new MetricRequest(metricName, labelFilter, tag);
    }

    public boolean hasLabelFilter() {
      return kvLabel != null;
    }
  }
}
