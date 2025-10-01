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

import static org.apache.solr.client.solrj.impl.InputStreamResponseParser.STREAM_KEY;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
        Object metrics = root.get(STREAM_KEY);
        if (metrics == null || metricName == null) return null;

        try (InputStream in = (InputStream) metrics) {
          return prometheusMetricStream(in)
              .filter(line -> line.startsWith(metricName))
              .mapToInt((value) -> extractPrometheusValue(value).intValue())
              .sum();
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
    private static Double extractFromPrometheusResponse(
        NamedList<Object> root, String metricName, String labelKey, String labelValue) {
      Object metrics = root.get(STREAM_KEY);

      if (metrics == null || metricName == null) {
        return null;
      }

      try (InputStream in = (InputStream) metrics) {
        return prometheusMetricStream(in)
            .filter(line -> line.startsWith(metricName))
            .filter(
                line -> {
                  // If metric with specific labels were requested, filter by those labels
                  if (labelKey != null && labelValue != null) {
                    String expectedLabel = labelKey + "=\"" + labelValue + "\"";
                    return line.contains(expectedLabel);
                  }
                  return true;
                })
            .findFirst()
            .map(Metrics::extractPrometheusValue)
            .orElse(null);
      } catch (Exception e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Unable to read prometheus metrics output", e);
      }
    }

    public static Double extractPrometheusValue(String line) {
      String s = line.trim();

      // Get the position after the labels if they exist.
      int afterLabelsPos = s.indexOf('}');
      String tail = (afterLabelsPos >= 0) ? s.substring(afterLabelsPos + 1).trim() : s;

      // Get the metric value after the first white space and chop off anything after such as
      // exemplars from Open Metrics Format
      int whiteSpacePos = tail.indexOf(' ');
      String firstToken = (whiteSpacePos >= 0) ? tail.substring(0, whiteSpacePos) : tail;

      return Double.parseDouble(firstToken);
    }

    /** Returns a Stream of Prometheus lines for processing with filtered out comment lines */
    public static java.util.stream.Stream<String> prometheusMetricStream(InputStream inputStream) {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

      return reader
          .lines()
          .filter(line -> !isPrometheusCommentLine(line))
          .onClose(
              () -> {
                try {
                  reader.close();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
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

    // Parse metric tags into structured MetricRequest objects
    for (String tag : requestedTagNames) {
      MetricRequest request = MetricRequest.fromTag(tag);
      metricRequests.add(request);
      requestedMetricNames.add(request.metricName());

      // Pre-populate the map tag key to match its corresponding prometheus metrics
      ctx.tags.put(tag, null);
    }

    if (requestedMetricNames.isEmpty()) {
      return;
    }

    // Process prometheus stream response using structured MetricRequest objects
    try (java.util.stream.Stream<String> stream =
        SolrClientNodeStateProvider.fetchMetricStream(ctx.getNode(), ctx, requestedMetricNames)) {

      stream.forEach(
          line -> {
            String lineMetricName = extractMetricNameFromLine(line);
            Double value = Metrics.extractPrometheusValue(line);

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
          });
    }
  }

  /**
   * Extracts the metric name from a prometheus formatted metric:
   *
   * <p>With labels: solr_metrics_core_requests_total{core="demo",...}
   *
   * <p>Without labels: solr_metrics_core_requests_total 123
   */
  public static String extractMetricNameFromLine(String line) {
    int brace = line.indexOf('{');
    int space = line.indexOf(' ');
    int end;
    if (brace >= 0) {
      // Labels present in metric
      end = brace;
    } else {
      // No labels in metric
      end = space;
    }
    return line.substring(0, end);
  }

  public static String extractLabelValueFromLine(String line, String labelKey) {
    String labelPattern = labelKey + "=\"";
    if (!line.contains(labelPattern)) return null;

    int startIdx = line.indexOf(labelPattern) + labelPattern.length();
    int endIdx = line.indexOf("\"", startIdx);
    return endIdx > startIdx ? line.substring(startIdx, endIdx) : null;
  }

  /** Helper method to check if a Prometheus line should be skipped (comments or empty lines). */
  public static boolean isPrometheusCommentLine(String line) {
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
