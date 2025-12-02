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

import static org.apache.solr.client.solrj.response.InputStreamResponseParser.STREAM_KEY;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
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
  public static final String SYSPROP_PREFIX = "sysprop.";
  public static final Set<String> tags =
      Set.of(
          NODE,
          PORT,
          HOST,
          CORES,
          Metrics.FREEDISK.tagName,
          Metrics.TOTALDISK.tagName,
          Metrics.SYSLOADAVG.tagName);
  public static final Pattern hostAndPortPattern = Pattern.compile("(?:https?://)?([^:]+):(\\d+)");
  public static final String METRICS_PREFIX = "metrics:";

  /** Various well known tags that can be fetched from a node */
  public enum Metrics {
    FREEDISK("freedisk", "solr_disk_space_megabytes", "type", "usable_space"),
    TOTALDISK("totaldisk", "solr_disk_space_megabytes", "type", "total_space"),
    CORES("cores", "solr_cores_loaded") {
      @Override
      public Object extractFromPrometheus(List<String> prometheusLines) {
        return prometheusLines.stream()
            .filter(line -> extractMetricNameFromLine(line).equals(metricName))
            .mapToInt((value) -> extractPrometheusValue(value).intValue())
            .sum();
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

    /**
     * Extract metric value from Prometheus response lines, optionally filtering by label. This
     * consolidated method handles both labeled and unlabeled metrics. This method assumes 1 metric,
     * so will get the first metricName it sees with associated label and value.
     */
    public Object extractFromPrometheus(List<String> prometheusLines) {
      return prometheusLines.stream()
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
    }

    /**
     * Extracts the numeric value from a Prometheus metric line. Sample inputs: - With labels:
     * solr_metrics_core_requests_total{core="demo",...} 123.0 - Without labels:
     * solr_metrics_core_requests_total 123.0 - With exemplars:
     * solr_metrics_core_requests_total{core="demo"} 123.0 # {trace_id="abc123"} 2.0 1234567890
     */
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
    public static Stream<String> prometheusMetricStream(InputStream inputStream) {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

      // Prometheus comment or empty lines are filtered out
      return reader.lines().filter(line -> !line.startsWith("#") || line.isBlank());
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
      NamedList<Object> response = ctx.httpSolrClient().requestWithBaseUrl(baseUrl, req, null);

      // TODO come up with a better solution to stream this response instead of loading in memory
      try (InputStream prometheusStream = (InputStream) response.get(STREAM_KEY)) {
        List<String> prometheusLines = Metrics.prometheusMetricStream(prometheusStream).toList();
        for (Metrics t : requestedMetricNames) {
          Object value = t.extractFromPrometheus(prometheusLines);
          if (value != null) {
            ctx.tags.put(t.tagName, value);
          }
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
            if (tag.startsWith(SYSPROP_PREFIX)) {
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
        Object property = systemPropsRsp.get(requestedProperty.substring(SYSPROP_PREFIX.length()));
        if (property != null) ctx.tags.put(requestedProperty, property.toString());
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error getting remote info", e);
    }
  }

  /**
   * Retrieve values that match metrics. Metrics names are structured like below:
   *
   * <p>"metrics:solr_disk_space_megabytes:type=usable_space" or "metrics:jvm_cpu_count". Metrics
   * are fetched from /admin/metrics and parsed using shared utility methods.
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
    SolrClientNodeStateProvider.processMetricStream(
        ctx.getNode(),
        ctx,
        requestedMetricNames,
        (line) -> {
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

  /**
   * Extracts the metric name from a prometheus formatted metric:
   *
   * <p>- With labels: solr_metrics_core_requests_total{core="demo",...} 123.0 - Without labels:
   * solr_metrics_core_requests_total 123.0 - With exemplars:
   * solr_metrics_core_requests_total{core="demo"} 123.0 # {trace_id="abc123"} 2.0 1234567890 The
   * sample inputs would return solr_metrics_core_requests_total
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

  /**
   * Represents a structured metric request instead of using string parsing. This eliminates the
   * need for complex string manipulation and parsing.
   */
  record MetricRequest(String metricName, String kvLabel, String originalTag) {

    /**
     * Create a MetricRequest from a metric tag string like "metrics:jvm_cpu_count" or
     * "metrics:solr_disk_space_megabytes:type=usable_space"
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
