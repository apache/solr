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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static methods to proxy calls to an Admin (GET) API to other nodes in the cluster and return a
 * combined response
 */
public class AdminHandlersProxy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_NODES = "nodes";
  private static final int PROMETHEUS_PROXY_THREAD_POOL_SIZE = 8;
  private static final long PROMETHEUS_FETCH_TIMEOUT_SECONDS = 10;

  // Proxy this request to a different remote node if 'node' parameter is provided
  public static boolean maybeProxyToNodes(
      SolrQueryRequest req, SolrQueryResponse rsp, CoreContainer container)
      throws IOException, SolrServerException, InterruptedException {
    String nodeNames = req.getParams().get(PARAM_NODES);
    if (nodeNames == null || nodeNames.isEmpty()) {
      return false; // local request
    }

    if (!container.isZooKeeperAware()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Parameter " + PARAM_NODES + " only supported in Cloud mode");
    }

    // Resolve the set of nodes to query
    Set<String> nodes = resolveNodes(nodeNames, container);
    String pathStr = req.getPath();

    if (log.isDebugEnabled()) {
      log.debug("{} parameter {} specified on {} request", PARAM_NODES, nodeNames, pathStr);
    }

    ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
    params.remove(PARAM_NODES);

    // Check if response format is Prometheus/OpenMetrics
    String wt = params.get("wt");
    boolean isPrometheusFormat = "prometheus".equals(wt) || "openmetrics".equals(wt);

    if (isPrometheusFormat) {
      // Handle Prometheus format: fetch from nodes and merge MetricSnapshots
      handlePrometheusFormat(nodes, pathStr, params, container, rsp);
    } else {
      // Handle other formats (JSON, etc.): use existing NamedList behavior
      handleNamedListFormat(nodes, pathStr, params, container.getZkController(), rsp);
    }

    return true;
  }

  /** Handle non-Prometheus formats using the existing NamedList approach. */
  private static void handleNamedListFormat(
      Set<String> nodes,
      String pathStr,
      SolrParams params,
      ZkController zkController,
      SolrQueryResponse rsp)
      throws IOException, SolrServerException, InterruptedException {

    Map<String, Future<NamedList<Object>>> responses = new LinkedHashMap<>();
    for (String node : nodes) {
      responses.put(node, callRemoteNode(node, pathStr, params, zkController));
    }

    for (Map.Entry<String, Future<NamedList<Object>>> entry : responses.entrySet()) {
      try {
        NamedList<Object> resp = entry.getValue().get(10, TimeUnit.SECONDS);
        rsp.add(entry.getKey(), resp);
      } catch (ExecutionException ee) {
        log.warn("Exception when fetching result from node {}", entry.getKey(), ee);
      } catch (TimeoutException te) {
        log.warn("Timeout when fetching result from node {}", entry.getKey(), te);
      }
    }
    if (log.isDebugEnabled()) {
      log.debug(
          "Fetched response from {} nodes: {}", responses.keySet().size(), responses.keySet());
    }
  }

  /** Makes a remote request asynchronously. */
  public static CompletableFuture<NamedList<Object>> callRemoteNode(
      String nodeName, String uriPath, SolrParams params, ZkController zkController)
      throws IOException, SolrServerException {
    log.debug("Proxying {} request to node {}", uriPath, nodeName);
    URI baseUri = URI.create(zkController.zkStateReader.getBaseUrlForNodeName(nodeName));
    SolrRequest<?> proxyReq = new GenericSolrRequest(SolrRequest.METHOD.GET, uriPath, params);

    // Set response parser based on wt parameter to ensure correct format is used
    String wt = params.get("wt");
    if ("prometheus".equals(wt) || "openmetrics".equals(wt)) {
      proxyReq.setResponseParser(new InputStreamResponseParser(wt));
    }

    return zkController
        .getCoreContainer()
        .getDefaultHttpSolrClient()
        .requestWithBaseUrl(baseUri.toString(), c -> c.requestAsync(proxyReq));
  }

  /**
   * Resolve node names from the "nodes" parameter into a set of live node names.
   *
   * @param nodeNames the value of the "nodes" parameter ("all" or comma-separated node names)
   * @param container the CoreContainer
   * @return set of resolved node names
   * @throws SolrException if node format is invalid or node is not in cluster
   */
  private static Set<String> resolveNodes(String nodeNames, CoreContainer container) {
    Set<String> liveNodes =
        container.getZkController().zkStateReader.getClusterState().getLiveNodes();

    if (nodeNames.equals("all")) {
      log.debug("All live nodes requested");
      return liveNodes;
    }

    Set<String> nodes = new HashSet<>(Arrays.asList(nodeNames.split(",")));
    for (String nodeName : nodes) {
      if (!nodeName.matches("^[^/:]+:\\d+_[\\w/]+$")) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Parameter " + PARAM_NODES + " has wrong format");
      }
      if (!liveNodes.contains(nodeName)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Requested node " + nodeName + " is not part of cluster");
      }
    }
    log.debug("Nodes requested: {}", nodes);
    return nodes;
  }

  /** Handle Prometheus format by fetching from nodes and merging text responses. */
  private static void handlePrometheusFormat(
      Set<String> nodes,
      String pathStr,
      SolrParams params,
      CoreContainer container,
      SolrQueryResponse rsp)
      throws IOException, SolrServerException, InterruptedException {

    // Bounded parallel executor - max concurrent fetches using Solr's ExecutorUtil
    ExecutorService executor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            PROMETHEUS_PROXY_THREAD_POOL_SIZE, // corePoolSize
            PROMETHEUS_PROXY_THREAD_POOL_SIZE, // maximumPoolSize
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new SolrNamedThreadFactory("metricsProxyExecutor"));

    try {
      // Submit all fetches at once - executor will handle bounded parallelism
      Map<String, Future<String>> futures = new LinkedHashMap<>();
      for (String node : nodes) {
        futures.put(node, fetchNodePrometheusTextAsync(executor, node, pathStr, params, container));
      }

      // Collect all Prometheus text responses
      StringBuilder mergedText = new StringBuilder();
      for (Map.Entry<String, Future<String>> entry : futures.entrySet()) {
        try {
          String prometheusText =
              entry.getValue().get(PROMETHEUS_FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          if (prometheusText != null && !prometheusText.isEmpty()) {
            // Inject node label into each metric line
            String labeledText = injectNodeLabelIntoText(prometheusText, entry.getKey());
            mergedText.append(labeledText);
          }
        } catch (ExecutionException ee) {
          log.warn("Exception when fetching Prometheus result from node {}", entry.getKey(), ee);
        } catch (TimeoutException te) {
          log.warn("Timeout when fetching Prometheus result from node {}", entry.getKey(), te);
        }
      }

      // Store the merged text in response - will be written as-is
      rsp.add("prometheusText", mergedText.toString());

    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }

  /** Fetch Prometheus text from a remote node asynchronously. */
  private static Future<String> fetchNodePrometheusTextAsync(
      ExecutorService executor,
      String nodeName,
      String pathStr,
      SolrParams params,
      CoreContainer container) {

    return executor.submit(
        () -> {
          try {
            ZkController zkController = container.getZkController();
            if (zkController == null) {
              log.warn("ZkController not available for node {}", nodeName);
              return null;
            }

            // Ensure wt=prometheus is set for inter-node requests
            ModifiableSolrParams prometheusParams = new ModifiableSolrParams(params);
            if (!prometheusParams.get("wt", "").equals("prometheus")) {
              prometheusParams.set("wt", "prometheus");
            }

            // Use existing callRemoteNode() to fetch metrics
            CompletableFuture<NamedList<Object>> future =
                callRemoteNode(nodeName, pathStr, prometheusParams, zkController);

            NamedList<Object> response =
                future.get(PROMETHEUS_FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Response has "stream" key with InputStream when using InputStreamResponseParser
            Object streamObj = response.get("stream");
            if (streamObj == null) {
              log.warn("No stream in response from node {}", nodeName);
              return null;
            }
            if (!(streamObj instanceof InputStream)) {
              log.warn(
                  "Invalid stream type in response from node {}: {}",
                  nodeName,
                  streamObj.getClass());
              return null;
            }
            try (InputStream stream = (InputStream) streamObj) {
              return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            }

          } catch (Exception e) {
            log.warn("Error fetching metrics from node {}", nodeName, e);
            return null;
          }
        });
  }

  /**
   * Inject node="nodeName" label into Prometheus text format. Each metric line gets the node label
   * added.
   */
  private static String injectNodeLabelIntoText(String prometheusText, String nodeName) {
    StringBuilder result = new StringBuilder();
    String[] lines = prometheusText.split("\n");

    for (String line : lines) {
      // Skip comments and empty lines
      if (line.startsWith("#") || line.trim().isEmpty()) {
        result.append(line).append("\n");
        continue;
      }

      // Metric line format: metric_name{labels} value timestamp
      // or: metric_name value timestamp
      int braceIndex = line.indexOf('{');
      int spaceIndex = line.indexOf(' ');

      if (braceIndex == -1) {
        // No labels, add node label before value
        // Format: metric_name value timestamp
        if (spaceIndex > 0) {
          String metricName = line.substring(0, spaceIndex);
          String valueAndTime = line.substring(spaceIndex);
          result
              .append(metricName)
              .append("{node=\"")
              .append(nodeName)
              .append("\"}")
              .append(valueAndTime)
              .append("\n");
        } else {
          result.append(line).append("\n");
        }
      } else {
        // Has labels, inject node label
        // Format: metric_name{existing_labels} value timestamp
        int closeBraceIndex = line.indexOf('}', braceIndex);
        if (closeBraceIndex > braceIndex) {
          String before = line.substring(0, closeBraceIndex);
          String after = line.substring(closeBraceIndex);

          // Add comma if there are existing labels
          String separator = (closeBraceIndex > braceIndex + 1) ? "," : "";

          result
              .append(before)
              .append(separator)
              .append("node=\"")
              .append(nodeName)
              .append("\"")
              .append(after)
              .append("\n");
        } else {
          result.append(line).append("\n");
        }
      }
    }

    return result.toString();
  }
}
