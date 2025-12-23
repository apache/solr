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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.JavaBinResponseParser;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The <em>real</em> {@link NodeStateProvider}, which communicates with Solr via SolrJ. */
public class SolrClientNodeStateProvider implements NodeStateProvider, MapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CloudHttp2SolrClient solrClient;
  protected final Map<String, Map<String, Map<String, List<Replica>>>>
      nodeVsCollectionVsShardVsReplicaInfo = new HashMap<>();

  @SuppressWarnings({"rawtypes"})
  private Map<String, Map> nodeVsTags = new HashMap<>();

  public SolrClientNodeStateProvider(CloudSolrClient solrClient) {
    if (!(solrClient instanceof CloudHttp2SolrClient)) {
      throw new IllegalArgumentException(
          "The passed-in CloudSolrClient must be a " + CloudHttp2SolrClient.class);
    }
    this.solrClient = (CloudHttp2SolrClient) solrClient;
    try {
      readReplicaDetails();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  protected ClusterStateProvider getClusterStateProvider() {
    return solrClient.getClusterStateProvider();
  }

  protected void readReplicaDetails() throws IOException {
    ClusterStateProvider clusterStateProvider = getClusterStateProvider();
    ClusterState clusterState = clusterStateProvider.getClusterState();
    if (clusterState == null) { // zkStateReader still initializing
      return;
    }
    clusterState
        .collectionStream()
        .forEach(
            coll ->
                coll.forEachReplica(
                    (shard, replica) -> {
                      Map<String, Map<String, List<Replica>>> nodeData =
                          nodeVsCollectionVsShardVsReplicaInfo.computeIfAbsent(
                              replica.getNodeName(), k -> new HashMap<>());
                      Map<String, List<Replica>> collData =
                          nodeData.computeIfAbsent(coll.getName(), k -> new HashMap<>());
                      List<Replica> replicas =
                          collData.computeIfAbsent(shard, k -> new ArrayList<>());
                      replicas.add((Replica) replica.clone());
                    }));
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("replicaInfo", Utils.getDeepCopy(nodeVsCollectionVsShardVsReplicaInfo, 5));
    ew.put("nodeValues", nodeVsTags);
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    Map<String, Object> tagVals = fetchTagValues(node, tags);
    nodeVsTags.put(node, tagVals);
    return tagVals;
  }

  protected Map<String, Object> fetchTagValues(String node, Collection<String> tags) {
    NodeValueFetcher nodeValueFetcher = new NodeValueFetcher();
    RemoteCallCtx ctx = new RemoteCallCtx(node, solrClient);
    nodeValueFetcher.getTags(new HashSet<>(tags), ctx);
    return ctx.tags;
  }

  public void forEachReplica(String node, Consumer<Replica> consumer) {
    forEachReplica(nodeVsCollectionVsShardVsReplicaInfo.get(node), consumer);
  }

  public static void forEachReplica(
      Map<String, Map<String, List<Replica>>> collectionVsShardVsReplicas,
      Consumer<Replica> consumer) {
    collectionVsShardVsReplicas.forEach(
        (coll, shardVsReplicas) ->
            shardVsReplicas.forEach(
                (shard, replicaInfos) -> {
                  for (int i = 0; i < replicaInfos.size(); i++) {
                    Replica r = replicaInfos.get(i);
                    consumer.accept(r);
                  }
                }));
  }

  @Override
  public Map<String, Map<String, List<Replica>>> getReplicaInfo(
      String node, Collection<String> keys) {
    Map<String, Map<String, List<Replica>>> result =
        nodeVsCollectionVsShardVsReplicaInfo.computeIfAbsent(node, o -> new HashMap<>());

    if (keys.isEmpty()) {
      return result;
    }

    // Build mapping from core name to (replica, metric) {coreName: <Replica, Prometheus Metric
    // Name>}
    Map<String, List<Pair<Replica, String>>> coreToReplicaProps = new HashMap<>();
    Set<String> requestedMetricNames = new HashSet<>();

    forEachReplica(
        result,
        replica -> {
          for (String key : keys) {
            if (replica.getProperties().containsKey(key)) continue;

            // Build core name as the key to the replica and the metric it needs
            String coreName =
                replica.getCollection()
                    + "_"
                    + replica.getShard()
                    + "_"
                    + Utils.parseMetricsReplicaName(replica.getCollection(), replica.getCoreName());

            coreToReplicaProps
                .computeIfAbsent(coreName, k -> new ArrayList<>())
                .add(new Pair<>(replica, key));
            requestedMetricNames.add(key);
          }
        });

    if (coreToReplicaProps.isEmpty()) {
      return result;
    }

    RemoteCallCtx ctx = new RemoteCallCtx(node, solrClient);
    processMetricStream(
        node,
        ctx,
        requestedMetricNames,
        (line) -> {
          String prometheusMetricName = NodeValueFetcher.extractMetricNameFromLine(line);

          // Extract core name from prometheus line and the core label
          String coreParam = NodeValueFetcher.extractLabelValueFromLine(line, "core");
          if (coreParam == null) return;

          // Find the matching core and set the metric value to its corresponding replica
          // properties
          List<Pair<Replica, String>> replicaProps = coreToReplicaProps.get(coreParam);
          if (replicaProps != null) {
            Double value = NodeValueFetcher.Metrics.extractPrometheusValue(line);
            replicaProps.stream()
                .filter(pair -> pair.second().equals(prometheusMetricName))
                .forEach(pair -> pair.first().getProperties().put(pair.second(), value));
          }
        });
    return result;
  }

  /** Process a stream of prometheus metrics lines */
  static void processMetricStream(
      String solrNode, RemoteCallCtx ctx, Set<String> metricNames, Consumer<String> lineProcessor) {
    if (!ctx.isNodeAlive(solrNode)) return;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("wt", "prometheus");
    params.add("name", String.join(",", metricNames));

    var req =
        new GenericSolrRequest(
            SolrRequest.METHOD.GET, "/admin/metrics", SolrRequest.SolrRequestType.ADMIN, params);
    req.setResponseParser(new InputStreamResponseParser("prometheus"));

    String baseUrl =
        ctx.zkClientClusterStateProvider.getZkStateReader().getBaseUrlForNodeName(solrNode);

    try (InputStream in =
        (InputStream) ctx.httpSolrClient().requestWithBaseUrl(baseUrl, req, null).get(STREAM_KEY)) {

      NodeValueFetcher.Metrics.prometheusMetricStream(in).forEach(lineProcessor);
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Unable to read prometheus metrics output", e);
    }
  }

  @Override
  public void close() throws IOException {}

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }

  /**
   * Mostly an info object that stores all the values for a given session to fetch various values
   * from a node
   */
  static class RemoteCallCtx {

    ZkClientClusterStateProvider zkClientClusterStateProvider;
    CloudHttp2SolrClient cloudSolrClient;
    public final Map<String, Object> tags = new HashMap<>();
    private final String node;
    public Map<String, Object> session;

    public boolean isNodeAlive(String node) {
      if (zkClientClusterStateProvider != null) {
        return zkClientClusterStateProvider.getLiveNodes().contains(node);
      }
      return true;
    }

    public RemoteCallCtx(String node, CloudHttp2SolrClient cloudSolrClient) {
      this.node = node;
      this.cloudSolrClient = cloudSolrClient;
      this.zkClientClusterStateProvider =
          (ZkClientClusterStateProvider) cloudSolrClient.getClusterStateProvider();
    }

    protected HttpSolrClientBase httpSolrClient() {
      return cloudSolrClient.getHttpClient();
    }

    /**
     * Will attempt to call {@link #invoke(String, String, SolrParams)} up to five times, retrying
     * on any IO Exceptions
     */
    public SimpleSolrResponse invokeWithRetry(String solrNode, String path, SolrParams params)
        throws InterruptedException, IOException, SolrServerException {
      int retries = 5;
      int cnt = 0;

      while (cnt++ < retries) {
        try {
          return invoke(solrNode, path, params);
        } catch (SolrException | SolrServerException | IOException e) {
          boolean hasIOExceptionCause = false;

          Throwable t = e;
          while (t != null) {
            if (t instanceof IOException) {
              hasIOExceptionCause = true;
              break;
            }
            t = t.getCause();
          }

          if (hasIOExceptionCause) {
            if (log.isInfoEnabled()) {
              log.info("Error on getting remote info, trying again: ", e);
            }
            Thread.sleep(500);
          } else {
            throw e;
          }
        }
      }

      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "Could not get remote info after many retries on NoHttpResponseException");
    }

    public SimpleSolrResponse invoke(String solrNode, String path, SolrParams params)
        throws IOException, SolrServerException {
      String url = zkClientClusterStateProvider.getZkStateReader().getBaseUrlForNodeName(solrNode);

      GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.POST, path, params);
      request.setResponseParser(new JavaBinResponseParser());

      try {
        return request.processWithBaseUrl(cloudSolrClient.getHttpClient(), url, null);
      } catch (SolrServerException | IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Fetching replica metrics failed", e);
      }
    }

    public String getNode() {
      return node;
    }
  }
}
