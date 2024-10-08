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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The <em>real</em> {@link NodeStateProvider}, which communicates with Solr via SolrJ. */
public class SolrClientNodeStateProvider implements NodeStateProvider, MapWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CloudLegacySolrClient solrClient;
  protected final Map<String, Map<String, Map<String, List<Replica>>>>
      nodeVsCollectionVsShardVsReplicaInfo = new HashMap<>();

  @SuppressWarnings({"rawtypes"})
  private Map<String, Map> nodeVsTags = new HashMap<>();

  public SolrClientNodeStateProvider(CloudLegacySolrClient solrClient) {
    this.solrClient = solrClient;
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
    Map<String, ClusterState.CollectionRef> all =
        clusterStateProvider.getClusterState().getCollectionStates();
    all.forEach(
        (collName, ref) -> {
          DocCollection coll = ref.get();
          if (coll == null) return;
          coll.forEachReplica(
              (shard, replica) -> {
                Map<String, Map<String, List<Replica>>> nodeData =
                    nodeVsCollectionVsShardVsReplicaInfo.computeIfAbsent(
                        replica.getNodeName(), k -> new HashMap<>());
                Map<String, List<Replica>> collData =
                    nodeData.computeIfAbsent(collName, k -> new HashMap<>());
                List<Replica> replicas = collData.computeIfAbsent(shard, k -> new ArrayList<>());
                replicas.add((Replica) replica.clone());
              });
        });
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
    nodeValueFetcher.getTags(node, new HashSet<>(tags), ctx);
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
    if (!keys.isEmpty()) {
      Map<String, Pair<String, Replica>> metricsKeyVsTagReplica = new HashMap<>();
      forEachReplica(
          result,
          r -> {
            for (String key : keys) {
              if (r.getProperties().containsKey(key)) continue; // it's already collected
              String perReplicaMetricsKey =
                  "solr.core."
                      + r.getCollection()
                      + "."
                      + r.getShard()
                      + "."
                      + Utils.parseMetricsReplicaName(r.getCollection(), r.getCoreName())
                      + ":";
              String perReplicaValue = key;
              perReplicaMetricsKey += perReplicaValue;
              metricsKeyVsTagReplica.put(perReplicaMetricsKey, new Pair<>(key, r));
            }
          });

      if (!metricsKeyVsTagReplica.isEmpty()) {
        Map<String, Object> tagValues = fetchReplicaMetrics(node, metricsKeyVsTagReplica);
        tagValues.forEach(
            (k, o) -> {
              Pair<String, Replica> p = metricsKeyVsTagReplica.get(k);
              if (p.second() != null) p.second().getProperties().put(p.first(), o);
            });
      }
    }
    return result;
  }

  protected Map<String, Object> fetchReplicaMetrics(
      String node, Map<String, Pair<String, Replica>> metricsKeyVsTagReplica) {
    Map<String, Set<Object>> collect =
        metricsKeyVsTagReplica.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> Set.of(e.getKey())));
    RemoteCallCtx ctx = new RemoteCallCtx(null, solrClient);
    fetchReplicaMetrics(node, ctx, collect);
    return ctx.tags;
  }

  static void fetchReplicaMetrics(
      String solrNode, RemoteCallCtx ctx, Map<String, Set<Object>> metricsKeyVsTag) {
    if (!ctx.isNodeAlive(solrNode)) return;
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("key", metricsKeyVsTag.keySet().toArray(new String[0]));
    try {
      SimpleSolrResponse rsp = ctx.invokeWithRetry(solrNode, CommonParams.METRICS_PATH, params);
      metricsKeyVsTag.forEach(
          (key, tags) -> {
            Object v =
                Utils.getObjectByPath(rsp.getResponse(), true, Arrays.asList("metrics", key));
            for (Object tag : tags) {
              if (tag instanceof Function) {
                @SuppressWarnings({"unchecked"})
                Pair<String, Object> p = (Pair<String, Object>) ((Function) tag).apply(v);
                ctx.tags.put(p.first(), p.second());
              } else {
                if (v != null) ctx.tags.put(tag.toString(), v);
              }
            }
          });
    } catch (Exception e) {
      log.warn("could not get tags from node {}", solrNode, e);
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
    CloudLegacySolrClient solrClient;
    public final Map<String, Object> tags = new HashMap<>();
    private String node;
    public Map<String, Object> session;

    public boolean isNodeAlive(String node) {
      if (zkClientClusterStateProvider != null) {
        return zkClientClusterStateProvider.getLiveNodes().contains(node);
      }
      return true;
    }

    public RemoteCallCtx(String node, CloudLegacySolrClient solrClient) {
      this.node = node;
      this.solrClient = solrClient;
      this.zkClientClusterStateProvider =
          (ZkClientClusterStateProvider) solrClient.getClusterStateProvider();
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
      try (var client =
          new HttpSolrClient.Builder()
              .withHttpClient(solrClient.getHttpClient())
              .withBaseSolrUrl(url)
              .withResponseParser(new BinaryResponseParser())
              .build()) {
        NamedList<Object> rsp = client.request(request);
        request.response.setResponse(rsp);
        return request.response;
      }
    }

    public String getNode() {
      return node;
    }
  }
}
