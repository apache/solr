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

package org.apache.solr.handler.component;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A replica source for solr cloud mode
 */
class CloudReplicaSource implements ReplicaSource {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Builder builder;

  private String[] slices;
  private List<String>[] replicas;

  private List<Replica>[] cloudReplicas;

  private CloudReplicaSource(Builder builder) {
    this.builder = builder;
    final String shards = builder.params.get(ShardParams.SHARDS);
    if (shards != null) {
      withShardsParam(builder, shards);
    } else {
      withClusterState(builder, builder.params);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void withClusterState(Builder builder, SolrParams params) {
    ClusterState clusterState = builder.zkStateReader.getClusterState();
    String shardKeys = params.get(ShardParams._ROUTE_);

    // This will be the complete list of slices we need to query for this request.
    Map<String, Slice> sliceMap = new HashMap<>();

    // we need to find out what collections this request is for.

    // A comma-separated list of specified collections.
    // Eg: "collection1,collection2,collection3"
    String collections = params.get("collection");
    if (collections != null) {
      // If there were one or more collections specified in the query, split
      // each parameter and store as a separate member of a List.
      List<String> collectionList = StrUtils.splitSmart(collections, ",",
          true);
      // The "collection" param may contain alias names (e.g. a request forwarded with the raw alias still
      // in the param, or a client that addressed an alias directly). addSlices() below resolves each name
      // straight against cluster state and would throw "Could not find collection" for an alias, so resolve
      // aliases here first. resolveAliases() returns the input unchanged when it is not an alias.
      Aliases aliases = builder.zkStateReader.getAliases();
      // In turn, retrieve the slices that cover each collection from the
      // cloud state and add them to the Map 'slices'.
      for (String inputName : collectionList) {
        for (String collectionName : aliases.resolveAliases(inputName)) {
          // The original code produced <collection-name>_<shard-name> when the collections
          // parameter was specified (see ClientUtils.appendMap)
          // Is this necessary if ony one collection is specified?
          // i.e. should we change multiCollection to collectionList.size() > 1?
          addSlices(sliceMap, clusterState, params, collectionName, shardKeys, true);
        }
      }
    } else {
      // just this collection
      addSlices(sliceMap, clusterState, params, builder.collection, shardKeys, false);
    }

    this.slices = sliceMap.keySet().toArray(new String[0]);
    this.replicas = new List[slices.length];
    this.cloudReplicas = new List[slices.length];
    for (int i = 0; i < slices.length; i++) {
      String sliceName = slices[i];
      cloudReplicas[i] = findCloudReplicas(builder, null, clusterState, sliceMap.get(sliceName));
      replicas[i] = new ArrayList<>(cloudReplicas[i].size());
      for (int j = 0; j < cloudReplicas[i].size(); j++){
        replicas[i].add(cloudReplicas[i].get(j).getCoreUrl());
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private void withShardsParam(Builder builder, String shardsParam) {
    List<String> sliceOrUrls = StrUtils.splitSmart(shardsParam, ",", true);
    this.slices = new String[sliceOrUrls.size()];
    this.replicas = new List[sliceOrUrls.size()];

    ClusterState clusterState = builder.zkStateReader.getClusterState();

    for (int i = 0; i < sliceOrUrls.size(); i++) {
      String sliceOrUrl = sliceOrUrls.get(i);
      if (sliceOrUrl.indexOf('/') < 0) {
        // this is a logical shard
        this.slices[i] = sliceOrUrl;
        replicas[i] = findReplicas(builder, shardsParam, clusterState, clusterState.getCollection(builder.collection).getSlice(sliceOrUrl));
      } else {
        // this has urls
        this.replicas[i] = StrUtils.splitSmart(sliceOrUrl, "|", true);
        builder.replicaListTransformer.transform(replicas[i]);
        builder.hostChecker.checkWhitelist(builder.zkStateReader, shardsParam, replicas[i]);
      }
    }
  }

  private static List<String> findReplicas(Builder builder, String shardsParam, ClusterState clusterState, Slice slice) {
    if (slice == null) {
      // Treat this the same as "all servers down" for a slice, and let things continue
      // if partial results are acceptable
      return Collections.emptyList();
    } else {
      final Predicate<Replica> isShardLeader = new IsLeaderPredicate(builder.zkStateReader, clusterState, slice.getCollection(), slice.getName());
      List<Replica> list = slice.getReplicas()
          .stream()
          .filter(replica -> replica.getState() == Replica.State.ACTIVE)
          .filter(replica -> !builder.onlyNrt || (replica.getType() == Replica.Type.NRT || (replica.getType() == Replica.Type.TLOG && isShardLeader.test(replica))))
          .collect(Collectors.toList());
      list = filterInSyncByTerm(builder, slice, list, isShardLeader);
      builder.replicaListTransformer.transform(list);
      List<String> collect = list.stream().map(Replica::getCoreUrl).collect(Collectors.toList());
      builder.hostChecker.checkWhitelist(builder.zkStateReader, shardsParam, collect);
      return collect;
    }
  }

  private static List<Replica> findCloudReplicas(Builder builder, String shardsParam, ClusterState clusterState, Slice slice) {
    if (slice == null) {
      // Treat this the same as "all servers down" for a slice, and let things continue
      // if partial results are acceptable
      return Collections.emptyList();
    } else {
      try {
        final Predicate<Replica> isShardLeader = new IsLeaderPredicate(builder.zkStateReader, clusterState, slice.getCollection(), slice.getName());
        List<Replica> list = slice.getReplicas().stream().filter(replica -> replica.getState() == Replica.State.ACTIVE).filter(replica -> !builder.onlyNrt || (replica.getType() == Replica.Type.NRT || (replica.getType() == Replica.Type.TLOG && isShardLeader.test(replica)))).collect(Collectors.toList());
        list = filterInSyncByTerm(builder, slice, list, isShardLeader);
        builder.replicaListTransformer.transform(list);
        List<String> collect = list.stream().map(Replica::getCoreUrl).collect(Collectors.toList());
        builder.hostChecker.checkWhitelist(builder.zkStateReader, shardsParam, collect);
        return list;
      } catch (Exception e) {
        log.error("Exception building cloud replicas", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  /**
   * Drop replicas whose shard term is below the leader's from the read-eligible set. A replica can be
   * published ACTIVE yet have fallen behind the leader: when a leader-&gt;follower update forward is
   * skipped/fails the leader raises its own shard term above the follower
   * ({@link org.apache.solr.update.processor.DistributedZkUpdateProcessor} via
   * {@link ZkShardTerms#skipSendingUpdatesTo}) and stops forwarding to it until it recovers. There is
   * an async window between that term drop and the follower's recovery publishing BUFFERING (which is
   * what would remove it from the ACTIVE set), during which a distributed query load-balanced to the
   * behind-but-ACTIVE follower returns stale/missing docs (ShardRoutingTest). Gate read-eligibility on
   * the same term signal the leader uses to fan out writes: a replica may serve a shard's sub-query
   * only if it is the leader or it still holds the highest term ({@code !skipSendingUpdatesTo}).
   *
   * <p>Best-effort: only applies when this node has the shard's terms cached (it hosts a replica of the
   * shard — {@link ZkController#getShardTerms} returns the live, ZK-watched terms). Never filters the
   * list empty: the leader is always retained, and any unexpected error falls back to the unfiltered
   * list (no worse than before).
   */
  private static List<Replica> filterInSyncByTerm(Builder builder, Slice slice, List<Replica> list, Predicate<Replica> isShardLeader) {
    if (builder.zkController == null || list.size() <= 1) {
      return list;
    }
    try {
      ZkShardTerms terms = builder.zkController.getShardTerms(slice.getCollection(), slice.getName());
      if (terms == null) {
        return list;
      }
      List<Replica> inSync = list.stream()
          .filter(replica -> isShardLeader.test(replica) || !terms.skipSendingUpdatesTo(replica.getName()))
          .collect(Collectors.toList());
      // Never return an empty set due to term filtering (e.g. terms not yet populated): if everything
      // was filtered out, keep the original ACTIVE list rather than failing the shard.
      return inSync.isEmpty() ? list : inSync;
    } catch (Exception e) {
      if (log.isDebugEnabled()) log.debug("term-based read filtering skipped for {}/{}", slice.getCollection(), slice.getName(), e);
      return list;
    }
  }

  private static void addSlices(Map<String,Slice> target, ClusterState state, SolrParams params, String collectionName, String shardKeys,
      boolean multiCollection) {
    DocCollection coll = state.getCollection(collectionName);
    Collection<Slice> slices = coll.getRouter().getSearchSlices(shardKeys, params, coll);
    ClientUtils.addSlices(target, collectionName, slices, multiCollection);
  }

  @Override
  public List<String> getSliceNames() {
    // NOTE: must NOT shuffle here. Arrays.asList(slices) is backed by the slices[] array,
    // so Collections.shuffle would mutate slices[] in place and desynchronize it from the
    // parallel cloudReplicas[]/replicas[] arrays that getReplicasBySlice(i) indexes by
    // position. That misalignment caused RealTimeGetComponent.sliceToShards() to map a
    // slice name to the wrong shard's replica URLs (e.g. s3 -> s4 replicas).
    return Collections.unmodifiableList(Arrays.asList(slices));
  }

  @Override
  public List<String> getReplicasBySlice(int sliceNumber) {
    if (cloudReplicas == null) {
      ArrayList urls = new ArrayList(replicas[sliceNumber]);
      Collections.shuffle(urls);
      return urls;
    }
    List<Replica> replicas = cloudReplicas[sliceNumber];
    ArrayList urls = new ArrayList(replicas.size());
    for (Replica replica : replicas) {
      if (this.builder.zkStateReader.isNodeLive(replica.getNodeName())) {
        urls.add(replica.getCoreUrl());
      }
    }
    // Do NOT shuffle here: the replicaListTransformer in findCloudReplicas() has already
    // sorted replicas by preference (type, location, etc.) and randomised within equivalent
    // groups. Shuffling afterwards would destroy that ordering and break shards.preference
    // routing (preferLocalShards, replica.type, etc.).
    return urls;
  }

  @Override
  public int getSliceCount() {
    return slices.length;
  }

  /**
   * A predicate to test if a replica is the leader according to {@link ZkStateReader#getLeaderRetry(String, String)}.
   * <p>
   * The result of getLeaderRetry is cached in the first call so that subsequent tests are faster and do not block.
   */
  private static class IsLeaderPredicate implements Predicate<Replica> {
    private final ZkStateReader zkStateReader;
    private final ClusterState clusterState;
    private final String collectionName;
    private final String sliceName;
    private Replica shardLeader = null;

    public IsLeaderPredicate(ZkStateReader zkStateReader, ClusterState clusterState, String collectionName, String sliceName) {
      this.zkStateReader = zkStateReader;
      this.clusterState = clusterState;
      this.collectionName = collectionName;
      this.sliceName = sliceName;
    }

    @Override
    public boolean test(Replica replica) {
      if (shardLeader == null) {
        try {
          shardLeader = zkStateReader.getLeaderRetry(collectionName, sliceName);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Exception finding leader for shard " + sliceName + " in collection "
                  + collectionName, e);
        } catch (SolrException e) {
          if (log.isDebugEnabled()) {
            log.debug("Exception finding leader for shard {} in collection {}. Collection State: {}",
                sliceName, collectionName, clusterState.getCollectionOrNull(collectionName));
          }
          throw e;
        } catch (TimeoutException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
      return replica.getName().equals(shardLeader.getName());
    }
  }

  static class Builder {
    private String collection;
    private ZkStateReader zkStateReader;
    private ZkController zkController;
    private SolrParams params;
    private boolean onlyNrt;
    private ReplicaListTransformer replicaListTransformer;
    private HttpShardHandlerFactory.WhitelistHostChecker hostChecker;

    public Builder collection(String collection) {
      this.collection = collection;
      return this;
    }

    public Builder zkStateReader(ZkStateReader stateReader) {
      this.zkStateReader = stateReader;
      return this;
    }

    /** Optional; when set, distributed reads are gated on shard terms so a behind-but-ACTIVE replica is
     *  excluded (see {@link CloudReplicaSource#filterInSyncByTerm}). */
    public Builder zkController(ZkController zkController) {
      this.zkController = zkController;
      return this;
    }

    public Builder params(SolrParams params) {
      this.params = params;
      return this;
    }

    public Builder onlyNrt(boolean onlyNrt) {
      this.onlyNrt = onlyNrt;
      return this;
    }

    public Builder replicaListTransformer(ReplicaListTransformer replicaListTransformer) {
      this.replicaListTransformer = replicaListTransformer;
      return this;
    }

    public Builder whitelistHostChecker(HttpShardHandlerFactory.WhitelistHostChecker hostChecker) {
      this.hostChecker = hostChecker;
      return this;
    }

    public CloudReplicaSource build() {
      return new CloudReplicaSource(this);
    }
  }
}
