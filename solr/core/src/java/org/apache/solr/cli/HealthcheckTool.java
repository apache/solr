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

package org.apache.solr.cli;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthcheckTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public List<Option> getOptions() {
    return List.of(
        SolrCLI.OPTION_SOLRURL,
        SolrCLI.OPTION_ZKHOST,
        Option.builder("c")
            .argName("COLLECTION")
            .hasArg()
            .required(false)
            .desc("Name of collection; no default.")
            .longOpt("collection")
            .build());
  }

  enum ShardState {
    healthy,
    degraded,
    down,
    no_leader
  }

  /** Requests health information about a specific collection in SolrCloud. */
  public HealthcheckTool() {
    this(CLIO.getOutStream());
  }

  public HealthcheckTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);
    try (CloudHttp2SolrClient cloudSolrClient =
        new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
            .build()) {
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
      cloudSolrClient.connect();
      runCloudTool(cloudSolrClient, cli);
    }
  }

  @Override
  public String getName() {
    return "healthcheck";
  }

  protected void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String collection = cli.getOptionValue("collection");
    if (collection == null) {
      throw new IllegalArgumentException("Must provide a collection to run a healthcheck against!");
    }

    log.debug("Running healthcheck for {}", collection);

    ZkStateReader zkStateReader = ZkStateReader.from(cloudSolrClient);

    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> liveNodes = clusterState.getLiveNodes();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection == null || docCollection.getSlices() == null) {
      throw new IllegalArgumentException("Collection " + collection + " not found!");
    }

    Collection<Slice> slices = docCollection.getSlices();

    SolrQuery q = new SolrQuery("*:*");
    q.setRows(0);
    QueryResponse qr = cloudSolrClient.query(collection, q);
    SolrCLI.checkCodeForAuthError(qr.getStatus());
    String collErr = null;
    long docCount = -1;
    try {
      docCount = qr.getResults().getNumFound();
    } catch (Exception exc) {
      collErr = String.valueOf(exc);
    }

    List<Object> shardList = new ArrayList<>();
    boolean collectionIsHealthy = (docCount != -1);

    for (Slice slice : slices) {
      String shardName = slice.getName();
      // since we're reporting health of this shard, there's no guarantee of a leader
      String leaderUrl = null;
      try {
        leaderUrl = zkStateReader.getLeaderUrl(collection, shardName, 1000);
      } catch (Exception exc) {
        log.warn("Failed to get leader for shard {} due to: {}", shardName, exc);
      }

      List<ReplicaHealth> replicaList = new ArrayList<>();
      for (Replica r : slice.getReplicas()) {

        String uptime = null;
        String memory = null;
        String replicaStatus;
        long numDocs = -1L;

        ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
        String coreUrl = replicaCoreProps.getCoreUrl();
        boolean isLeader = coreUrl.equals(leaderUrl);

        // if replica's node is not live, its status is DOWN
        String nodeName = replicaCoreProps.getNodeName();
        if (nodeName == null || !liveNodes.contains(nodeName)) {
          replicaStatus = Replica.State.DOWN.toString();
        } else {
          // query this replica directly to get doc count and assess health
          q = new SolrQuery("*:*");
          q.setRows(0);
          q.set(DISTRIB, "false");
          try (var solrClientForCollection = SolrCLI.getSolrClient(coreUrl)) {
            qr = solrClientForCollection.query(q);
            numDocs = qr.getResults().getNumFound();
            try (var solrClient = SolrCLI.getSolrClient(replicaCoreProps.getBaseUrl())) {
              NamedList<Object> systemInfo =
                  solrClient.request(
                      new GenericSolrRequest(
                          SolrRequest.METHOD.GET, CommonParams.SYSTEM_INFO_PATH));
              uptime = SolrCLI.uptime((Long) systemInfo.findRecursive("jvm", "jmx", "upTimeMS"));
              String usedMemory = (String) systemInfo.findRecursive("jvm", "memory", "used");
              String totalMemory = (String) systemInfo.findRecursive("jvm", "memory", "total");
              memory = usedMemory + " of " + totalMemory;
            }

            // if we get here, we can trust the state
            replicaStatus = replicaCoreProps.getState();
          } catch (Exception exc) {
            log.error("ERROR: {} when trying to reach: {}", exc, coreUrl);

            if (SolrCLI.checkCommunicationError(exc)) {
              replicaStatus = Replica.State.DOWN.toString();
            } else {
              replicaStatus = "error: " + exc;
            }
          }
        }

        replicaList.add(
            new ReplicaHealth(
                shardName, r.getName(), coreUrl, replicaStatus, numDocs, isLeader, uptime, memory));
      }

      ShardHealth shardHealth = new ShardHealth(shardName, replicaList);
      if (ShardState.healthy != shardHealth.getShardState()) {
        collectionIsHealthy = false; // at least one shard is un-healthy
      }

      shardList.add(shardHealth.asMap());
    }

    Map<String, Object> report = new LinkedHashMap<>();
    report.put("collection", collection);
    report.put("status", collectionIsHealthy ? "healthy" : "degraded");
    if (collErr != null) {
      report.put("error", collErr);
    }
    report.put("numDocs", docCount);
    report.put("numShards", slices.size());
    report.put("shards", shardList);

    CharArr arr = new CharArr();
    new JSONWriter(arr, 2).write(report);
    echo(arr.toString());
  }
}

class ReplicaHealth implements Comparable<ReplicaHealth> {
  String shard;
  String name;
  String url;
  String status;
  long numDocs;
  boolean isLeader;
  String uptime;
  String memory;

  ReplicaHealth(
      String shard,
      String name,
      String url,
      String status,
      long numDocs,
      boolean isLeader,
      String uptime,
      String memory) {
    this.shard = shard;
    this.name = name;
    this.url = url;
    this.numDocs = numDocs;
    this.status = status;
    this.isLeader = isLeader;
    this.uptime = uptime;
    this.memory = memory;
  }

  public Map<String, Object> asMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(NAME, name);
    map.put("url", url);
    map.put("numDocs", numDocs);
    map.put("status", status);
    if (uptime != null) map.put("uptime", uptime);
    if (memory != null) map.put("memory", memory);
    if (isLeader) map.put("leader", true);
    return map;
  }

  @Override
  public String toString() {
    CharArr arr = new CharArr();
    new JSONWriter(arr, 2).write(asMap());
    return arr.toString();
  }

  @Override
  public int hashCode() {
    return this.shard.hashCode() + (isLeader ? 1 : 0);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (!(obj instanceof ReplicaHealth)) return true;
    ReplicaHealth that = (ReplicaHealth) obj;
    return this.shard.equals(that.shard) && this.isLeader == that.isLeader;
  }

  @Override
  public int compareTo(ReplicaHealth other) {
    if (this == other) return 0;
    if (other == null) return 1;

    int myShardIndex = Integer.parseInt(this.shard.substring("shard".length()));

    int otherShardIndex = Integer.parseInt(other.shard.substring("shard".length()));

    if (myShardIndex == otherShardIndex) {
      // same shard index, list leaders first
      return this.isLeader ? -1 : 1;
    }

    return myShardIndex - otherShardIndex;
  }
}

class ShardHealth {
  String shard;
  List<ReplicaHealth> replicas;

  ShardHealth(String shard, List<ReplicaHealth> replicas) {
    this.shard = shard;
    this.replicas = replicas;
  }

  public HealthcheckTool.ShardState getShardState() {
    boolean healthy = true;
    boolean hasLeader = false;
    boolean atLeastOneActive = false;
    for (ReplicaHealth replicaHealth : replicas) {
      if (replicaHealth.isLeader) hasLeader = true;

      if (!Replica.State.ACTIVE.toString().equals(replicaHealth.status)) {
        healthy = false;
      } else {
        atLeastOneActive = true;
      }
    }

    if (!hasLeader) return HealthcheckTool.ShardState.no_leader;

    return healthy
        ? HealthcheckTool.ShardState.healthy
        : (atLeastOneActive
            ? HealthcheckTool.ShardState.degraded
            : HealthcheckTool.ShardState.down);
  }

  public Map<String, Object> asMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("shard", shard);
    map.put("status", getShardState().toString());
    List<Object> replicaList = new ArrayList<>();
    for (ReplicaHealth replica : replicas) replicaList.add(replica.asMap());
    map.put("replicas", replicaList);
    return map;
  }

  @Override
  public String toString() {
    CharArr arr = new CharArr();
    new JSONWriter(arr, 2).write(asMap());
    return arr.toString();
  }
}
