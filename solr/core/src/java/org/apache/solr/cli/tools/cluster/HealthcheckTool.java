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

package org.apache.solr.cli.tools.cluster;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.cli.CLIO;
import org.apache.solr.cli.CLIUtils;
import org.apache.solr.cli.CommonCLIOptions;
import org.apache.solr.cli.SolrCLI;
import org.apache.solr.cli.ToolBase;
import org.apache.solr.cli.ToolRuntime;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.SystemInfoRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SystemInfoResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.EnvUtils;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports healthcheck command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "healthcheck",
    description =
        "Verifies that a collection is functioning: queries every replica directly, compares"
            + " document counts and checks that each shard has a leader and every replica is"
            + " ACTIVE. Requires SolrCloud.",
    footerHeading = "%nExamples:%n",
    footer = {"  # Check the health of a collection", "  bin/solr healthcheck -c gettingstarted"})
public class HealthcheckTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("COLLECTION")
          .required()
          .desc("Name of the collection to check.")
          .get();

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(COLLECTION_NAME_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  enum ShardState {
    healthy,
    degraded,
    down,
    no_leader
  }

  /** Parameters for the healthcheck command, independent of the command line parser. */
  record HealthcheckParams(String collection, String credentials) {}

  // --- picocli fields ---

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private ConnectionOptions connectionOptions;

  @picocli.CommandLine.Mixin private CredentialsOptions credentialsOptions;

  @picocli.CommandLine.Option(
      names = {"-c", "--name"},
      required = true,
      paramLabel = "COLLECTION",
      description = "Name of the collection to check.")
  private String nameOpt;

  public HealthcheckTool() {
    this(new DefaultToolRuntime());
  }

  /** Requests health information about a specific collection in SolrCloud. */
  public HealthcheckTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    var solrConnection = CLIUtils.getSolrConnection(cli);
    if (solrConnection == null) {
      CLIO.err("Healthcheck tool only works in Solr Cloud mode.");
      runtime.exit(1);
    }
    HealthcheckParams params =
        new HealthcheckParams(
            cli.getOptionValue(COLLECTION_NAME_OPTION),
            cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));
    var builder =
        new HttpJettySolrClient.Builder().withOptionalBasicAuthCredentials(params.credentials());
    try (var cloudSolrClient = CLIUtils.getCloudSolrClient(solrConnection, builder)) {
      echoIfVerbose("Connecting to Solr at " + solrConnection.toString());
      runCloudTool(cloudSolrClient, params);
    }
  }

  @Override
  public String getName() {
    return "healthcheck";
  }

  protected void runCloudTool(CloudSolrClient cloudSolrClient, HealthcheckParams params)
      throws Exception {
    String collection = params.collection();

    log.debug("Running healthcheck for {}", collection);

    ClusterState clusterState = cloudSolrClient.getClusterStateProvider().getClusterState();
    Set<String> liveNodes = clusterState.getLiveNodes();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection == null || docCollection.getSlices() == null) {
      throw new IllegalArgumentException("Collection " + collection + " not found!");
    }

    Collection<Slice> slices = docCollection.getSlices();

    SolrQuery q = new SolrQuery("*:*");
    q.setRows(0);
    QueryResponse qr = cloudSolrClient.query(collection, q);
    CLIUtils.checkCodeForAuthError(qr.getStatus());
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
      List<ReplicaHealth> replicaList = new ArrayList<>();
      for (Replica r : slice.getReplicas()) {

        String uptime = null;
        String memory = null;
        String replicaStatus;
        long numDocs = -1L;

        String coreUrl = r.getCoreUrl();
        boolean isLeader = r.isLeader();

        // if replica's node is not live, its status is DOWN
        String nodeName = r.getNodeName();
        if (nodeName == null || !liveNodes.contains(nodeName)) {
          replicaStatus = Replica.State.DOWN.toString();
        } else {
          // query this replica directly to get doc count and assess health
          q = new SolrQuery("*:*");
          q.setRows(0);
          q.set(DISTRIB, "false");
          try (var solrClientForCollection =
              CLIUtils.getSolrClient(coreUrl, params.credentials())) {
            qr = solrClientForCollection.query(q);
            numDocs = qr.getResults().getNumFound();
            try (var solrClient = CLIUtils.getSolrClient(r.getBaseUrl(), params.credentials())) {
              SystemInfoResponse sysResponse = (new SystemInfoRequest()).process(solrClient);
              uptime = SolrCLI.uptime(sysResponse.getJVMUpTimeMillis());
              memory =
                  sysResponse.getHumanReadableJVMMemoryUsed()
                      + " of "
                      + sysResponse.getHumanReadableJVMMemoryTotal();
            }

            // if we get here, we can trust the state
            replicaStatus = String.valueOf(r.getState());
          } catch (Exception exc) {
            log.error("ERROR: {} when trying to reach: {}", exc, coreUrl);

            if (CLIUtils.checkCommunicationError(exc)) {
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
        collectionIsHealthy = false; // at least one shard is unhealthy
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

  @Override
  public int callTool() throws Exception {
    var solrConnection = resolveSolrConnection(credentialsOptions.credentials);
    if (solrConnection == null) {
      CLIO.err("Healthcheck tool only works in Solr Cloud mode.");
      return 1;
    }
    HealthcheckParams params = new HealthcheckParams(nameOpt, credentialsOptions.credentials);
    var builder =
        new HttpJettySolrClient.Builder().withOptionalBasicAuthCredentials(params.credentials());
    try (var cloudSolrClient = CLIUtils.getCloudSolrClient(solrConnection, builder)) {
      echoIfVerbose("Connecting to Solr at " + solrConnection.toString());
      runCloudTool(cloudSolrClient, params);
    }
    return 0;
  }

  /**
   * Mirrors {@link CLIUtils#getSolrConnection(CommandLine)}: an explicit {@code --solr-connection}
   * or {@code --zk-host} (or the matching property) wins, otherwise a running Solr is asked for its
   * ZooKeeper, and null means it is not in SolrCloud mode.
   */
  private CloudSolrClient.CloudSolrClientConnection resolveSolrConnection(String credentials)
      throws Exception {
    String solrConnection =
        (connectionOptions != null && connectionOptions.solrConnection != null)
            ? connectionOptions.solrConnection
            : EnvUtils.getProperty("solr.connection");
    if (solrConnection != null && !solrConnection.isBlank()) {
      return CloudSolrClient.CloudSolrClientConnection.parse(solrConnection);
    }
    String zkHost =
        (connectionOptions != null && connectionOptions.zkHost != null)
            ? connectionOptions.zkHost
            : EnvUtils.getProperty("zkHost");
    if (zkHost != null && !zkHost.isBlank()) {
      var zkConnection = CloudSolrClient.CloudSolrClientConnection.parse(zkHost);
      if (!zkConnection.isZookeeper()) {
        throw new IOException(
            String.format(
                Locale.ROOT, "Expected ZooKeeper connection string, but got: '%s'.", zkHost));
      }
      return zkConnection;
    }
    String solrUrl =
        (connectionOptions != null && connectionOptions.solrUrl != null)
            ? CLIUtils.normalizeSolrUrl(connectionOptions.solrUrl)
            : CLIUtils.getDefaultSolrUrl();
    try (SolrClient solrClient = CLIUtils.getSolrClient(solrUrl, credentials)) {
      Map<String, Object> status = StatusTool.reportStatus(solrClient);
      @SuppressWarnings("unchecked")
      Map<String, Object> cloud = (Map<String, Object>) status.get("cloud");
      if (cloud == null) {
        return null;
      }
      String zookeeper = (String) cloud.get("ZooKeeper");
      if (zookeeper.endsWith("(embedded)")) {
        zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
      }
      return CloudSolrClient.CloudSolrClientConnection.parse(zookeeper);
    }
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
    if (!(obj instanceof ReplicaHealth that)) return true;
    return this.shard.equals(that.shard) && this.isLeader == that.isLeader;
  }

  @Override
  public int compareTo(ReplicaHealth other) {
    if (this == other) return 0;

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
