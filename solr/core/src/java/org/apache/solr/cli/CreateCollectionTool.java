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

import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.JsonMapResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetService;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/** Supports create_collection command in the bin/solr script. */
public class CreateCollectionTool extends ToolBase {

  public CreateCollectionTool() {
    this(CLIO.getOutStream());
  }

  public CreateCollectionTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "create_collection";
  }

  @Override
  public List<Option> getOptions() {
    return SolrCLI.CREATE_COLLECTION_OPTIONS;
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);
    if (zkHost == null) {
      throw new IllegalStateException(
          "Solr at "
              + cli.getOptionValue("solrUrl")
              + " is running in standalone server mode, please use the create_core command instead;\n"
              + "create_collection can only be used when running in SolrCloud mode.\n");
    }

    try (CloudHttp2SolrClient cloudSolrClient =
        new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
            .build()) {
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
      cloudSolrClient.connect();
      runCloudTool(cloudSolrClient, cli);
    }
  }

  protected void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {

    Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
    if (liveNodes.isEmpty())
      throw new IllegalStateException(
          "No live nodes found! Cannot create a collection until "
              + "there is at least 1 live node in the cluster.");

    String solrUrl = cli.getOptionValue("solrUrl");
    if (solrUrl == null) {
      String firstLiveNode = liveNodes.iterator().next();
      solrUrl = ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
    }

    String collectionName = cli.getOptionValue(NAME);

    // build a URL to create the collection
    int numShards = Integer.parseInt(cli.getOptionValue("shards", String.valueOf(1)));
    int replicationFactor =
        Integer.parseInt(cli.getOptionValue("replicationFactor", String.valueOf(1)));

    String confname = cli.getOptionValue("confname");
    String confdir = cli.getOptionValue("confdir");
    String configsetsDir = cli.getOptionValue("configsetsDir");

    boolean configExistsInZk =
        confname != null
            && !confname.trim().isEmpty()
            && ZkStateReader.from(cloudSolrClient)
                .getZkClient()
                .exists("/configs/" + confname, true);

    if (CollectionAdminParams.SYSTEM_COLL.equals(collectionName)) {
      // do nothing
    } else if (configExistsInZk) {
      echo("Re-using existing configuration directory " + confname);
    } else if (confdir != null && !confdir.trim().isEmpty()) {
      if (confname == null || confname.trim().isEmpty()) {
        confname = collectionName;
      }
      Path confPath = ConfigSetService.getConfigsetPath(confdir, configsetsDir);

      echoIfVerbose(
          "Uploading "
              + confPath.toAbsolutePath()
              + " for config "
              + confname
              + " to ZooKeeper at "
              + cloudSolrClient.getClusterStateProvider().getQuorumHosts(),
          cli);
      ZkMaintenanceUtils.uploadToZK(
          ZkStateReader.from(cloudSolrClient).getZkClient(),
          confPath,
          ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confname,
          ZkMaintenanceUtils.UPLOAD_FILENAME_EXCLUDE_PATTERN);
    }

    // since creating a collection is a heavy-weight operation, check for existence first
    if (SolrCLI.safeCheckCollectionExists(solrUrl, collectionName)) {
      throw new IllegalStateException(
          "\nCollection '"
              + collectionName
              + "' already exists!\nChecked collection existence using CollectionAdminRequest");
    }

    // doesn't seem to exist ... try to create
    echoIfVerbose(
        "\nCreating new collection '" + collectionName + "' using CollectionAdminRequest", cli);

    NamedList<Object> response;
    try {
      var req =
          CollectionAdminRequest.createCollection(
              collectionName, confname, numShards, replicationFactor);
      req.setResponseParser(new JsonMapResponseParser());
      response =
          cloudSolrClient.request(
              CollectionAdminRequest.createCollection(
                  collectionName, confname, numShards, replicationFactor));
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to create collection '" + collectionName + "' due to: " + sse.getMessage());
    }

    if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
      // pretty-print the response to stdout
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(response.asMap());
      echo(arr.toString());
      echo("\n");
    } else {
      String endMessage =
          String.format(
              Locale.ROOT,
              "Created collection '%s' with %d shard(s), %d replica(s)",
              collectionName,
              numShards,
              replicationFactor);
      if (confname != null && !confname.trim().isEmpty()) {
        endMessage += String.format(Locale.ROOT, " with config-set '%s'", confname);
      }

      echo(endMessage);
    }
  }
}
