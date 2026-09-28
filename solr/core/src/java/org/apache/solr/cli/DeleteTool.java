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

import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionsApi;
import org.apache.solr.client.solrj.request.ConfigsetsApi;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports delete command in the bin/solr script. */
public class DeleteTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of the core / collection to delete.")
          .get();

  private static final Option DELETE_CONFIG_OPTION =
      Option.builder()
          .longOpt("delete-config")
          .desc(
              "Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is true.")
          .get();

  // No longer has any effect: the Overseer's configset-delete command unconditionally refuses to
  // delete a configset that's still in use by another collection, so this flag was never actually
  // able to bypass that safety check. Configset deletion now always requests the safe ("only if
  // unused") behavior. Kept, as a no-op, for backward compatibility with existing scripts.
  private static final Option FORCE_OPTION =
      Option.builder("f")
          .longOpt("force")
          .deprecated(
              DeprecatedAttributes.builder()
                  .setDescription(
                      "no longer has any effect; configset deletion is always safely skipped if"
                          + " the configset is still in use by another collection")
                  .setForRemoval(true)
                  .get())
          .desc("No longer has any effect; retained for backward compatibility.")
          .get();

  public DeleteTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "delete";
  }

  @Override
  public String getHeader() {
    return """
        Deletes a collection or core depending on whether Solr is running in SolrCloud or standalone mode. \
        Deleting a collection does not delete it's configuration unless you pass in the --delete-config flag.

        List of options:""";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(COLLECTION_NAME_OPTION)
        .addOption(DELETE_CONFIG_OPTION)
        .addOption(FORCE_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    try (var solrClient = CLIUtils.getSolrClient(cli)) {
      if (CLIUtils.isCloudMode(solrClient)) {
        deleteCollection(cli, solrClient);
      } else {
        deleteCore(cli, solrClient);
      }
    }
  }

  protected void deleteCollection(CommandLine cli, SolrClient solrClient) throws Exception {
    String collectionName = cli.getOptionValue(COLLECTION_NAME_OPTION);

    // Uses the V1 CLUSTERSTATUS request rather than the V2 CollectionsApi.GetCollectionStatus:
    // the latter goes through a Jersey code path that currently throws under basic-auth-secured
    // clusters. Still a plain HTTP admin call, not a direct ZK connection. Scoping the request to
    // this one collection also serves as the existence check below, instead of a separate
    // ListCollections call that would have to scan every collection in the cluster.
    Map<String, Object> collectionInfo;
    try {
      var statusReq = new CollectionAdminRequest.ClusterStatus().setCollectionName(collectionName);
      var statusResponse = statusReq.process(solrClient);
      @SuppressWarnings("unchecked")
      Map<String, Object> cluster =
          (Map<String, Object>) statusResponse.getResponse().get("cluster");
      @SuppressWarnings("unchecked")
      Map<String, Object> collections =
          cluster != null ? (Map<String, Object>) cluster.get("collections") : null;
      @SuppressWarnings("unchecked")
      Map<String, Object> info =
          collections != null ? (Map<String, Object>) collections.get(collectionName) : null;
      collectionInfo = info;
    } catch (SolrException e) {
      if (e.code() == SolrException.ErrorCode.BAD_REQUEST.code) {
        throw new IllegalArgumentException("Collection " + collectionName + " not found!");
      }
      throw e;
    }
    String configName = collectionInfo != null ? (String) collectionInfo.get("configName") : null;
    boolean deleteConfig = cli.hasOption(DELETE_CONFIG_OPTION);

    echoIfVerbose("\nDeleting collection '" + collectionName + "' using V2 Collections API");

    try {
      var req = new CollectionsApi.DeleteCollection(collectionName);
      var response = req.process(solrClient);
      echoIfVerbose(response);
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to delete collection '" + collectionName + "' due to: " + sse.getMessage());
    }

    if (deleteConfig && configName != null) {
      try {
        var req = new ConfigsetsApi.DeleteConfigSet(configName);
        req.setIfUnused(true);
        // The collection was already deleted above, but that deletion may not have propagated to
        // whichever node services this request yet; excludeCollection makes the "still in use"
        // check correct regardless of that timing instead of depending on it.
        req.setExcludeCollection(collectionName);
        var response = req.process(solrClient);
        if (!response.deleted) {
          log.warn(
              "Configuration directory {} is also being used by {}; configuration will not be"
                  + " deleted.",
              configName,
              response.collectionsUsingConfigSet);
        }
      } catch (Exception exc) {
        echo(
            "\nWARNING: Failed to delete configSet "
                + configName
                + " in solr due to: "
                + exc.getMessage()
                + "\nYou'll need to manually delete this znode using the bin/solr zk rm command.");
      }
    }

    echo(String.format(Locale.ROOT, "\nDeleted collection '%s'", collectionName));
  }

  protected void deleteCore(CommandLine cli, SolrClient solrClient) throws Exception {
    String coreName = cli.getOptionValue(COLLECTION_NAME_OPTION);

    echo("\nDeleting core '" + coreName + "' using V2 Cores API\n");

    try {
      var req = new CoresApi.UnloadCore(coreName);
      req.setDeleteIndex(true);
      req.setDeleteDataDir(true);
      req.setDeleteInstanceDir(true);
      var response = req.process(solrClient);
      echoIfVerbose(response);
    } catch (SolrServerException sse) {
      throw new Exception("Failed to delete core '" + coreName + "' due to: " + sse.getMessage());
    }
  }
}
