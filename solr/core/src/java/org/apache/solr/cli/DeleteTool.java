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

import java.util.Locale;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionsApi;
import org.apache.solr.client.solrj.request.ConfigsetsApi;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;

/** Supports delete command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "delete",
    description =
        "Deletes a collection or core depending on whether Solr is running in SolrCloud or standalone mode.",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0:Collection or core deleted successfully.",
      "1:Failed to delete; collection or core may not exist, or Solr may not be running."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Delete a collection in SolrCloud mode",
      "  bin/solr delete -c myCollection",
      "",
      "  # Delete and also remove the associated configset",
      "  bin/solr delete -c myCollection --delete-config"
    })
public class DeleteTool extends ToolBase {

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

  /**
   * @deprecated Since Solr 11.0. No longer has any effect: the Overseer's configset-delete command
   *     unconditionally refuses to delete a configset that's still in use by another collection, so
   *     this flag was never actually able to bypass that safety check. Kept, as a no-op, for
   *     backward compatibility with existing scripts.
   */
  @Deprecated(since = "11.0")
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

  /** Options bean shared between commons-cli and picocli paths. */
  record DeleteParams(String name, boolean deleteConfig) {}

  // --- picocli fields ---

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private ConnectionOptions connectionOptions;

  @picocli.CommandLine.Mixin private CredentialsOptions credentialsOptions;

  @picocli.CommandLine.Option(
      names = {"-c", "--name"},
      required = true,
      description = "Name of the core / collection to delete.")
  private String name;

  @picocli.CommandLine.Option(
      names = {"--delete-config"},
      description =
          "Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is true.")
  private boolean deleteConfig;

  /**
   * @deprecated Since Solr 11.0. See {@link #FORCE_OPTION}.
   */
  @Deprecated(since = "11.0")
  @picocli.CommandLine.Option(
      names = {"-f", "--force"},
      hidden = true,
      description = "No longer has any effect; retained for backward compatibility.")
  private boolean force;

  public DeleteTool() {
    this(new DefaultToolRuntime());
  }

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
      DeleteParams params =
          new DeleteParams(
              cli.getOptionValue(COLLECTION_NAME_OPTION), cli.hasOption(DELETE_CONFIG_OPTION));
      delete(params, solrClient);
    }
  }

  @Override
  public int callTool() throws Exception {
    String zkHostArg =
        (connectionOptions != null)
            ? connectionOptions.effectiveZkHost()
            : EnvUtils.getProperty("zkHost");
    String solrUrlArg = (connectionOptions != null) ? connectionOptions.effectiveSolrUrl() : null;
    String credentials = (credentialsOptions != null) ? credentialsOptions.credentials : null;

    String resolvedSolrUrl;
    if (solrUrlArg != null) {
      resolvedSolrUrl = CLIUtils.normalizeSolrUrl(solrUrlArg);
    } else if (zkHostArg != null) {
      resolvedSolrUrl =
          CLIUtils.solrUrlFromConnection(
              CloudSolrClient.CloudSolrClientConnection.parse(zkHostArg), credentials);
    } else {
      resolvedSolrUrl = CLIUtils.getDefaultSolrUrl();
      CLIO.err(
          "Neither --zk-host or --solr-url parameters, nor ZK_HOST env var provided, so assuming solr url is "
              + resolvedSolrUrl
              + ".");
    }

    try (var solrClient = CLIUtils.getSolrClient(resolvedSolrUrl, credentials)) {
      delete(new DeleteParams(name, deleteConfig), solrClient);
    }
    return 0;
  }

  private void delete(DeleteParams params, SolrClient solrClient) throws Exception {
    if (CLIUtils.isCloudMode(solrClient)) {
      deleteCollection(params, solrClient);
    } else {
      deleteCore(params, solrClient);
    }
  }

  protected void deleteCollection(DeleteParams params, SolrClient solrClient) throws Exception {
    String collectionName = params.name();

    // Scoping the request to this one collection also serves as the existence check below,
    // instead of a separate ListCollections call that would have to scan every collection in
    // the cluster.
    String configName;
    try {
      var statusReq = new CollectionsApi.GetCollectionStatus(collectionName);
      var statusResponse = statusReq.process(solrClient);
      configName = statusResponse.properties != null ? statusResponse.properties.configName : null;
    } catch (RemoteSolrException e) {
      if (e.code() == SolrException.ErrorCode.NOT_FOUND.code) {
        throw new IllegalArgumentException("Collection " + collectionName + " not found!");
      }
      throw e;
    }

    echoIfVerbose("\nDeleting collection '" + collectionName + "' using V2 Collections API");

    try {
      var req = new CollectionsApi.DeleteCollection(collectionName);
      var response = req.process(solrClient);
      echoIfVerbose(response);
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to delete collection '" + collectionName + "' due to: " + sse.getMessage());
    }

    if (params.deleteConfig() && configName != null) {
      try {
        var req = new ConfigsetsApi.DeleteConfigSet(configName);
        req.process(solrClient);
      } catch (Exception exc) {
        // Most commonly, this configset is still in use by another collection -- the
        // configset-delete command unconditionally refuses to delete it in that case.
        echo(
            "\nWARNING: configSet "
                + configName
                + " was not deleted.  Most commonly it is still useed by another collection.  Error: "
                + exc.getMessage());
      }
    }

    echo(String.format(Locale.ROOT, "\nDeleted collection '%s'", collectionName));
  }

  protected void deleteCore(DeleteParams params, SolrClient solrClient) throws Exception {
    String coreName = params.name();

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
