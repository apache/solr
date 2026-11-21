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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionSnapshotsApi;

/** Supports snapshot-describe command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "describe",
    description = "Describes a named snapshot of a collection.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Describe a snapshot",
      "  bin/solr snapshot describe -c mycollection --snapshot-name snap1"
    })
public class SnapshotDescribeTool extends ToolBase {

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of collection to be snapshot.")
          .get();

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option SNAPSHOT_NAME_OPTION =
      Option.builder()
          .longOpt("snapshot-name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of the snapshot to describe")
          .get();

  /** Parameters for the snapshot-describe command, independent of the command line parser. */
  record SnapshotDescribeParams(
      String solrUrl, String credentials, String collectionName, String snapshotName) {}

  // --- picocli fields ---

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private ConnectionOptions connectionOptions;

  @picocli.CommandLine.Mixin private CredentialsOptions credentialsOptions;

  @picocli.CommandLine.Mixin private CollectionNameOptions collection;

  @picocli.CommandLine.Mixin private SnapshotNameOptions snapshot;

  public SnapshotDescribeTool() {
    this(new DefaultToolRuntime());
  }

  public SnapshotDescribeTool(ToolRuntime runtime) {
    super(runtime);
  }

  private static final DateFormat dateFormat =
      new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.getDefault());

  @Override
  public String getName() {
    return "snapshot-describe";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(COLLECTION_NAME_OPTION)
        .addOption(SNAPSHOT_NAME_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SnapshotDescribeParams params =
        new SnapshotDescribeParams(
            CLIUtils.normalizeSolrUrl(cli),
            cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION),
            cli.getOptionValue(COLLECTION_NAME_OPTION),
            cli.getOptionValue(SNAPSHOT_NAME_OPTION));
    describeSnapshot(params);
  }

  void describeSnapshot(SnapshotDescribeParams params) throws Exception {
    try (var solrClient = CLIUtils.getSolrClient(params.solrUrl(), params.credentials())) {
      describeSnapshot(solrClient, params.collectionName(), params.snapshotName());
    }
  }

  public void describeSnapshot(SolrClient solrClient, String collectionName, String snapshotName) {
    try {
      var response = new CollectionSnapshotsApi.ListSnapshots(collectionName).process(solrClient);
      var metadata = response.snapshots.get(snapshotName);
      if (metadata != null) {
        echo("Name: " + metadata.name);
        echo("Status: " + metadata.status);
        echo("Time of creation: " + dateFormat.format(new Date(metadata.creationDate)));
        echo("Total number of cores with snapshot: " + metadata.replicaSnapshots.size());
        echo("-----------------------------------");
        for (var replica : metadata.replicaSnapshots) {
          echo(
              "Core [name="
                  + replica.coreName
                  + ", leader="
                  + replica.leader
                  + ", generation="
                  + replica.generationNumber
                  + ", indexDirPath="
                  + replica.indexDirPath
                  + "]\n");
        }
      }
    } catch (Exception e) {
      echo("Failed to fetch snapshot details due to following error : " + e.getLocalizedMessage());
    }
  }

  @Override
  public int callTool() throws Exception {
    SnapshotDescribeParams params =
        new SnapshotDescribeParams(
            CLIUtils.resolveSolrUrl(connectionOptions, credentialsOptions.credentials),
            credentialsOptions.credentials,
            collection.name,
            snapshot.name);
    describeSnapshot(params);
    return 0;
  }
}
