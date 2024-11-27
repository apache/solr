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

import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;

/** Supports snapshot-create command in the bin/solr script. */
public class SnapshotCreateTool extends ToolBase {

  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of collection to be snapshot.")
          .build();

  private static final Option SNAPSHOT_NAME_OPTION =
      Option.builder()
          .longOpt("snapshot-name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of the snapshot to produce")
          .build();

  public SnapshotCreateTool() {
    this(CLIO.getOutStream());
  }

  public SnapshotCreateTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "snapshot-create";
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
    String snapshotName = cli.getOptionValue(SNAPSHOT_NAME_OPTION);
    String collectionName = cli.getOptionValue(COLLECTION_NAME_OPTION);
    try (var solrClient = CLIUtils.getSolrClient(cli)) {
      createSnapshot(solrClient, collectionName, snapshotName);
    }
  }

  public void createSnapshot(SolrClient solrClient, String collectionName, String snapshotName) {
    CollectionAdminRequest.CreateSnapshot createSnapshot =
        new CollectionAdminRequest.CreateSnapshot(collectionName, snapshotName);
    CollectionAdminResponse resp;
    try {
      resp = createSnapshot.process(solrClient);
      if (resp.getStatus() != 0) {
        throw new IllegalStateException(
            "The CREATESNAPSHOT request failed. The status code is " + resp.getStatus());
      }
      echo(
          "Successfully created snapshot with name "
              + snapshotName
              + " for collection "
              + collectionName);

    } catch (Exception e) {
      echo(
          "Failed to create a snapshot with name "
              + snapshotName
              + " for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
    }
  }
}
