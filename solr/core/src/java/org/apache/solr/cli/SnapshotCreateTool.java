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
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;

/** Supports snapshot-create command in the bin/solr script. */
public class SnapshotCreateTool extends ToolBase {

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
  public List<Option> getOptions() {
    return List.of(
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_SOLRURL,
        Option.builder("c")
            .longOpt("name")
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of collection to be snapshot.")
            .build(),
        Option.builder()
            .longOpt("snapshot-name")
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the snapshot to produce")
            .build(),
        SolrCLI.OPTION_CREDENTIALS,
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);

    String snapshotName = cli.getOptionValue("snapshot-name");
    String collectionName = cli.getOptionValue("name");
    try (var solrClient = SolrCLI.getSolrClient(cli)) {
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
