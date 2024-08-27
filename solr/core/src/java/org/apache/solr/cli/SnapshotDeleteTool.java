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

/** Supports snapshot-delete command in the bin/solr script. */
public class SnapshotDeleteTool extends ToolBase {

  public SnapshotDeleteTool() {
    this(CLIO.getOutStream());
  }

  public SnapshotDeleteTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "snapshot-delete";
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
            .desc("Name of collection to manage.")
            .build(),
        Option.builder()
            .longOpt("snapshot-name")
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the snapshot to delete")
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
      deleteSnapshot(solrClient, collectionName, snapshotName);
    }
  }

  public void deleteSnapshot(SolrClient solrClient, String collectionName, String snapshotName) {
    CollectionAdminRequest.DeleteSnapshot deleteSnapshot =
        new CollectionAdminRequest.DeleteSnapshot(collectionName, snapshotName);
    CollectionAdminResponse resp;
    try {
      resp = deleteSnapshot.process(solrClient);
      if (resp.getStatus() != 0) {
        throw new IllegalStateException(
            "The DELETESNAPSHOT request failed. The status code is " + resp.getStatus());
      }
      echo(
          "Successfully deleted snapshot with name "
              + snapshotName
              + " for collection "
              + collectionName);

    } catch (Exception e) {
      echo(
          "Failed to delete a snapshot with name "
              + snapshotName
              + " for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
    }
  }
}
