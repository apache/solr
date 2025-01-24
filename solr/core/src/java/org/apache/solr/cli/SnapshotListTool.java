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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.SolrSnapshotManager;

/** Supports snapshot-list command in the bin/solr script. */
public class SnapshotListTool extends ToolBase {

  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of collection to list snapshots for.")
          .build();

  public SnapshotListTool() {
    this(CLIO.getOutStream());
  }

  public SnapshotListTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "snapshot-list";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(COLLECTION_NAME_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String collectionName = cli.getOptionValue(COLLECTION_NAME_OPTION);
    try (var solrClient = CLIUtils.getSolrClient(cli)) {
      listSnapshots(solrClient, collectionName);
    }
  }

  public void listSnapshots(SolrClient solrClient, String collectionName) {
    CollectionAdminRequest.ListSnapshots listSnaps =
        new CollectionAdminRequest.ListSnapshots(collectionName);
    CollectionAdminResponse resp;
    try {
      resp = listSnaps.process(solrClient);
      if (resp.getStatus() != 0) {
        throw new IllegalStateException(
            "The LISTSNAPSHOTS request failed. The status code is " + resp.getStatus());
      }

      NamedList<?> apiResult =
          (NamedList<?>) resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO);
      for (int i = 0; i < apiResult.size(); i++) {
        echo(apiResult.getName(i));
      }

    } catch (Exception e) {
      echo(
          "Failed to list snapshots for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
    }
  }
}
