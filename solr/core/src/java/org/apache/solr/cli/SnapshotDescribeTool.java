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

import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotManager;

/** Supports snapshot-describe command in the bin/solr script. */
public class SnapshotDescribeTool extends ToolBase {

  public SnapshotDescribeTool() {
    this(CLIO.getOutStream());
  }

  public SnapshotDescribeTool(PrintStream stdout) {
    super(stdout);
  }

  private static final DateFormat dateFormat =
      new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z", Locale.getDefault());

  @Override
  public String getName() {
    return "snapshot-describe";
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
            .desc("Name of the snapshot to describe")
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
      describeSnapshot(solrClient, collectionName, snapshotName);
    }
  }

  public void describeSnapshot(SolrClient solrClient, String collectionName, String snapshotName) {
    try {
      Collection<CollectionSnapshotMetaData> snaps =
          listCollectionSnapshots(solrClient, collectionName);
      for (CollectionSnapshotMetaData m : snaps) {
        if (snapshotName.equals(m.getName())) {
          echo("Name: " + m.getName());
          echo("Status: " + m.getStatus());
          echo("Time of creation: " + dateFormat.format(m.getCreationDate()));
          echo("Total number of cores with snapshot: " + m.getReplicaSnapshots().size());
          echo("-----------------------------------");
          for (CollectionSnapshotMetaData.CoreSnapshotMetaData n : m.getReplicaSnapshots()) {
            String builder =
                "Core [name="
                    + n.getCoreName()
                    + ", leader="
                    + n.isLeader()
                    + ", generation="
                    + n.getGenerationNumber()
                    + ", indexDirPath="
                    + n.getIndexDirPath()
                    + "]\n";
            echo(builder);
          }
        }
      }
    } catch (Exception e) {
      echo("Failed to fetch snapshot details due to following error : " + e.getLocalizedMessage());
    }
  }

  private Collection<CollectionSnapshotMetaData> listCollectionSnapshots(
      SolrClient solrClient, String collectionName) throws SolrServerException, IOException {
    CollectionAdminRequest.ListSnapshots listSnapshots =
        new CollectionAdminRequest.ListSnapshots(collectionName);
    CollectionAdminResponse resp = listSnapshots.process(solrClient);

    if (resp.getStatus() != 0) {
      throw new IllegalStateException(
          "The LISTSNAPSHOTS request failed. The status code is " + resp.getStatus());
    }

    NamedList<?> apiResult =
        (NamedList<?>) resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO);

    Collection<CollectionSnapshotMetaData> result = new ArrayList<>();
    for (int i = 0; i < apiResult.size(); i++) {
      result.add(new CollectionSnapshotMetaData((NamedList<?>) apiResult.getVal(i)));
    }

    return result;
  }
}
