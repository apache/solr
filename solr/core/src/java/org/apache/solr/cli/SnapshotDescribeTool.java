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
import java.util.Locale;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotManager;

/** Supports snapshot-describe command in the bin/solr script. */
public class SnapshotDescribeTool extends ToolBase {

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
          .desc("Name of the snapshot to describe")
          .build();

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
