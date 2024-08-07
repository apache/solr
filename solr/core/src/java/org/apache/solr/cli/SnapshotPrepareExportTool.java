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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotManager;

/**
 * Supports prepare-snapshot-export command in the bin/solr script. This command should only be used
 * only if Solr is deployed with Hadoop and collection index files are stored on a shared
 * file-system e.g. HDFS This class should probably be in the hdfs module.
 */
public class SnapshotPrepareExportTool extends ToolBase {

  public SnapshotPrepareExportTool() {
    this(CLIO.getOutStream());
  }

  public SnapshotPrepareExportTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "snapshot-prepare-export";
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
            .desc("Name of the snapshot export to produce.")
            .build(),
        Option.builder()
            .longOpt("temp-dir")
            .argName("DIR")
            .hasArg()
            .required(true)
            .desc(
                "Path of a temporary directory on local filesystem during snapshot-prepare-export command.")
            .build(),
        Option.builder()
            .longOpt("dest-dir")
            .argName("DIR")
            .hasArg()
            .required(true)
            .desc(
                "Path of a path on shared file-system (e.g. HDFS) where the snapshot related information should be stored.")
            .build(),
        Option.builder()
            .longOpt("hdfs-path-prefix")
            .argName("DIR")
            .hasArg()
            .required(false)
            .desc(
                "Specifies the path on shared file-system (e.g. HDFS) where the snapshot related information should be stored.")
            .build(),
        SolrCLI.OPTION_CREDENTIALS,
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);

    String snapshotName = cli.getOptionValue("snapshot-name");
    String collectionName = cli.getOptionValue("name");
    String localFsDir = cli.getOptionValue("temp-dir");
    String hdfsOpDir = cli.getOptionValue("dest-dir");
    String pathPrefix = cli.getOptionValue("hdfs-path-prefix");

    if (pathPrefix != null) {
      try {
        new URI(pathPrefix);
      } catch (URISyntaxException e) {
        throw new IllegalStateException(
            "The specified File system path prefix "
                + pathPrefix
                + " is invalid. The error is "
                + e.getLocalizedMessage());
      }
    }

    try (var solrClient = SolrCLI.getSolrClient(cli)) {
      prepareForExport(
          (CloudSolrClient) solrClient,
          collectionName,
          snapshotName,
          localFsDir,
          pathPrefix,
          hdfsOpDir);
    }
  }

  /**
   * @param pathPrefix optional
   */
  public void prepareForExport(
      CloudSolrClient solrClient,
      String collectionName,
      String snapshotName,
      String localFsPath,
      String pathPrefix,
      String destPath) {
    try {
      buildCopyListings(solrClient, collectionName, snapshotName, localFsPath, pathPrefix);
      echo("Successfully prepared copylisting for the snapshot export.");
    } catch (Exception e) {

      throw new IllegalStateException(
          "Failed to prepare a copylisting for snapshot with name "
              + snapshotName
              + " for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
    }

    try {
      backupCollectionMetaData(solrClient, collectionName, snapshotName, destPath);
      echo("Successfully backed up collection meta-data");
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to backup collection meta-data for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
    }
  }

  /**
   * @param pathPrefix optional
   */
  public void buildCopyListings(
      CloudSolrClient solrClient,
      String collectionName,
      String snapshotName,
      String localFsPath,
      String pathPrefix)
      throws SolrServerException, IOException {
    Map<String, List<String>> paths =
        getIndexFilesPathForSnapshot(solrClient, collectionName, snapshotName, pathPrefix);
    for (Map.Entry<String, List<String>> entry : paths.entrySet()) {
      // TODO: this used to trim - check if that's needed
      // Using Paths.get instead of Path.of because of conflict with o.a.hadoop.fs.Path
      Files.write(Paths.get(localFsPath, entry.getKey()), entry.getValue());
    }
  }

  /**
   * @param pathPrefix optional
   */
  public Map<String, List<String>> getIndexFilesPathForSnapshot(
      CloudSolrClient solrClient, String collectionName, String snapshotName, String pathPrefix)
      throws SolrServerException, IOException {
    Map<String, List<String>> result = new HashMap<>();

    Collection<CollectionSnapshotMetaData> snapshots =
        listCollectionSnapshots(solrClient, collectionName);
    CollectionSnapshotMetaData meta = null;
    for (CollectionSnapshotMetaData snapshot : snapshots) {
      if (snapshotName.equals(snapshot.getName())) {
        meta = snapshot;
      }
    }

    if (meta != null) {
      throw new IllegalArgumentException(
          "The snapshot named " + snapshotName + " is not found for collection " + collectionName);
    }

    DocCollection collectionState = solrClient.getClusterState().getCollection(collectionName);
    for (Slice s : collectionState.getSlices()) {
      List<CollectionSnapshotMetaData.CoreSnapshotMetaData> replicaSnaps =
          meta.getReplicaSnapshotsForShard(s.getName());
      // Prepare a list of *existing* replicas (since one or more replicas could have been deleted
      // after the snapshot creation).
      List<CollectionSnapshotMetaData.CoreSnapshotMetaData> availableReplicas = new ArrayList<>();
      for (CollectionSnapshotMetaData.CoreSnapshotMetaData m : replicaSnaps) {
        if (isReplicaAvailable(s, m.getCoreName())) {
          availableReplicas.add(m);
        }
      }

      if (availableReplicas.isEmpty()) {
        throw new IllegalArgumentException(
            "The snapshot named "
                + snapshotName
                + " not found for shard "
                + s.getName()
                + " of collection "
                + collectionName);
      }

      // Prefer a leader replica (at the time when the snapshot was created).
      CollectionSnapshotMetaData.CoreSnapshotMetaData coreSnap = availableReplicas.get(0);
      for (CollectionSnapshotMetaData.CoreSnapshotMetaData m : availableReplicas) {
        if (m.isLeader()) {
          coreSnap = m;
        }
      }

      String indexDirPath = coreSnap.getIndexDirPath();
      if (pathPrefix != null) {
        // If the path prefix is specified, rebuild the path to the index directory.
        // indexDirPath = new Path(pathPrefix, coreSnap.getIndexDirPath()).toString();
        indexDirPath = Path.of(pathPrefix, coreSnap.getIndexDirPath()).toString();
      }

      List<String> paths = new ArrayList<>();
      for (String fileName : coreSnap.getFiles()) {
        Path p = Path.of(indexDirPath, fileName);
        paths.add(p.toString());
      }

      result.put(s.getName(), paths);
    }

    return result;
  }

  private Collection<CollectionSnapshotMetaData> listCollectionSnapshots(
      SolrClient solrClient, String collectionName) throws SolrServerException, IOException {
    CollectionAdminRequest.ListSnapshots listSnapshots =
        new CollectionAdminRequest.ListSnapshots(collectionName);
    CollectionAdminResponse resp = listSnapshots.process(solrClient);

    checkResponse(resp, "LISTSNAPSHOTS");

    NamedList<?> apiResult =
        (NamedList<?>) resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO);

    Collection<CollectionSnapshotMetaData> result = new ArrayList<>();
    for (int i = 0; i < apiResult.size(); i++) {
      result.add(new CollectionSnapshotMetaData((NamedList<?>) apiResult.getVal(i)));
    }

    return result;
  }

  public void backupCollectionMetaData(
      SolrClient solrClient, String collectionName, String snapshotName, String backupLoc)
      throws SolrServerException, IOException {
    // Backup the collection meta-data
    CollectionAdminRequest.Backup backup =
        new CollectionAdminRequest.Backup(collectionName, snapshotName);
    backup.setIndexBackupStrategy(CollectionAdminParams.NO_INDEX_BACKUP_STRATEGY);
    backup.setLocation(backupLoc);
    CollectionAdminResponse resp = backup.process(solrClient);
    if (resp.getStatus() != 0) {
      throw new IllegalStateException(
          "The BACKUP request failed. The status code is " + resp.getStatus());
    }
  }

  private static boolean isReplicaAvailable(Slice s, String coreName) {
    for (Replica r : s.getReplicas()) {
      if (coreName.equals(r.getCoreName())) {
        return true;
      }
    }
    return false;
  }

  private static void checkResponse(CollectionAdminResponse resp, String requestType) {
    if (resp.getStatus() != 0) {
      throw new IllegalStateException(
          "The " + requestType + " request failed. The status code is " + resp.getStatus());
    }
  }
}
