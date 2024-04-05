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

package org.apache.solr.hdfs.snapshots;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.solr.cli.CLIO;
import org.apache.solr.cli.SolrCLI;
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
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotManager;

/**
 * This class provides the commandline to prepare a snapshot export with HDFS. Other snapshot
 * related commands live in org.apache.solr.cli package.
 */
public class PrepareSnapshotExportCLI implements Closeable, CLIO {

  private static final String COLLECTION = "c";
  private static final String TEMP_DIR = "t";
  private static final String DEST_DIR = "d";
  private static final String HDFS_PATH_PREFIX = "p";
  private static final String BACKUP_REPO_NAME = "r";
  private static final String ASYNC_REQ_ID = "i";


  private final CloudSolrClient solrClient;

  public PrepareSnapshotExportCLI(String solrZkEnsemble) {
    solrClient =
        new CloudSolrClient.Builder(Collections.singletonList(solrZkEnsemble), Optional.empty())
            .build();
  }

  @Override
  public void close() throws IOException {
    if (solrClient != null) {
      solrClient.close();
    }
  }

  private static void checkResponse(CollectionAdminResponse resp, String requestType) {
    if (resp.getStatus() != 0) {
      throw new IllegalStateException(
          "The " + requestType + " request failed. The status code is " + resp.getStatus());
    }
  }

  /**
   * @param pathPrefix optional
   */
  public Map<String, List<String>> getIndexFilesPathForSnapshot(
      String collectionName, String snapshotName, String pathPrefix)
      throws SolrServerException, IOException {
    Map<String, List<String>> result = new HashMap<>();

    Collection<CollectionSnapshotMetaData> snaps = listCollectionSnapshots(collectionName);
    CollectionSnapshotMetaData meta = null;
    for (CollectionSnapshotMetaData m : snaps) {
      if (snapshotName.equals(m.getName())) {
        meta = m;
      }
    }

    if (meta != null) {
      throw new IllegalArgumentException(
          "The snapshot named " + snapshotName + " is not found for collection " + collectionName);
    }

    DocCollection collectionState = solrClient.getClusterState().getCollection(collectionName);
    for (Slice s : collectionState.getSlices()) {
      List<CoreSnapshotMetaData> replicaSnaps = meta.getReplicaSnapshotsForShard(s.getName());
      // Prepare a list of *existing* replicas (since one or more replicas could have been deleted
      // after the snapshot creation).
      List<CoreSnapshotMetaData> availableReplicas = new ArrayList<>();
      for (CoreSnapshotMetaData m : replicaSnaps) {
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
      CoreSnapshotMetaData coreSnap = availableReplicas.get(0);
      for (CoreSnapshotMetaData m : availableReplicas) {
        if (m.isLeader()) {
          coreSnap = m;
        }
      }

      String indexDirPath = coreSnap.getIndexDirPath();
      if (pathPrefix != null) {
        // If the path prefix is specified, rebuild the path to the index directory.
        indexDirPath = new Path(pathPrefix, coreSnap.getIndexDirPath()).toString();
      }

      List<String> paths = new ArrayList<>();
      for (String fileName : coreSnap.getFiles()) {
        Path p = new Path(indexDirPath, fileName);
        paths.add(p.toString());
      }

      result.put(s.getName(), paths);
    }

    return result;
  }

  /**
   * @param pathPrefix optional
   */
  public void buildCopyListings(
      String collectionName, String snapshotName, String localFsPath, String pathPrefix)
      throws SolrServerException, IOException {
    Map<String, List<String>> paths =
        getIndexFilesPathForSnapshot(collectionName, snapshotName, pathPrefix);
    for (Map.Entry<String, List<String>> entry : paths.entrySet()) {
      // TODO: this used to trim - check if that's needed
      // Using Paths.get instead of Path.of because of conflict with o.a.hadoop.fs.Path
      Files.write(Paths.get(localFsPath, entry.getKey()), entry.getValue());
    }
  }

  public void backupCollectionMetaData(String collectionName, String snapshotName, String backupLoc)
      throws SolrServerException, IOException {
    // Backup the collection meta-data
    CollectionAdminRequest.Backup backup =
        new CollectionAdminRequest.Backup(collectionName, snapshotName);
    backup.setIndexBackupStrategy(CollectionAdminParams.NO_INDEX_BACKUP_STRATEGY);
    backup.setLocation(backupLoc);
    CollectionAdminResponse resp = backup.process(solrClient);
    checkResponse(resp, "BACKUP");
  }

  /**
   * @param pathPrefix optional
   */
  public void prepareForExport(
      String collectionName,
      String snapshotName,
      String localFsPath,
      String pathPrefix,
      String destPath) {
    try {
      buildCopyListings(collectionName, snapshotName, localFsPath, pathPrefix);
      CLIO.out("Successfully prepared copylisting for the snapshot export.");
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
      backupCollectionMetaData(collectionName, snapshotName, destPath);
      CLIO.out("Successfully backed up collection meta-data");
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to backup collection meta-data for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
    }
  }

  public static void main(String[] args) throws IOException {
    CommandLineParser parser = new DefaultParser();
    Options options = new Options();

    options.addOption(SolrCLI.OPTION_HELP);

    options.addOption(Option.builder()
            .longOpt("snapshot-name")
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the snapshot to be exported.")
            .build());
    options.addOption(
        TEMP_DIR,
        true,
        "This parameter specifies the path of a temporary directory on local filesystem"
            + " during prepare-snapshot-export command.");
    options.addOption(
        DEST_DIR,
        true,
        "This parameter specifies the path on shared file-system (e.g. HDFS) where the snapshot related"
            + " information should be stored.");
    options.addOption(
        COLLECTION,
        true,
        "This parameter specifies the name of the collection to be used during snapshot operation");
    options.addOption(SolrCLI.OPTION_ZKHOST);
    options.addOption(
        HDFS_PATH_PREFIX,
        true,
        "This parameter specifies the HDFS URI prefix to be used"
            + " during snapshot export preparation. This is applicable only if the Solr collection index files are stored on HDFS.");
    options.addOption(
        BACKUP_REPO_NAME,
        true,
        "This parameter specifies the name of the backup repository to be used"
            + " during snapshot export preparation");
    options.addOption(
        ASYNC_REQ_ID,
        true,
        "This parameter specifies the async request identifier to be used"
            + " during snapshot export preparation");

    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      CLIO.out(e.getLocalizedMessage());
      printHelp(options);
      System.exit(1);
    }

    try (SolrSnapshotsTool tool =
        new SolrSnapshotsTool(requiredArg(options, cmd, SolrCLI.OPTION_ZKHOST.getOpt()))) {

        String snapshotName = requiredArg(options, cmd, "snapshot-name");
        String collectionName = requiredArg(options, cmd, COLLECTION);
        String localFsDir = requiredArg(options, cmd, TEMP_DIR);
        String hdfsOpDir = requiredArg(options, cmd, DEST_DIR);
        String pathPrefix = cmd.getOptionValue(HDFS_PATH_PREFIX);

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
        tool.prepareForExport(collectionName, snapshotName, localFsDir, pathPrefix, hdfsOpDir);

    }
  }

  private static String requiredArg(Options options, CommandLine cmd, String optVal) {
    if (!cmd.hasOption(optVal)) {
      CLIO.out("Please specify the value for option " + optVal);
      printHelp(options);
      System.exit(1);
    }
    return cmd.getOptionValue(optVal);
  }

  private static boolean isReplicaAvailable(Slice s, String coreName) {
    for (Replica r : s.getReplicas()) {
      if (coreName.equals(r.getCoreName())) {
        return true;
      }
    }
    return false;
  }

  private Collection<CollectionSnapshotMetaData> listCollectionSnapshots(String collectionName)
      throws SolrServerException, IOException {
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

  private static void printHelp(Options options) {

    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("prepare-snapshot-export", options);
  }
}
