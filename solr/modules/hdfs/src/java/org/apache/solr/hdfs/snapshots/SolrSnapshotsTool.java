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
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.Path;
import org.apache.solr.cli.CLIO;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides utility functions required for Solr on HDFS specific snapshots'
 * functionality.
 *
 * <p>For general purpose snapshot tooling see the related classes in the {@link
 * org.apache.solr.cli} package.
 */
public class SolrSnapshotsTool implements Closeable, CLIO {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PREPARE_FOR_EXPORT = "prepare-snapshot-export";
  private static final String HELP = "help";
  private static final String COLLECTION = "c";
  private static final String TEMP_DIR = "t";
  private static final String DEST_DIR = "d";
  private static final String SOLR_ZK_ENSEMBLE = "z";
  private static final String HDFS_PATH_PREFIX = "p";
  private static final List<String> OPTION_HELP_ORDER =
      Arrays.asList(
          PREPARE_FOR_EXPORT,
          HELP,
          SOLR_ZK_ENSEMBLE,
          COLLECTION,
          DEST_DIR,
          TEMP_DIR,
          HDFS_PATH_PREFIX);

  private final CloudSolrClient solrClient;

  public SolrSnapshotsTool(String solrZkEnsemble) {
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
   * HDFS specific step required before exporting a snapshot
   *
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

      log.error(
          "Failed to prepare a copylisting for snapshot with name {} for collection {}",
          snapshotName,
          collectionName,
          e);

      CLIO.out(
          "Failed to prepare a copylisting for snapshot with name "
              + snapshotName
              + " for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
      System.exit(1);
    }

    try {
      backupCollectionMetaData(collectionName, snapshotName, destPath);
      CLIO.out("Successfully backed up collection meta-data");
    } catch (Exception e) {
      log.error("Failed to backup collection meta-data for collection {}", collectionName, e);
      CLIO.out(
          "Failed to backup collection meta-data for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
      System.exit(1);
    }
  }

  public static void main(String[] args) throws IOException {
    CommandLineParser parser = new PosixParser();
    Options options = new Options();

    options.addOption(
        null,
        PREPARE_FOR_EXPORT,
        true,
        "This command will prepare copylistings for the specified snapshot."
            + " This command should only be used only if Solr is deployed with Hadoop and collection index files are stored on a shared"
            + " file-system e.g. HDFS");

    options.addOption(
        null,
        HELP,
        false,
        "This command will print the help message for the snapshots related commands.");
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
    options.addOption(
        SOLR_ZK_ENSEMBLE, true, "This parameter specifies the Solr Zookeeper ensemble address");
    options.addOption(
        HDFS_PATH_PREFIX,
        true,
        "This parameter specifies the HDFS URI prefix to be used"
            + " during snapshot export preparation. This is applicable only if the Solr collection index files are stored on HDFS.");

    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      CLIO.out(e.getLocalizedMessage());
      printHelp(options);
      System.exit(1);
    }

    if (cmd.hasOption(PREPARE_FOR_EXPORT)) {
      try (SolrSnapshotsTool tool =
          new SolrSnapshotsTool(requiredArg(options, cmd, SOLR_ZK_ENSEMBLE))) {
        if (cmd.hasOption(PREPARE_FOR_EXPORT)) {
          String snapshotName = cmd.getOptionValue(PREPARE_FOR_EXPORT);
          String collectionName = requiredArg(options, cmd, COLLECTION);
          String localFsDir = requiredArg(options, cmd, TEMP_DIR);
          String hdfsOpDir = requiredArg(options, cmd, DEST_DIR);
          String pathPrefix = cmd.getOptionValue(HDFS_PATH_PREFIX);

          if (pathPrefix != null) {
            try {
              new URI(pathPrefix);
            } catch (URISyntaxException e) {
              CLIO.out(
                  "The specified File system path prefix "
                      + pathPrefix
                      + " is invalid. The error is "
                      + e.getLocalizedMessage());
              System.exit(1);
            }
          }
          tool.prepareForExport(collectionName, snapshotName, localFsDir, pathPrefix, hdfsOpDir);
        }
      }
    } else if (cmd.hasOption(HELP)) {
      printHelp(options);
    } else {
      CLIO.out("Unknown command specified.");
      printHelp(options);
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
    StringBuilder helpFooter = new StringBuilder();
    helpFooter.append("Examples: \n");
    helpFooter.append(
        "prepare-snapshot-export.sh --prepare-snapshot-export snapshot-1 -c books -z localhost:2181 -b repo -l backupPath -i req_0 \n");

    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptionComparator(new OptionComarator<>());
    formatter.printHelp("SolrSnapshotsTool", null, options, helpFooter.toString(), false);
  }

  private static class OptionComarator<T extends Option> implements Comparator<T> {

    @Override
    public int compare(T o1, T o2) {
      String s1 = o1.hasLongOpt() ? o1.getLongOpt() : o1.getOpt();
      String s2 = o2.hasLongOpt() ? o2.getLongOpt() : o2.getOpt();
      return OPTION_HELP_ORDER.indexOf(s1) - OPTION_HELP_ORDER.indexOf(s2);
    }
  }
}
