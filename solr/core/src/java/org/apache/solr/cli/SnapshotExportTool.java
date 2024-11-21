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
import java.util.Optional;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.CollectionAdminParams;

/** Supports snapshot-export command in the bin/solr script. */
public class SnapshotExportTool extends ToolBase {

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
          .desc("Name of the snapshot to be exported.")
          .build();

  private static final Option DEST_DIR_OPTION =
      Option.builder()
          .longOpt("dest-dir")
          .hasArg()
          .argName("DIR")
          .required()
          .desc("Path of a temporary directory on local filesystem during snapshot export command.")
          .build();

  private static final Option BACKUP_REPO_NAME_OPTION =
      Option.builder()
          .longOpt("backup-repo-name")
          .hasArg()
          .argName("DIR")
          .desc(
              "Specifies name of the backup repository to be used during snapshot export preparation.")
          .build();

  private static final Option ASYNC_ID_OPTION =
      Option.builder()
          .longOpt("async-id")
          .hasArg()
          .argName("ID")
          .desc(
              "Specifies the async request identifier to be used during snapshot export preparation.")
          .build();

  public SnapshotExportTool() {
    this(CLIO.getOutStream());
  }

  public SnapshotExportTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "snapshot-export";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(COLLECTION_NAME_OPTION)
        .addOption(SNAPSHOT_NAME_OPTION)
        .addOption(DEST_DIR_OPTION)
        .addOption(BACKUP_REPO_NAME_OPTION)
        .addOption(ASYNC_ID_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String snapshotName = cli.getOptionValue(SNAPSHOT_NAME_OPTION);
    String collectionName = cli.getOptionValue(COLLECTION_NAME_OPTION);
    String destDir = cli.getOptionValue(DEST_DIR_OPTION);
    Optional<String> backupRepo = Optional.ofNullable(cli.getOptionValue(BACKUP_REPO_NAME_OPTION));
    Optional<String> asyncReqId = Optional.ofNullable(cli.getOptionValue(ASYNC_ID_OPTION));

    try (var solrClient = CLIUtils.getSolrClient(cli)) {
      exportSnapshot(solrClient, collectionName, snapshotName, destDir, backupRepo, asyncReqId);
    }
  }

  public void exportSnapshot(
      SolrClient solrClient,
      String collectionName,
      String snapshotName,
      String destPath,
      Optional<String> backupRepo,
      Optional<String> asyncReqId) {
    try {
      CollectionAdminRequest.Backup backup =
          new CollectionAdminRequest.Backup(collectionName, snapshotName);
      backup.setCommitName(snapshotName);
      backup.setIndexBackupStrategy(CollectionAdminParams.COPY_FILES_STRATEGY);
      backup.setLocation(destPath);
      if (backupRepo.isPresent()) {
        backup.setRepositoryName(backupRepo.get());
      }
      // if asyncId is null, processAsync will block and throw an Exception with any error
      backup.processAsync(asyncReqId.orElse(null), solrClient);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to backup collection meta-data for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
    }
  }
}
