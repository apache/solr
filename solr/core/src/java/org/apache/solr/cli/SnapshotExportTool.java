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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.CollectionAdminParams;

/** Supports snapshot-export command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "export",
    description = "Exports a named snapshot of a collection to a local directory.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Export a snapshot of a collection",
      "  bin/solr snapshot export -c mycollection --snapshot-name snap1 --dest-dir /tmp/backups --backup-repo-name local"
    })
public class SnapshotExportTool extends ToolBase {

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of collection to be snapshot.")
          .get();

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option SNAPSHOT_NAME_OPTION =
      Option.builder()
          .longOpt("snapshot-name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of the snapshot to be exported.")
          .get();

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option DEST_DIR_OPTION =
      Option.builder()
          .longOpt("dest-dir")
          .hasArg()
          .argName("DIR")
          .required()
          .desc("Path of a temporary directory on local filesystem during snapshot export command.")
          .get();

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option BACKUP_REPO_NAME_OPTION =
      Option.builder()
          .longOpt("backup-repo-name")
          .hasArg()
          .argName("DIR")
          .desc(
              "Specifies name of the backup repository to be used during snapshot export preparation.")
          .get();

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option ASYNC_ID_OPTION =
      Option.builder()
          .longOpt("async-id")
          .hasArg()
          .argName("ID")
          .desc(
              "Specifies the async request identifier to be used during snapshot export preparation.")
          .get();

  /** Parameters for the snapshot-export command, independent of the command line parser. */
  record SnapshotExportParams(
      String solrUrl,
      String credentials,
      String collectionName,
      String snapshotName,
      String destDir,
      String backupRepo,
      String asyncReqId) {}

  // --- picocli fields ---

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private ConnectionOptions connectionOptions;

  @picocli.CommandLine.Mixin private CredentialsOptions credentialsOptions;

  @picocli.CommandLine.Mixin private CollectionNameOptions collection;

  @picocli.CommandLine.Option(
      names = "--snapshot-name",
      required = true,
      paramLabel = "NAME",
      description = "Name of the snapshot to be exported.")
  private String snapshotNameOpt;

  @picocli.CommandLine.Option(
      names = "--dest-dir",
      required = true,
      paramLabel = "DIR",
      description =
          "Path of a temporary directory on local filesystem during snapshot export command.")
  private String destDirOpt;

  @picocli.CommandLine.Option(
      names = "--backup-repo-name",
      paramLabel = "NAME",
      description =
          "Specifies name of the backup repository to be used during snapshot export preparation.")
  private String backupRepoNameOpt;

  @picocli.CommandLine.Option(
      names = "--async-id",
      paramLabel = "ID",
      description =
          "Specifies the async request identifier to be used during snapshot export preparation.")
  private String asyncIdOpt;

  public SnapshotExportTool() {
    this(new DefaultToolRuntime());
  }

  public SnapshotExportTool(ToolRuntime runtime) {
    super(runtime);
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
    SnapshotExportParams params =
        new SnapshotExportParams(
            CLIUtils.normalizeSolrUrl(cli),
            cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION),
            cli.getOptionValue(COLLECTION_NAME_OPTION),
            cli.getOptionValue(SNAPSHOT_NAME_OPTION),
            cli.getOptionValue(DEST_DIR_OPTION),
            cli.getOptionValue(BACKUP_REPO_NAME_OPTION),
            cli.getOptionValue(ASYNC_ID_OPTION));
    exportSnapshot(params);
  }

  void exportSnapshot(SnapshotExportParams params) throws Exception {
    try (var solrClient = CLIUtils.getSolrClient(params.solrUrl(), params.credentials())) {
      exportSnapshot(
          solrClient,
          params.collectionName(),
          params.snapshotName(),
          params.destDir(),
          params.backupRepo(),
          params.asyncReqId());
    }
  }

  public void exportSnapshot(
      SolrClient solrClient,
      String collectionName,
      String snapshotName,
      String destPath,
      String backupRepo,
      String asyncReqId) {
    try {
      CollectionAdminRequest.Backup backup =
          new CollectionAdminRequest.Backup(collectionName, snapshotName);
      backup.setCommitName(snapshotName);
      backup.setIncremental(false);
      backup.setIndexBackupStrategy(CollectionAdminParams.COPY_FILES_STRATEGY);
      backup.setLocation(destPath);
      if (backupRepo != null) {
        backup.setRepositoryName(backupRepo);
      }
      // if asyncId is null, processAsync will block and throw an Exception with any error
      backup.processAsync(asyncReqId, solrClient);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to backup collection meta-data for collection "
              + collectionName
              + " due to following error : "
              + e.getLocalizedMessage());
    }
  }

  @Override
  public int callTool() throws Exception {
    SnapshotExportParams params =
        new SnapshotExportParams(
            CLIUtils.resolveSolrUrl(connectionOptions, credentialsOptions.credentials),
            credentialsOptions.credentials,
            collection.name,
            snapshotNameOpt,
            destDirOpt,
            backupRepoNameOpt,
            asyncIdOpt);
    exportSnapshot(params);
    return 0;
  }
}
