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
package org.apache.solr.core.backup;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements functionality to create a backup with extension points provided to
 * integrate with different types of file-systems.
 */
public class BackupManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String COLLECTION_PROPS_FILE = "collection_state.json";
  public static final String TRADITIONAL_BACKUP_PROPS_FILE = "backup.properties";
  public static final String ZK_STATE_DIR = "zk_backup";
  public static final String CONFIG_STATE_DIR = "configs";

  // Backup properties
  public static final String COLLECTION_NAME_PROP = "collection";
  public static final String COLLECTION_ALIAS_PROP = "collectionAlias";
  public static final String BACKUP_NAME_PROP = "backupName";
  public static final String INDEX_VERSION_PROP = "indexVersion";
  public static final String START_TIME_PROP = "startTime";
  public static final String END_TIME_PROP = "endTime";

  protected final ZkStateReader zkStateReader;
  protected final BackupRepository repository;
  protected final BackupId backupId;
  protected final URI backupPath;
  protected final String existingPropsFile;

  private BackupManager(
      BackupRepository repository,
      URI backupPath,
      ZkStateReader zkStateReader,
      String existingPropsFile,
      BackupId backupId) {
    this.repository = Objects.requireNonNull(repository);
    this.backupPath = backupPath;
    this.zkStateReader = Objects.requireNonNull(zkStateReader);
    this.existingPropsFile = existingPropsFile;
    this.backupId = backupId;
  }

  public static BackupManager forIncrementalBackup(
      BackupRepository repository, ZkStateReader stateReader, URI backupPath) {
    Objects.requireNonNull(repository);
    Objects.requireNonNull(stateReader);

    Optional<BackupId> lastBackupId =
        BackupFilePaths.findMostRecentBackupIdFromFileListing(
            repository.listAllOrEmpty(backupPath));

    return new BackupManager(
        repository,
        backupPath,
        stateReader,
        lastBackupId.map(id -> BackupFilePaths.getBackupPropsName(id)).orElse(null),
        lastBackupId.map(BackupId::nextBackupId).orElse(BackupId.zero()));
  }

  public static BackupManager forBackup(
      BackupRepository repository, ZkStateReader stateReader, URI backupPath) {
    Objects.requireNonNull(repository);
    Objects.requireNonNull(stateReader);

    return new BackupManager(
        repository, backupPath, stateReader, null, BackupId.traditionalBackup());
  }

  public static BackupManager forRestore(
      BackupRepository repository, ZkStateReader stateReader, URI backupPath, int bid)
      throws IOException {
    Objects.requireNonNull(repository);
    Objects.requireNonNull(stateReader);

    BackupId backupId = new BackupId(bid);
    String backupPropsName = BackupFilePaths.getBackupPropsName(backupId);
    if (!repository.exists(repository.resolve(backupPath, backupPropsName))) {
      throw new IllegalStateException("Backup id " + bid + " was not found");
    }

    return new BackupManager(repository, backupPath, stateReader, backupPropsName, backupId);
  }

  public static BackupManager forRestore(
      BackupRepository repository, ZkStateReader stateReader, URI backupPath) throws IOException {
    Objects.requireNonNull(repository);
    Objects.requireNonNull(stateReader);

    if (!repository.exists(backupPath)) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR, "Couldn't restore since doesn't exist: " + backupPath);
    }

    Optional<BackupId> opFileGen =
        BackupFilePaths.findMostRecentBackupIdFromFileListing(repository.listAll(backupPath));
    if (opFileGen.isPresent()) {
      BackupId backupPropFile = opFileGen.get();
      return new BackupManager(
          repository,
          backupPath,
          stateReader,
          BackupFilePaths.getBackupPropsName(backupPropFile),
          backupPropFile);
    } else if (repository.exists(repository.resolve(backupPath, TRADITIONAL_BACKUP_PROPS_FILE))) {
      return new BackupManager(
          repository, backupPath, stateReader, TRADITIONAL_BACKUP_PROPS_FILE, null);
    } else {
      throw new IllegalStateException(
          "No "
              + TRADITIONAL_BACKUP_PROPS_FILE
              + " was found, the backup does not exist or not complete");
    }
  }

  public final BackupId getBackupId() {
    return backupId;
  }

  /**
   * @return The version of this backup implementation.
   */
  public final String getVersion() {
    return "1.0";
  }

  /**
   * This method returns the configuration parameters for the specified backup.
   *
   * @return the configuration parameters for the specified backup.
   * @throws IOException In case of errors.
   */
  public BackupProperties readBackupProperties() throws IOException {
    if (existingPropsFile == null) {
      throw new IllegalStateException(
          "No "
              + TRADITIONAL_BACKUP_PROPS_FILE
              + " was found, the backup does not exist or not complete");
    }

    return BackupProperties.readFrom(repository, backupPath, existingPropsFile);
  }

  public Optional<BackupProperties> tryReadBackupProperties() throws IOException {
    if (existingPropsFile != null) {
      return Optional.of(BackupProperties.readFrom(repository, backupPath, existingPropsFile));
    }

    return Optional.empty();
  }

  /**
   * This method stores the backup properties at the specified location in the repository.
   *
   * @param props The backup properties
   * @throws IOException in case of I/O error
   */
  public void writeBackupProperties(BackupProperties props) throws IOException {
    URI dest = repository.resolve(backupPath, BackupFilePaths.getBackupPropsName(backupId));
    try (Writer propsWriter =
        new OutputStreamWriter(repository.createOutput(dest), StandardCharsets.UTF_8)) {
      props.store(propsWriter);
    }
  }

  /**
   * This method reads the meta-data information for the backed-up collection.
   *
   * @param collectionName The name of the collection whose meta-data is to be returned.
   * @return the meta-data information for the backed-up collection.
   * @throws IOException in case of errors.
   */
  public DocCollection readCollectionState(String collectionName) throws IOException {
    Objects.requireNonNull(collectionName);

    URI zkStateDir = getZkStateDir();
    try (IndexInput is =
        repository.openInput(zkStateDir, COLLECTION_PROPS_FILE, IOContext.DEFAULT)) {
      byte[] arr = new byte[(int) is.length()]; // probably ok since the json file should be small.
      is.readBytes(arr, 0, (int) is.length());
      ClusterState c_state = ClusterState.createFromJson(-1, arr, Collections.emptySet());
      return c_state.getCollection(collectionName);
    }
  }

  /**
   * This method writes the collection meta-data to the specified location in the repository.
   *
   * @param collectionName The name of the collection whose meta-data is being stored.
   * @param collectionState The collection meta-data to be stored.
   * @throws IOException in case of I/O errors.
   */
  public void writeCollectionState(String collectionName, DocCollection collectionState)
      throws IOException {
    URI dest = repository.resolve(getZkStateDir(), COLLECTION_PROPS_FILE);
    try (OutputStream collectionStateOs = repository.createOutput(dest)) {
      collectionStateOs.write(
          Utils.toJSON(Collections.singletonMap(collectionName, collectionState)));
    }
  }

  /**
   * This method uploads the Solr configuration files to the desired location in Zookeeper.
   *
   * @param sourceConfigName The name of the config to be copied
   * @param targetConfigName The name of the config to be created.
   * @param configSetService The name of the configset used to upload the config
   * @throws IOException in case of I/O errors.
   */
  public void uploadConfigDir(
      String sourceConfigName, String targetConfigName, ConfigSetService configSetService)
      throws IOException {
    URI source = repository.resolveDirectory(getZkStateDir(), CONFIG_STATE_DIR, sourceConfigName);
    Preconditions.checkState(repository.exists(source), "Path %s does not exist", source);
    uploadConfigToSolrCloud(configSetService, source, targetConfigName, "");
  }

  /**
   * This method stores the contents of a specified Solr config at the specified location in
   * repository.
   *
   * @param configName The name of the config to be saved.
   * @param configSetService The name of the configset used to download the config
   * @throws IOException in case of I/O errors.
   */
  public void downloadConfigDir(String configName, ConfigSetService configSetService)
      throws IOException {
    URI dest = repository.resolveDirectory(getZkStateDir(), CONFIG_STATE_DIR, configName);
    repository.createDirectory(getZkStateDir());
    repository.createDirectory(repository.resolveDirectory(getZkStateDir(), CONFIG_STATE_DIR));
    repository.createDirectory(dest);

    downloadConfigToRepo(configSetService, configName, dest);
  }

  public void uploadCollectionProperties(String collectionName) throws IOException {
    URI sourceDir = getZkStateDir();
    URI source = repository.resolve(sourceDir, ZkStateReader.COLLECTION_PROPS_ZKNODE);
    if (!repository.exists(source)) {
      // No collection properties to restore
      return;
    }
    String zkPath =
        ZkStateReader.COLLECTIONS_ZKNODE
            + '/'
            + collectionName
            + '/'
            + ZkStateReader.COLLECTION_PROPS_ZKNODE;

    try (IndexInput is =
        repository.openInput(sourceDir, ZkStateReader.COLLECTION_PROPS_ZKNODE, IOContext.DEFAULT)) {
      byte[] arr = new byte[(int) is.length()];
      is.readBytes(arr, 0, (int) is.length());
      zkStateReader.getZkClient().create(zkPath, arr, CreateMode.PERSISTENT, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(
          "Error uploading file to zookeeper path " + source.toString() + " to " + zkPath,
          SolrZkClient.checkInterrupted(e));
    }
  }

  public void downloadCollectionProperties(String collectionName) throws IOException {
    URI dest = repository.resolve(getZkStateDir(), ZkStateReader.COLLECTION_PROPS_ZKNODE);
    String zkPath =
        ZkStateReader.COLLECTIONS_ZKNODE
            + '/'
            + collectionName
            + '/'
            + ZkStateReader.COLLECTION_PROPS_ZKNODE;

    try {
      if (!zkStateReader.getZkClient().exists(zkPath, true)) {
        // Nothing to back up
        return;
      }

      try (OutputStream os = repository.createOutput(dest)) {
        byte[] data = zkStateReader.getZkClient().getData(zkPath, null, null, true);
        os.write(data);
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(
          "Error downloading file from zookeeper path " + zkPath + " to " + dest.toString(),
          SolrZkClient.checkInterrupted(e));
    }
  }

  private void downloadConfigToRepo(ConfigSetService configSetService, String configName, URI dir)
      throws IOException {
    List<String> filePaths = configSetService.getAllConfigFiles(configName);
    for (String filePath : filePaths) {
      URI uri = repository.resolve(dir, filePath);
      if (!filePath.endsWith("/")) {
        log.debug("Writing file {}", filePath);
        byte[] data = configSetService.downloadFileFromConfig(configName, filePath);
        try (OutputStream os = repository.createOutput(uri)) {
          os.write(data);
        }
      } else {
        if (!repository.exists(uri)) {
          repository.createDirectory(uri);
        }
      }
    }
  }

  private void uploadConfigToSolrCloud(
      ConfigSetService configSetService, URI sourceDir, String configName, String filePrefix)
      throws IOException {
    for (String file : repository.listAll(sourceDir)) {
      String filePath = filePrefix + file;
      URI path = repository.resolve(sourceDir, file);
      BackupRepository.PathType t = repository.getPathType(path);
      switch (t) {
        case FILE:
          {
            try (IndexInput is = repository.openInput(sourceDir, file, IOContext.DEFAULT)) {
              // probably ok since the config file should be small.
              byte[] arr = new byte[(int) is.length()];
              is.readBytes(arr, 0, (int) is.length());
              configSetService.uploadFileToConfig(configName, filePath, arr, false);
            }
            break;
          }
        case DIRECTORY:
          {
            if (!file.startsWith(".")) {
              uploadConfigToSolrCloud(configSetService, path, configName, filePath + "/");
            }
            break;
          }
        default:
          throw new IllegalStateException("Unknown path type " + t);
      }
    }
  }

  private URI getZkStateDir() {
    URI zkStateDir;
    if (backupId != null) {
      final String zkBackupFolder = BackupFilePaths.getZkStateDir(backupId);
      zkStateDir = repository.resolveDirectory(backupPath, zkBackupFolder);
    } else {
      zkStateDir = repository.resolveDirectory(backupPath, ZK_STATE_DIR);
    }
    return zkStateDir;
  }
}
