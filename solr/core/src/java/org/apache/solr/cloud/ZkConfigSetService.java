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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SolrCloud Zookeeper ConfigSetService impl. */
public class ZkConfigSetService extends ConfigSetService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ZkController zkController;
  private final SolrZkClient zkClient;
  /** ZkNode where named configs are stored */
  public static final String CONFIGS_ZKNODE = "/configs";

  public ZkConfigSetService(CoreContainer cc) {
    super(cc.getResourceLoader(), cc.getConfig().hasSchemaCache());
    this.zkController = cc.getZkController();
    this.zkClient = cc.getZkController().getZkClient();
  }

  /** This is for ZkCLI and some tests */
  public ZkConfigSetService(SolrZkClient zkClient) {
    super(null, false);
    this.zkController = null;
    this.zkClient = zkClient;
  }

  @Override
  public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
    final String colName = cd.getCollectionName();

    // For back compat with cores that can create collections without the collections API
    try {
      if (!zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + colName, true)) {
        // TODO remove this functionality or maybe move to a CLI mechanism
        log.warn(
            "Auto-creating collection (in ZK) from core descriptor (on disk).  This feature may go away!");
        CreateCollectionCmd.createCollectionZkNode(
            zkController.getSolrCloudManager().getDistribStateManager(),
            colName,
            cd.getCloudDescriptor().getParams(),
            zkController.getCoreContainer().getConfigSetService());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR, "Interrupted auto-creating collection", e);
    } catch (KeeperException e) {
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR, "Failure auto-creating collection", e);
    }

    // The configSet is read from ZK and populated.  Ignore CD's pre-existing configSet; only
    // populated in standalone
    String configSetName = zkController.getClusterState().getCollection(colName).getConfigName();
    cd.setConfigSet(configSetName);

    return new ZkSolrResourceLoader(
        cd.getInstanceDir(), configSetName, parentLoader.getClassLoader(), zkController);
  }

  @Override
  protected NamedList<Object> loadConfigSetFlags(SolrResourceLoader loader) throws IOException {
    try {
      // ConfigSet flags are loaded from the metadata of the ZK node of the configset.
      return ConfigSetProperties.readFromResourceLoader(loader, ".");
    } catch (Exception ex) {
      log.debug("No configSet flags", ex);
      return null;
    }
  }

  @Override
  protected Long getCurrentSchemaModificationVersion(
      String configSet, SolrConfig solrConfig, String schemaFile) throws IOException {
    String zkPath = CONFIGS_ZKNODE + "/" + configSet + "/" + schemaFile;
    Stat stat;
    try {
      stat = zkClient.exists(zkPath, null, true);
    } catch (KeeperException e) {
      log.warn("Unexpected exception when getting modification time of {}", zkPath, e);
      return null; // debatable; we'll see an error soon if there's a real problem
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    if (stat == null) { // not found
      return null;
    }
    return (long) stat.getVersion();
  }

  @Override
  public String configSetName(CoreDescriptor cd) {
    return "configset " + cd.getConfigSet();
  }

  @Override
  public boolean checkConfigExists(String configName) throws IOException {
    try {
      return zkClient.exists(CONFIGS_ZKNODE + "/" + configName, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(
          "Error checking whether config exists", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void deleteConfig(String configName) throws IOException {
    try {
      zkClient.clean(CONFIGS_ZKNODE + "/" + configName);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error deleting config", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void deleteFilesFromConfig(String configName, List<String> filesToDelete)
      throws IOException {
    Objects.requireNonNull(filesToDelete);
    try {
      for (String fileToDelete : filesToDelete) {
        if (fileToDelete.endsWith("/")) {
          fileToDelete = fileToDelete.substring(0, fileToDelete.length() - 1);
        }
        zkClient.clean(CONFIGS_ZKNODE + "/" + configName + "/" + fileToDelete);
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error deleting files in config", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void copyConfig(String fromConfig, String toConfig) throws IOException {
    String fromConfigPath = CONFIGS_ZKNODE + "/" + fromConfig;
    String toConfigPath = CONFIGS_ZKNODE + "/" + toConfig;
    try {
      copyData(fromConfigPath, toConfigPath);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(
          "Error config " + fromConfig + " to " + toConfig, SolrZkClient.checkInterrupted(e));
    }
    copyConfigDirFromZk(fromConfigPath, toConfigPath);
  }

  @Override
  public void uploadConfig(String configName, Path dir) throws IOException {
    zkClient.uploadToZK(
        dir, CONFIGS_ZKNODE + "/" + configName, ConfigSetService.UPLOAD_FILENAME_EXCLUDE_PATTERN);
  }

  @Override
  public void uploadFileToConfig(
      String configName, String fileName, byte[] data, boolean overwriteOnExists)
      throws IOException {
    String filePath = CONFIGS_ZKNODE + "/" + configName + "/" + fileName;
    try {
      if (ZkMaintenanceUtils.isFileForbiddenInConfigSets(fileName)) {
        log.warn("Not including uploading file to config, as it is a forbidden type: {}", fileName);
      } else {
        // if overwriteOnExists is true then zkClient#makePath failOnExists is set to false
        zkClient.makePath(filePath, data, CreateMode.PERSISTENT, null, !overwriteOnExists, true);
      }
    } catch (KeeperException.NodeExistsException nodeExistsException) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The path "
              + filePath
              + " for configSet "
              + configName
              + " already exists. "
              + "In order to overwrite, provide overwrite=true or use an HTTP PUT with the V2 API.");
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error creating file in config", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void setConfigMetadata(String configName, Map<String, Object> data) throws IOException {
    try {
      zkClient.makePath(
          CONFIGS_ZKNODE + "/" + configName,
          Utils.toJSON(data),
          CreateMode.PERSISTENT,
          null,
          false,
          true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error setting config metadata", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public Map<String, Object> getConfigMetadata(String configName) throws IOException {
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> data =
          (Map<String, Object>)
              Utils.fromJSON(zkClient.getData(CONFIGS_ZKNODE + "/" + configName, null, null, true));
      return data;
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error getting config metadata", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void downloadConfig(String configName, Path dir) throws IOException {
    zkClient.downloadFromZK(CONFIGS_ZKNODE + "/" + configName, dir);
  }

  @Override
  public byte[] downloadFileFromConfig(String configName, String filePath) throws IOException {
    try {
      return zkClient.getData(CONFIGS_ZKNODE + "/" + configName + "/" + filePath, null, null, true);
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error downloading file from config", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public List<String> listConfigs() throws IOException {
    try {
      return zkClient.getChildren(CONFIGS_ZKNODE, null, true);
    } catch (KeeperException.NoNodeException e) {
      return Collections.emptyList();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error listing configs", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public List<String> getAllConfigFiles(String configName) throws IOException {
    String zkPath = CONFIGS_ZKNODE + "/" + configName;
    try {
      List<String> filePaths = new ArrayList<>();
      ZkMaintenanceUtils.traverseZkTree(
          zkClient, zkPath, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, filePaths::add);
      filePaths.remove(zkPath);

      String prevPath = "";
      for (int i = 0; i < filePaths.size(); i++) {
        String currPath = filePaths.get(i);

        // stripping /configs/configName/
        assert currPath.startsWith(zkPath + "/");
        currPath = currPath.substring(zkPath.length() + 1);

        // if currentPath is a directory, concatenate '/'
        if (prevPath.startsWith(currPath)) {
          currPath = currPath + "/";
        }
        prevPath = currPath;
        filePaths.set(i, currPath);
      }
      Collections.sort(filePaths);
      return filePaths;
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error getting all configset files", SolrZkClient.checkInterrupted(e));
    }
  }

  // This method is used by configSetUploadTool and CreateTool to resolve the configset directory.
  // Check several possibilities:
  // 1> confDir/solrconfig.xml exists
  // 2> confDir/conf/solrconfig.xml exists
  // 3> configSetDir/confDir/conf/solrconfig.xml exists (canned configs)

  private void copyConfigDirFromZk(String fromZkPath, String toZkPath) throws IOException {
    try {
      List<String> files = zkClient.getChildren(fromZkPath, null, true);
      for (String file : files) {
        List<String> children = zkClient.getChildren(fromZkPath + "/" + file, null, true);
        if (children.size() == 0) {
          copyData(fromZkPath + "/" + file, toZkPath + "/" + file);
        } else {
          copyConfigDirFromZk(fromZkPath + "/" + file, toZkPath + "/" + file);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(
          "Error copying nodes from zookeeper path " + fromZkPath + " to " + toZkPath,
          SolrZkClient.checkInterrupted(e));
    }
  }

  private void copyData(String fromZkFilePath, String toZkFilePath)
      throws KeeperException, InterruptedException {
    if (ZkMaintenanceUtils.isFileForbiddenInConfigSets(fromZkFilePath)) {
      log.warn(
          "Skipping copy of file in ZK, as the source file is a forbidden type: {}",
          fromZkFilePath);
    } else if (ZkMaintenanceUtils.isFileForbiddenInConfigSets(toZkFilePath)) {
      log.warn(
          "Skipping download of file from ZK, as the target file is a forbidden type: {}",
          toZkFilePath);
    } else {
      log.debug("Copying zk node {} to {}", fromZkFilePath, toZkFilePath);
      byte[] data = zkClient.getData(fromZkFilePath, null, null, true);
      zkClient.makePath(toZkFilePath, data, true);
    }
  }

  public SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }
}
