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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
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
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SolrCloud Zookeeper ConfigSetService impl.
 */
public class ZkConfigSetService extends ConfigSetService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ZkController zkController;
  private final SolrZkClient zkClient;
  /** ZkNode where named configs are stored */
  public static final String CONFIGS_ZKNODE = "/configs";
  public static final String UPLOAD_FILENAME_EXCLUDE_REGEX = "^\\..*$";
  public static final Pattern UPLOAD_FILENAME_EXCLUDE_PATTERN = Pattern.compile(UPLOAD_FILENAME_EXCLUDE_REGEX);

  public ZkConfigSetService(CoreContainer cc) {
    super(cc.getResourceLoader(), cc.getConfig().hasSchemaCache());
    this.zkController = cc.getZkController();
    this.zkClient = cc.getZkController().getZkClient();
    try {
      bootstrapDefaultConfigSet();
    } catch (IOException e) {
      log.error("Error in bootstrapping default config");
    }
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
        log.warn("Auto-creating collection (in ZK) from core descriptor (on disk).  This feature may go away!");
        CreateCollectionCmd.createCollectionZkNode(
            zkController.getSolrCloudManager().getDistribStateManager(),
            colName,
            cd.getCloudDescriptor().getParams(),
            zkController.getCoreContainer().getConfigSetService());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Interrupted auto-creating collection", e);
    } catch (KeeperException e) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Failure auto-creating collection", e);
    }

    // The configSet is read from ZK and populated.  Ignore CD's pre-existing configSet; only populated in standalone
    final String configSetName;
    try {
      configSetName = zkController.getZkStateReader().readConfigName(colName);
      cd.setConfigSet(configSetName);
    } catch (KeeperException ex) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Trouble resolving configSet for collection " + colName + ": " + ex.getMessage());
    }

    return new ZkSolrResourceLoader(cd.getInstanceDir(), configSetName, parentLoader.getClassLoader(), zkController);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  protected NamedList loadConfigSetFlags(CoreDescriptor cd, SolrResourceLoader loader) {
    try {
      return ConfigSetProperties.readFromResourceLoader(loader, ".");
    } catch (Exception ex) {
      log.debug("No configSet flags", ex);
      return null;
    }
  }

  @Override
  protected Long getCurrentSchemaModificationVersion(String configSet, SolrConfig solrConfig, String schemaFile) {
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
      throw new IOException("Error checking whether config exists",
              SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public boolean checkFileExistsInConfig(String configName, String fileName) throws IOException {
    try {
      return zkClient.exists(CONFIGS_ZKNODE + "/" + configName + "/" + fileName, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteConfig(String configName) throws IOException {
    try {
      zkClient.clean(CONFIGS_ZKNODE + "/" + configName);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error deleting config",
              SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void deleteFileFromConfig(String configName, String fileName) throws IOException {
    try {
      zkClient.clean(CONFIGS_ZKNODE + "/" + configName + "/" + fileName);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error deleting a file in config",
              SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void copyConfig(String fromConfig, String toConfig) throws IOException {
    copyConfig(fromConfig, toConfig, null);
  }

  @Override
  public void copyConfig(String fromConfig, String toConfig, Set<String> copiedToZkPaths) throws IOException {
    String fromConfigPath = CONFIGS_ZKNODE + "/" + fromConfig;
    String toConfigPath = CONFIGS_ZKNODE + "/" + toConfig;
    try {
      copyData(copiedToZkPaths, fromConfigPath, toConfigPath);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error config " + fromConfig + " to " + toConfig,
              SolrZkClient.checkInterrupted(e));
    }
    copyConfigDirFromZk(fromConfigPath, toConfigPath, copiedToZkPaths);
  }

  @Override
  public void uploadConfig(Path dir, String configName) throws IOException {
    zkClient.uploadToZK(dir, CONFIGS_ZKNODE + "/" + configName, ZkConfigSetService.UPLOAD_FILENAME_EXCLUDE_PATTERN);
  }

  @Override
  public void createFilePathInConfig(String configName, String fileName, boolean failOnExists) throws IOException {
    try {
      zkClient.makePath(CONFIGS_ZKNODE + "/" + configName + "/" + fileName, failOnExists,true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error creating file path in config", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void uploadFileToConfig(String configName, String fileName, byte[] data) throws IOException {
    try {
      zkClient.makePath(CONFIGS_ZKNODE + "/" + configName + "/" + fileName, data, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error creating file in config", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void uploadFileToConfig(String configName, String fileName, byte[] data, boolean failOnExists) throws IOException {
    String filePath = CONFIGS_ZKNODE + "/" + configName + "/" + fileName;
    try {
      zkClient.makePath(filePath, data, CreateMode.PERSISTENT, null, failOnExists, true);
    } catch (KeeperException.NodeExistsException nodeExistsException) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The path " + filePath + " for configSet " + configName + " already exists. In order to overwrite, provide overwrite=true or use an HTTP PUT with the V2 API.");
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error creating file in config", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void setConfigMetadata(String configName, Map<Object, Object> data) throws IOException {
    try {
      zkClient.makePath(CONFIGS_ZKNODE + "/" + configName, Utils.toJSON(data), true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error setting config metadata", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void setConfigMetadata(String configName, byte[] data) throws IOException {
    try {
      zkClient.makePath(CONFIGS_ZKNODE + "/" + configName, data, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error setting config metadata", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void updateConfigMetadata(String configName, Map<Object, Object> data) throws IOException {
    try {
      zkClient.setData(CONFIGS_ZKNODE + "/" + configName, Utils.toJSON(data), true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error updating config metadata", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void updateConfigMetadata(String configName, byte[] data) throws IOException {
    try {
      zkClient.setData(CONFIGS_ZKNODE + "/" + configName, data, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error updating config metadata", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public byte[] getConfigMetadata(String configName) throws IOException {
    try {
      return zkClient.getData(CONFIGS_ZKNODE + "/" + configName, null, null, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error getting config metadata", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void downloadConfig(String configName, Path dir) throws IOException {
    zkClient.downloadFromZK(CONFIGS_ZKNODE + "/" + configName, dir);
  }

  @Override
  public byte[] downloadFileFromConfig(String configName, String fileName) throws IOException {
    try {
      return zkClient.getData(CONFIGS_ZKNODE + "/" + configName + "/" + fileName, null, null, true);
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
  public List<String> listFilesInConfig(String configName) throws IOException {
    try {
      return zkClient.getChildren(CONFIGS_ZKNODE + "/" + configName, null, true);
    } catch (KeeperException.NoNodeException e) {
      return Collections.emptyList();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<String> listFilesInConfig(String configName, String fileName) throws IOException {
    try {
      return zkClient.getChildren(CONFIGS_ZKNODE + "/" + configName + "/" + fileName, null, true);
    } catch (KeeperException.NoNodeException e) {
      return Collections.emptyList();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }


  @Override
  public List<String> getAllConfigFiles(String configName) throws IOException {
    String zkPath = CONFIGS_ZKNODE + "/" + configName;
    if (!zkPath.startsWith(ZkConfigSetService.CONFIGS_ZKNODE + "/")) {
      throw new IllegalArgumentException("\"" + zkPath + "\" not recognized as a configset path");
    }
    try {
      return zkClient.getAllConfigsetFiles(zkPath);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error getting all configset files", SolrZkClient.checkInterrupted(e));
    }
  }

  private void bootstrapDefaultConfigSet() throws IOException {
    if (this.checkConfigExists("_default") == false) {
      String configDirPath = getDefaultConfigDirPath();
      if (configDirPath == null) {
        log.warn(
            "The _default configset could not be uploaded. Please provide 'solr.default.confdir' parameter that points to a configset {} {}",
            "intended to be the default. Current 'solr.default.confdir' value:",
            System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE));
      } else {
        this.uploadConfig(Paths.get(configDirPath), ConfigSetsHandler.DEFAULT_CONFIGSET_NAME);
      }
    }
  }

  /**
   * Gets the absolute filesystem path of the _default configset to bootstrap from. First tries the
   * sysprop "solr.default.confdir". If not found, tries to find the _default dir relative to the
   * sysprop "solr.install.dir". Returns null if not found anywhere.
   *
   * @lucene.internal
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   */
  public static String getDefaultConfigDirPath() {
    String configDirPath = null;
    String serverSubPath =
        "solr"
            + File.separator
            + "configsets"
            + File.separator
            + "_default"
            + File.separator
            + "conf";
    String subPath = File.separator + "server" + File.separator + serverSubPath;
    if (System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE) != null
        && new File(System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE))
            .exists()) {
      configDirPath =
          new File(System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE))
              .getAbsolutePath();
    } else if (System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) != null
        && new File(System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) + subPath)
            .exists()) {
      configDirPath =
          new File(System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) + subPath)
              .getAbsolutePath();
    }
    return configDirPath;
  }

  // This method is used by configSetUploadTool and CreateTool to resolve the configset directory.
  // Check several possibilities:
  // 1> confDir/solrconfig.xml exists
  // 2> confDir/conf/solrconfig.xml exists
  // 3> configSetDir/confDir/conf/solrconfig.xml exists (canned configs)

  // Order is important here since "confDir" may be
  // 1> a full path to the parent of a solrconfig.xml or parent of /conf/solrconfig.xml
  // 2> one of the canned config sets only, e.g. _default
  // and trying to assemble a path for configsetDir/confDir is A Bad Idea. if confDir is a full path.
  public static Path getConfigsetPath(String confDir, String configSetDir) throws IOException {

    // A local path to the source, probably already includes "conf".
    Path ret = Paths.get(confDir, "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Paths.get(confDir).normalize();
    }

    // a local path to the parent of a "conf" directory
    ret = Paths.get(confDir, "conf", "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Paths.get(confDir, "conf").normalize();
    }

    // one of the canned configsets.
    ret = Paths.get(configSetDir, confDir, "conf", "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Paths.get(configSetDir, confDir, "conf").normalize();
    }

    throw new IllegalArgumentException(String.format(Locale.ROOT,
            "Could not find solrconfig.xml at %s, %s or %s",
            Paths.get(configSetDir, "solrconfig.xml").normalize().toAbsolutePath().toString(),
            Paths.get(configSetDir, "conf", "solrconfig.xml").normalize().toAbsolutePath().toString(),
            Paths.get(configSetDir, confDir, "conf", "solrconfig.xml").normalize().toAbsolutePath().toString()
    ));
  }

  private void copyConfigDirFromZk(String fromZkPath, String toZkPath, Set<String> copiedToZkPaths) throws IOException {
    try {
      List<String> files = zkClient.getChildren(fromZkPath, null, true);
      for (String file : files) {
        List<String> children = zkClient.getChildren(fromZkPath + "/" + file, null, true);
        if (children.size() == 0) {
          copyData(copiedToZkPaths, fromZkPath + "/" + file, toZkPath + "/" + file);
        } else {
          copyConfigDirFromZk(fromZkPath + "/" + file, toZkPath + "/" + file, copiedToZkPaths);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error copying nodes from zookeeper path " + fromZkPath + " to " + toZkPath,
              SolrZkClient.checkInterrupted(e));
    }
  }

  private void copyData(Set<String> copiedToZkPaths, String fromZkFilePath, String toZkFilePath) throws KeeperException, InterruptedException {
    log.info("Copying zk node {} to {}", fromZkFilePath, toZkFilePath);
    byte[] data = zkClient.getData(fromZkFilePath, null, null, true);
    zkClient.makePath(toZkFilePath, data, true);
    if (copiedToZkPaths != null) copiedToZkPaths.add(toZkFilePath);
  }

  public SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }
}
