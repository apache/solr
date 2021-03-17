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
import java.lang.ref.WeakReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SolrCloud Zookeeper ConfigSetService impl.
 */
public class ZkConfigSetService extends ConfigSetService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Map<String, ConfigCacheEntry> cache = new ConcurrentHashMap<>();
  private final ZkController zkController;
  /** ZkNode where named configs are stored */
  public static final String CONFIGS_ZKNODE = "/configs";
  public static final String UPLOAD_FILENAME_EXCLUDE_REGEX = "^\\..*$";
  public static final Pattern UPLOAD_FILENAME_EXCLUDE_PATTERN = Pattern.compile(UPLOAD_FILENAME_EXCLUDE_REGEX);

  public ZkConfigSetService(SolrResourceLoader loader, boolean shareSchema, ZkController zkController) {
    super(loader, shareSchema);
    this.zkController = zkController;
  }

  public void storeConfig(String resource, ConfigNode config, int znodeVersion) {
    cache.put(resource, new ConfigCacheEntry(config, znodeVersion));
  }

  public ConfigNode getConfig(String resource, int znodeVersion) {
    ConfigCacheEntry e = cache.get(resource);
    if (e == null) return null;
    ConfigNode configNode = e.configNode.get();
    if (configNode == null) cache.remove(resource);
    if (e.znodeVersion == znodeVersion) return configNode;
    if (e.znodeVersion < znodeVersion) cache.remove(resource);
    return null;
  }

  @Override
  public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
    final String colName = cd.getCollectionName();

    // For back compat with cores that can create collections without the collections API
    try {
      if (!zkController.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + colName, true)) {
        // TODO remove this functionality or maybe move to a CLI mechanism
        log.warn("Auto-creating collection (in ZK) from core descriptor (on disk).  This feature may go away!");
        CreateCollectionCmd.createCollectionZkNode(zkController.getSolrCloudManager().getDistribStateManager(), colName, cd.getCloudDescriptor().getParams());
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
      stat = zkController.getZkClient().exists(zkPath, null, true);
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

  /**
   * Check whether a config exists in Zookeeper
   *
   * @param configName the config to check existance on
   * @return whether the config exists or not
   * @throws IOException if an I/O error occurs
   */
  public static Boolean configExists(SolrZkClient zkClient, String configName) throws IOException {
    try {
      return zkClient.exists(CONFIGS_ZKNODE + "/" + configName, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error checking whether config exists",
              SolrZkClient.checkInterrupted(e));
    }
  }

  /**
   * Delete a config in ZooKeeper
   *
   * @param configName the config to delete
   * @throws IOException if an I/O error occurs
   */
  public static void deleteConfigDir(SolrZkClient zkClient, String configName) throws IOException {
    try {
      zkClient.clean(CONFIGS_ZKNODE + "/" + configName);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error checking whether config exists",
              SolrZkClient.checkInterrupted(e));
    }
  }

  /**
   * Copy a config in ZooKeeper
   *
   * @param fromConfig the config to copy from
   * @param toConfig   the config to copy to
   * @throws IOException if an I/O error occurs
   */
  public static void copyConfigDir(SolrZkClient zkClient, String fromConfig, String toConfig) throws IOException {
    copyConfigDir(zkClient, fromConfig, toConfig, null);
  }

  /**
   * Copy a config in ZooKeeper
   *
   * @param fromConfig      the config to copy from
   * @param toConfig        the config to copy to
   * @param copiedToZkPaths should be an empty Set, will be filled in by function
   *                        with the paths that were actually copied to.
   * @throws IOException if an I/O error occurs
   */
  public static void copyConfigDir(SolrZkClient zkClient, String fromConfig, String toConfig, Set<String> copiedToZkPaths) throws IOException {
    String fromConfigPath = CONFIGS_ZKNODE + "/" + fromConfig;
    String toConfigPath = CONFIGS_ZKNODE + "/" + toConfig;
    try {
      copyData(zkClient, copiedToZkPaths, fromConfigPath, toConfigPath);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error config " + fromConfig + " to " + toConfig,
              SolrZkClient.checkInterrupted(e));
    }
    copyConfigDirFromZk(zkClient, fromConfigPath, toConfigPath, copiedToZkPaths);
  }

  /**
   * Upload files from a given path to a config in Zookeeper
   *
   * @param dir        {@link java.nio.file.Path} to the files
   * @param configName the name to give the config
   * @throws IOException if an I/O error occurs or the path does not exist
   */
  public static void uploadConfigDir(SolrZkClient zkClient, Path dir, String configName) throws IOException {
    zkClient.uploadToZK(dir, CONFIGS_ZKNODE + "/" + configName, ZkConfigSetService.UPLOAD_FILENAME_EXCLUDE_PATTERN);
  }

  /**
   * Upload matching files from a given path to a config in Zookeeper
   *
   * @param dir                {@link java.nio.file.Path} to the files
   * @param configName         the name to give the config
   * @param filenameExclusions files matching this pattern will not be uploaded
   * @throws IOException if an I/O error occurs or the path does not exist
   */
  public static void uploadConfigDir(SolrZkClient zkClient, Path dir, String configName, Pattern filenameExclusions) throws IOException {
    zkClient.uploadToZK(dir, CONFIGS_ZKNODE + "/" + configName, filenameExclusions);
  }

  /**
   * Download a config from Zookeeper and write it to the filesystem
   *
   * @param configName the config to download
   * @param dir        the {@link Path} to write files under
   * @throws IOException if an I/O error occurs or the config does not exist
   */
  public static void downloadConfigDir(SolrZkClient zkClient, String configName, Path dir) throws IOException {
    zkClient.downloadFromZK(CONFIGS_ZKNODE + "/" + configName, dir);
  }

  public static List<String> listConfigs(SolrZkClient zkClient) throws IOException {
    try {
      return zkClient.getChildren(CONFIGS_ZKNODE, null, true);
    } catch (KeeperException.NoNodeException e) {
      return Collections.emptyList();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error listing configs", SolrZkClient.checkInterrupted(e));
    }
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

  private static void copyConfigDirFromZk(SolrZkClient zkClient, String fromZkPath, String toZkPath, Set<String> copiedToZkPaths) throws IOException {
    try {
      List<String> files = zkClient.getChildren(fromZkPath, null, true);
      for (String file : files) {
        List<String> children = zkClient.getChildren(fromZkPath + "/" + file, null, true);
        if (children.size() == 0) {
          copyData(zkClient, copiedToZkPaths, fromZkPath + "/" + file, toZkPath + "/" + file);
        } else {
          copyConfigDirFromZk(zkClient,fromZkPath + "/" + file, toZkPath + "/" + file, copiedToZkPaths);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error copying nodes from zookeeper path " + fromZkPath + " to " + toZkPath,
              SolrZkClient.checkInterrupted(e));
    }
  }

  private static void copyData(SolrZkClient zkClient, Set<String> copiedToZkPaths, String fromZkFilePath, String toZkFilePath) throws KeeperException, InterruptedException {
    log.info("Copying zk node {} to {}", fromZkFilePath, toZkFilePath);
    byte[] data = zkClient.getData(fromZkFilePath, null, null, true);
    zkClient.makePath(toZkFilePath, data, true);
    if (copiedToZkPaths != null) copiedToZkPaths.add(toZkFilePath);
  }

  private static class ConfigCacheEntry {
    final WeakReference<ConfigNode> configNode;
    final int znodeVersion;

    private ConfigCacheEntry(ConfigNode configNode, int znodeVersion) {
      this.configNode = new WeakReference<>(configNode);
      this.znodeVersion = znodeVersion;
    }
  }
  public SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }
}
