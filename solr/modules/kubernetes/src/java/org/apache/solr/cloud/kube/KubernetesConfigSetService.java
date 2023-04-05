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
package org.apache.solr.cloud.kube;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Status;
import io.kubernetes.client.util.CallGeneratorParams;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.labels.LabelSelector;
import io.kubernetes.client.util.labels.SetMatcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.configuration.providers.EnvSSLCredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kubernetes ConfigSetService impl.
 *
 * <p>Loads a ConfigSet defined by the core's configSet property, looking for a directory named for
 * the configSet property value underneath a base directory. If no configSet property is set, loads
 * the ConfigSet instead from the core's instance directory.
 */
public class KubernetesConfigSetService extends ConfigSetService implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ApiClient kubeClient;
  private final CoreV1Api coreV1Api;

  private final String solrCloudNamespace;
  private final String solrCloudName;

  private final Map<String, V1ConfigMap> existingConfigSetConfigMaps;

  /** ConfigMapLabel */
  public static final String CONFIG_SET_LABEL_KEY = "solr.apache.org/cloud/%s/resource";
  public static final String CONFIG_SET_LABEL_VALUE = "configSet";
  public static final String CONFIG_SET_NAME_ANNOTATION_KEY = "solr.apache.org/configSet/name";

  // These are always set on pods by the Solr Operator
  public static final String POD_NAMESPACE_ENV_VAR = "POD_NAMESPACE";
  public static final String SOLR_CLOUD_NAME_ENV_VAR = "SOLR_CLOUD_NAME";

  public KubernetesConfigSetService(CoreContainer cc) throws IOException {
    super(cc.getResourceLoader(), cc.getConfig().hasSchemaCache());
    kubeClient = ClientBuilder.cluster().build();
    coreV1Api = new CoreV1Api(kubeClient);

    existingConfigSetConfigMaps = new ConcurrentHashMap<>();
    // TODO: Finalize these
    solrCloudNamespace = System.getenv(POD_NAMESPACE_ENV_VAR);
    solrCloudName = System.getenv(SOLR_CLOUD_NAME_ENV_VAR);
  }

  public void init() {
    SharedInformerFactory factory = new SharedInformerFactory(kubeClient);

    // Node informer
    SharedIndexInformer<V1ConfigMap> nodeInformer =
        factory.sharedIndexInformerFor(
            // **NOTE**:
            // The following "CallGeneratorParams" lambda merely generates a stateless
            // HTTPs requests, the effective apiClient is the one specified when constructing
            // the informer-factory.
            (CallGeneratorParams params) ->
              coreV1Api.listNamespacedConfigMapCall(
                  solrCloudNamespace,
                  null,
                  null,
                  null,
                  null,
                  SetMatcher.in(String.format(Locale.ROOT, CONFIG_SET_LABEL_KEY, solrCloudName), CONFIG_SET_LABEL_VALUE).toString(),
                  null,
                  params.resourceVersion,
                  null,
                  params.timeoutSeconds,
                  params.watch,
                  null),
            V1ConfigMap.class,
            V1ConfigMapList.class);

    nodeInformer.addEventHandler(
        new ResourceEventHandler<>() {
          @Override
          public void onAdd(V1ConfigMap configMap) {
            log.info("{} configMap added!\n", configMap.getMetadata().getName());
            existingConfigSetConfigMaps.put(extractConfigSetName(configMap), configMap);
          }

          @Override
          public void onUpdate(V1ConfigMap oldConfigMap, V1ConfigMap newConfigMap) {
            log.info(
                "{} => {} configMap updated!\n",
                oldConfigMap.getMetadata().getName(), newConfigMap.getMetadata().getName());
            existingConfigSetConfigMaps.put(extractConfigSetName(newConfigMap), newConfigMap);
          }

          @Override
          public void onDelete(V1ConfigMap configMap, boolean deletedFinalStateUnknown) {
            log.info("{} configMap deleted!\n", configMap.getMetadata().getName());
            existingConfigSetConfigMaps.remove(extractConfigSetName(configMap));
          }
        });

    factory.startAllRegisteredInformers();
  }

  private String extractConfigSetName(V1ConfigMap configMap) {
    return
        Optional.ofNullable(configMap.getMetadata())
            .map(V1ObjectMeta::getAnnotations)
            .map(ann -> ann.get(CONFIG_SET_NAME_ANNOTATION_KEY))
            .orElse(configMap.getMetadata().getName());
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
  protected NamedList<Object> loadConfigSetFlags(CoreDescriptor cd, SolrResourceLoader loader)
      throws IOException {
    try {
      // TODO: Makesure this name is right (maybe properties/flags/metadata)
      return ConfigSetProperties.readFromResourceLoader(loader, "properties");
    } catch (Exception ex) {
      log.debug("No configSet flags", ex);
      return null;
    }
  }

  @Override
  protected Long getCurrentSchemaModificationVersion(
      String configSet, SolrConfig solrConfig, String schemaFile) {
    // Individual values/files in ConfigMaps do not have versions,
    // we can only use the ConfigMap version as a whole.
    //
    // Return null if this configMap does not exist.
    return
        Optional.ofNullable(existingConfigSetConfigMaps.get(configSet))
            .map(V1ConfigMap::getMetadata)
            .map(V1ObjectMeta::getGeneration)
            .orElse(null);
  }

  @Override
  public String configSetName(CoreDescriptor cd) {
    return "configmap " + cd.getConfigSet();
  }

  @Override
  public boolean checkConfigExists(String configName) {
    return existingConfigSetConfigMaps.containsKey(configName);
  }

  @Override
  public void deleteConfig(String configName) throws IOException {
    String configMapName = ""
    try {
      if (existingConfigSetConfigMaps.containsKey(configName)) {
        V1ConfigMap configMap = existingConfigSetConfigMaps.get(configName);
        configMapName = configMap.getMetadata().getName();
        coreV1Api.deleteNamespacedConfigMap(
            configMapName,
            configMap.getMetadata().getNamespace(),
            null,
            null,
            15, // TODO: What should this be?
            null,
            "Background",
            null
            );
      }
    } catch (ApiException e) {
      throw new IOException(String.format(Locale.ROOT, "Error deleting configMap %s, representing the configSet %s", configMapName, configName), e);
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
    String configMapName = "";
    try {
      if (ZkMaintenanceUtils.isFileForbiddenInConfigSets(fileName)) {
        log.warn("Not including uploading file to config, as it is a forbidden type: {}", fileName);
      } else {
        V1ConfigMap configMap = existingConfigSetConfigMaps.get(configName);
        if (configMap == null) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, String.format(Locale.ROOT, "ConfigSet %s does not exist", configName))
        }
        configMapName = configMap.getMetadata().getName();
        var existingData = configMap.getData();
        if (existingData == null || !existingData.containsKey(fileName) || overwriteOnExists) {
          // TODO: Patch the data
        }
      }
    } catch (ApiException e) {
      throw new IOException(String.format(Locale.ROOT, "Error creating item %s in configMap %s, representing the configSet %s", fileName, configMapName, configName), e);
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
