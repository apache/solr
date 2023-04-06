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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.CallGeneratorParams;
import io.kubernetes.client.util.ClientBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kubernetes ConfigSetService impl.
 *
 * <p>Loads a ConfigSet defined by the core's configSet property, looking for a directory named for
 * the configSet property value underneath a base directory. If no configSet property is set, loads
 * the ConfigSet instead from the core's instance directory.
 */
public class KubernetesConfigSetService extends ConfigSetService {
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

  // Special ConfigMap data paths
  public static final String CONFIG_SET_METADATA_KEY = "_metadata.json";
  public static final String CONFIG_SET_PROPERTIES_KEY = "_properties.json";

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

  private V1ConfigMap getCachedConfigMap(String configName) throws SolrException {
    V1ConfigMap configMap = existingConfigSetConfigMaps.get(configName);
    if (configMap == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, String.format(Locale.ROOT, "No ConfigMap exists for ConfigSet %s ", configName));
    }
    if (configMap.getMetadata() == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, String.format(Locale.ROOT, "ConfigMap exists for ConfigSet %s, but it contains no metadata", configName));
    }
    return configMap;
  }

  @Override
  public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
    final String colName = cd.getCollectionName();

    // The configSet is read from ZK and populated.  Ignore CD's pre-existing configSet; only
    // populated in standalone
    String configSetName = zkController.getClusterState().getCollection(colName).getConfigName();
    cd.setConfigSet(configSetName);

    return new KubeSolrResourceLoader(
        cd.getInstanceDir(), configSetName, parentLoader.getClassLoader(), zkController);
  }

  @Override
  protected NamedList<Object> loadConfigSetFlags(CoreDescriptor cd, SolrResourceLoader loader) {
    try {
      return ConfigSetProperties.readFromResourceLoader(loader, CONFIG_SET_METADATA_KEY);
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
    String configMapName = "";
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
    if (filesToDelete == null) {
      return;
    }
    String configMapName = "";
    try {
      V1ConfigMap configMap = getCachedConfigMap(configName);
      configMapName = configMap.getMetadata().getName();
      var existingData = configMap.getData();
      Set<String> chosenFilesToDelete = new HashSet<>(filesToDelete);
      if (existingData != null) {
        // If there is existing data cached, then only delete the files we know to exist
        // If there is no existing data cached, then try to do a patch delete and fail if it doesn't work.
        chosenFilesToDelete.retainAll(existingData.keySet());
      }
      // TODO: Patch the data
    } catch (ApiException e) {
      throw new IOException(String.format(Locale.ROOT, "Error deleting files in configMap %s, representing the configSet %s", configMapName, configName), e);
    }
  }

  private V1ConfigMap newConfigMap(String configName) {
    // Follow Kubernetes name rules: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
    configName = configName.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9-.]", "-");
    if (configName.endsWith("-") || configName.endsWith(".")) {
      configName = configName.substring(0, configName.length() - 1);
    }
    return
        new V1ConfigMap()
            .metadata(
                new V1ObjectMeta()
                    .name(String.format(Locale.ROOT, "solrcloud-%s-configset-%s", solrCloudName, configName))
                    .namespace(solrCloudNamespace)
                    .putLabelsItem(String.format(Locale.ROOT, CONFIG_SET_LABEL_KEY, solrCloudName), CONFIG_SET_LABEL_VALUE)
                    .putAnnotationsItem(CONFIG_SET_NAME_ANNOTATION_KEY, configName)
            );
  }

  @Override
  public void copyConfig(String fromConfig, String toConfig) throws IOException {
    String configMapName = "";
    try {
      V1ConfigMap fromConfigMap = getCachedConfigMap(fromConfig);
      var existingData = fromConfigMap.getData();
      if (existingData == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, String.format(Locale.ROOT, "Cannot copy configSet %s, it has no data in its configMap %s", fromConfig, fromConfigMap.getMetadata().getName()))
      }
      if (existingConfigSetConfigMaps.containsKey(toConfig)) {
        // Patch an existing configSet
        // TODO: Should this be an option or an error?
      } else {
        V1ConfigMap toConfigMap = newConfigMap(toConfig).data(existingData);
        coreV1Api.createNamespacedConfigMap(
            solrCloudNamespace,
            toConfigMap,
            null,
            null,
            null,
            null);
      }
    } catch (ApiException e) {
      throw new IOException("Could not create new configMap for configSet " + toConfig, e);
    }
  }

  @Override
  public void uploadConfig(String configName, Path dir) throws IOException {
    Map<String,String> dataToPut = new HashMap<>();
    String path = dir.toString();
    if (path.endsWith("*")) {
      path = path.substring(0, path.length() - 1);
    }

    final Path rootPath = Paths.get(path);

    if (!Files.exists(rootPath)) throw new IOException("Path " + rootPath + " does not exist");

    Files.walkFileTree(
        rootPath,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            String filename = file.getFileName().toString();
            if ((ConfigSetService.UPLOAD_FILENAME_EXCLUDE_PATTERN.matcher(filename).matches())) {
              log.info(
                  "uploadToZK skipping '{}' due to filenameExclusions '{}'",
                  filename,
                  ConfigSetService.UPLOAD_FILENAME_EXCLUDE_PATTERN);
              return FileVisitResult.CONTINUE;
            }
            if (ZkMaintenanceUtils.isFileForbiddenInConfigSets(filename)) {
              log.info(
                  "skipping '{}' in configMap due to forbidden file type",
                  filename);
              return FileVisitResult.CONTINUE;
            }
            String configMapKey = ZkMaintenanceUtils.createZkNodeName("", rootPath, file);
            if (configMapKey.isEmpty()) {
              configMapKey = CONFIG_SET_METADATA_KEY;
            }
            dataToPut.put(configMapKey, Files.readString(file, StandardCharsets.UTF_8));
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            if (dir.getFileName().toString().startsWith(".")) return FileVisitResult.SKIP_SUBTREE;

            return FileVisitResult.CONTINUE;
          }
        });

    // TODO: Patch with new Data
    coreV1Api.patchNamespacedConfigMap()
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
        V1ConfigMap configMap = getCachedConfigMap(configName);
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
    String configMapName = "";
    try {
      V1ConfigMap configMap = getCachedConfigMap(configName);
      configMapName = configMap.getMetadata().getName();
      var existingData = configMap.getData();
      String newMetadata = Utils.toJSONString(data);

      String patchType;
      if (existingData == null || !existingData.containsKey(CONFIG_SET_METADATA_KEY)) {
        patchType = "insert";
      } else if (!data.get(CONFIG_SET_METADATA_KEY).equals(newMetadata)) {
        patchType = "replace";
      } else {
        return;
      }
      // TODO: Patch the data
    } catch (ApiException e) {
      throw new IOException(String.format(Locale.ROOT, "Error updating metadata item %s in configMap %s, representing the configSet %s", CONFIG_SET_METADATA_KEY, configMapName, configName), e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> getConfigMetadata(String configName) throws IOException {
    V1ConfigMap configMap = getCachedConfigMap(configName);
    return
        Optional.ofNullable(configMap.getData())
            .map(data -> data.get(CONFIG_SET_METADATA_KEY))
            .map(md -> (Map<String,Object>)Utils.fromJSON(md))
            .orElseGet(HashMap::new);
  }

  @Override
  public void downloadConfig(String configName, Path dir) throws IOException {
    V1ConfigMap configMap = getCachedConfigMap(configName);
    if (configMap.getData() == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, String.format(Locale.ROOT, "Could not get data for configMap %s, representing the configSet %s", configMap.getMetadata().getName(), configName));
    }
    var files = new ArrayList<>(configMap.getData().keySet());
    files.sort(String::compareTo);

    for (int i = 0; i < files.size(); i++) {
      String fileName = files.get(i);
      if (ZkMaintenanceUtils.isFileForbiddenInConfigSets(fileName)) {
        log.warn("Skipping download of file from Kubernetes, as it is a forbidden type: {}", fileName);
        continue;
      }
      String fileData = configMap.getData().get(fileName);
      boolean isParentFile = i < files.size() - 1 && files.get(i + 1).startsWith(fileName + "/");

      if (fileName.equals(CONFIG_SET_METADATA_KEY)) {
        // Support compatibility with ZK configSet file structure
        fileName = ZkMaintenanceUtils.ZKNODE_DATA_FILE;
      }
      if (!"/".equals(File.pathSeparator)) {
        fileName = fileName.replaceAll(File.pathSeparator, "/");
      }
      Path filePath = dir.resolve(fileName);
      if (isParentFile) {
        Files.createDirectories(filePath); // Make parent dir.
        if (fileData.length() > 0) {
          // ZK nodes, whether leaf or not can have data. If it's a non-leaf node and has associated
          // data write it into the special file.
          // We need to support the same file structure as Zookeeper, so write files these ways
          Files.write(filePath.resolve(ZkMaintenanceUtils.ZKNODE_DATA_FILE), fileData.getBytes(StandardCharsets.UTF_8));
        }
      } else {
        Files.createDirectories(filePath.getParent()); // Make parent dir.
        if (fileData.length() > 0) {
          Files.write(filePath, fileData.getBytes(StandardCharsets.UTF_8));
        } else {
          Files.createFile(filePath);
        }
      }
    }
  }

  @Override
  public byte[] downloadFileFromConfig(String configName, String filePath) {
    V1ConfigMap configMap = getCachedConfigMap(configName);
    return
        Optional.ofNullable(configMap.getData())
            .map(data -> data.get(filePath))
            .map(fileData -> fileData.getBytes(StandardCharsets.UTF_8))
            .orElse(null);
  }

  @Override
  public List<String> listConfigs() {
    return new ArrayList<>(existingConfigSetConfigMaps.keySet());
  }

  @Override
  public List<String> getAllConfigFiles(String configName) {
    V1ConfigMap configMap = getCachedConfigMap(configName);
    return
        Optional.ofNullable(configMap.getData())
          .map(Map::keySet)
          .map(ArrayList::new)
          .orElseGet(ArrayList::new);
  }

  public SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }
}
