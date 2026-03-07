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

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResourceLoader that works with Kubernetes ConfigMaps.
 *
 * <p>This loader attempts to load resources from a Kubernetes ConfigMap corresponding to the
 * configured configSet. If a resource is not found in the ConfigMap, it falls back to the classpath
 * loader.
 */
public class KubernetesSolrResourceLoader extends SolrResourceLoader {

  private final String configSetName;
  private final String namespace;
  private final CoreV1Api coreV1Api;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Creates a new KubernetesSolrResourceLoader.
   *
   * <p>This loader will first attempt to load resources from a Kubernetes ConfigMap for the given
   * configSet. If not found, it will delegate to the context classloader.
   *
   * @param instanceDir the instance directory for the core
   * @param configSetName the name of the configSet (used to look up the ConfigMap)
   * @param parent the parent classloader
   * @param coreV1Api the Kubernetes CoreV1Api client
   */
  public KubernetesSolrResourceLoader(
      Path instanceDir, String configSetName, ClassLoader parent, CoreV1Api coreV1Api) {
    super(instanceDir, parent);
    this.configSetName = configSetName;
    this.coreV1Api = coreV1Api;
    // TODO: namespace should come from the environment (POD_NAMESPACE env var)
    this.namespace = System.getenv(KubernetesConfigSetService.POD_NAMESPACE_ENV_VAR);
  }

  /**
   * Opens any resource by its name. First attempts to load the resource from the Kubernetes
   * ConfigMap for the configSet. If not found, delegates to the parent classloader.
   *
   * @return the stream for the named resource
   */
  @Override
  public InputStream openResource(String resource) throws IOException {
    // Try to get the resource from the Kubernetes ConfigMap
    try {
      // TODO: Cache the ConfigMap locally rather than fetching from the API each time.
      // Consider passing in the existingConfigSetConfigMaps cache from KubernetesConfigSetService.
      String solrCloudName = System.getenv(KubernetesConfigSetService.SOLR_CLOUD_NAME_ENV_VAR);
      String labelSelector =
          String.format(
              Locale.ROOT,
              "%s=%s",
              String.format(
                  Locale.ROOT, KubernetesConfigSetService.CONFIG_SET_LABEL_KEY, solrCloudName),
              KubernetesConfigSetService.CONFIG_SET_LABEL_VALUE);

      V1ConfigMap configMap =
          coreV1Api
              .listNamespacedConfigMap(namespace)
              .labelSelector(labelSelector)
              .execute()
              .getItems()
              .stream()
              .filter(
                  cm -> {
                    if (cm.getMetadata() == null || cm.getMetadata().getAnnotations() == null) {
                      return false;
                    }
                    return configSetName.equals(
                        cm.getMetadata()
                            .getAnnotations()
                            .get(KubernetesConfigSetService.CONFIG_SET_NAME_ANNOTATION_KEY));
                  })
              .findFirst()
              .orElse(null);

      if (configMap != null && configMap.getData() != null) {
        String data = configMap.getData().get(resource);
        if (data != null) {
          return new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
        }
      }
    } catch (ApiException e) {
      log.debug(
          "Could not retrieve resource '{}' from Kubernetes ConfigMap for configSet '{}'",
          resource,
          configSetName,
          e);
    }

    // Fall back to the classpath loader
    InputStream is;
    try {
      is = classLoader.getResourceAsStream(resource.replace('\\', '/'));
    } catch (Exception e) {
      throw new IOException("Error opening " + resource, e);
    }
    if (is == null) {
      throw new SolrResourceNotFoundException(
          "Can't find resource '"
              + resource
              + "' in classpath or in Kubernetes ConfigMap '"
              + configSetName
              + "', cwd="
              + System.getProperty("user.dir"));
    }
    return is;
  }

  public String getConfigSetName() {
    return configSetName;
  }
}
