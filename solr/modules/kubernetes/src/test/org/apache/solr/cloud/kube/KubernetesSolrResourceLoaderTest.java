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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.core.SolrResourceNotFoundException;
import org.junit.BeforeClass;
import org.junit.Test;

public class KubernetesSolrResourceLoaderTest extends SolrTestCase {

  @BeforeClass
  public static void setUpClass() {
    assumeWorkingMockito();
  }

  private V1ConfigMap makeConfigMap(String configSetName, Map<String, String> data) {
    V1ObjectMeta meta =
        new V1ObjectMeta()
            .putAnnotationsItem(
                KubernetesConfigSetService.CONFIG_SET_NAME_ANNOTATION_KEY, configSetName);
    return new V1ConfigMap().metadata(meta).data(data);
  }

  private KubernetesSolrResourceLoader makeLoader(CoreV1Api api, String configSetName) {
    return new KubernetesSolrResourceLoader(
        createTempDir(),
        configSetName,
        KubernetesSolrResourceLoaderTest.class.getClassLoader(),
        api);
  }

  @Test
  public void testOpenResourceFromConfigMap() throws Exception {
    CoreV1Api api = mock(CoreV1Api.class, RETURNS_DEEP_STUBS);
    String resource = "solrconfig.xml";
    String content = "<config/>";
    V1ConfigMap cm = makeConfigMap("my-configset", Map.of(resource, content));
    when(api.listNamespacedConfigMap(any()).labelSelector(any()).execute().getItems())
        .thenReturn(List.of(cm));

    KubernetesSolrResourceLoader loader = makeLoader(api, "my-configset");
    try (InputStream is = loader.openResource(resource)) {
      String result = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(content, result);
    }
  }

  @Test
  public void testOpenResourceFallbackToClasspath() throws Exception {
    CoreV1Api api = mock(CoreV1Api.class, RETURNS_DEEP_STUBS);
    // ConfigMap exists but does not contain the requested resource
    V1ConfigMap cm = makeConfigMap("my-configset", Map.of("other-file.xml", "data"));
    when(api.listNamespacedConfigMap(any()).labelSelector(any()).execute().getItems())
        .thenReturn(List.of(cm));

    KubernetesSolrResourceLoader loader = makeLoader(api, "my-configset");
    try (InputStream is = loader.openResource("test-classpath-resource.txt")) {
      assertNotNull(is);
      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(content.contains("classpath"));
    }
  }

  @Test
  public void testOpenResourceNotFoundAnywhere() throws Exception {
    CoreV1Api api = mock(CoreV1Api.class, RETURNS_DEEP_STUBS);
    when(api.listNamespacedConfigMap(any()).labelSelector(any()).execute().getItems())
        .thenReturn(List.of());

    KubernetesSolrResourceLoader loader = makeLoader(api, "my-configset");
    assertThrows(
        SolrResourceNotFoundException.class,
        () -> loader.openResource("absolutely-nonexistent-resource.xml"));
  }

  @Test
  public void testApiExceptionFallsBackToClasspath() throws Exception {
    CoreV1Api api = mock(CoreV1Api.class, RETURNS_DEEP_STUBS);
    when(api.listNamespacedConfigMap(any()).labelSelector(any()).execute())
        .thenThrow(new ApiException("simulated Kubernetes API failure"));

    KubernetesSolrResourceLoader loader = makeLoader(api, "my-configset");
    try (InputStream is = loader.openResource("test-classpath-resource.txt")) {
      assertNotNull(is);
    }
  }

  @Test
  public void testConfigMapAnnotationMismatchFallsBackToClasspath() throws Exception {
    CoreV1Api api = mock(CoreV1Api.class, RETURNS_DEEP_STUBS);
    // ConfigMap has annotation for a different configSet, so filter does not match
    V1ConfigMap cm = makeConfigMap("other-configset", Map.of("solrconfig.xml", "<config/>"));
    when(api.listNamespacedConfigMap(any()).labelSelector(any()).execute().getItems())
        .thenReturn(List.of(cm));

    KubernetesSolrResourceLoader loader = makeLoader(api, "my-configset");
    try (InputStream is = loader.openResource("test-classpath-resource.txt")) {
      assertNotNull(is);
    }
  }

  @Test
  public void testGetConfigSetName() {
    CoreV1Api api = mock(CoreV1Api.class, RETURNS_DEEP_STUBS);
    KubernetesSolrResourceLoader loader = makeLoader(api, "test-configset");
    assertEquals("test-configset", loader.getConfigSetName());
  }
}
