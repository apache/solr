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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class KubernetesConfigSetServiceTest extends SolrTestCase {

  private CoreV1Api coreV1Api;
  private KubernetesConfigSetService service;

  @BeforeClass
  public static void setUpMockito() {
    assumeWorkingMockito();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    coreV1Api = mock(CoreV1Api.class, RETURNS_DEEP_STUBS);
    service =
        new KubernetesConfigSetService(
            new SolrResourceLoader(createTempDir()), false, coreV1Api, "default", "my-cloud");
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private V1ConfigMap makeConfigMap(String cmName, String configSetName, Map<String, String> data) {
    V1ObjectMeta meta = new V1ObjectMeta().name(cmName).namespace("default");
    if (configSetName != null) {
      meta.putAnnotationsItem(
          KubernetesConfigSetService.CONFIG_SET_NAME_ANNOTATION_KEY, configSetName);
    }
    return new V1ConfigMap().metadata(meta).data(data);
  }

  // --- extractConfigSetName ---

  @Test
  public void testExtractConfigSetNameWithAnnotation() {
    V1ConfigMap cm = makeConfigMap("cm-name", "my-configset", null);
    assertEquals("my-configset", service.extractConfigSetName(cm));
  }

  @Test
  public void testExtractConfigSetNameFallsBackToMapName() {
    V1ConfigMap cm = makeConfigMap("cm-name", null, null);
    assertEquals("cm-name", service.extractConfigSetName(cm));
  }

  @Test
  public void testExtractConfigSetNameNullMetadata() {
    // No NPE when metadata is null
    assertNull(service.extractConfigSetName(new V1ConfigMap()));
  }

  // --- checkConfigExists ---

  @Test
  public void testCheckConfigExists_present() {
    service.cacheConfigMap("my-configset", makeConfigMap("cm-name", "my-configset", null));
    assertTrue(service.checkConfigExists("my-configset"));
  }

  @Test
  public void testCheckConfigExists_absent() {
    assertFalse(service.checkConfigExists("nonexistent"));
  }

  // --- listConfigs ---

  @Test
  public void testListConfigs() {
    service.cacheConfigMap("a", makeConfigMap("cm-a", "a", null));
    service.cacheConfigMap("b", makeConfigMap("cm-b", "b", null));
    service.cacheConfigMap("c", makeConfigMap("cm-c", "c", null));
    List<String> configs = service.listConfigs();
    assertEquals(3, configs.size());
    assertTrue(configs.containsAll(List.of("a", "b", "c")));
  }

  // --- getAllConfigFiles ---

  @Test
  public void testGetAllConfigFiles() {
    service.cacheConfigMap(
        "config",
        makeConfigMap(
            "cm-name", "config", Map.of("solrconfig.xml", "<config/>", "schema.xml", "<schema/>")));
    List<String> files = service.getAllConfigFiles("config");
    assertEquals(2, files.size());
    assertTrue(files.containsAll(List.of("solrconfig.xml", "schema.xml")));
  }

  // --- getConfigMetadata ---

  @Test
  @SuppressWarnings("unchecked")
  public void testGetConfigMetadata_present() throws Exception {
    Map<String, Object> meta = Map.of("immutable", true);
    String metaJson = Utils.toJSONString(meta);
    service.cacheConfigMap(
        "config",
        makeConfigMap(
            "cm-name",
            "config",
            Map.of(KubernetesConfigSetService.CONFIG_SET_METADATA_KEY, metaJson)));
    Map<String, Object> result = service.getConfigMetadata("config");
    assertEquals(Boolean.TRUE, result.get("immutable"));
  }

  @Test
  public void testGetConfigMetadata_absent() throws Exception {
    service.cacheConfigMap("config", makeConfigMap("cm-name", "config", Map.of()));
    Map<String, Object> result = service.getConfigMetadata("config");
    assertTrue(result.isEmpty());
  }

  // --- deleteConfig ---

  @Test
  public void testDeleteConfig_callsKubeApi() throws Exception {
    service.cacheConfigMap("config", makeConfigMap("my-cm", "config", null));
    service.deleteConfig("config");
    verify(coreV1Api).deleteNamespacedConfigMap(eq("my-cm"), any());
  }

  @Test
  public void testDeleteConfig_noopWhenMissing() throws Exception {
    service.deleteConfig("nonexistent");
    verify(coreV1Api, never()).deleteNamespacedConfigMap(any(), any());
  }

  // --- copyConfig ---

  @Test
  public void testCopyConfig_createsNewConfigMap() throws Exception {
    service.cacheConfigMap(
        "source", makeConfigMap("cm-source", "source", Map.of("solrconfig.xml", "<config/>")));
    service.copyConfig("source", "dest");
    verify(coreV1Api).createNamespacedConfigMap(eq("default"), any());
  }

  @Test
  public void testCopyConfig_throwsWhenDestExists() {
    service.cacheConfigMap("source", makeConfigMap("cm-source", "source", Map.of()));
    service.cacheConfigMap("dest", makeConfigMap("cm-dest", "dest", Map.of()));
    SolrException ex =
        assertThrows(SolrException.class, () -> service.copyConfig("source", "dest"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
  }

  @Test
  public void testCopyConfig_throwsWhenSourceHasNoData() {
    // null data in source configMap
    service.cacheConfigMap("source", makeConfigMap("cm-source", "source", null));
    assertThrows(SolrException.class, () -> service.copyConfig("source", "dest"));
  }

  // --- downloadConfig ---

  @Test
  public void testDownloadConfig_writesFiles() throws Exception {
    service.cacheConfigMap(
        "config", makeConfigMap("cm-name", "config", Map.of("solrconfig.xml", "<config/>")));
    Path tmpDir = createTempDir();
    service.downloadConfig("config", tmpDir);
    assertTrue(Files.exists(tmpDir.resolve("solrconfig.xml")));
    assertEquals(
        "<config/>", Files.readString(tmpDir.resolve("solrconfig.xml"), StandardCharsets.UTF_8));
  }

  @Test
  public void testDownloadConfig_nullDataThrows() {
    service.cacheConfigMap("config", makeConfigMap("cm-name", "config", null));
    SolrException ex =
        assertThrows(SolrException.class, () -> service.downloadConfig("config", createTempDir()));
    assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, ex.code());
  }

  // --- downloadFileFromConfig ---

  @Test
  public void testDownloadFileFromConfig_found() {
    service.cacheConfigMap(
        "config", makeConfigMap("cm-name", "config", Map.of("solrconfig.xml", "hello")));
    byte[] result = service.downloadFileFromConfig("config", "solrconfig.xml");
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testDownloadFileFromConfig_absent() {
    service.cacheConfigMap("config", makeConfigMap("cm-name", "config", Map.of()));
    assertNull(service.downloadFileFromConfig("config", "nonexistent.xml"));
  }

  @Test
  public void testDownloadFileFromConfig_configNotInCache() {
    assertThrows(SolrException.class, () -> service.downloadFileFromConfig("missing", "file.xml"));
  }

  // --- getCurrentSchemaModificationVersion ---

  @Test
  public void testCurrentSchemaModificationVersion_present() {
    V1ConfigMap cm = makeConfigMap("cm-name", "config", null);
    cm.getMetadata().generation(42L);
    service.cacheConfigMap("config", cm);
    assertEquals(
        Long.valueOf(42L), service.getCurrentSchemaModificationVersion("config", null, null));
  }

  @Test
  public void testCurrentSchemaModificationVersion_absent() {
    // configSet not in cache → returns null
    assertNull(service.getCurrentSchemaModificationVersion("nonexistent", null, null));
  }

  // --- uploadFileToConfig ---

  @Test
  public void testUploadFileToConfig_forbiddenFiletype() throws Exception {
    service.cacheConfigMap("config", makeConfigMap("cm-name", "config", Map.of()));
    // Forbidden file type → logs warning, returns silently
    service.uploadFileToConfig("config", "bad.class", new byte[] {}, false);
  }

  @Test
  public void testUploadFileToConfig_existingNoOverwrite() throws Exception {
    // File already exists in configMap and overwriteOnExists=false → no-op
    service.cacheConfigMap(
        "config", makeConfigMap("cm-name", "config", Map.of("solrconfig.xml", "<existing/>")));
    service.uploadFileToConfig(
        "config", "solrconfig.xml", "<new/>".getBytes(StandardCharsets.UTF_8), false);
  }

  @Test
  public void testUploadFileToConfig_newFile_throwsUnsupported() {
    // File does not exist in configMap → placeholder throws UnsupportedOperationException
    service.cacheConfigMap("config", makeConfigMap("cm-name", "config", Map.of()));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            service.uploadFileToConfig(
                "config", "solrconfig.xml", "<config/>".getBytes(StandardCharsets.UTF_8), false));
  }

  // --- setConfigMetadata ---

  @Test
  public void testSetConfigMetadata_noopWhenSame() throws Exception {
    Map<String, Object> meta = Map.of("immutable", true);
    String metaJson = Utils.toJSONString(meta);
    service.cacheConfigMap(
        "config",
        makeConfigMap(
            "cm-name",
            "config",
            Map.of(KubernetesConfigSetService.CONFIG_SET_METADATA_KEY, metaJson)));
    // Same metadata → early return, no exception
    service.setConfigMetadata("config", meta);
  }

  @Test
  public void testSetConfigMetadata_throwsWhenDifferent() {
    service.cacheConfigMap(
        "config",
        makeConfigMap(
            "cm-name",
            "config",
            Map.of(KubernetesConfigSetService.CONFIG_SET_METADATA_KEY, "old-value")));
    assertThrows(
        UnsupportedOperationException.class,
        () -> service.setConfigMetadata("config", Map.of("new", "data")));
  }

  // --- createCoreResourceLoader ---

  @Test
  public void testCreateCoreResourceLoader() {
    CoreDescriptor cd = mock(CoreDescriptor.class, RETURNS_DEEP_STUBS);
    when(cd.getConfigSet()).thenReturn("my-configset");
    when(cd.getInstanceDir()).thenReturn(createTempDir());

    org.apache.solr.core.SolrResourceLoader loader = service.createCoreResourceLoader(cd);
    assertTrue(loader instanceof KubernetesSolrResourceLoader);
    assertEquals("my-configset", ((KubernetesSolrResourceLoader) loader).getConfigSetName());
  }
}
