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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.ConfigApi;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** HTTP tests for fetching the full config through the generated SolrJ request. */
public class GetConfigTest extends SolrTestCase {

  private static final String CORE_NAME = "configApiTestCore";

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    System.setProperty(ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule.newCollection(CORE_NAME).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Test
  public void testGetConfigFromCore() throws Exception {
    var request = new ConfigApi.GetConfig(IndexType.CORE, CORE_NAME);
    var response = request.process(solrTestRule.getSolrClient(CORE_NAME));

    assertNotNull(response);
    assertNull(response.error);
    assertNotNull(response.config);
    assertTrue(response.config.containsKey("luceneMatchVersion"));
    assertTrue(response.config.containsKey("updateHandler"));
    assertTrue(response.config.containsKey("query"));
    assertTrue(response.config.containsKey("requestHandler"));
    assertTrue(response.config.containsKey("searchComponent"));
    assertTrue(response.config.containsKey("updateProcessor"));
  }

  @Test
  public void testGetOverlayFromCore() throws Exception {
    var request = new ConfigApi.GetOverlay(IndexType.CORE, CORE_NAME);
    var response = request.process(solrTestRule.getSolrClient(CORE_NAME));

    assertNotNull(response);
    assertNull(response.error);
    assertNotNull(response.overlay);
    assertTrue(response.overlay.containsKey("znodeVersion"));
    assertEquals(-1, response.overlay.get("znodeVersion"));
  }

  @Test
  public void testGetZnodeVersionFromCore() throws Exception {
    var request = new ConfigApi.GetZnodeVersion(IndexType.CORE, CORE_NAME);
    var response = request.process(solrTestRule.getSolrClient(CORE_NAME));

    assertNotNull(response);
    assertNull(response.error);
    assertNotNull(response.znodeVersion);
    assertTrue(response.znodeVersion.overlay >= -1);
    assertTrue(response.znodeVersion.params >= -1);
  }

  @Test
  public void testGetConfigComponentFromCore() throws Exception {
    for (String component : GetConfig.MIGRATED_CONFIG_COMPONENTS) {
      String pathComponent = GetConfig.toExternalComponentName(component);
      var request = new ConfigApi.GetConfigComponent(IndexType.CORE, CORE_NAME, pathComponent);
      var response = request.process(solrTestRule.getSolrClient(CORE_NAME));

      assertNotNull(component, response);
      assertNull(component, response.error);
      assertNotNull(component, response.config);
      assertTrue(component, response.config.containsKey(pathComponent));
      assertEquals(component, 1, response.config.size());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetConfigComponentFilteredByNameFromCore() throws Exception {
    var request =
        new ConfigApi.GetConfigComponentByName(
            IndexType.CORE, CORE_NAME, "searchComponents", "spellcheck");
    request.setMeta(true);
    var response = request.process(solrTestRule.getSolrClient(CORE_NAME));

    assertNotNull(response);
    assertNull(response.error);
    assertNotNull(response.config);
    assertEquals(1, response.config.size());

    Map<String, Object> section = (Map<String, Object>) response.config.get("searchComponents");
    assertNotNull(section);
    assertTrue(section.containsKey("spellcheck"));

    Map<String, Object> spellcheck = (Map<String, Object>) section.get("spellcheck");
    assertNotNull(spellcheck);
    assertEquals("solr.SpellCheckComponent", spellcheck.get("class"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetConfigComponentFilteredByNameOmitsPackageInfoByDefault() throws Exception {
    var request =
        new ConfigApi.GetConfigComponentByName(
            IndexType.CORE, CORE_NAME, "searchComponents", "spellcheck");
    // meta is left unset, so it defaults to false and no package lookup should happen.
    var response = request.process(solrTestRule.getSolrClient(CORE_NAME));

    Map<String, Object> section = (Map<String, Object>) response.config.get("searchComponents");
    Map<String, Object> spellcheck = (Map<String, Object>) section.get("spellcheck");
    assertFalse(spellcheck.containsKey("_packageinfo_"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetConfigComponentFilteredByNameForOtherMigratedComponents() throws Exception {
    // filterCache is one of the few <query> entries the default configset doesn't comment out.
    var queryRequest =
        new ConfigApi.GetConfigComponentByName(IndexType.CORE, CORE_NAME, "query", "filterCache");
    var queryResponse = queryRequest.process(solrTestRule.getSolrClient(CORE_NAME));
    assertNotNull(queryResponse);
    assertNull(queryResponse.error);
    Map<String, Object> querySection = (Map<String, Object>) queryResponse.config.get("query");
    assertNotNull(querySection);
    assertTrue(querySection.containsKey("filterCache"));
  }

  @Test
  public void testGetConfigComponentByUnknownNameReturnsNotFound() {
    var request =
        new ConfigApi.GetConfigComponentByName(
            IndexType.CORE, CORE_NAME, "searchComponents", "doesNotExist");
    final RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () -> request.process(solrTestRule.getSolrClient(CORE_NAME)));
    assertEquals(404, ex.code());
  }

  @Test
  public void testGetConfigComponentRejectsOldSingularSearchComponentPath() {
    // v2 intentionally renamed this one section to the plural "searchComponents" in the URL;
    // the old singular form (still used by v1) shouldn't match the v2 route at all.
    var request = new ConfigApi.GetConfigComponent(IndexType.CORE, CORE_NAME, "searchComponent");
    final RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () -> request.process(solrTestRule.getSolrClient(CORE_NAME)));
    assertEquals(404, ex.code());
  }
}
