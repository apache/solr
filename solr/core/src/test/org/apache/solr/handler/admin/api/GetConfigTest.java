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

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.IndexType;
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
  public void testGetConfigComponentFromCore() throws Exception {
    for (String component : GetConfig.MIGRATED_CONFIG_COMPONENTS) {
      var request = new ConfigApi.GetConfigComponent(IndexType.CORE, CORE_NAME, component);
      var response = request.process(solrTestRule.getSolrClient(CORE_NAME));

      assertNotNull(component, response);
      assertNull(component, response.error);
      assertNotNull(component, response.config);
      assertTrue(component, response.config.containsKey(component));
      assertEquals(component, 1, response.config.size());
    }
  }
}
