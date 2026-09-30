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

import java.util.Collections;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.NodePropertiesResponse;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * HTTP tests for {@code GET /api/node/properties} and {@code GET
 * /api/node/properties/{propertyName}} via the generated SolrJ client classes.
 */
public class GetNodePropertiesTest extends SolrTestCase {

  private static final String VISIBLE_PROP = "GetNodePropertiesTest.visible";
  private static final String SECRET_PROP = "GetNodePropertiesTest.password";
  private static final String PASSWORD = "secret123";

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    solrTestRule.startSolr(createTempDir());
  }

  @After
  public void clearTestProperties() {
    System.clearProperty(VISIBLE_PROP);
    System.clearProperty(SECRET_PROP);
  }

  @Test
  public void testNamedProperty() throws Exception {
    var req = new NodeApi.GetNodeProperty("java.version");
    var rsp = req.process(solrTestRule.getAdminClient());

    assertNotNull(rsp);
    assertNull(rsp.error);
    assertEquals(1, rsp.systemProperties.size());
    assertEquals(System.getProperty("java.version"), rsp.systemProperties.get("java.version"));
  }

  @Test
  public void testAllProperties() throws Exception {
    System.setProperty(VISIBLE_PROP, "hello");

    NodePropertiesResponse rsp = fetchProperties(null);

    assertEquals(
        Collections.list(System.getProperties().propertyNames()).size(),
        rsp.systemProperties.size());
    assertEquals(System.getProperty("java.version"), rsp.systemProperties.get("java.version"));
    assertEquals("hello", rsp.systemProperties.get(VISIBLE_PROP));
  }

  @Test
  public void testRedactsHiddenProperties() throws Exception {
    System.setProperty(SECRET_PROP, PASSWORD);

    NodePropertiesResponse named = fetchProperties(SECRET_PROP);
    assertEquals(1, named.systemProperties.size());
    assertEquals(NodeConfig.REDACTED_SYS_PROP_VALUE, named.systemProperties.get(SECRET_PROP));
    assertFalse(named.systemProperties.containsValue(PASSWORD));

    NodePropertiesResponse all = fetchProperties(null);
    assertEquals(NodeConfig.REDACTED_SYS_PROP_VALUE, all.systemProperties.get(SECRET_PROP));
    assertFalse(all.systemProperties.containsValue(PASSWORD));
  }

  @Test
  public void testUnknownPropertyReturns404() {
    var req = new NodeApi.GetNodeProperty("GetNodePropertiesTest.doesNotExist");
    final RemoteSolrException ex =
        expectThrows(RemoteSolrException.class, () -> req.process(solrTestRule.getAdminClient()));
    assertEquals(404, ex.code());
  }

  @Test
  public void testUnknownHiddenPropertyDoesNotRevealExistence() throws Exception {
    final String hiddenUnset = "GetNodePropertiesTest.doesNotExist.password";
    assertFalse(System.getProperties().containsKey(hiddenUnset));

    NodePropertiesResponse rsp = fetchProperties(hiddenUnset);
    assertEquals(1, rsp.systemProperties.size());
    assertEquals(NodeConfig.REDACTED_SYS_PROP_VALUE, rsp.systemProperties.get(hiddenUnset));
  }

  private NodePropertiesResponse fetchProperties(String name) throws Exception {
    NodePropertiesResponse rsp =
        name == null
            ? new NodeApi.GetNodeProperties().process(solrTestRule.getAdminClient())
            : new NodeApi.GetNodeProperty(name).process(solrTestRule.getAdminClient());
    assertNotNull(rsp);
    assertNull(rsp.error);
    assertNotNull(rsp.systemProperties);
    return rsp;
  }
}
