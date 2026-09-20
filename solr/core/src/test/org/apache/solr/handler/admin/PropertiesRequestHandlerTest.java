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
package org.apache.solr.handler.admin;

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.NodeConfig;
import org.junit.BeforeClass;
import org.junit.Test;

public class PropertiesRequestHandlerTest extends SolrTestCaseJ4 {

  public static final String PASSWORD = "secret123";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testRedaction() throws Exception {
    for (String propName :
        new String[] {
          "some.password",
          "javax.net.ssl.trustStorePassword",
          "solr.security.auth.basicauth.credentials",
          "some.Secret"
        }) {
      System.setProperty(propName, PASSWORD);
      Map<String, Object> properties = readProperties();

      assertEquals(
          "Failed to redact " + propName,
          NodeConfig.REDACTED_SYS_PROP_VALUE,
          properties.get(propName));
    }
  }

  @Test
  public void testSingleProperty() throws Exception {
    System.setProperty("GetNodeProperties.v1.visible", "hello");
    try {
      Map<String, Object> properties = readProperties("GetNodeProperties.v1.visible");
      assertEquals(1, properties.size());
      assertEquals("hello", properties.get("GetNodeProperties.v1.visible"));
    } finally {
      System.clearProperty("GetNodeProperties.v1.visible");
    }
  }

  @Test
  public void testMissingPropertyStillReturned() throws Exception {
    Map<String, Object> properties = readProperties("GetNodeProperties.v1.doesNotExist");
    assertEquals(1, properties.size());
    assertTrue(properties.containsKey("GetNodeProperties.v1.doesNotExist"));
    assertNull(properties.get("GetNodeProperties.v1.doesNotExist"));
  }

  private Map<String, Object> readProperties() throws Exception {
    return readProperties(null);
  }

  @SuppressWarnings({"unchecked"})
  private Map<String, Object> readProperties(String name) throws Exception {
    SolrClient client = new EmbeddedSolrServer(h.getCore());
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (name != null) {
      params.set("name", name);
    }
    NamedList<Object> properties =
        client.request(
            new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/properties", params));

    return (Map<String, Object>) properties.get("system.properties");
  }
}
