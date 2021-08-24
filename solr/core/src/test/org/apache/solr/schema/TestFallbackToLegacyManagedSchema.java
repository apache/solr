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
package org.apache.solr.schema;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceNotFoundException;
import org.apache.solr.util.RestTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test updating a Managed Schema that is the legacy "managed-schema" instead of the
 * default "managed-schema.xml" file properly falls back to "managed-schema"
 */

public class TestFallbackToLegacyManagedSchema extends RestTestBase {

  private static final String collection = "collection1";

  @Before
  public void before() throws Exception {
    File tmpSolrHome = createTempDir().toFile();
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    System.setProperty("managed.schema.mutable", "true");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "managed-schema",
        "/solr", true, null);
    
 
  }

  @After
  public void after() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    client = null;
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;
  }


  @Test
  public void testUpdatingSchemaWithLegacyManagedSchema() throws Exception {
    String payload = "{\n" +
        "    'add-field' : {\n" +
        "                 'name':'a1',\n" +
        "                 'type': 'string',\n" +
        "                 'stored':true,\n" +
        "                 'indexed':false\n" +
        "                 },\n" +
        "    }";

    String response = restTestHarness.post("/schema", json(payload));
    Map<?, ?> map = (Map<?, ?>) Utils.fromJSONString(response);
    Map<?, ?> responseHeader = (Map<?, ?>)map.get("responseHeader");
    Long status = (Long)responseHeader.get("status");
    assertEquals((long)status, 0L);

    
    SolrCore core = jetty.getCoreContainer().getCore(collection);
    try {
      String schemaResourceName = core.getSchemaResource();
      assertEquals("managed-schema", core.getSchemaResource());
      assertEquals("legacy-managed-schema", core.getLatestSchema().getSchemaName());
      
      String managedSchemaText = new String(core.getResourceLoader().openResource(schemaResourceName).readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(managedSchemaText.contains("<field name=\"a1\" type=\"string\" indexed=\"false\" stored=\"true\"/>"));
      
      assertExceptionThrownWithMessageContaining(SolrResourceNotFoundException.class, Lists.newArrayList("Can't find resource 'managed-schema.xml' in classpath"), () -> {
        core.getResourceLoader().openResource("managed-schema.xml");
      });
      
    }
    finally{
      core.close();
    }
  }

}
