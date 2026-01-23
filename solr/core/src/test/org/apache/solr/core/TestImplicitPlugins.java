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
package org.apache.solr.core;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.response.BuiltInResponseWriterRegistry;
import org.apache.solr.response.QueryResponseWriter;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for implicit plugins loaded from ImplicitPlugins.json.
 *
 * <p>This test class verifies:
 *
 * <ul>
 *   <li>Request handlers are loaded from ImplicitPlugins.json
 *   <li>Response writers are loaded from ImplicitPlugins.json for core requests
 *   <li>Built in response writers use a minimal set defined in {@link
 *       org.apache.solr.response.BuiltInResponseWriterRegistry}.
 * </ul>
 */
public class TestImplicitPlugins extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  // ========== Core vs Built-in Writer Separation Tests ==========

  @Test
  public void testCoreAndBuiltInWriterIntegration() {
    final SolrCore core = h.getCore();

    // Test that core has extended writers from ImplicitPlugins.json
    assertNotNull("Core should have csv writer", core.getQueryResponseWriter("csv"));
    assertNotNull("Core should have geojson writer", core.getQueryResponseWriter("geojson"));
    assertNotNull("Core should have graphml writer", core.getQueryResponseWriter("graphml"));
    assertNotNull("Core should have smile writer", core.getQueryResponseWriter("smile"));

    // Test that built-in registry has minimal set and falls back for extended formats
    QueryResponseWriter standardWriter = BuiltInResponseWriterRegistry.getWriter("standard");
    assertSame(
        "Built-in csv request should fall back to standard",
        standardWriter,
        BuiltInResponseWriterRegistry.getWriter("csv"));
    assertSame(
        "Built-in geojson request should fall back to standard",
        standardWriter,
        BuiltInResponseWriterRegistry.getWriter("geojson"));

    // Test that both systems have common essential formats (though may be different instances)
    QueryResponseWriter coreJsonWriter = core.getQueryResponseWriter(CommonParams.JSON);
    QueryResponseWriter builtInJsonWriter =
        BuiltInResponseWriterRegistry.getWriter(CommonParams.JSON);
    assertNotNull("Core json writer should not be null", coreJsonWriter);
    assertNotNull("Built-in json writer should not be null", builtInJsonWriter);

    QueryResponseWriter coreXmlWriter = core.getQueryResponseWriter("xml");
    QueryResponseWriter builtInXmlWriter = BuiltInResponseWriterRegistry.getWriter("xml");
    assertNotNull("Core xml writer should not be null", coreXmlWriter);
    assertNotNull("Built-in xml writer should not be null", builtInXmlWriter);
  }
}
