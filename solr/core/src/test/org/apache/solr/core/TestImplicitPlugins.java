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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
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
 *   <li>Admin response writers use a minimal set (ADMIN_RESPONSE_WRITERS)
 *   <li>Backward compatibility is maintained (DEFAULT_RESPONSE_WRITERS still exists)
 * </ul>
 */
public class TestImplicitPlugins extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  // ========== Request Handler Tests ==========

  @Test
  public void testImplicitRequestHandlers() {
    final SolrCore core = h.getCore();
    final List<PluginInfo> implicitHandlers = core.getImplicitHandlers();

    assertNotNull("Implicit handlers should not be null", implicitHandlers);
    assertFalse("Should have implicit handlers", implicitHandlers.isEmpty());

    // Verify some key handlers are present
    Set<String> handlerNames = new HashSet<>();
    for (PluginInfo handler : implicitHandlers) {
      handlerNames.add(handler.name);
    }

    assertTrue("Should have /update handler", handlerNames.contains("/update"));
    assertTrue("Should have /admin/ping handler", handlerNames.contains("/admin/ping"));
    assertTrue("Should have /config handler", handlerNames.contains("/config"));
    assertTrue("Should have /schema handler", handlerNames.contains("/schema"));
  }

  // ========== Core Response Writer Tests (ImplicitPlugins.json) ==========

  @Test
  public void testImplicitResponseWriters() {
    final SolrCore core = h.getCore();
    final List<PluginInfo> implicitWriters = core.getImplicitResponseWriters();

    assertNotNull("Implicit response writers should not be null", implicitWriters);
    assertFalse("Should have implicit response writers", implicitWriters.isEmpty());

    // Verify key writers are present
    Set<String> writerNames = new HashSet<>();
    for (PluginInfo writer : implicitWriters) {
      writerNames.add(writer.name);
    }

    assertTrue("Should have xml writer", writerNames.contains("xml"));
    assertTrue("Should have json writer", writerNames.contains("json"));
    assertTrue("Should have csv writer", writerNames.contains("csv"));
    assertTrue("Should have javabin writer", writerNames.contains("javabin"));
  }

  @Test
  public void testImplicitWritersHaveCorrectClass() {
    final SolrCore core = h.getCore();
    final List<PluginInfo> implicitWriters = core.getImplicitResponseWriters();

    for (PluginInfo writer : implicitWriters) {
      assertNotNull("Writer should have a name", writer.name);
      assertNotNull("Writer should have a className", writer.className);
      assertTrue(
          "Writer className should start with 'solr.'", writer.className.startsWith("solr."));
    }
  }

  @Test
  public void testCoreResponseWritersAreAvailable() {
    final SolrCore core = h.getCore();

    // Verify that writers loaded from ImplicitPlugins.json are actually available for core requests
    assertNotNull("Core should have xml writer", core.getQueryResponseWriter("xml"));
    assertNotNull("Core should have json writer", core.getQueryResponseWriter("json"));
    assertNotNull("Core should have csv writer", core.getQueryResponseWriter("csv"));
    assertNotNull("Core should have javabin writer", core.getQueryResponseWriter("javabin"));
    assertNotNull("Core should have standard writer", core.getQueryResponseWriter("standard"));
    assertNotNull("Core should have geojson writer", core.getQueryResponseWriter("geojson"));
    assertNotNull("Core should have graphml writer", core.getQueryResponseWriter("graphml"));
    assertNotNull("Core should have smile writer", core.getQueryResponseWriter("smile"));
  }

  // ========== Admin Response Writer Tests (ADMIN_RESPONSE_WRITERS) ==========

  @Test
  public void testAdminResponseWritersMinimalSet() {
    // Admin requests have no SolrCore, so they use ADMIN_RESPONSE_WRITERS
    // which contains only the essential formats: javabin, json, xml, prometheus, openmetrics, standard

    // Verify all admin writers exist
    assertNotNull(
        "Admin javabin writer should exist", SolrCore.getAdminResponseWriter(CommonParams.JAVABIN));
    assertNotNull(
        "Admin json writer should exist", SolrCore.getAdminResponseWriter(CommonParams.JSON));
    assertNotNull("Admin xml writer should exist", SolrCore.getAdminResponseWriter("xml"));
    assertNotNull(
        "Admin prometheus writer should exist", SolrCore.getAdminResponseWriter("prometheus"));
    assertNotNull(
        "Admin openmetrics writer should exist", SolrCore.getAdminResponseWriter("openmetrics"));
    assertNotNull(
        "Admin standard writer should exist", SolrCore.getAdminResponseWriter("standard"));
  }

  @Test
  public void testAdminResponseWritersFallbackBehavior() {
    // Test that null/empty defaults to standard
    QueryResponseWriter nullWriter = SolrCore.getAdminResponseWriter(null);
    QueryResponseWriter emptyWriter = SolrCore.getAdminResponseWriter("");
    QueryResponseWriter standardWriter = SolrCore.getAdminResponseWriter("standard");

    assertNotNull("Null writer should not be null", nullWriter);
    assertNotNull("Empty writer should not be null", emptyWriter);
    assertNotNull("Standard writer should not be null", standardWriter);
    assertSame("Null writer should return standard writer", standardWriter, nullWriter);
    assertSame("Empty writer should return standard writer", standardWriter, emptyWriter);
  }

  @Test
  public void testAdminResponseWritersUnknownFormat() {
    // Test that unknown formats default to standard (json)
    QueryResponseWriter standardWriter = SolrCore.getAdminResponseWriter("standard");
    QueryResponseWriter unknownWriter = SolrCore.getAdminResponseWriter("csv");

    assertNotNull("Unknown writer should not be null", unknownWriter);
    assertSame(
        "Unknown formats (like csv) should return standard writer for admin requests",
        standardWriter,
        unknownWriter);
  }

  // ========== Core vs Admin Writer Separation Tests ==========

  @Test
  public void testCoreAndAdminWritersSeparation() {
    final SolrCore core = h.getCore();

    // Core requests have access to all writers from ImplicitPlugins.json
    assertNotNull("Core should have csv writer", core.getQueryResponseWriter("csv"));
    assertNotNull("Core should have geojson writer", core.getQueryResponseWriter("geojson"));
    assertNotNull("Core should have graphml writer", core.getQueryResponseWriter("graphml"));
    assertNotNull("Core should have smile writer", core.getQueryResponseWriter("smile"));

    // Admin requests only have access to minimal set
    // (csv, geojson, graphml, smile are NOT in ADMIN_RESPONSE_WRITERS)
    QueryResponseWriter standardWriter = SolrCore.getAdminResponseWriter("standard");
    assertSame(
        "Admin csv request should fall back to standard",
        standardWriter,
        SolrCore.getAdminResponseWriter("csv"));
    assertSame(
        "Admin geojson request should fall back to standard",
        standardWriter,
        SolrCore.getAdminResponseWriter("geojson"));
    assertSame(
        "Admin graphml request should fall back to standard",
        standardWriter,
        SolrCore.getAdminResponseWriter("graphml"));
    assertSame(
        "Admin smile request should fall back to standard",
        standardWriter,
        SolrCore.getAdminResponseWriter("smile"));
  }

  @Test
  public void testCoreAndAdminBothHaveCommonWriters() {
    final SolrCore core = h.getCore();

    // Both core and admin should have these common formats
    // (though they may be different instances)
    QueryResponseWriter coreJsonWriter = core.getQueryResponseWriter(CommonParams.JSON);
    QueryResponseWriter adminJsonWriter = SolrCore.getAdminResponseWriter(CommonParams.JSON);
    assertNotNull("Core json writer should not be null", coreJsonWriter);
    assertNotNull("Admin json writer should not be null", adminJsonWriter);

    QueryResponseWriter coreXmlWriter = core.getQueryResponseWriter("xml");
    QueryResponseWriter adminXmlWriter = SolrCore.getAdminResponseWriter("xml");
    assertNotNull("Core xml writer should not be null", coreXmlWriter);
    assertNotNull("Admin xml writer should not be null", adminXmlWriter);
  }

  // ========== Backward Compatibility Tests ==========

  @Test
  public void testDefaultResponseWritersBackwardCompatibility() {
    // Verify the deprecated DEFAULT_RESPONSE_WRITERS map still exists for external code
    assertNotNull(
        "DEFAULT_RESPONSE_WRITERS should still exist for backward compatibility",
        SolrCore.DEFAULT_RESPONSE_WRITERS);
    assertFalse(
        "DEFAULT_RESPONSE_WRITERS should not be empty",
        SolrCore.DEFAULT_RESPONSE_WRITERS.isEmpty());

    // Verify it contains all the expected writers (both core and admin formats)
    assertTrue(
        "Should have json in DEFAULT_RESPONSE_WRITERS",
        SolrCore.DEFAULT_RESPONSE_WRITERS.containsKey(CommonParams.JSON));
    assertTrue(
        "Should have xml in DEFAULT_RESPONSE_WRITERS",
        SolrCore.DEFAULT_RESPONSE_WRITERS.containsKey("xml"));
    assertTrue(
        "Should have csv in DEFAULT_RESPONSE_WRITERS",
        SolrCore.DEFAULT_RESPONSE_WRITERS.containsKey("csv"));
    assertTrue(
        "Should have geojson in DEFAULT_RESPONSE_WRITERS",
        SolrCore.DEFAULT_RESPONSE_WRITERS.containsKey("geojson"));
    assertTrue(
        "Should have javabin in DEFAULT_RESPONSE_WRITERS",
        SolrCore.DEFAULT_RESPONSE_WRITERS.containsKey(CommonParams.JAVABIN));
  }
}