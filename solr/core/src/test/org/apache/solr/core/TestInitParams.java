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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestInitParams extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-paramset.xml", "schema.xml");
  }

  @Test
  public void testComponentWithInitParams() {

    for (String s : Arrays.asList("/dump1", "/dump3", "/root/dump5", "/root1/anotherlevel/dump6")) {
      SolrRequestHandler handler = h.getCore().getRequestHandler(s);
      SolrQueryResponse rsp = new SolrQueryResponse();
      handler.handleRequest(req("initArgs", "true"), rsp);
      NamedList<?> nl = (NamedList<?>) rsp.getValues().get("initArgs");
      NamedList<?> def = (NamedList<?>) nl.get(PluginInfo.DEFAULTS);
      assertEquals("A", def.get("a"));
      def = (NamedList<?>) nl.get(PluginInfo.INVARIANTS);
      assertEquals("B", def.get("b"));
      def = (NamedList<?>) nl.get(PluginInfo.APPENDS);
      assertEquals("C", def.get("c"));
    }

    InitParams initParams = h.getCore().getSolrConfig().getInitParams().get("a");

    PluginInfo pluginInfo =
        new PluginInfo(
            "requestHandler",
            new HashMap<>(),
            new NamedList<>(Map.of("defaults", new NamedList<>(Map.of("a", "A1")))),
            null);
    initParams.apply(pluginInfo);
    assertEquals("A", initParams.defaults.get("a"));
  }

  @Test
  public void testMultiInitParams() {
    SolrRequestHandler handler = h.getCore().getRequestHandler("/dump6");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    NamedList<?> nl = (NamedList<?>) rsp.getValues().get("initArgs");
    NamedList<?> def = (NamedList<?>) nl.get(PluginInfo.DEFAULTS);
    assertEquals("A", def.get("a"));
    assertEquals("P", def.get("p"));
    def = (NamedList<?>) nl.get(PluginInfo.INVARIANTS);
    assertEquals("B", def.get("b"));
    def = (NamedList<?>) nl.get(PluginInfo.APPENDS);
    assertEquals("C", def.get("c"));
  }

  @Test
  public void testComponentWithConflictingInitParams() {
    SolrRequestHandler handler = h.getCore().getRequestHandler("/dump2");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    NamedList<?> nl = (NamedList<?>) rsp.getValues().get("initArgs");
    NamedList<?> def = (NamedList<?>) nl.get(PluginInfo.DEFAULTS);
    assertEquals("A1", def.get("a"));
    def = (NamedList<?>) nl.get(PluginInfo.INVARIANTS);
    assertEquals("B1", def.get("b"));
    def = (NamedList<?>) nl.get(PluginInfo.APPENDS);
    assertEquals(Arrays.asList("C1", "C"), def.getAll("c"));
  }

  public void testNestedRequestHandler() {
    assertNotNull(h.getCore().getRequestHandler("/greedypath"));
    assertNotNull(h.getCore().getRequestHandler("/greedypath/some/path"));
    assertNotNull(h.getCore().getRequestHandler("/greedypath/some/other/path"));
    assertNull(h.getCore().getRequestHandler("/greedypath/unknownpath"));
  }

  public void testElevateExample() {
    SolrRequestHandler handler = h.getCore().getRequestHandler("/elevate");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    NamedList<?> nl = (NamedList<?>) rsp.getValues().get("initArgs");
    NamedList<?> def = (NamedList<?>) nl.get(PluginInfo.DEFAULTS);
    assertEquals("text", def.get("df"));
  }

  public void testArbitraryAttributes() {
    SolrRequestHandler handler = h.getCore().getRequestHandler("/dump7");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    NamedList<?> nl = (NamedList<?>) rsp.getValues().get("initArgs");
    assertEquals("server-enabled.txt", nl.get("healthcheckFile"));
  }

  public void testMatchPath() {
    InitParams initParams =
        new InitParams(new PluginInfo(InitParams.TYPE, Map.of("path", "/update/json/docs")));
    assertFalse(initParams.matchPath("/update"));
    assertTrue(initParams.matchPath("/update/json/docs"));
    initParams = new InitParams(new PluginInfo(InitParams.TYPE, Map.of("path", "/update/**")));
    assertTrue(initParams.matchPath("/update/json/docs"));
    assertTrue(initParams.matchPath("/update/json"));
    assertTrue(initParams.matchPath("/update"));
    initParams = new InitParams(new PluginInfo(InitParams.TYPE, Map.of("path", "/update/*")));
    assertFalse(initParams.matchPath("/update/json/docs"));
    assertTrue(initParams.matchPath("/update/json"));
    assertTrue(initParams.matchPath("/update"));
  }

  @Test
  public void testUpdateJsonDocsInitParamsRejectsFieldMappings() throws Exception {
    initCore("solrconfig-update-json-docs-initparams.xml", "schema.xml");
    try {
      SolrRequestHandler handler = h.getCore().getRequestHandler("/update/json/docs");
      SolrException ex =
          expectThrows(
              SolrException.class,
              () ->
                  handler.handleRequest(
                      jsonDocsRequest("f", "title:/title"), new SolrQueryResponse()));
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
      assertTrue(ex.getMessage(), ex.getMessage().contains("mapUniqueKeyOnly"));

      SolrQueryResponse rsp = new SolrQueryResponse();
      handler.handleRequest(
          jsonDocsRequest("mapUniqueKeyOnly", "false", "echo", "true", "f", "title:/title"), rsp);
      assertEquals("[{title=t}]", String.valueOf(rsp.getValues().get("docs")));
    } finally {
      initCore("solrconfig-paramset.xml", "schema.xml");
    }
  }

  private SolrQueryRequestBase jsonDocsRequest(String... keysAndValues) {
    ModifiableSolrParams solrParams = params("json.command", "false");
    for (int i = 0; i < keysAndValues.length; i += 2) {
      solrParams.set(keysAndValues[i], keysAndValues[i + 1]);
    }
    SolrQueryRequestBase req = new SolrQueryRequestBase(h.getCore(), solrParams) {};
    String json = "{\"id\":\"1\",\"title\":\"t\"}";
    req.setContentStreams(List.of(new ContentStreamBase.StringStream(json, "application/json")));
    return req;
  }
}
