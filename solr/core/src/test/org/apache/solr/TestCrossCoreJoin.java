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
package org.apache.solr;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.join.TestScoreJoinQPNoScore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCrossCoreJoin extends SolrTestCaseJ4 {

  private static SolrCore fromCore;
  private static EmbeddedSolrServer fromServer;

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty(
        "solr.index.updatelog.enabled", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.filterCache.async", "true");

    initCore("solrconfig.xml", "schema12.xml", TEST_HOME(), "collection1");
    final CoreContainer coreContainer = h.getCoreContainer();

    fromCore = coreContainer.create("fromCore", Map.of("configSet", "minimal"));
    fromServer = new EmbeddedSolrServer(fromCore.getCoreContainer(), fromCore.getName());

    // Add documents to the main core
    assertU(
        add(
            doc(
                "id",
                "1",
                "id_s_dv",
                "1",
                "name",
                "john",
                "title",
                "Director",
                "dept_s",
                "Engineering")));
    assertU(
        add(doc("id", "2", "id_s_dv", "2", "name", "mark", "title", "VP", "dept_s", "Marketing")));
    assertU(
        add(doc("id", "3", "id_s_dv", "3", "name", "nancy", "title", "MTS", "dept_s", "Sales")));
    assertU(
        add(
            doc(
                "id",
                "4",
                "id_s_dv",
                "4",
                "name",
                "dave",
                "title",
                "MTS",
                "dept_s",
                "Support",
                "dept_s",
                "Engineering")));
    assertU(
        add(
            doc(
                "id",
                "5",
                "id_s_dv",
                "5",
                "name",
                "tina",
                "title",
                "VP",
                "dept_s",
                "Engineering")));
    assertU(commit());

    // Add documents to the fromCore
    List<SolrInputDocument> docs =
        sdocs(
            sdoc(
                "id", "10",
                "id_s_dv", "10",
                "dept_id_s", "Engineering",
                "text", "These guys develop stuff",
                "cat", "dev"),
            sdoc(
                "id", "11",
                "id_s_dv", "11",
                "dept_id_s", "Marketing",
                "text", "These guys make you look good"),
            sdoc(
                "id", "12",
                "id_s_dv", "12",
                "dept_id_s", "Sales",
                "text", "These guys sell stuff"),
            sdoc(
                "id", "13",
                "id_s_dv", "13",
                "dept_id_s", "Support",
                "text", "These guys help customers"));

    // Add all documents to fromCore
    fromServer.add(docs);
    fromServer.commit();
  }

  @Test
  public void testJoin() throws Exception {
    doTestJoin("{!join");
  }

  @Test
  public void testScoreJoin() throws Exception {
    doTestJoin("{!join " + TestScoreJoinQPNoScore.whateverScore());
  }

  void doTestJoin(String joinPrefix) throws Exception {
    assertJQ(
        req(
            "q",
            joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev",
            "fl",
            "id",
            "debugQuery",
            random().nextBoolean() ? "true" : "false"),
        "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'},{'id':'5'}]}");

    assertJQ(
        req(
            "qt",
            "/export",
            "q",
            joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev",
            "fl",
            "id_s_dv",
            "sort",
            "id_s_dv asc",
            "debugQuery",
            random().nextBoolean() ? "true" : "false"),
        "/response=={'numFound':3,'docs':[{'id_s_dv':'1'},{'id_s_dv':'4'},{'id_s_dv':'5'}]}");
    assertFalse(fromCore.isClosed());
    assertFalse(h.getCore().isClosed());

    // find people that develop stuff - but limit via filter query to a name of "john",
    // this tests filters being pushed down to queries (SOLR-3062)
    assertJQ(
        req(
            "q",
            joinPrefix + " from=dept_id_s to=dept_s fromIndex=fromCore}cat:dev",
            "fl",
            "id",
            "fq",
            "name:john",
            "debugQuery",
            random().nextBoolean() ? "true" : "false"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1'}]}");
  }

  @Test
  public void testCoresAreDifferent() throws Exception {
    assertQEx("schema12.xml" + " has no \"cat\" field", req("cat:*"), ErrorCode.BAD_REQUEST);
    try (var req =
        new LocalSolrQueryRequest(fromCore, "cat:*", "/select", 0, 100, Collections.emptyMap())) {
      final String resp = query(fromCore, req);
      assertTrue(resp, resp.contains("numFound=\"1\""));
      assertTrue(resp, resp.contains("<str name=\"id\">10</str>"));
    }
  }

  public String query(SolrCore core, SolrQueryRequest req) throws Exception {
    String handler = "standard";
    if (req.getParams().get("qt") != null) {
      handler = req.getParams().get("qt");
    }
    if (req.getParams().get("wt") == null) {
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.set("wt", "xml");
      req.setParams(params);
    }
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
    try {
      core.execute(core.getRequestHandler(handler), req, rsp);
      if (rsp.getException() != null) {
        throw rsp.getException();
      }
      return req.getResponseWriter().writeToString(req, rsp);
    } finally {
      SolrRequestInfo.clearRequestInfo();
    }
  }

  @AfterClass
  public static void nukeAll() {
    if (fromServer != null) {
      try {
        fromServer.close();
      } catch (Exception e) {
        // ignore
      }
    }
    fromCore = null;
    fromServer = null;
  }
}
