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
package org.apache.solr.client.solrj.response;

import static org.apache.solr.SolrTestCaseJ4.sdoc;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * End-to-end: a real HTTP query with the JSON map response parser must return a fully typed
 * QueryResponse, proving SolrRequest.process() normalizes the non-canonical JSON response at the
 * boundary before the response classes read it (SOLR-17316).
 */
public class QueryResponseJsonParserIntegrationTest extends SolrTestCase {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.security.allow.paths", "*");
    solrTestRule.startSolr();
    solrTestRule.newCollection().withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET).create();

    try (SolrClient client = solrTestRule.getSolrClient()) {
      client.add(
          java.util.List.of(
              sdoc("id", "1", "cat", "electronics"),
              sdoc("id", "2", "cat", "electronics"),
              sdoc("id", "3", "cat", "books")));
      client.commit();
    }
  }

  /** The default (Jetty) transport. */
  @Test
  public void testTypedQueryResponseOverJsonJetty() throws Exception {
    try (SolrClient client =
        solrTestRule
            .newSolrClientBuilder()
            .withResponseParser(new JsonMapResponseParser())
            .build()) {
      assertTypedResponse(client);
    }
  }

  /** The JDK transport shares the same response boundary, so it must behave identically. */
  @Test
  public void testTypedQueryResponseOverJsonJdk() throws Exception {
    try (SolrClient client =
        new org.apache.solr.client.solrj.impl.HttpJdkSolrClient.Builder(solrTestRule.getBaseUrl())
            .withResponseParser(new JsonMapResponseParser())
            .build()) {
      assertTypedResponse(client);
    }
  }

  private void assertTypedResponse(SolrClient client) throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    q.setRows(10);
    q.addFacetField("cat");
    q.setParam("json.nl", "map"); // the round-trippable NamedList style

    QueryResponse rsp = client.query("collection1", q);

    assertEquals(0, rsp.getStatus());
    assertEquals(3, rsp.getResults().getNumFound());
    assertNotNull(rsp.getResults().get(0).getFirstValue("id"));

    FacetField cat = rsp.getFacetField("cat");
    assertNotNull("facet field cat", cat);
    assertEquals(2, cat.getValueCount());
  }
}
