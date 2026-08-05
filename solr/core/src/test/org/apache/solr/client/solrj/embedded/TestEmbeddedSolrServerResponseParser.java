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
package org.apache.solr.client.solrj.embedded;

import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * EmbeddedSolrServer reads the response with the configured parser just as the HTTP clients do, so
 * a non-binary parser has to work here too.
 */
public class TestEmbeddedSolrServerResponseParser extends SolrTestCase {

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(SolrTestCaseJ4.TEST_HOME());
    SolrTestCaseJ4.newRandomConfig();
    solrTestRule
        .newCollection()
        .withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF())
        .withSchemaFile("schema-nest.xml")
        .create();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    doc.addField("name_s", "embedded json");
    SolrClient client = solrTestRule.getSolrClient();
    client.add(doc);
    client.commit();
  }

  @Test
  public void testQueryResponseWithJsonParser() throws Exception {
    SolrQuery q = new SolrQuery("id:1");
    q.addFacetField("name_s");
    QueryRequest req = new QueryRequest(q);
    req.setResponseParser(new JsonMapResponseParser());

    QueryResponse rsp = req.process(solrTestRule.getSolrClient());

    // Header getters cast the values they read, and the JSON writer emits Long where javabin emits
    // Integer.
    assertEquals(0, rsp.getStatus());
    assertNotNull(rsp.getResponseHeader());

    // A facet section is a NamedList; under the default json.nl=flat it arrives as an array of
    // alternating names and values, which cannot be recovered.
    assertNotNull("facet_counts must be readable", rsp.getFacetField("name_s"));

    // The documents section has to arrive as a SolrDocumentList for getResults() to work at all.
    assertEquals(1, rsp.getResults().getNumFound());
    assertEquals("1", rsp.getResults().get(0).getFirstValue("id"));
  }

  /**
   * A named nested document has to come back as a document rather than a plain map, matching what
   * the binary and XML parsers produce for the same response.
   */
  @Test
  public void testNamedNestedDocumentsWithJsonParser() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();

    SolrInputDocument child = new SolrInputDocument();
    child.addField("id", "20");
    child.addField("name_s", "a comment");

    SolrInputDocument parent = new SolrInputDocument();
    parent.addField("id", "10");
    parent.addField("name_s", "a parent");
    parent.addField("comment", child);

    client.add(parent);
    client.commit();

    SolrQuery q = new SolrQuery("id:10");
    q.setFields("*", "[child]");
    QueryRequest req = new QueryRequest(q);
    req.setResponseParser(new JsonMapResponseParser());

    QueryResponse rsp = req.process(client);

    SolrDocument doc = rsp.getResults().get(0);
    Object comment = doc.getFieldValue("comment");
    assertNotNull("the named child must be present", comment);
    assertTrue(
        "a named child must be a SolrDocument, not " + comment.getClass().getName(),
        comment instanceof SolrDocument);
    assertEquals("a comment", ((SolrDocument) comment).getFirstValue("name_s"));
  }
}
