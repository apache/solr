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
package org.apache.solr.handler.export;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.json.CanonicalJsonResponseParser;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.index.LogDocMergePolicyFactory;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * SOLR-8291: {@code useFilterForSortedQuery=true} sends {@code /export} through {@code sortDocSet},
 * which never calls {@code ExportCollector.getLeafCollector} for trailing unmatched leaves. The
 * writer must treat those null bitsets as empty leaves instead of constructing a {@code
 * BitSetIterator}.
 */
public class TestExportWriterUseFilterForSortedQuery extends SolrTestCase {

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.tests.mergePolicyFactory", LogDocMergePolicyFactory.class.getName());
    solrTestRule.startSolr();
    solrTestRule
        .newCollection()
        .withConfigSet(
            ExternalPaths.SOURCE_HOME.resolve("core/src/test-files/solr/collection1/conf"))
        .withConfigFile("solrconfig-export-usefilter.xml")
        .withSchemaFile("schema-export-usefilter.xml")
        .create();
  }

  @Before
  public void clearIndex() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.deleteByQuery("*:*");
    client.commit();
  }

  @Test
  public void testZeroHitsDoesNotNpe() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.add(doc("1"));
    client.commit();

    QueryResponse response = query(client, "id:does-not-exist");
    assertEquals(0, response.getResults().getNumFound());
  }

  @Test
  public void testHitsOnlyInEarlierSegment() throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.add(doc("1"));
    client.commit();
    client.add(doc("2"));
    client.commit();

    QueryResponse response = query(client, "id:1");
    assertEquals(1, response.getResults().getNumFound());
    assertEquals("1", response.getResults().get(0).getFieldValue("id"));

    // Null must mean "this leaf is empty", not "skip the rest of the index".
    response = query(client, "*:*");
    assertEquals(2, response.getResults().getNumFound());
    assertEquals("1", response.getResults().get(0).getFieldValue("id"));
    assertEquals("2", response.getResults().get(1).getFieldValue("id"));
  }

  private static SolrQuery exportQuery(String queryString) {
    SolrQuery query = new SolrQuery(queryString);
    query.setRequestHandler("/export");
    query.setFields("id");
    query.setSort("id", SolrQuery.ORDER.asc);
    return query;
  }

  private static QueryResponse query(SolrClient client, String queryString) throws Exception {
    QueryRequest request = new QueryRequest(exportQuery(queryString));
    request.setResponseParser(new CanonicalJsonResponseParser());
    return request.process(client);
  }

  private static SolrInputDocument doc(String id) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    return doc;
  }
}
