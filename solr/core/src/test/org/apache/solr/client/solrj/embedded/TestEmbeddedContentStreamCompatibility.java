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

import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Checks that legacy embedded updates retain every stream, including unnamed strings. */
@SuppressWarnings("deprecation")
public class TestEmbeddedContentStreamCompatibility extends SolrTestCase {

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(SolrTestCaseJ4.TEST_HOME());
    SolrTestCaseJ4.newRandomConfig();
    solrTestRule.newCollection().withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF()).create();
  }

  @Test
  public void testUnnamedContentStreamsRetainAllDocuments() throws Exception {
    assertAllDocuments(false);
  }

  @Test
  public void testNamedContentStreamsRetainAllDocuments() throws Exception {
    assertAllDocuments(true);
  }

  private void assertAllDocuments(boolean named) throws Exception {
    SolrClient client = solrTestRule.getSolrClient();
    client.deleteByQuery("*:*");
    ContentStreamUpdateRequest request = new ContentStreamUpdateRequest("/update");
    for (String id : List.of("first", "second")) {
      ContentStreamBase stream =
          new ContentStreamBase.StringStream(
              "<add><doc><field name=\"id\">" + id + "</field></doc></add>", "application/xml");
      if (named) {
        stream.setName(id + ".xml");
      } else {
        assertNull(stream.getName());
      }
      request.addContentStream(stream);
    }

    assertEquals(0, request.process(client).getStatus());
    client.commit();
    SolrQuery query = new SolrQuery("*:*");
    query.setFields("id");
    query.setSort("id", SolrQuery.ORDER.asc);
    SolrDocumentList documents = client.query(query).getResults();
    assertEquals("Both content streams must be indexed", 2L, documents.getNumFound());
    assertEquals(
        List.of("first", "second"),
        documents.stream().map(document -> document.getFieldValue("id")).toList());
  }
}
