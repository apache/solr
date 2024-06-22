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

import java.util.List;
import org.apache.solr.EmbeddedSolrServerTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.TermsResponse.Term;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for TermComponent's response in Solrj */
public class TermsResponseTest extends EmbeddedSolrServerTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrClientTestRule.startSolr();

    solrClientTestRule.newCollection().withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET).create();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    solrClientTestRule.clearIndex();
  }

  @Test
  public void testTermsResponse() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", 1);
    doc.setField("terms_s", "samsung");
    getSolrClient().add(doc);
    getSolrClient().commit(true, true);

    SolrQuery query = new SolrQuery();
    query.setRequestHandler("/terms");
    query.setTerms(true);
    query.setTermsLimit(5);
    query.setTermsLower("s");
    query.setTermsPrefix("s");
    query.addTermsField("terms_s");
    query.setTermsMinCount(1);

    QueryRequest request = new QueryRequest(query);
    List<Term> terms = request.process(getSolrClient()).getTermsResponse().getTerms("terms_s");

    assertNotNull(terms);
    assertEquals(terms.size(), 1);

    Term term = terms.get(0);
    assertEquals(term.getTerm(), "samsung");
    assertEquals(term.getFrequency(), 1);
  }
}
