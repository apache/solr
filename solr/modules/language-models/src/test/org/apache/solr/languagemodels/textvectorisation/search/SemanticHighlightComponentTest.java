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
package org.apache.solr.languagemodels.textvectorisation.search;

import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.languagemodels.TestLanguageModelBase;
import org.apache.solr.languagemodels.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SemanticHighlightComponentTest extends TestLanguageModelBase {
  @BeforeClass
  public static void init() throws Exception {
    setupTest("solrconfig-language-models.xml", "schema-language-models.xml", false, false);
    assertU(
        adoc(
            sdoc(
                "id",
                "3",
                "text_field",
                "... the queen bee, the hardest working bee, or the bee that does not fit in.")));
    // partial quote of The Three Bees by Suzy Kassem
    assertU(commit());
    loadModel("custom-model.json");
  }

  @AfterClass
  public static void cleanup() throws Exception {
    afterTest();
  }

  @After
  public void afterEachTest() throws Exception {
    restTestHarness.delete(ManagedTextToVectorModelStore.REST_END_POINT + "/custom-1");
  }

  @Test
  public void test() throws Exception {
    // not a realistic query but chosen just-so to help extract passages for illustrative use below
    final String q =
        "text_field:(\"the queen bee\" OR \"the hardest working bee\" OR \"the bee that\")";
    final SolrQuery query = new SolrQuery();
    query.setQuery(q);
    query.add("hl", "true");
    query.add("hl.fl", "text_field");
    query.add("hl.bs.type", "WORD");
    query.add("hl.fragsize", "8");
    query.add("hl.snippets", "3");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==1]",
        "/highlighting/3/text_field==['<em>the queen bee</em>','<em>the hardest working bee</em>','<em>the bee that</em>']");

    query.add("hl.method", "unified_with_semantic");
    query.add("hl.unified_with_semantic.model", "custom-1");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==1]",
        "/highlighting/3/text_field==['<em>the bee that</em>','<em>the hardest working bee</em>','<em>the queen bee</em>']");

    query.set("hl.unified_with_semantic.vector", "0.1,0.2,0.3,0.4,0.5");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==1]",
        "/highlighting/3/text_field==['<em>the queen bee</em>','<em>the bee that</em>','<em>the hardest working bee</em>']");

    query.set("hl.unified_with_semantic.vector", "0.5,0.6,0.7,0.8,0.9");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==1]",
        "/highlighting/3/text_field==['<em>the hardest working bee</em>','<em>the bee that</em>','<em>the queen bee</em>']");

    query.set("hl.unified_with_semantic.vector", "0.3,0.4,0.0,0.6,0.7");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==1]",
        "/highlighting/3/text_field==['<em>the bee that</em>','<em>the queen bee</em>','<em>the hardest working bee</em>']");
  }
}
