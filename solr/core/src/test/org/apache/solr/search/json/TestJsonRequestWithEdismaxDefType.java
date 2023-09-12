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
package org.apache.solr.search.json;

import java.nio.file.Path;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.ClassRule;

public class TestJsonRequestWithEdismaxDefType extends SolrTestCaseJ4 {

  @ClassRule
  public static final SolrClientTestRule solrClientTestRule = new EmbeddedSolrServerTestRule();

  public void test() throws Exception {
    solrClientTestRule.startSolr(LuceneTestCase.createTempDir());

    Path configSet = LuceneTestCase.createTempDir();
    SolrTestCaseJ4.copyMinConf(configSet.toFile());

    solrClientTestRule.newCollection().withConfigSet(configSet.toString()).create();

    SolrClient client = solrClientTestRule.getSolrClient();

    client.request(
        new ConfigRequest(
            "{"
                + "  'create-requesthandler':{"
                + "    'name':'/query',"
                + "    'class':'solr.SearchHandler',"
                + "    'defaults' : {'defType':'edismax'}" // the critical part
                + "  }"
                + "}"));

    addDocs(client);

    doQuery(client);
  }

  private static void addDocs(SolrClient client) throws Exception {
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY"));
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ"));
    client.add(sdoc("id", "3"));
    client.commit();
  }

  private static void doQuery(SolrClient client) throws Exception {
    final var jsonQuery =
        "{\"query\":{\"bool\":{\"should\":[{\"lucene\":{\"query\":\"id:1\"}}, \"id:2\"]}}}";
    final var req = new QueryRequest(params("json", jsonQuery, "qt", "/query"), METHOD.POST);
    final var rsp = req.process(client);
    assertEquals(2, rsp.getResults().getNumFound());
  }
}
