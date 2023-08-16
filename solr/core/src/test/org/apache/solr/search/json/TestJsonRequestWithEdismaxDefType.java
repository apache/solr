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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.util.SolrClientTestRule;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;


public class TestJsonRequestWithEdismaxDefType extends SolrTestCaseHS {

  @ClassRule
  public static final SolrClientTestRule solrClientTestRule = new EmbeddedSolrServerTestRule();

  public void test() throws Exception {

    final var configSet = LuceneTestCase.createTempDir();
    copyMinConf(configSet.toFile());

    solrClientTestRule.startSolr();
    solrClientTestRule
        .newCollection()
        .withConfigSet(configSet.toString())
        .create();

    final var client = solrClientTestRule.getSolrClient();

    client.request(new ConfigRequest("{"
        + "  'add-requesthandler':{"
        + "    'name':'/query',"
        + "    'class':'solr.SearchHandler',"
        + "    'defaults' : {'defType':'edismax'}"
        + "  }"
        + "}"));

    addDocs(client);

    doQuery(client);
  }
  
  private static void addDocs(SolrClient client) throws Exception {
    //client.deleteByQuery("*:*");
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY"));
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ"));
    client.add(sdoc("id", "3"));
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", "where_s", "NJ"));
    client.add(sdoc("id", "5", "cat_s", "B", "where_s", "NJ"));
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", "where_s", "NY"));
    client.commit();
  }
  
  private static void doQuery(SolrClient client) throws Exception {
    final var jsonQuery =
        "{\"query\":{\"bool\":{\"should\":[{\"lucene\":{\"query\":\"id:1\"}}, \"id:2\"]}}, \"params\":{\"debug\":\"true\"}}";
    SolrTestCaseHS.assertJQ(client, params("json", jsonQuery, "qt", "/query"), "response/numFound==2");
  }

}
