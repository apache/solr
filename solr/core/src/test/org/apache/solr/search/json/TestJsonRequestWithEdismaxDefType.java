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
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.util.SolrClientTestRule;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import java.nio.file.Path;


public class TestJsonRequestWithEdismaxDefType extends SolrTestCaseHS {
  private static final String COLLECTION_NAME = "collection1";
  private static final int NUM_TECHPRODUCTS_DOCS = 32;
  private static final int NUM_IN_STOCK = 17;
  private static final int NUM_ELECTRONICS = 12;
  private static final int NUM_CURRENCY = 4;
  private static final int NUM_MEMORY = 3;
  private static final int NUM_CORSAIR = 3;
  private static final int NUM_BELKIN = 2;
  private static final int NUM_CANON = 2;
  @ClassRule
  public static final SolrClientTestRule solrClientTestRule = new EmbeddedSolrServerTestRule();
  
  public void test() throws Exception {
    solrClientTestRule.startSolr(LuceneTestCase.createTempDir());
    

    solrClientTestRule
        .newCollection(COLLECTION_NAME)
        .withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET)
        .create();

    SolrClient client = solrClientTestRule.getSolrClient(COLLECTION_NAME);

    client.request(new ConfigRequest("{"
        + "  'update-requesthandler':{"
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
        "{\"query\":{\"bool\":{\"should\":[{\"lucene\":{\"query\":\"id:1\"}}, \"id:2\"]}}}";
    SolrTestCaseHS.assertJQ(client, params("json", jsonQuery, "qt", "/query"), "response/numFound==2");
  }

  // write an elevation config file to boost some docs
  private void writeElevationConfigFile(File file, String query, String... ids) throws Exception {
    try (PrintWriter out =
        new PrintWriter(Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8))) {
      out.println("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
      out.println("<elevate>");
      out.println("<query text=\"" + query + "\">");
      for (String id : ids) {
        out.println(" <doc id=\"" + id + "\"/>");
      }
      out.println("</query>");
      out.println("</elevate>");
      out.flush();
    }

  }

}
