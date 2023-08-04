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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({
  "Lucene3x",
  "Lucene40",
  "Lucene41",
  "Lucene42",
  "Lucene45",
  "Appending"
})
public class TestJsonRequestWithEdismaxDefType extends SolrTestCaseHS {

  private static SolrInstances servers; // for distributed testing

  @SuppressWarnings("deprecation")
  @BeforeClass
  public static void beforeTests() throws Exception {
    systemSetPropertySolrDisableUrlAllowList("true");
    System.setProperty("solr.enableStreamBody", "true");
    JSONTestUtil.failRepeatedKeys = true;
    initCore("solrconfig-tlog-edismax.xml", "schema_latest.xml");
  }

  @SuppressWarnings("deprecation")
  @AfterClass
  public static void afterTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = false;
    systemClearPropertySolrDisableUrlAllowList();
  }

  @Test
  public void testLocalJsonRequest() throws Exception {
    doJsonRequest(Client.localClient);
  }

  public static void doJsonRequest(Client client) throws Exception {
    addDocs(client);

    client.testJQ(
        params(
            "json",
            "{\"query\":{\"bool\":{\"should\":[{\"lucene\":{\"query\":\"id:1\"}}, \"id:2\"]}}, \"params\":{\"debug\":\"true\"}}"),
        "response/numFound==2");
  }

  private static void addDocs(Client client) throws Exception {
    client.deleteByQuery("*:*", null);
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY"), null);
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ"), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", "where_s", "NJ"), null);
    client.add(sdoc("id", "5", "cat_s", "B", "where_s", "NJ"), null);
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", "where_s", "NY"), null);
    client.commit();
  }
}
