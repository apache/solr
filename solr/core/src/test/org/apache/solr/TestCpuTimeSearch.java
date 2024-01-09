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
package org.apache.solr;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.security.AllowListUrlChecker;
import org.apache.solr.util.SolrJettyTestRule;
import org.apache.solr.util.ThreadStats;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

public class TestCpuTimeSearch extends SolrTestCaseJ4 {

  @ClassRule public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

  private static SolrClient collection1;
  private static SolrClient collection2;
  private static String shard1;
  private static String shard2;

  @BeforeClass
  public static void setupSolr() throws Exception {
    System.setProperty(ThreadStats.ENABLE_CPU_TIME, "true");
    System.setProperty(AllowListUrlChecker.DISABLE_URL_ALLOW_LIST, "true");

    Path configSet = createTempDir("configSet");
    copyMinConf(configSet.toFile());
    solrRule.startSolr(LuceneTestCase.createTempDir());

    solrRule.newCollection("collection1").withConfigSet(configSet.toString()).create();
    solrRule.newCollection("collection2").withConfigSet(configSet.toString()).create();
    collection1 = solrRule.getSolrClient("collection1");
    collection2 = solrRule.getSolrClient("collection2");

    String urlCollection1 = solrRule.getBaseUrl() + "/" + "collection1";
    String urlCollection2 = solrRule.getBaseUrl() + "/" + "collection2";
    shard1 = urlCollection1.replaceAll("https?://", "");
    shard2 = urlCollection2.replaceAll("https?://", "");

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("subject", "batman");
    doc.setField("title", "foo bar");
    collection1.add(doc);
    collection1.commit();

    doc.setField("id", "2");
    doc.setField("subject", "superman");
    collection2.add(doc);
    collection2.commit();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    if (null != collection1) {
      collection1.close();
      collection1 = null;
    }
    if (null != collection2) {
      collection2.close();
      collection2 = null;
    }

    System.setProperty(ThreadStats.ENABLE_CPU_TIME, "false");
    System.setProperty(AllowListUrlChecker.DISABLE_URL_ALLOW_LIST, "false");
  }

  public void testWithoutDistrib() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("subject:batman OR subject:superman");
    query.addField("id");
    query.addField("subject");

    QueryResponse response = collection1.query(query);

    SolrDocumentList results = response.getResults();
    int size = results.size();
    assertEquals("should have 1 result", 1, size);
    NamedList<Object> header = response.getHeader();
    List<Object> localCpuTimes = header.getAll(ThreadStats.LOCAL_CPU_TIME);
    List<Object> cpuTimes = header.getAll(ThreadStats.CPU_TIME);
    assertEquals("localCpuTime should not have values", 0, localCpuTimes.size());
    assertEquals("cpuTime should only have one value", 1, cpuTimes.size());
    long cpuTime = (long) cpuTimes.iterator().next();
    assertTrue("cpuTime (" + cpuTime + ") should be positive", cpuTime >= 0);
  }

  public void testWithDistrib() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("subject:batman OR subject:superman");
    query.addField("id");
    query.addField("subject");
    query.set("distrib", "true");
    query.set("shards", shard1 + "," + shard2);
    query.set(ShardParams.SHARDS_INFO, "true");
    query.set("debug", "true");
    query.set("stats", "true");
    query.set("stats.field", "id");
    query.set(ShardParams.SHARDS_TOLERANT, "true");

    QueryResponse response = collection1.query(query);

    SolrDocumentList results = response.getResults();
    int size = results.size();
    assertEquals("should have 2 results", 2, size);
    NamedList<Object> header = response.getHeader();
    List<Object> localCpuTimes = header.getAll(ThreadStats.LOCAL_CPU_TIME);
    List<Object> cpuTimes = header.getAll(ThreadStats.CPU_TIME);
    assertEquals("localCpuTime should not have values", 0, localCpuTimes.size());
    assertEquals("cpuTime should only have one value", 1, cpuTimes.size());
    long cpuTime = (long) cpuTimes.iterator().next();
    assertTrue("cpuTime (" + cpuTime + ") should be positive", cpuTime >= 0);
  }
}
