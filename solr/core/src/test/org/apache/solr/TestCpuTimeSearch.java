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

import com.carrotsearch.randomizedtesting.annotations.Seed;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;

@Seed("861F5769E4B6F696")
public class TestCpuTimeSearch extends SolrTestCaseJ4 {

  @ClassRule public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();
  
  private static String shard1;
  private static String shard2;

  @BeforeClass
  public static void setupSolr() throws Exception {
    System.setProperty(ThreadStats.ENABLE_CPU_TIME, "true");
    System.setProperty(AllowListUrlChecker.DISABLE_URL_ALLOW_LIST, "true");

    Path configSet = createTempDir("configSet");
    copyMinConf(configSet.toFile());
    solrRule.startSolr(LuceneTestCase.createTempDir());

    solrRule.newCollection("core1").withConfigSet(configSet.toString()).create();
    solrRule.newCollection("core2").withConfigSet(configSet.toString()).create();
    var clientCore1 = solrRule.getSolrClient("core1");
    var clientCore2 = solrRule.getSolrClient("core2");

    String urlCore1 = solrRule.getBaseUrl() + "/" + "core1";
    String urlCore2 = solrRule.getBaseUrl() + "/" + "core2";
    shard1 = urlCore1.replaceAll("https?://", "");
    shard2 = urlCore2.replaceAll("https?://", "");

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("subject", "batman");
    doc.setField("title", "foo bar");
    clientCore1.add(doc);
    clientCore1.commit();

    doc.setField("id", "2");
    doc.setField("subject", "superman");
    clientCore2.add(doc);
    clientCore2.commit();
  }

  public void testWithoutDistrib() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("subject:batman OR subject:superman");
    query.addField("id");
    query.addField("subject");

    QueryResponse response = solrRule.getSolrClient("core1").query(query);

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

  @Ignore
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

    QueryResponse response = solrRule.getSolrClient("core1").query(query);

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
