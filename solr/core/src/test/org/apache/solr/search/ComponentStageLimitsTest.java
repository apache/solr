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
package org.apache.solr.search;

import static org.apache.solr.response.SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_DETAILS_KEY;

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComponentStageLimitsTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "test";

  private static Path createConfigSet() throws Exception {
    Path configSet = createTempDir();
    copyMinConf(configSet.toFile());
    // insert an expensive search component
    Path solrConfig = configSet.resolve("conf/solrconfig.xml");
    Files.writeString(
        solrConfig,
        Files.readString(solrConfig)
            .replace(
                "<requestHandler",
                "<searchComponent name=\"expensiveSearchComponent\"\n"
                    + "                   class=\"org.apache.solr.search.ExpensiveSearchComponent\"/>\n"
                    + "\n"
                    + "  <requestHandler")
            .replace(
                "class=\"solr.SearchHandler\">",
                "class=\"solr.SearchHandler\">\n"
                    + "    <arr name=\"first-components\">\n"
                    + "      <str>expensiveSearchComponent</str>\n"
                    + "    </arr>\n"));
    return configSet.resolve("conf");
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    // Note: copied from TestCpuAllowedLimit.java, Segments not likely important
    // at the moment, but setup retained for now.

    // Using NoMergePolicy and 100 commits we should get 100 segments (across all shards).
    // At this point of writing MAX_SEGMENTS_PER_SLICE in lucene is 5, so we should be
    // ensured that any multithreaded testing will create 20 executable tasks for the
    // executor that was provided to index-searcher.
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());
    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");
    Path configset = createConfigSet();
    configureCluster(1).addConfig("conf", configset).configure();
    SolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 2);
    create.process(solrClient);
    waitForState("active", COLLECTION, clusterShape(3, 6));
    for (int j = 0; j < 100; j++) {
      solrClient.add(COLLECTION, sdoc("id", "id-" + j, "val_i", j % 5));
      solrClient.commit(COLLECTION); // need to commit every doc to create many segments.
    }
  }

  @AfterClass
  public static void tearDownClass() {
    TestInjection.cpuTimerDelayInjectedNS = null;
    systemClearPropertySolrTestsMergePolicyFactory();
  }

  @Test
  public void testLimitPrepare() throws Exception {
    Assume.assumeTrue("Thread CPU time monitoring is not available", ThreadCpuTimer.isSupported());
    SolrClient solrClient = cluster.getSolrClient();
    long sleepMs = 1000;
    log.info("--- 500ms limit, 1000ms  prepare phase - partial results ---");
    QueryResponse rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                String.valueOf(sleepMs),
                "stages",
                "prepare",
                "timeAllowed",
                "500"));
    System.err.println("rsp=" + rsp.jsonStr());
    assertEquals(rsp.getHeader().get("status"), 0);
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));
    String details = (String) rsp.getHeader().get(RESPONSE_HEADER_PARTIAL_RESULTS_DETAILS_KEY);
    assertNotNull(details);
    assertTrue(details.contains("[prepare]"));
    assertTrue(
        details.contains("exceeded prior to query in [expensiveSearchComponent, query, facet,"));
  }

  @Test
  public void testLimitProcess() throws Exception {
    Assume.assumeTrue("Thread CPU time monitoring is not available", ThreadCpuTimer.isSupported());
    SolrClient solrClient = cluster.getSolrClient();
    String msg = "--- 500ms limit, 1000ms process phase - partial results ---";
    log.info(msg);
    System.out.println(msg);
    int sleepMs = 2000;
    QueryResponse rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                String.valueOf(sleepMs),
                "stages",
                "process",
                "timeAllowed",
                "1000"));
    System.err.println("rsp=" + rsp.jsonStr());
    assertEquals(rsp.getHeader().get("status"), 0);
    String details = (String) rsp.getHeader().get(RESPONSE_HEADER_PARTIAL_RESULTS_DETAILS_KEY);
    System.out.println("details=" + details);
    assertNotNull(details);
    assertTrue(details.contains("[process]"));
    assertTrue(
        details.contains("exceeded prior to query in [expensiveSearchComponent, query, facet,"));
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));
  }

  @Test
  public void testLimitFinish() throws Exception {
    Assume.assumeTrue("Thread CPU time monitoring is not available", ThreadCpuTimer.isSupported());
    SolrClient solrClient = cluster.getSolrClient();
    long sleepMs = 1000;
    log.info("--- 500ms limit, 1000ms finish phase - partial results ---");
    sleepMs = 1000;
    QueryResponse rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                String.valueOf(sleepMs),
                "stages",
                "finish",
                "timeAllowed",
                "500"));
    System.err.println("rsp=" + rsp.jsonStr());
    assertEquals(rsp.getHeader().get("status"), 0);
    String details = (String) rsp.getHeader().get(RESPONSE_HEADER_PARTIAL_RESULTS_DETAILS_KEY);
    assertNotNull(details);
    assertTrue(details.contains("[finishStage stage:PARSE_QUERY]"));
    assertTrue(
        details.contains("exceeded prior to query in [expensiveSearchComponent, query, facet,"));
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));
  }

  @Test
  public void testLimitDistrib() throws Exception {
    Assume.assumeTrue("Thread CPU time monitoring is not available", ThreadCpuTimer.isSupported());
    SolrClient solrClient = cluster.getSolrClient();
    long sleepMs = 1000;
    log.info("--- 500ms limit, 1000ms distrib phase - partial results ---");
    sleepMs = 1000;
    QueryResponse rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                String.valueOf(sleepMs),
                "stages",
                "distrib",
                "timeAllowed",
                "500"));
    System.err.println("rsp=" + rsp.jsonStr());
    assertEquals(rsp.getHeader().get("status"), 0);
    String details = (String) rsp.getHeader().get(RESPONSE_HEADER_PARTIAL_RESULTS_DETAILS_KEY);
    assertNotNull(details);
    assertTrue(details.contains("[distrib]"));
    assertTrue(
        details.contains("exceeded prior to query in [expensiveSearchComponent, query, facet,"));
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));
  }
}
