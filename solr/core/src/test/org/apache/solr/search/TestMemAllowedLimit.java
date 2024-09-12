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

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel("org.apache.solr.search.MemAllowedLimit=DEBUG")
public class TestMemAllowedLimit extends SolrCloudTestCase {
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
  public static void setup() throws Exception {
    // Using NoMergePolicy and 100 commits we should get 100 segments (across all shards).
    // At this point of writing MAX_SEGMENTS_PER_SLICE in lucene is 5, so we should be
    // ensured that any multithreaded testing will create 20 executable tasks for the
    // executor that was provided to index-searcher.
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());
    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");
    System.setProperty("metricsEnabled", "true");
    Path configset = createConfigSet();
    configureCluster(1).addConfig("conf", configset).configure();
    SolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 2);
    create.process(solrClient);
    CloudUtil.waitForState(
        cluster.getOpenOverseer().getSolrCloudManager(), "active", COLLECTION, clusterShape(3, 6));
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
  public void testLimit() throws Exception {
    Assume.assumeTrue("Thread memory monitoring is not available", MemAllowedLimit.isSupported());
    long limitMs = 100000;
    // 1 MiB
    MemAllowedLimit memLimit = new MemAllowedLimit(1f);
    ArrayList<byte[]> data = new ArrayList<>();
    long startNs = System.nanoTime();
    int wakeups = 0;
    while (!memLimit.shouldExit()) {
      Thread.sleep(100);
      // allocate memory
      for (int i = 0; i < 20; i++) {
        data.add(new byte[5000]);
      }
      wakeups++;
    }
    long endNs = System.nanoTime();
    assertTrue(data.size() > 1);
    long wallTimeDeltaMs = TimeUnit.MILLISECONDS.convert(endNs - startNs, TimeUnit.NANOSECONDS);
    log.info(
        "Time limit: {} ms, elapsed wall-clock: {} ms, wakeups: {}",
        limitMs,
        wallTimeDeltaMs,
        wakeups);
    assertTrue("Number of wakeups should be smaller than 100 but was " + wakeups, wakeups < 100);
    assertTrue(
        "Elapsed wall-clock time expected much smaller than 100ms but was " + wallTimeDeltaMs,
        limitMs > wallTimeDeltaMs);
  }

  @Test
  public void testDistribLimit() throws Exception {
    Assume.assumeTrue("Thread memory monitoring is not available", MemAllowedLimit.isSupported());
    SolrClient solrClient = cluster.getSolrClient();
    // no limits set - should complete
    long dataSize = 150; // 150 KiB
    QueryResponse rsp =
        solrClient.query(
            COLLECTION,
            params("q", "id:*", "sort", "id desc", "dataSize", String.valueOf(dataSize)));
    assertEquals(rsp.getHeader().get("status"), 0);
    assertNull("should not have partial results", rsp.getHeader().get("partialResults"));

    // memAllowed set with large value, should return full results
    rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                "memLoadCount",
                String.valueOf(dataSize),
                "stages",
                "prepare,process",
                "memAllowed",
                "1.5"));
    assertNull("should have full results", rsp.getHeader().get("partialResults"));

    // memAllowed set, should return partial results
    rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                "memLoadCount",
                String.valueOf(dataSize),
                "stages",
                "prepare,process",
                "memAllowed",
                "0.2"));
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));

    // multi-threaded search
    // memAllowed set, should return partial results
    rsp =
        solrClient.query(
            COLLECTION,
            params(
                "q",
                "id:*",
                "sort",
                "id asc",
                "memLoadCount",
                String.valueOf(dataSize),
                "stages",
                "prepare,process",
                "multiThreaded",
                "true",
                "memAllowed",
                "0.2"));
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));

  }
}
