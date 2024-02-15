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
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.security.AllowListUrlChecker;
import org.apache.solr.util.SolrJettyTestRule;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCpuAllowedLimit extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @ClassRule public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

  private static int NUM_CORES = 3;
  private static String[] CORE_NAMES;
  private static String[] SHARD_URLS;

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
    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");
    System.setProperty(AllowListUrlChecker.DISABLE_URL_ALLOW_LIST, "true");
    solrRule.startSolr(createTempDir());
    Path configset = createConfigSet();
    String baseShardUrl = solrRule.getBaseUrl().replaceAll("https?://", "");
    CORE_NAMES = new String[NUM_CORES];
    SHARD_URLS = new String[NUM_CORES];
    for (int i = 0; i < NUM_CORES; i++) {
      String core = "core" + (i + 1);
      CORE_NAMES[i] = core;
      SHARD_URLS[i] = baseShardUrl + "/" + core;
      solrRule.newCollection(core).withConfigSet(configset.toString()).create();
      SolrClient solrClient = solrRule.getSolrClient();
      // add some docs
      for (int j = 0; j < 10; j++) {
        solrClient.add(core, sdoc("id", "id-" + core + "-" + j));
      }
      solrClient.commit(core);
    }
  }

  private static SolrQuery createQuery(boolean distrib, String... params) {
    SolrQuery query = new SolrQuery();
    query.addField("id");
    if (distrib) {
      query.set("distrib", "true");
      query.set("shards", String.join(",", SHARD_URLS));
      query.set(ShardParams.SHARDS_INFO, "true");
      query.set("debug", "true");
      query.set("stats", "true");
      query.set("stats.field", "id");
      query.set(ShardParams.SHARDS_TOLERANT, "true");
    }
    if (params != null) {
      for (int i = 0; i < params.length; i += 2) {
        query.set(params[i], params[i + 1]);
      }
    }
    return query;
  }

  @Test
  public void testCompareToWallClock() throws Exception {
    Assume.assumeTrue("Thread CPU time monitoring is not available", ThreadCpuTimer.isSupported());
    long limitMs = 100;
    CpuAllowedLimit cpuLimit = new CpuAllowedLimit(limitMs);
    int[] randoms = new int[100];
    long startNs = System.nanoTime();
    int wakeups = 0;
    while (!cpuLimit.shouldExit()) {
      Thread.sleep(1);
      // do some busywork
      for (int i = 0; i < randoms.length; i++) {
        randoms[i] = random().nextInt();
      }
      wakeups++;
    }
    long endNs = System.nanoTime();
    long wallTimeDeltaMs = TimeUnit.MILLISECONDS.convert(endNs - startNs, TimeUnit.NANOSECONDS);
    log.info(
        "CPU limit: {} ms, elapsed wall-clock: {} ms, wakeups: {}",
        limitMs,
        wallTimeDeltaMs,
        wakeups);
    assertTrue(
        "Elapsed wall-clock time expected much larger than 100ms but was " + wallTimeDeltaMs,
        limitMs < wallTimeDeltaMs);
  }

  @Test
  public void testDistribLimit() throws Exception {
    Assume.assumeTrue("Thread CPU time monitoring is not available", ThreadCpuTimer.isSupported());

    SolrClient solrClient = solrRule.getSolrClient(CORE_NAMES[0]);

    // no limits set - should eventually complete
    long sleepMs = 1000;
    QueryResponse rsp =
        solrClient.query(
            createQuery(
                true,
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                String.valueOf(sleepMs)));
    // System.err.println("rsp=" + rsp.jsonStr());
    assertEquals(rsp.getHeader().get("status"), 0);
    Number qtime = (Number) rsp.getHeader().get("QTime");
    assertTrue("QTime expected " + qtime + " >> " + sleepMs, qtime.longValue() > sleepMs);
    assertNull("should not have partial results", rsp.getHeader().get("partialResults"));

    // timeAllowed set, should return partial results
    rsp =
        solrClient.query(
            createQuery(
                true,
                "q",
                "id:*",
                "sort",
                "id asc",
                ExpensiveSearchComponent.SLEEP_MS_PARAM,
                String.valueOf(sleepMs),
                "timeAllowed",
                "500"));
    // System.err.println("rsp=" + rsp.jsonStr());
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));

    // cpuAllowed set with large value, should return full results
    rsp =
        solrClient.query(
            createQuery(
                true,
                "q",
                "id:*",
                "sort",
                "id desc",
                ExpensiveSearchComponent.CPU_LOAD_COUNT_PARAM,
                "1",
                "cpuAllowed",
                "1000"));
    // System.err.println("rsp=" + rsp.jsonStr());
    assertNull("should have full results", rsp.getHeader().get("partialResults"));

    // cpuAllowed set, should return partial results
    rsp =
        solrClient.query(
            createQuery(
                true,
                "q",
                "id:*",
                "sort",
                "id desc",
                ExpensiveSearchComponent.CPU_LOAD_COUNT_PARAM,
                "10",
                "cpuAllowed",
                "50"));
    // System.err.println("rsp=" + rsp.jsonStr());
    assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));
  }
}
