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

import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class TestCpuTimeLimit extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testCompareToWallClock() throws Exception {
    long limitMs = 100;
    CpuTimeLimit cpuLimit = new CpuTimeLimit(limitMs);
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
    log.info("CPU limit: {} ms, elapsed wall-clock: {} ms, wakeups: {}", limitMs, wallTimeDeltaMs, wakeups);
    assertTrue("Elapsed wall-clock time expected much larger than 100ms but was " +
        wallTimeDeltaMs, limitMs < wallTimeDeltaMs);
  }

  @Test
  public void testDistribLimit() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(2).addConfig("conf", configset("query-limits")).configure();
    String COLLECTION = "test";
    try {
      SolrClient solrClient = cluster.getSolrClient();
      CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1);
      create.process(solrClient);
      CloudUtil.waitForState(cluster.getOpenOverseer().getSolrCloudManager(), "active", COLLECTION, clusterShape(2, 2));

      // add some docs
      for (int i = 0; i < 10; i++) {
        solrClient.add(COLLECTION, sdoc("id", "id-" + i));
      }
      solrClient.commit(COLLECTION);

      // no limits set - should eventually complete
      long sleepMs = 1000;
      QueryResponse rsp = solrClient.query(COLLECTION, params("q", "*:*", "sleepMs", String.valueOf(sleepMs)));
      System.err.println("rsp=" + rsp.jsonStr());
      assertEquals(rsp.getHeader().get("status"), 0);
      Number qtime = (Number) rsp.getHeader().get("QTime");
      assertTrue("QTime  expected " + qtime + " >> " + sleepMs, qtime.longValue() > sleepMs);
      assertNull("should not have partial results", rsp.getHeader().get("partialResults"));

      // timeAllowed set, should return partial results
      rsp = solrClient.query(COLLECTION, params("q", "*:*", "sleepMs", String.valueOf(sleepMs), "timeAllowed", "500"));
      System.err.println("rsp=" + rsp.jsonStr());
      assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));

      // cpuAllowed set, should return partial results
      rsp = solrClient.query(COLLECTION, params("q", "*:*", "spinWaitCount", "10000", "cpuAllowed", "20"));
      System.err.println("rsp=" + rsp.jsonStr());
      assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));
    } finally {
      cluster.shutdown();
    }
  }
}
