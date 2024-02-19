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

import com.codahale.metrics.Histogram;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMemQueryLimit extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testHardLimit() throws Exception {
    Assume.assumeTrue("Thread memory monitoring is not available", MemQueryLimit.isAvailable());
    long limitMs = 100;
    // 1 MiB
    MemQueryLimit memLimit = new MemQueryLimit(1f, -1f, MemQueryLimit.createHistogram());
    ArrayList<byte[]> data = new ArrayList<>();
    long startNs = System.nanoTime();
    int wakeups = 0;
    while (!memLimit.shouldExit()) {
      Thread.sleep(1);
      // allocate memory
      for (int i = 0; i < 20; i++) {
        data.add(new byte[1000]);
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
  public void testDynamicLimit() throws Exception {
    Assume.assumeTrue("Thread memory monitoring is not available", MemQueryLimit.isAvailable());
    long limitMs = 100;
    Histogram histogram = MemQueryLimit.createHistogram();
    ArrayList<byte[]> data = new ArrayList<>();
    MemQueryLimit memLimit = new MemQueryLimit(-1f, 1.1f, histogram);

    // initial warmup
    int dataSize = 10240; // 10 KiB
    // may have been updated in init() from a leftover thread local value
    int count = MemQueryLimit.MIN_COUNT - (int) histogram.getCount();
    for (int i = 0; i < count; i++) {
      histogram.update(random().nextInt(dataSize));
    }
    // make allocation larger than p99 - but not enough datapoints yet
    data.add(new byte[3 * dataSize]);
    if (log.isInfoEnabled()) {
      log.info("Data size: {}", RamUsageEstimator.sizeOfObject(data));
    }
    assertFalse("not enough datapoints yet", memLimit.shouldExit());

    // one additional point to enable dynamic limit
    histogram.update(dataSize);
    assertTrue("enough datapoints reached", memLimit.shouldExit());
  }

  @Test
  public void testDistribLimit() throws Exception {
    Assume.assumeTrue("Thread memory monitoring is not available", MemQueryLimit.isAvailable());
    MiniSolrCloudCluster cluster =
        configureCluster(1).addConfig("conf", configset("query-limits")).configure();
    String COLLECTION = "test";
    try {
      SolrClient solrClient = cluster.getSolrClient();
      CollectionAdminRequest.Create create =
          CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
      create.process(solrClient);
      CloudUtil.waitForState(
          cluster.getOpenOverseer().getSolrCloudManager(),
          "active",
          COLLECTION,
          clusterShape(1, 1));

      // add some docs
      for (int i = 0; i < 10; i++) {
        solrClient.add(COLLECTION, sdoc("id", "id-" + i));
      }
      solrClient.commit(COLLECTION);

      // no limits set - should complete
      long dataSize = 1000;
      QueryResponse rsp =
          solrClient.query(
              COLLECTION,
              params("q", "id:*", "sort", "id desc", "dataSize", String.valueOf(dataSize)));
      assertEquals(rsp.getHeader().get("status"), 0);
      assertNull("should not have partial results", rsp.getHeader().get("partialResults"));

      // memAllowed set, should return partial results
      rsp =
          solrClient.query(
              COLLECTION,
              params(
                  "q",
                  "id:*",
                  "sort",
                  "id asc",
                  "dataSize",
                  String.valueOf(dataSize),
                  "memAllowed",
                  "0.1"));
      assertNotNull("should have partial results", rsp.getHeader().get("partialResults"));
    } finally {
      cluster.shutdown();
    }
  }
}
