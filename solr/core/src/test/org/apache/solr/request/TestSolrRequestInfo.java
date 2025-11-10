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
package org.apache.solr.request;

import static org.apache.solr.request.SolrRequestInfo.LIMITS_KEY;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QueryLimits;
import org.junit.BeforeClass;

public class TestSolrRequestInfo extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema11.xml");
  }

  public void testCloseHookTwice() {
    final SolrRequestInfo info =
        new SolrRequestInfo(
            new LocalSolrQueryRequest(h.getCore(), params()), new SolrQueryResponse());
    AtomicInteger counter = new AtomicInteger();
    info.addCloseHook(counter::incrementAndGet);
    SolrRequestInfo.setRequestInfo(info);
    SolrRequestInfo.setRequestInfo(info);
    SolrRequestInfo.clearRequestInfo();
    assertNotNull(SolrRequestInfo.getRequestInfo());
    SolrRequestInfo.clearRequestInfo();
    assertEquals("hook should be closed only once", 1, counter.get());
    assertNull(SolrRequestInfo.getRequestInfo());
  }

  public void testThreadPool() throws InterruptedException {
    final SolrRequestInfo info =
        new SolrRequestInfo(
            new LocalSolrQueryRequest(h.getCore(), params()), new SolrQueryResponse());
    AtomicInteger counter = new AtomicInteger();

    SolrRequestInfo.setRequestInfo(info);
    ExecutorUtil.MDCAwareThreadPoolExecutor pool =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            1, 1, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1));
    AtomicBoolean run = new AtomicBoolean(false);
    pool.execute(
        () -> {
          final SolrRequestInfo poolInfo = SolrRequestInfo.getRequestInfo();
          assertSame(info, poolInfo);
          info.addCloseHook(counter::incrementAndGet);
          run.set(true);
        });
    if (random().nextBoolean()) {
      pool.shutdown();
    } else {
      pool.shutdownNow();
    }
    SolrRequestInfo.clearRequestInfo();
    SolrRequestInfo.reset();

    pool.awaitTermination(1, TimeUnit.MINUTES);
    assertTrue(run.get());
    assertEquals("hook should be closed only once", 1, counter.get());
    assertNull(SolrRequestInfo.getRequestInfo());
  }

  /**
   * This test verifies that if the original request has a timeout pushing another SolrRequestInfo
   * onto the stack will not allow a sub-request that is unlimited (or has a different limit)
   */
  public void testLimitsMaintained() {
    try {
      LocalSolrQueryRequest timeAllowed1000 =
          new LocalSolrQueryRequest(h.getCore(), params("timeAllowed", "1000"));
      LocalSolrQueryRequest timeAllowed20000 =
          new LocalSolrQueryRequest(h.getCore(), params("timeAllowed", "20000"));

      assertNull(timeAllowed1000.getContext().get(LIMITS_KEY));
      assertNull(timeAllowed20000.getContext().get(LIMITS_KEY));

      final SolrRequestInfo info1k = new SolrRequestInfo(timeAllowed1000, new SolrQueryResponse());
      final SolrRequestInfo info20k =
          new SolrRequestInfo(timeAllowed20000, new SolrQueryResponse());

      // request not modified yet
      Object limitFrom1k = timeAllowed1000.getContext().get(LIMITS_KEY);
      assertNull(limitFrom1k);
      Object limitFrom20k = timeAllowed20000.getContext().get(LIMITS_KEY);
      assertNull(limitFrom20k);

      SolrRequestInfo.setRequestInfo(info1k);
      SolrRequestInfo solrRequestInfo = Objects.requireNonNull(SolrRequestInfo.getRequestInfo());
      assertEquals(solrRequestInfo, info1k);

      QueryLimits limits1k = solrRequestInfo.getLimits();
      QueryLimits limitsFromReq = (QueryLimits) timeAllowed1000.getContext().get(LIMITS_KEY);
      assertEquals(limitsFromReq, limits1k);

      SolrRequestInfo.setRequestInfo(info20k); // sub-request

      solrRequestInfo = Objects.requireNonNull(SolrRequestInfo.getRequestInfo());
      assertEquals(solrRequestInfo, info20k); // pushed onto stack successfully

      // Now verify that the sub-request inherited the limit from the parent
      limitsFromReq = (QueryLimits) timeAllowed20000.getContext().get(LIMITS_KEY);
      assertEquals(limitsFromReq, limits1k);
      QueryLimits limitsFromSRI = SolrRequestInfo.getRequestInfo().getLimits();
      assertEquals(limitsFromSRI, limits1k);
    } finally {
      SolrRequestInfo.clearRequestInfo();
      SolrRequestInfo.clearRequestInfo();
      SolrRequestInfo.reset();
    }
  }
}
